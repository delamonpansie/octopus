/*
 * Copyright (C) 2012 Mail.RU
 * Copyright (C) 2012 Yuriy Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#import <util.h>
#import <fiber.h>
#import <object.h>
#import <log_io.h>
#import <palloc.h>
#import <pickle.h>
#import <tbuf.h>

#include <third_party/crc32.h>

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sysexits.h>

@implementation Recovery (writers)

- (void)
configure_wal_writer
{
	if (wal_writer != NULL) {
		struct tbuf *m = tbuf_alloc(wal_writer->out->pool);
		tbuf_append(m, &lsn, sizeof(lsn));
		write_inbox(wal_writer->out, m);
	}
}

- (void)
submit_change:(struct tbuf *)change
{
	if (feeder_addr != NULL)
		raise("replica is readonly");

	if ([self wal_request_write:change
				tag:wal_tag
			     cookie:0] == 0)
		raise("wal write error");
}

- (i64)
wal_request_write:(struct tbuf *)row_data tag:(u16)tag cookie:(u64)cookie
{
	struct tbuf *m;
	struct msg *a;

	if (!local_writes) {
		say_warn("local writes disabled");
		return 0;
	}


	/* packet is : packet_len: u32
	   	       fid: u32
		       repeat_count: u32
		       tag: u16
		       cookie: u64
		       data_len: u32
	               data: u8[data_len]
	*/

	m = tbuf_alloc(wal_writer->out->pool);

	u32 repeat_count = 1;
	u32 data_len = tbuf_len(row_data);
	u32 packet_len = sizeof(fiber->fid) + sizeof(repeat_count) +
			 sizeof(tag) + sizeof(cookie) +
			 sizeof(data_len) + data_len;

	tbuf_append(m, &packet_len, sizeof(packet_len));
	tbuf_append(m, &fiber->fid, sizeof(fiber->fid));
	tbuf_append(m, &repeat_count, sizeof(repeat_count));
	tbuf_append(m, &tag, sizeof(tag));
	tbuf_append(m, &cookie, sizeof(cookie));
	tbuf_append(m, &data_len, sizeof(data_len));
	tbuf_append(m, row_data->data, tbuf_len(row_data));

	if (write_inbox(wal_writer->out, m) == false) {
		say_warn("wal writer inbox is full");
		return false;
	}
	a = read_inbox();

	i64 row_lsn = read_u64(a->msg);
	say_debug("wal_write read inbox lsn=%" PRIi64, row_lsn);
	if (row_lsn == 0)
		say_warn("wal writer returned error status");
	else
		lsn = row_lsn; /* update local lsn */
	return row_lsn;
}

- (int)
prepare_write
{
	if (current_wal == nil)
		/* Open WAL with '.inprogress' suffix. */
		current_wal = [wal_dir open_for_write:lsn + 1];
        if (current_wal == nil) {
                say_error("can't open wal");
                return -1;
        }

        if (current_wal->rows == 1) {
		/* rename wal after first successfull write to name without inprogress suffix*/
		if ([current_wal inprogress_rename] != 0) {
			say_error("can't rename inprogress wal");
			return -1;
		}
	}

	if (wal_to_close != nil) {
		[wal_to_close close];
		wal_to_close = nil;
	}
	return 0;
}

- (i64)
confirm_write:(int)rows
{
	static ev_tstamp last_flush;

	if (current_wal != nil) {
		say_debug("confirm_write: %i rows confirmed", rows);
		lsn += rows;
		assert(lsn == current_wal->next_lsn - 1);

		/* flush stdio buffer to keep feeder in sync */
		if (fflush(current_wal->fd) < 0)
			say_syserror("can't flush wal");

		ev_tstamp fsync_delay = current_wal->dir->fsync_delay;
		if (fsync_delay == 0 || ev_now() - last_flush >= fsync_delay) {
			if ([current_wal flush] < 0)
				say_syserror("can't flush wal");
			else
				last_flush = ev_now();
		}
	}

	if (current_wal->dir->rows_per_file <= current_wal->rows ||
	    (lsn + 1) % current_wal->dir->rows_per_file == 0)
	{
		wal_to_close = current_wal;
		current_wal = nil;
	}

	return lsn;
}


int
wal_disk_writer(int fd, void *state)
{
	Recovery *rcvr = state;
	struct tbuf *wbuf, *rbuf;
	int result = EXIT_FAILURE;
	i64 lsn;

	rbuf = tbuf_alloca(fiber->pool);
	palloc_register_gc_root(fiber->pool, (void *)rbuf, tbuf_gc);

	if (recv(fd, &lsn, sizeof(lsn), 0) != sizeof(lsn)) {
		say_syserror("recv: failed");
		panic("unable to start WAL writer");
	}

	[rcvr initial_lsn:lsn];
	for (;;) {
		tbuf_ensure(rbuf, 16 * 1024);
		ssize_t r = recv(fd, rbuf->data + tbuf_len(rbuf),
				 rbuf->size - tbuf_len(rbuf), 0);
		if (r < 0 && (errno == EINTR))
			continue;
		else if (r < 0) {
			say_syserror("recv");
			result = EX_OSERR;
			break;
		} else if (r == 0) {
			result = EX_OK;
			break;
		}

		rbuf->len += r;

		wbuf = tbuf_alloc(fiber->pool);

		/* we're not running inside ev_loop, so update ev_now manually */
		ev_now_update();

		while (tbuf_len(rbuf) > sizeof(u32) && tbuf_len(rbuf) > *(u32 *)rbuf->data) {
			u32 packet_len = read_u32(rbuf);
			u32 fid = read_u32(rbuf);
			u32 row_count = 0, repeat_count = read_u32(rbuf);
			i64 row_lsn = 0;
			packet_len -= sizeof(fid) + sizeof(repeat_count);

			if ([rcvr prepare_write] == -1)
				goto reply;

			assert(repeat_count == 1);
			while (repeat_count-- > 0) {
				u16 tag = read_u16(rbuf);
				u64 cookie = read_u64(rbuf);
				u32 data_len = read_u32(rbuf);
				void *data = read_bytes(rbuf, data_len);
				packet_len -= sizeof(tag) + sizeof(cookie) +
					      sizeof(data_len) + data_len;

				struct tbuf row_data = { .data = data,
							 .len = data_len,
							 .size = data_len,
							 .pool = NULL };

				if ([rcvr->current_wal append_row:&row_data tag:tag cookie:cookie] < 0) {
					say_error("append_row failed");
					break;
				}
				row_count++;
			}
			row_lsn = [rcvr confirm_write:row_count];
		reply:
			tbuf_ltrim(rbuf, packet_len);

			u32 len = sizeof(row_lsn);
			tbuf_append(wbuf, &len, sizeof(u32));
			tbuf_append(wbuf, &fid, sizeof(fid));
			tbuf_append(wbuf, &row_lsn, sizeof(row_lsn));
			say_debug("sending lsn:%"PRIi64" to parent", row_lsn);
		}

		while (tbuf_len(wbuf) > 0) {
			ssize_t r = write(fd, wbuf->data, tbuf_len(wbuf));
			if (r < 0) {
				if (errno == EINTR)
					continue;
				/* parent is dead, exit quetly */
				result = EX_OK;
				break;
			}
			tbuf_ltrim(wbuf, r);
		}

		fiber_gc();
	}

	[rcvr->current_wal close];
	rcvr->current_wal = nil;
	return result;
}

void
snapshot_write_row(XLog *l, u16 tag, struct tbuf *data)
{
	static int rows;
	static int bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;
	const int io_rate_limit = l->dir->recovery_state->snap_io_rate_limit;

	if ([l append_row:data tag:tag cookie:default_cookie] < 0)
		panic("unable write row");

	if (++rows % 100000 == 0)
		say_crit("%.1fM rows written", rows / 1000000.);

	prelease_after(fiber->pool, 128 * 1024);

	if (io_rate_limit > 0) {
		if (last == 0) {
			ev_now_update();
			last = ev_now();
		}

		bytes += tbuf_len(data) + sizeof(struct row_v12);

		while (bytes >= io_rate_limit) {
			[l flush];

			ev_now_update();
			elapsed = ev_now() - last;
			if (elapsed < 1)
				usleep(((1 - elapsed) * 1000000));

			ev_now_update();
			last = ev_now();
			bytes -= io_rate_limit;
		}
	}
}

- (void)
snapshot_save:(void (*)(XLog *))callback
{
        XLog *snap;
	const char *final_filename;

	if (!local_writes) {
		say_warn("local writes disabled");
		return;
	}

	snap = [snap_dir open_for_write:lsn];
	if (snap == nil) {
		say_error("can't open snap for writing");
		return;
	}

	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */

	final_filename = [snap final_filename];

	say_info("saving snapshot `%s'", final_filename);

	struct tbuf *init = tbuf_alloc(fiber->pool);
	tbuf_printf(init, "%s", "make world");

	if ([snap append_row:init tag:snap_initial_tag cookie:default_cookie] < 0) {
		say_error("unable write initial row");
		return;
	}
	callback(snap);

	if (fsync(fileno(snap->fd)) < 0) {
		say_syserror("fsync");
		return;
	}

	if (link(snap->filename, final_filename) == -1) {
		say_syserror("can't create hard link to snapshot");
		return;
	}

	if (unlink(snap->filename) == -1) {
		say_syserror("can't unlink 'inprogress' snapshot");
		return;
	}

	[snap close];
	snap = nil;
	say_info("done");
}

@end
