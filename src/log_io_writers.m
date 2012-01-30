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
wal_request_write:(struct tbuf *)row_data tag:(u16)tag cookie:(u64)row_cookie
{
	struct tbuf *m;
	struct msg *a;
	struct row_v12 row;

	if (!local_writes) {
		say_warn("local writes disabled");
		return 0;
	}

	m = tbuf_alloc(wal_writer->out->pool);
	memset(&row, 0, sizeof(row));
	row.scn = scn;
	row.tag = tag;
	row.cookie = row_cookie;
	row.len = tbuf_len(row_data);

	/* packet is : <data_len: u32><fid: u32><data: u8[data_len]> */

	u32 len = sizeof(row) + tbuf_len(row_data);
	tbuf_append(m, &len, sizeof(u32));
	tbuf_append(m, &fiber->fid, sizeof(u32));
	tbuf_append(m, &row, sizeof(row));
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

- (i64)
wal_write_row:(struct row_v12 *)row
{
	static XLog *wal_to_close = nil;

	lsn++;
	if (current_wal == nil)
		/* Open WAL with '.inprogress' suffix. */
		current_wal = [wal_dir open_for_write:lsn
					  saved_errno:NULL];
        if (current_wal == nil) {
                say_error("can't open wal");
                goto fail;
        }

        if (current_wal->rows == 1) {
		/* rename wal after first successfull write to name without inprogress suffix*/
		if ([current_wal inprogress_rename] != 0) {
			say_error("can't rename inprogress wal");
			goto fail;
		}
	}

	if (wal_to_close != nil) {
		[wal_to_close close];
		wal_to_close = nil;
	}

	row->lsn = lsn;
	row->tm = ev_now();
	row->data_crc32c = crc32c(0, row->data, row->len);
	row->header_crc32c = crc32c(0, (u8 *)row + field_sizeof(struct row_v12, header_crc32c),
				    sizeof(*row) - field_sizeof(struct row_v12, header_crc32c));

	if (fwrite(&marker, sizeof(marker), 1, current_wal->fd) != 1) {
		say_syserror("can't write marker to wal");
		goto fail;
	}

	if (fwrite(row, sizeof(*row) + row->len, 1, current_wal->fd) != 1) {
		say_syserror("can't write row data to wal");
		goto fail;
	}

	current_wal->rows++;
	if (current_wal->dir->rows_per_file <= current_wal->rows ||
	    (lsn + 1) % current_wal->dir->rows_per_file == 0)
	{
		wal_to_close = current_wal;
		current_wal = nil;
	}

	return lsn;

      fail:
	return 0;
}


int
wal_disk_writer(int fd, void *state)
{
	Recovery *rcvr = state;
	struct tbuf *wbuf, *rbuf;
	int result = EXIT_FAILURE;
	ev_tstamp last_flush = 0;
	i64 lsn;

	rbuf = tbuf_alloca(fiber->pool);
	palloc_register_gc_root(fiber->pool, (void *)rbuf, tbuf_gc);

	if (recv(fd, &lsn, sizeof(lsn), 0) != sizeof(lsn)) {
		say_syserror("recv: failed");
		panic("unable to start WAL writer");
	}

	[rcvr initial_lsn:lsn];
	for (;;) {
	reread:
		tbuf_ensure(rbuf, 16 * 1024);
		ssize_t r = recv(fd, rbuf->data + tbuf_len(rbuf),
				 rbuf->size - tbuf_len(rbuf), 0);
		if (r < 0 && (errno == EINTR))
			goto reread;
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

		while (tbuf_len(rbuf) > sizeof(u32) * 2) {
			/* packet is : <data_len: u32><fid: u32><data: u8[data_len]> */
			size_t packet_size = sizeof(u32) * 2 + *(u32 *)rbuf->data;
			if (tbuf_len(rbuf) < packet_size) {
				/* either rbuf was too small or data wasn't ready
				   since we have nothing to send, procceed to reading packet again */
				if (tbuf_len(wbuf) == 0) {
					tbuf_ensure(rbuf, packet_size);
					goto reread;
				} else {
					break;
				}
			}

			u32 data_len = read_u32(rbuf);
			u32 fid = read_u32(rbuf);
			struct row_v12 *row = read_bytes(rbuf, data_len);

			i64 row_lsn = [rcvr wal_write_row:row];

			u32 len = sizeof(row_lsn);
			tbuf_append(wbuf, &len, sizeof(u32));
			tbuf_append(wbuf, &fid, sizeof(fid));
			tbuf_append(wbuf, &row_lsn, sizeof(row_lsn));
			say_debug("sending lsn:%"PRIi64" to parent", row_lsn);
		}

		if (rcvr->current_wal != nil) {
			/* flush stdio buffer to keep feeder in sync */
			if (fflush(rcvr->current_wal->fd) < 0)
				say_syserror("can't flush wal");

			float fsync_delay = rcvr->current_wal->dir->fsync_delay;
			if (fsync_delay == 0 || ev_now() - last_flush >= fsync_delay)
			{
				if ([rcvr->current_wal flush] < 0)
					say_syserror("can't flush wal");
				else
					last_flush = ev_now();
			}
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

static void
write_row(XLog *l, i64 scn, i64 lsn, u16 tag, u64 cookie, struct tbuf *data)
{
	struct row_v12 row;
	row.scn = scn;
	row.lsn = lsn;
	row.tm = ev_now();
	row.tag = tag;
	row.cookie = cookie;
	row.len = tbuf_len(data);
	row.data_crc32c = crc32c(0, data->data, tbuf_len(data));
	row.header_crc32c = crc32c(0, (unsigned char *)&row + sizeof(row.header_crc32c),
				   sizeof(row) - sizeof(row.header_crc32c));

	if (fwrite(&marker, sizeof(marker), 1, l->fd) != 1)
		panic("fwrite");
	if (fwrite(&row, sizeof(row), 1, l->fd) != 1)
		panic("fwrite");
	if (fwrite(data->data, tbuf_len(data), 1, l->fd) != 1)
		panic("fwrite");
}

void
snapshot_write_row(XLog *l, u16 tag, struct tbuf *data)
{
	static int rows;
	static int bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;
	const int io_rate_limit = l->dir->recovery_state->snap_io_rate_limit;

	write_row(l, 0, 0, tag, default_cookie, data);

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
	int saved_errno;

	if (!local_writes) {
		say_warn("local writes disabled");
		return;
	}

        snap = [snap_dir open_for_write:lsn saved_errno:&saved_errno];
	if (snap == nil)
		panic_status(saved_errno, "can't open snap for writing");

	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */

	final_filename = [snap final_filename];

	say_info("saving snapshot `%s'", final_filename);

	struct tbuf *init = tbuf_alloc(fiber->pool);
	tbuf_printf(init, "%s", "make world");
	write_row(snap, scn, lsn, snap_initial_tag, default_cookie, init);

	callback(snap);

	if (fsync(fileno(snap->fd)) < 0)
		panic("fsync");

	if (link(snap->filename, final_filename) == -1)
		panic_status(errno, "can't create hard link to snapshot");

	if (unlink(snap->filename) == -1)
		say_syserror("can't unlink 'inprogress' snapshot");

	[snap close];
	snap = nil;
	say_info("done");
}

@end
