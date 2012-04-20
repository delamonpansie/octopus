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
#import <net_io.h>
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
	say_info("Configuring WAL writer lsn:%"PRIi64, lsn);

	struct netmsg *n = netmsg_tail(&wal_writer->c->out_messages);
	net_add_iov(&n, &lsn, sizeof(lsn));
	ev_io_start(&wal_writer->c->out);
}

/* packet is : packet_len: u32
	       fid: u32
	       repeat_count: u32
	       tag: u16
	       cookie: u64
	       data_len: u32
	       data: u8[data_len]
*/

struct wal_row_header {
	u16 tag;
	u64 cookie;
	u32 data_len;
} __attribute__((packed));

- (void)
submit_change:(struct tbuf *)change
{
	if (feeder_addr != NULL)
		raise("replica is readonly");

	int len = sizeof(struct wal_pack) + sizeof(struct wal_row_header);
	void *msg = palloc(fiber->pool, len);

	struct wal_pack *pack = msg;
	struct wal_row_header *h = msg + sizeof(*pack);

	pack->netmsg = netmsg_tail(&wal_writer->c->out_messages);
	pack->packet_len = sizeof(*pack) - sizeof(pack->netmsg) + sizeof(*h) + tbuf_len(change);
	pack->fid = fiber->fid;
	pack->repeat_count = 1;

	h->tag = wal_tag;
	h->cookie = 0;
	h->data_len = tbuf_len(change);

	net_add_iov(&pack->netmsg, msg + sizeof(pack->netmsg), len - sizeof(pack->netmsg));
	net_add_iov(&pack->netmsg, change->data, tbuf_len(change));

	if ([self wal_pack_submit] <= 0)
		raise("unable write wal row");
}

- (struct wal_pack *)
wal_pack_prepare
{
	struct wal_pack *pack = palloc(fiber->pool, sizeof(*pack));

	pack->netmsg = netmsg_tail(&wal_writer->c->out_messages);
	pack->packet_len = sizeof(*pack) - sizeof(pack->netmsg);
	pack->fid = fiber->fid;
	pack->repeat_count = 0;

	net_add_iov(&pack->netmsg, &pack->packet_len, pack->packet_len);

	return pack;
}


- (u32)
wal_pack_append:(struct wal_pack *)pack data:(void *)data len:(u32)data_len tag:(u16)tag cookie:(u64)cookie
{
	struct wal_row_header *h = palloc(fiber->pool, sizeof(*h));

	pack->packet_len += sizeof(*h) + data_len;
	pack->repeat_count++;
	assert(pack->repeat_count <= WAL_PACK_MAX);


	h->tag = tag;
	h->cookie = cookie;
	h->data_len = data_len;

	net_add_iov(&pack->netmsg, h, sizeof(*h));
	net_add_iov(&pack->netmsg, data, data_len);

	return WAL_PACK_MAX - pack->repeat_count;
}

- (int)
wal_pack_submit
{
	if (!local_writes) {
		say_warn("local writes disabled");
		return 0;
	}

	ev_io_start(&wal_writer->c->out);
	struct wal_reply *r = yield();
	say_debug("wal_write read inbox lsn=%"PRIi64" rows:%i", r->lsn, r->repeat_count);
	if (r->lsn == 0)
		say_warn("wal writer returned error status");
	else
		lsn = r->lsn; /* update local lsn */
	return r->repeat_count;
}


- (int)
append_row:(const void *)data len:(u32)len tag:(u16)tag cookie:(u64)cookie
{
        if ([current_wal rows] == 1) {
		/* rename wal after first successfull write to name without inprogress suffix*/
		if ([current_wal inprogress_rename] != 0) {
			say_error("can't rename inprogress wal");
			return -1;
		}
	}

	return [current_wal append_row:data len:len tag:tag cookie:cookie];
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

	if (wal_to_close != nil) {
		[wal_to_close close];
		wal_to_close = nil;
	}
	return 0;
}

- (i64)
confirm_write
{
	static ev_tstamp last_flush;

	if (current_wal != nil) {
		lsn = [current_wal confirm_write];

		ev_tstamp fsync_delay = current_wal->dir->fsync_delay;
		if (fsync_delay == 0 || ev_now() - last_flush >= fsync_delay) {
			if ([current_wal flush] < 0)
				say_syserror("can't flush wal");
			else
				last_flush = ev_now();
		}
	}

	if (current_wal->dir->rows_per_file <= [current_wal rows] ||
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
	bool io_failure = false, reparse = false;
	ssize_t r;
	struct {
		u32 fid;
		u32 repeat_count;
	} *reply = malloc(sizeof(*reply) * 1024);
	ev_tstamp start_time = ev_now();
	rbuf = tbuf_alloca(fiber->pool);
	palloc_register_gc_root(fiber->pool, (void *)rbuf, tbuf_gc);

	if ((r = recv(fd, &lsn, sizeof(lsn), 0)) != sizeof(lsn)) {
		if (r == 0) {
			result = EX_OK;
			goto exit;
		}
		say_syserror("recv: failed");
		panic("unable to start WAL writer");
	}

	[rcvr initial_lsn:lsn];
	for (;;) {
		if (!reparse) {
			tbuf_ensure(rbuf, 16 * 1024);
			r = recv(fd, rbuf->data + tbuf_len(rbuf),
				 rbuf->size - tbuf_len(rbuf), 0);
			if (r < 0 && (errno == EINTR))
				continue;
			else if (r < 0) {
				say_syserror("recv");
				result = EX_OSERR;
				goto exit;
			} else if (r == 0) {
				result = EX_OK;
				goto exit;
			}
			rbuf->len += r;
		}

		/* we're not running inside ev_loop, so update ev_now manually */
		ev_now_update();

		if (cfg.coredump > 0 && ev_now() - start_time > cfg.coredump * 60) {
			maximize_core_rlimit();
			cfg.coredump = 0;
		}

		int p = 0;
		reparse = false;
		while (tbuf_len(rbuf) > sizeof(u32) && tbuf_len(rbuf) >= *(u32 *)rbuf->data) {
			if (p == 0) {
				if ([rcvr prepare_write] != -1) {
					io_failure = false;
					lsn = [rcvr lsn];
				} else
					io_failure = true;
			}

			u32 repeat_count = ((u32 *)rbuf->data)[2];
			if (repeat_count > [rcvr->current_wal wet_rows_offset_available]) {
				assert(p != 0);
				reparse = true;
				break;
			}

			tbuf_ltrim(rbuf, sizeof(u32)); /* drop packet_len */
			reply[p].fid = read_u32(rbuf);
			reply[p].repeat_count = read_u32(rbuf);

			for (int i = 0; i < reply[p].repeat_count; i++) {
				u16 tag = read_u16(rbuf);
				u64 cookie = read_u64(rbuf);
				u32 data_len = read_u32(rbuf);
				void *data = read_bytes(rbuf, data_len);

				if (io_failure)
					continue;

				if ([rcvr append_row:data len:data_len tag:tag cookie:cookie] <= 0) {
					say_error("append_row failed");
					io_failure = true;
				}
			}
			p++;
		}
		if (p == 0)
			continue;

		u32 rows = [rcvr confirm_write] - lsn + 1;

		wbuf = tbuf_alloc(fiber->pool);
		for (int i = 0; i < p; i++) {
			if (rows > 0) {
				if (rows < reply[i].repeat_count)
					reply[i].repeat_count = rows;

				rows -= reply[i].repeat_count;
				lsn += reply[i].repeat_count;
			} else {
				lsn = 0;
				reply[i].repeat_count = 0;
			}

			/* struct wal_reply */
			u32 data_len = sizeof(struct wal_reply);
			tbuf_append(wbuf, &data_len, sizeof(data_len));
			tbuf_append(wbuf, &lsn, sizeof(lsn));
			tbuf_append(wbuf, &reply[i].fid, sizeof(reply[i].fid));
			tbuf_append(wbuf, &reply[i].repeat_count, sizeof(reply[i].repeat_count));

			say_debug("sending lsn:%"PRIi64" rows:%i to parent", lsn, rows);
		}


		while (tbuf_len(wbuf) > 0) {
			ssize_t r = write(fd, wbuf->data, tbuf_len(wbuf));
			if (r < 0) {
				if (errno == EINTR)
					continue;
				/* parent is dead, exit quetly */
				result = EX_OK;
				goto exit;
			}
			tbuf_ltrim(wbuf, r);
		}

		fiber_gc();
	}
exit:
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

	if ([l append_row:data->data len:tbuf_len(data) tag:tag cookie:default_cookie] < 0)
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
	const char *final_filename, *filename;

	snap = [snap_dir open_for_write:lsn];
	if (snap == nil) {
		say_error("can't open snap for writing");
		return;
	}
	snap->no_wet = true; /* disable wet row tracking */;

	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */

	final_filename = strdup([snap final_filename]);
	filename = strdup(snap->filename);

	say_info("saving snapshot `%s'", final_filename);

	const char init[] = "make world";
	if ([snap append_row:init len:strlen(init) tag:snap_initial_tag cookie:default_cookie] < 0) {
		say_error("unable write initial row");
		return;
	}
	callback(snap);

	if ([snap flush] == -1)
		return;
	if ([snap close] == -1)
		return;
	snap = nil;

	if (link(filename, final_filename) == -1) {
		say_syserror("can't create hard link to snapshot");
		return;
	}

	if (unlink(filename) == -1) {
		say_syserror("can't unlink 'inprogress' snapshot");
		return;
	}

	say_info("done");
}

@end
