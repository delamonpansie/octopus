/*
 * Copyright (C) 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2012, 2013, 2014 Yuriy Vostrikov
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
#import <objc.h>
#import <log_io.h>
#import <net_io.h>
#import <palloc.h>
#import <pickle.h>
#import <tbuf.h>
#import <say.h>
#import <spawn_child.h>

#include <third_party/crc32.h>

#include <stdio.h>
#include <sysexits.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

struct wal_disk_writer_conf {
	i64 lsn, scn;
	u32 run_crc;
};

struct wal_reply {
	u32 packet_len;
	u32 row_count;
	struct fiber *sender;
	u32 fid;

	struct row_commit_info row_info[];
} __attribute__((packed));


@interface WALDiskWriter: Object {
@public
	i64 lsn;
	XLog *current_wal;	/* the WAL we'r currently reading/writing from/to */
	XLog *wal_to_close;
}
- (id) init_conf:(const struct wal_disk_writer_conf *)conf_;
@end


@implementation WALDiskWriter

- (id)
init_conf:(const struct wal_disk_writer_conf *)conf
{
	[self init];
	lsn = conf->lsn;
	return self;
}

- (const struct row_v12 *)
append_row:(struct row_v12 *)row data:(const void *)data
{
	row->tm = ev_now();
	return [current_wal append_row:row data:data];
}

- (int)
prepare_write:(i64)scn_
{
	if (current_wal == nil)
		current_wal = [wal_dir open_for_write:lsn + 1 scn:scn_];

        if (current_wal == nil) {
                say_syserror("can't open wal");
                return -1;
        }

	return 0;
}

- (i64)
confirm_write
{
	static ev_tstamp last_flush;

	if (current_wal != nil) {
		i64 confirmed_lsn = [current_wal confirm_write];

		if (current_wal->inprogress && [current_wal rows] > 0) {
			/* invariant: .xlog must have at least one valid row
			   rename .xlog.inprogress to .xlog only after [confirm_write]
			   successfully writes some rows.
			   it's ok to discard rows on rename failure: they are not confirmed yet */

			if ([current_wal inprogress_rename] != 0) {
				unlink(current_wal->filename);
				[current_wal free];
				current_wal = nil;
				return lsn;
			}

			say_info("created `%s'", current_wal->filename);
			[wal_to_close close];
			wal_to_close = nil;
		}

		lsn = confirmed_lsn;

		if (cfg.wal_fsync_delay >= 0 && ev_now() - last_flush >= cfg.wal_fsync_delay) {
			/* note: [flush] silently drops unwritten rows.
			   it's ok here because of previous call to [confirm_write] */
			if ([current_wal flush] < 0) {
				say_syserror("can't flush wal");
			} else {
				ev_now_update();
				last_flush = ev_now();
			}
		}

		if (cfg.rows_per_wal <= [current_wal rows] ||
		    (lsn + 1) % cfg.rows_per_wal == 0)
		{
			wal_to_close = current_wal;
			current_wal = nil;
		}
	}

	return lsn;
}

@end

int
wal_disk_writer(int fd, void *state)
{

	const struct wal_disk_writer_conf *conf = state;

	WALDiskWriter *writer = [[WALDiskWriter alloc] init_conf:conf];

	struct tbuf rbuf = TBUF(NULL, 0, fiber->pool);
	int result = EXIT_FAILURE;
	i64 start_lsn, next_scn = conf->scn;
	u32 run_crc = conf->run_crc;
	bool io_failure = false, delay_read = false;
	ssize_t r;
	int reply_count = 0;

	struct wal_reply *reply[1024];
	struct iovec reply_iov[nelem(reply)];

	ev_tstamp start_time = ev_now();
	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);

	say_debug("%s: configured LSN:%"PRIi64 " SCN:%"PRIi64" run_crc_log:0x%x",
		  __func__, conf->lsn, conf->scn, conf->run_crc);

	/* since wal_writer have bidirectional communiction to master
	   and checks for errors on send/recv,
	   there is no need in util.m:keepalive() */
	signal(SIGPIPE, SIG_IGN);

	/* ignore SIGUSR1, so accidental miss in 'kill -USR1' won't cause crash */
	signal(SIGUSR1, SIG_IGN);

	for (;;) {
		if (!delay_read) {
			tbuf_ensure(&rbuf, 16 * 1024);
			r = tbuf_recv(&rbuf, fd);
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
		}


		if (cfg.coredump > 0 && ev_now() - start_time > cfg.coredump * 60) {
			maximize_core_rlimit();
			cfg.coredump = 0;
		}

		start_lsn = writer->lsn;
		delay_read = false;
		io_failure = [writer prepare_write:next_scn + 1] == -1;

		/* we're not running inside ev_loop, so update ev_now manually just before write */
		ev_now_update();

		for (int i = 0; i < nelem(reply); i++) {
			if (tbuf_len(&rbuf) < sizeof(u32) ||
			    tbuf_len(&rbuf) < *(u32 *)rbuf.ptr)
				break;

			u32 row_count = ((u32 *)rbuf.ptr)[1];
			if (!io_failure && row_count > [writer->current_wal wet_rows_offset_available]) {
				assert(reply_count != 0);
				delay_read = true;
				break;
			}
			say_debug("request[%i] rows:%i", i, row_count);

			tbuf_ltrim(&rbuf, sizeof(u32) * 2); /* drop packet_len & row_count */
			reply[i] = p0alloc(fiber->pool,
					   sizeof(struct wal_reply) +
					   row_count * sizeof(struct row_commit_info));
			reply[i]->row_count = row_count;
			reply[i]->sender = read_ptr(&rbuf);
			reply[i]->fid = read_u32(&rbuf);


			struct row_commit_info *row_info = reply[i]->row_info;
			for (int j = 0; j < row_count; j++) {
				struct row_v12 *h = read_bytes(&rbuf, sizeof(*h));
				void *data = read_bytes(&rbuf, h->len);

				if (io_failure)
					continue;

				say_debug("|	SCN:%"PRIi64" tag:%s data_len:%u",
					  h->scn, xlog_tag_to_a(h->tag), h->len);

				const struct row_v12 *ret = [writer append_row:h data:data];
				if (ret == NULL) {
					say_error("append_row failed");
					io_failure = true;
					continue;
				}

				run_crc_calc(&run_crc, ret->tag, data, ret->len);

				*row_info++ = (struct row_commit_info){ .lsn = ret->lsn,
									.scn = ret->scn,
									.tag = ret->tag,
									.run_crc = run_crc };
			}
			reply_count++;
		}
		if (reply_count == 0)
			continue;

		assert(start_lsn > 0);
		u32 rows_confirmed = [writer confirm_write] - start_lsn;

		for (int i = 0; i < reply_count; i++) {
			int reply_row_count = MIN(rows_confirmed, reply[i]->row_count);
			int reply_len = sizeof(struct wal_reply) +
					reply_row_count * sizeof(struct row_commit_info);

			reply[i]->packet_len = reply_len;
			reply[i]->row_count = reply_row_count; /* real number rows written to disk */

			reply_iov[i].iov_base = reply[i];
			reply_iov[i].iov_len = reply_len;

			for (int k = 0; k < reply[i]->row_count; k++) {
				struct row_commit_info *rci = &reply[i]->row_info[k];
				/* next_scn is used for writing XLog header, which is turn used to
				   find correct file for replication replay.
				   so, next_scn should be updated only when data modification occurs */
				if (scn_changer(rci->tag)) {
					if (cfg.panic_on_scn_gap && rci->scn - next_scn != 1)
						panic("GAP %"PRIi64" rows:%i", rci->scn - next_scn,
						      reply[i]->row_count);
					next_scn = rci->scn;
				}
			}

			rows_confirmed -= reply_row_count;

			say_debug("reply[%i] rows:%i LSN:%"PRIi64" SCN:%"PRIi64,
				  i, reply[i]->row_count,
				  reply[i]->row_count ? reply[i]->row_info[reply[i]->row_count - 1].lsn : -1,
				  reply[i]->row_count ? reply[i]->row_info[reply[i]->row_count - 1].scn : -1);
		}

		struct iovec *iov = reply_iov;
		do {
			r = writev(fd, iov, reply_count);
			if (r < 0) {
				if (errno == EINTR)
					continue;
				/* parent is dead, exit quetly */
				result = EX_OK;
				goto exit;
			}
			do {
				if (iov->iov_len <= r) {
					r -= iov->iov_len;
					reply_count--;
					iov++;
				} else {
					iov->iov_base += r;
					iov->iov_len -= r;
					break;
				}
			} while (r);
		} while (reply_count > 0);

		fiber_gc();
	}
exit:
	[writer->current_wal close];
	writer->current_wal = nil;
	return result;
}

static void
wal_disk_writer_input_dispatch(ev_io *ev, int __attribute__((unused)) events)
{
	struct netmsg_io *io = container_of(ev, struct netmsg_io, in);
	struct tbuf *rbuf = &io->rbuf;
	tbuf_ensure(rbuf, 128 * 1024);

	ssize_t r = tbuf_recv(rbuf, io->in.fd);
	if (unlikely(r <= 0)) {
		if (r < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				return;
			say_syserror("%s: recv", __func__);
			panic("WAL writer connection read error");
		} else
			panic("WAL writer connection EOF");
	}

	while (tbuf_len(rbuf) > sizeof(u32) &&
	       tbuf_len(rbuf) >= *(u32 *)rbuf->ptr)
	{
		struct wal_reply *r = read_bytes(rbuf, *(u32 *)rbuf->ptr);
		if (unlikely(r->sender->fid != r->fid)) {
			say_warn("orphan WAL reply");
			continue;
		}
		resume(r->sender, r);
	}

	if (palloc_allocated(rbuf->pool) > 4 * 1024 * 1024)
		palloc_gc(io->pool);
}


@implementation XLogWriter

- (i64) lsn { return lsn; }
- (const struct child *) wal_writer { return &wal_writer; };

- (id)
init_lsn:(i64)init_lsn
   state:(id<RecoveryState>)state_
{
	assert(init_lsn > 0);
	assert([state_ scn] > 0);

	lsn = init_lsn;
	state = state_;

	if (cfg.rows_per_wal <= 4)
		panic("inacceptable value of 'rows_per_wal'");

	say_info("Configuring WAL writer LSN:%"PRIi64" SCN:%"PRIi64, lsn, [state scn]);

	struct wal_disk_writer_conf conf = { .lsn = lsn,
					     .scn = [state scn],
					     .run_crc = [state run_crc_log]};
	wal_writer = spawn_child("wal_writer", wal_disk_writer, &conf, sizeof(conf));
	netmsg_io_init(&io, palloc_create_pool("wal_writer"), NULL, wal_writer.fd);
	ev_init(&io.in, wal_disk_writer_input_dispatch);
	ev_set_priority(&io.in, 1);
	ev_set_priority(&io.out, 1);
	ev_io_start(&io.in);

	return self;
}


- (int)
submit:(const void *)data len:(u32)data_len tag:(u16)tag
{
	struct row_v12 row = { .scn = 0,
			       .tag = tag };

	struct wal_pack pack;
	if (!wal_pack_prepare(self, &pack))
		return 0;

	wal_pack_append_row(&pack, &row);

	/* this conversion is for box only. no other module
	   should ever use cfg.io_compat */
	if (cfg.io_compat && (tag & ~TAG_MASK) == TAG_WAL) {
		u16 op = (tag & TAG_MASK) >> 5;
		row.tag = wal_data|TAG_WAL;
		wal_pack_append_data(&pack, &row, &op, sizeof(op));
	}
	wal_pack_append_data(&pack, &row, data, data_len);
	return [self wal_pack_submit];
}

int
wal_pack_prepare(XLogWriter *w, struct wal_pack *pack)
{
	if (![w->state local_writes]) {
		say_warn("local writes disabled");
		return 0;
	}

	pack->netmsg = &w->io.wbuf;
	pack->packet_len = sizeof(*pack) - offsetof(struct wal_pack, packet_len);
	pack->fid = fiber->fid;
	pack->sender = fiber;
	pack->row_count = 0;
	net_add_iov(pack->netmsg, &pack->packet_len, pack->packet_len);
	return 1;
}

u32
wal_pack_append_row(struct wal_pack *pack, struct row_v12 *row)
{
	assert(pack->row_count <= WAL_PACK_MAX);
	assert(row->tag & ~TAG_MASK);

	pack->packet_len += sizeof(*row) + row->len;
	pack->row_count++;
	net_add_iov(pack->netmsg, row, sizeof(*row));
	if (row->len > 0)
		net_add_iov(pack->netmsg, row->data, row->len);
	return WAL_PACK_MAX - pack->row_count;
}

void
wal_pack_append_data(struct wal_pack *pack, struct row_v12 *row,
		     const void *data, size_t len)
{
	pack->packet_len += len;
	row->len += len;
	net_add_iov(pack->netmsg, data, len);
}

- (int)
wal_pack_submit
{
	ev_io_start(&io.out);
	struct wal_reply *reply = yield();
	if (reply->row_count == 0)
		say_warn("wal writer returned error status");
	else
		lsn = reply->row_info[reply->row_count - 1].lsn;

	[state update_state_rci:reply->row_info count:reply->row_count];
	say_debug("%s: => rows:%i LSN:%"PRIi64, __func__, reply->row_count, lsn);
	return reply->row_count;
}
@end

@implementation SnapWriter

- (id)
init_state:(id<RecoveryState>)state_
{
	[self init];
	state = state_;
	return self;
}

int
snapshot_write_row(XLog *l, u16 tag, struct tbuf *row)
{
	static int bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;
	const int io_rate_limit = cfg.snap_io_rate_limit * 1024 * 1024;

	if ([l append_row:row->ptr len:tbuf_len(row) scn:0 tag:tag|TAG_SNAP] == NULL) {
		say_syserror("unable write row");
		return -1;
	}

	if (io_rate_limit > 0) {
		if (last == 0) {
			ev_now_update();
			last = ev_now();
		}

		bytes += tbuf_len(row) + sizeof(struct row_v12);

		while (bytes >= io_rate_limit) {
			if ([l flush] < 0) {
				say_syserror("unable to flush");
				return -1;
			}

			ev_now_update();
			elapsed = ev_now() - last;
			if (elapsed < 1)
				usleep(((1 - elapsed) * 1000000));

			ev_now_update();
			last = ev_now();
			bytes -= io_rate_limit;
		}
	}
	return 0;
}


- (int)
snapshot_write_header_rows:(XLog *)snap
{
	(void)snap;
	return 0;
}

- (int)
snapshot_write
{
        XLog *snap;

	say_debug("%s: lsn:%"PRIi64" scn:%"PRIi64, __func__, [state lsn], [state scn]);
	snap = [snap_dir open_for_write:[state lsn] scn:[state scn]];
	if (snap == nil) {
		say_syserror("can't open snap for writing");
		return -1;
	}
	snap->no_wet = true; /* We don't handle write errors here because
				snapshot can't be partially saved.
				so, disable wet row tracking */;

	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */

	char *filename = strdup(snap->filename);
	char *suffix = strrchr(filename, '.');
	*suffix = 0;
	say_info("saving snapshot `%s'", filename);

	struct tbuf *snap_ini = tbuf_alloc(fiber->pool);
	u32 rows = [[state client] snapshot_estimate];
	tbuf_append(snap_ini, &rows, sizeof(rows));
	u32 run_crc_log = [state run_crc_log];
	tbuf_append(snap_ini, &run_crc_log, sizeof(run_crc_log));
	u32 run_crc_mod = 0;
	tbuf_append(snap_ini, &run_crc_mod, sizeof(run_crc_mod));

	if ([snap append_row:snap_ini->ptr len:tbuf_len(snap_ini) scn:[state scn] tag:snap_initial|TAG_SYS] == NULL) {
		say_error("unable write initial row");
		return -1;
	}

	if ([self snapshot_write_header_rows:snap] < 0) /* FIXME: this is ugly */
		return -1;

	if ([[state client] snapshot_write_rows:snap] < 0)
		return -1;

	const char end[] = "END";
	if ([snap append_row:end len:strlen(end) scn:[state scn] tag:snap_final|TAG_SYS] == NULL) {
		say_error("unable write final row");
		return -1;
	}
	if ([snap rows] == 0) /* initial snapshot in compat mode has no rows */
		[snap append_successful:1]; /* -[XLog close] won't rename empty .inprogress, trick it */

	if ([snap flush] == -1) {
		say_syserror("snap flush failed");
		return -1;
	}

	if ([snap inprogress_rename] == -1) {
		say_syserror("snap inprogress rename failed");
		return -1;
	}

	if ([snap close] == -1) {
		say_syserror("snap close failed");
		return -1;
	}
	snap = nil;

	say_info("done");
	return 0;
}


@end

register_source();
