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
#import <shard.h>

#include <third_party/crc32.h>

#include <stdio.h>
#include <sysexits.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#if HAVE_LINUX_FALLOC_H
#include <linux/falloc.h>
#endif


enum scn_mode { SCN_PASS, SCN_ASSIGN };

struct shard_state {
	i64 scn;
	u32 run_crc;
	u32 wet_rows;
	enum scn_mode scn_mode;
};

struct wal_disk_writer_conf {
	i64 lsn;
	struct shard_state st[MAX_SHARD];
};


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
prepare_write:(const struct shard_state *)st
{
	static i64 *tmp;
	if (tmp == NULL)
		tmp = xcalloc(MAX_SHARD, sizeof(*tmp));

	if (current_wal == nil) {
		for (int i = 0; i < MAX_SHARD; i++) {
			tmp[i] = st[i].scn ? st[i].scn + 1 : 0;
		}

		current_wal = [wal_dir open_for_write:lsn + 1 scn:tmp];
	}

        if (current_wal == nil) {
                say_syserror("can't open wal");
                return -1;
        }

#if HAVE_FALLOCATE && defined(FALLOC_FL_KEEP_SIZE)
	if (current_wal->alloced < current_wal->offset + 512*1024) {
		off_t old_alloced = current_wal->alloced;
		while (current_wal->alloced < current_wal->offset + 512*1024) {
			current_wal->alloced += 1024*1024;
		}
		int r = fallocate([current_wal fileno], FALLOC_FL_KEEP_SIZE, old_alloced, current_wal->alloced - old_alloced);
		if (r == -1)
			say_syserror("fallocate");
	}
#endif

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

struct request {
	u32 row_count;
	struct wal_reply *reply;
	const struct row_v12 **written_rows;
	bool clear_shard_state;
};

#define BATCH_SIZE 1024
static int flush(int fd, const struct request *requests, int count)
{
	struct iovec iovbuf[BATCH_SIZE], *iov = iovbuf;
	for (int i = 0; i < count; i++) {
		struct wal_reply *reply = requests[i].reply;
		iov[i] = (struct iovec){ .iov_base = reply,
					 .iov_len = reply->packet_len };
	}

	do {
		ssize_t r = writev(fd, iov, count);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		do {
			if (iov->iov_len <= r) {
				r -= iov->iov_len;
				count--;
				iov++;
			} else {
				iov->iov_base += r;
				iov->iov_len -= r;
				break;
			}
		} while (r);
	} while (count > 0);
	return 0;
}

static const char *
mode2str(int mode)
{
	switch(mode) {
	case SCN_PASS: return "PASS";
	case SCN_ASSIGN : return "ASSIGN";
	default: assert(false);
	}
}

int
wal_disk_writer(int fd, void *state, int len)
{
	struct wal_disk_writer_conf *conf = state;
	WALDiskWriter *writer = [[WALDiskWriter alloc] init_conf:conf];
	struct shard_state *st = conf->st;
	struct request requests[BATCH_SIZE];
	struct tbuf rbuf = TBUF(NULL, 0, fiber->pool);
	int result = EXIT_FAILURE;
	i64 start_lsn;
	bool io_failure = false, delay_read = false;
	ssize_t r;
	int request_count;

	assert(sizeof(*conf) == len);

	ev_tstamp start_time = ev_now();
	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);

	say_debug("%s: configured LSN:%"PRIi64, __func__, conf->lsn);
	for (int i = 0; i < MAX_SHARD; i++)
		if (st[i].scn)
			say_debug("\tShard:%i SCN:%"PRIi64" %s run_crc:0x%x",
				  i, st[i].scn,
				  mode2str(st[i].scn_mode),
				  st[i].run_crc);
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

		request_count = 0;
		start_lsn = writer->lsn;
		delay_read = false;
		io_failure = [writer prepare_write:st] == -1;
		for (int i = 0; i < MAX_SHARD; i++)
			st[i].wet_rows = 0;

		/* we're not running inside ev_loop, so update ev_now manually just before write */
		ev_now_update();


		for (int i = 0; i < nelem(requests); i++) {
			struct request *request = &requests[i];
			if (tbuf_len(&rbuf) < sizeof(u32) ||
			    tbuf_len(&rbuf) < *(u32 *)rbuf.ptr)
				break;

			u32 row_count = ((u32 *)rbuf.ptr)[1];
			if (!io_failure && row_count > [writer->current_wal wet_rows_offset_available]) {
				assert(request_count != 0);
				delay_read = true;
				break;
			}
			tbuf_ltrim(&rbuf, sizeof(u32) * 2); /* drop packet_len & row_count */

			say_debug("request[%i] rows:%i", i, row_count);

			request->row_count = row_count;
			request->written_rows = p0alloc(fiber->pool, sizeof(void *) * row_count);
			request->reply = p0alloc(fiber->pool,
						 sizeof(struct wal_reply) +
						 row_count * sizeof(request->reply->row_crc[0]));
			request->reply->sender = read_ptr(&rbuf);
			request->reply->fid = read_u32(&rbuf);
			request->reply->scn = -1;
			request->reply->lsn = -1;
			request->clear_shard_state = false;

			for (int j = 0; j < row_count; j++) {
				struct row_v12 *h = read_bytes(&rbuf, sizeof(*h));
				tbuf_ltrim(&rbuf, h->len); /* row data */

				if (io_failure)
					continue;

				struct shard_state *cst = &st[h->shard_id];
				if (unlikely(h->tag == (shard_tag|TAG_SYS))) {
					struct tbuf buf = TBUF(h->data, h->len, NULL);
					int ver = read_u8(&buf);
					int op = read_u8(&buf);
					int type = read_u8(&buf);
					tbuf_ltrim(&buf, 8); // row_count & run_crc_log
					tbuf_ltrim(&buf, 16); // mod_name
					bool our_shard = false;
					while (tbuf_len(&buf) > 0) {
						assert(tbuf_len(&buf) >= 16);
						char *str = read_bytes(&buf, 16);
						if (strcmp(str, cfg.hostname) == 0)
							our_shard = true;
					}

					assert(ver == 0);
					switch (op) {
					case 0:
						assert(h->shard_id == 0 || cst->scn == 0);
						assert(our_shard);
						cst->scn = h->scn;
						break;
					case 1:
						if (!our_shard)
							request->clear_shard_state = true;
						break;
					default:
						assert(false);
					}
					cst->scn_mode = type == 0 ? SCN_ASSIGN : SCN_PASS;
				}

				switch (cst->scn_mode) {
				case SCN_PASS:
					break;
				case SCN_ASSIGN:
					if (h->scn == 0)
						h->scn = cst->scn + ++cst->wet_rows;
					break;
				}

				request->written_rows[j] = [writer append_row:h data:h->data];
				if (request->written_rows[j] == NULL) {
					say_error("append_row failed");
					io_failure = true;
					request->clear_shard_state = false;
					continue;
				}

				say_debug("|	SCN:%"PRIi64" tag:%s data_len:%u",
					  request->written_rows[j]->scn,
					  xlog_tag_to_a(h->tag), h->len);
			}
			request_count++;
		}
		if (request_count == 0)
			continue;

		assert(start_lsn > 0);
		u32 rows_confirmed = [writer confirm_write] - start_lsn;

		for (int i = 0; i < request_count; i++) {
			struct request *request = &requests[i];
			struct wal_reply *reply = request->reply;

			reply->row_count = MIN(rows_confirmed, request->row_count); /* real number rows written to disk */
			reply->packet_len = sizeof(struct wal_reply) +
					    reply->row_count * sizeof(reply->row_crc[0]);

			if (reply->row_count > 0)
				reply->lsn = request->written_rows[reply->row_count - 1]->lsn;

			for (int k = 0; k < reply->row_count; k++) {
				const struct row_v12 *row = request->written_rows[k];
				if (!scn_changer(row->tag))
					continue;
				reply->scn = row->scn;

				u32 *run_crc = &st[row->shard_id].run_crc;
				run_crc_calc(run_crc, row->tag, row->data, row->len);
				reply->crc_count++;
				reply->row_crc[k] = (struct run_crc_hist){ .scn = row->scn, .value = *run_crc };

				/* next_scn is used for writing XLog header, which is turn used to
				   find correct file for replication replay.
				   so, next_scn should be updated only when data modification occurs */
				if (cfg.panic_on_scn_gap && row->scn - st[row->shard_id].scn != 1)
					panic("SCN GAP:%"PRIi64" rows:%i", row->scn - st[row->shard_id].scn, reply->row_count);
				st[row->shard_id].scn = row->scn;

				if (request->clear_shard_state)
					memset(&st[row->shard_id], 0, sizeof(st[row->shard_id]));
			}

			rows_confirmed -= reply->row_count;

			say_debug("reply[%i] rows:%i LSN:%"PRIi64" SCN:%"PRIi64,
				  i, reply->row_count, reply->lsn, reply->scn);
		}

		if (flush(fd, requests, request_count) < 0) {
			/* parent is dead, exit quetly */
			result = EX_OK;
			goto exit;
		}

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

@implementation DummyXLogWriter
- (i64) lsn { return lsn; }
- (void) incr_lsn:(int)diff { lsn += diff; }

- (id)
init_lsn:(i64)init_lsn
{
	[super init];
	lsn = init_lsn;
	return self;
}

- (struct wal_reply *)
submit:(const void *)data len:(u32)data_len tag:(u16)tag shard_id:(u16)shard_id
{
	(void)data;
	(void)data_len;
	(void)tag;
	(void)shard_id;
	lsn++;
	return NULL;
}

@end

@implementation XLogWriter

- (i64) lsn { return lsn; }
- (const struct child *) wal_writer { return &wal_writer; };

- (id)
init_lsn:(i64)init_lsn
   state:(id<RecoveryState>)state_
{
	assert(init_lsn > 0);
#if CFG_object_space
	assert(cfg.object_space == NULL || [[state_ shard:0] scn] > 0);
#endif
	lsn = init_lsn;
	state = state_;

	if (cfg.rows_per_wal <= 4)
		panic("inacceptable value of 'rows_per_wal'");

	say_info("Configuring WAL writer LSN:%"PRIi64, lsn);

	struct wal_disk_writer_conf *conf = xcalloc(1, sizeof(*conf));
	conf->lsn = lsn;
	for (int i = 0; i < MAX_SHARD; i++) {
		id<Shard> shard = [state shard:i];
		if (shard == nil)
			continue;
		conf->st[i].scn = [shard scn];
		conf->st[i].run_crc = [shard run_crc_log];
		conf->st[i].scn_mode = [(id)shard isKindOf:[POR class]] ? SCN_ASSIGN : SCN_PASS;
		if (conf->st[i].scn)
			say_info("\tShard:%i SCN:%"PRIi64" %s", i, conf->st[i].scn, mode2str(conf->st[i].scn_mode));
	}
	wal_writer = spawn_child("wal_writer", wal_disk_writer, conf, sizeof(*conf));
	if (wal_writer.pid < 0)
		panic("unable to start WAL writer");
	io = [netmsg_io alloc];
	netmsg_io_init(io, palloc_create_pool((struct palloc_config){.name = "wal_writer"}), wal_writer.fd);
	ev_init(&io->in, wal_disk_writer_input_dispatch);
	ev_set_priority(&io->in, 1);
	ev_set_priority(&io->out, 1);
	ev_io_start(&io->in);

	return self;
}


- (struct wal_reply *)
submit:(const void *)data len:(u32)data_len tag:(u16)tag shard_id:(u16)shard_id
{
	struct row_v12 row = { .scn = 0,
			       .tag = tag,
			       {.shard_id = shard_id} };
	struct wal_pack pack;
	wal_pack_prepare(self, &pack);
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

void
wal_pack_prepare(XLogWriter *w, struct wal_pack *pack)
{
	pack->netmsg = &w->io->wbuf;
	pack->packet_len = sizeof(*pack) - offsetof(struct wal_pack, packet_len);
	pack->fid = fiber->fid;
	pack->sender = fiber;
	pack->row_count = 0;
	net_add_iov(pack->netmsg, &pack->packet_len, pack->packet_len);
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

- (struct wal_reply *)
wal_pack_submit
{
	ev_io_start(&io->out);
	struct wal_reply *reply = yield();
	if (reply->row_count == 0)
		say_warn("wal writer returned error status");
	else
		lsn = reply->lsn;

	say_debug("%s: => rows:%i LSN:%"PRIi64, __func__, reply->row_count, lsn);
	return reply;
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


- (int)
snapshot_write
{
        XLog *snap;
	i64 *scn;
	u32 total_rows = 0;
	bool legacy_mode = 0;

	say_debug("%s: LSN:%"PRIi64, __func__, [state lsn]);

	if ([state lsn] < 0)
		return -1;

	scn = xcalloc(MAX_SHARD, sizeof(*scn));
	for (int i = 0; i < MAX_SHARD; i++) {
		Shard<Shard> *shard = [state shard:i];
		if (i == 0 && shard && shard->dummy)
			legacy_mode = 1;
		if (shard) {
			scn[i] = [shard scn];
			total_rows += [[shard executor] snapshot_estimate];
		}
	}

	snap = [snap_dir open_for_write:[state lsn] scn:scn];
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

	i64 snap_scn = -1;

	if (legacy_mode) {
		say_info("legacy snapshot without microsharding");
		struct tbuf *snap_ini = tbuf_alloc(fiber->pool);
		tbuf_append(snap_ini, &total_rows, sizeof(total_rows));

		u32 run_crc_log = [[state shard:0] run_crc_log];
		tbuf_append(snap_ini, &run_crc_log, sizeof(run_crc_log));
		u32 run_crc_mod = 0;
		tbuf_append(snap_ini, &run_crc_mod, sizeof(run_crc_mod));

		if ([state lsn] == 1) {
			snap_scn = 1;
		} else {
			assert([state shard:0]);
			snap_scn = [[state shard:0] scn];
		}

		if ([snap append_row:snap_ini->ptr len:tbuf_len(snap_ini)
				 scn:snap_scn tag:snap_initial|TAG_SYS] == NULL)
		{
			say_error("unable write initial row");
			return -1;
		}
		if ([[[state shard:0] executor] snapshot_write_rows:snap] < 0)
			return -1;
	} else {
		struct tbuf *snap_ini = tbuf_alloc(fiber->pool);
		u8 ver = 0;
		tbuf_append(snap_ini, &ver, sizeof(ver));
		tbuf_append(snap_ini, &total_rows, sizeof(total_rows));
		u32 flags = 0;
		tbuf_append(snap_ini, &flags, sizeof(flags));

		if ([snap append_row:snap_ini->ptr len:tbuf_len(snap_ini)
				 scn:snap_scn tag:snap_initial|TAG_SYS] == NULL)
		{
			say_error("unable write initial row");
			return -1;
		}

		for (int i = 0; i < MAX_SHARD; i++) {
			id<Shard> shard = [state shard:i];
			if (shard == nil)
				continue;

			if ([shard snapshot_write_header:snap] == NULL)
			{
				say_error("unable write initial row");
				return -1;
			}

			if ([[shard executor] snapshot_write_rows:snap] < 0)
				return -1;
		}
	}

	const char end[] = "END";
	if ([snap append_row:end len:strlen(end) scn:snap_scn tag:snap_final|TAG_SYS] == NULL) {
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
