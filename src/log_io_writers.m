/*
 * Copyright (C) 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2012, 2013, 2014, 2016, 2017 Yuriy Vostrikov
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


struct wal_disk_writer_conf {
	i64 lsn;
};

@interface WALDiskWriter: Object {
@public
	i64 lsn, batch_lsn;
	XLog *current_wal;	/* the WAL we'r currently reading/writing from/to */
	XLog *wal_to_close;
	i64 scn[MAX_SHARD];
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
	if (row->scn > 0)
		scn[row->shard_id] = MAX(scn[row->shard_id], row->scn);
	return [current_wal append_row:row data:data];
}

- (int)
prepare_write
{
	if (current_wal == nil) {
		current_wal = [wal_dir open_for_write:lsn + 1];
		[(XLog12 *)current_wal write_header_scn:scn];
		memset(scn, 0, sizeof(scn));
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
	batch_lsn = lsn;
	return 0;
}

- (int)
confirm_write
{
	static ev_tstamp last_flush;
	int count = 0;

	if (current_wal != nil) {
		i64 confirmed_lsn = [current_wal confirm_write];
		count = confirmed_lsn - batch_lsn;

		if (current_wal->inprogress && [current_wal rows] > 0) {
			/* invariant: .xlog must have at least one valid row
			   rename .xlog.inprogress to .xlog only after [confirm_write]
			   successfully writes some rows.
			   it's ok to discard rows on rename failure: they are not confirmed yet */

			if ([current_wal inprogress_rename] != 0) {
				unlink(current_wal->filename);
				[current_wal free];
				current_wal = nil;
				return 0;
			}

			say_info("created `%s'", current_wal->filename);
			[wal_to_close free];
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
	return count;
}

@end

struct request {
	u32 row_count;
	int shard_id;
	i64 epoch;
	struct wal_reply *reply;
	struct row_v12 **rows;
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

static int
request_row_count(struct tbuf *rbuf)
{
	const u32 *ptr = (const u32 *)rbuf->ptr;
	int len = tbuf_len(rbuf);

	if (len > sizeof(u32)) {
		if (len >= ptr[0])
			return ptr[1];
		tbuf_reserve(rbuf, ptr[0]);
	}
	return -1;
}

static void
request_parse(struct request *request, int row_count, struct tbuf *rbuf)
{
	tbuf_ltrim(rbuf, sizeof(u32[2])); /* drop packet_len & row_count */
	u32 magic = read_u32(rbuf);
	assert(magic == 0xba0babed);

	request->row_count = row_count;
	request->rows = p0alloc(fiber->pool, sizeof(void *) * row_count);
	request->reply = p0alloc(fiber->pool, sizeof(struct wal_reply));
	request->reply->seq = read_u64(rbuf);
	request->epoch = read_u64(rbuf);
	request->reply->lsn = -1;
	request->shard_id = -1;

	for (int i = 0; i < row_count; i++) {
		struct row_v12 *h = read_bytes(rbuf, sizeof(*h));
		tbuf_ltrim(rbuf, h->len); /* row data */
		request->rows[i] = h;
		assert(request->shard_id == -1 || request->shard_id == h->shard_id);
		request->shard_id = h->shard_id;
	}
}

int
wal_disk_writer(int fd, int cfd __attribute__((unused)), void *state, int len)
{
	struct wal_disk_writer_conf *conf = state;
	WALDiskWriter *writer = [[WALDiskWriter alloc] init_conf:conf];
	struct request requests[BATCH_SIZE];
	struct tbuf rbuf = TBUF(NULL, 0, fiber->pool);
	int result = EXIT_FAILURE;
	ssize_t r;
	int request_count;
	i64 epoch = 0;

	assert(sizeof(*conf) == len);

	ev_tstamp start_time = ev_now();
	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);

	say_debug("%s: configured LSN:%"PRIi64, __func__, conf->lsn);
	/* since wal_writer have bidirectional communiction to master
	   and checks for errors on send/recv,
	   there is no need in util.m:keepalive() */
	signal(SIGPIPE, SIG_IGN);

	/* ignore SIGUSR1, so accidental miss in 'kill -USR1' won't cause crash */
	signal(SIGUSR1, SIG_IGN);

	for (;;) {
		tbuf_reserve(&rbuf, 16 * 1024);
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

		if (cfg.coredump > 0 && ev_now() - start_time > cfg.coredump * 60) {
			maximize_core_rlimit();
			cfg.coredump = 0;
		}

		request_count = 0;
		if ([writer prepare_write] == -1)
			epoch++;

		/* we're not running inside ev_loop, so update ev_now manually just before write */
		ev_now_update();


		int rows_appended = 0;
		for (int i = 0; i < nelem(requests); i++) {
			struct request *request = &requests[i];
			int row_count = request_row_count(&rbuf);

			if (row_count < 0) // buffer too short
				break;

			request_parse(request, row_count, &rbuf);
			say_debug("request[%i] shard:%i rows:%i", i, request->shard_id, row_count);

			int j = 0;
			for (; j < row_count && request->epoch == epoch; j++) {
				struct row_v12 *row = request->rows[j];

				if ([writer append_row:row data:row->data] == NULL) {
					say_error("append_row failed");
					epoch++;
					break;
				}

				rows_appended++;
				say_debug("|	shard:%i SCN:%"PRIi64" tag:%s data_len:%u",
					  row->shard_id, row->scn, xlog_tag_to_a(row->tag), row->len);
			}
			for (; j < row_count && request->epoch != epoch; j++)
				request->rows[j] = NULL;

			request_count++;
		}
		if (request_count == 0)
			continue;

		int rows_confirmed = [writer confirm_write];
		assert(rows_appended >= rows_confirmed);

		if (rows_appended != rows_confirmed) /* some rows failed to flush */
			epoch++;

		for (int i = 0; i < request_count; i++) {
			struct request *request = &requests[i];
			struct wal_reply *reply = request->reply;

			reply->row_count = MIN(rows_confirmed, request->row_count); /* real number rows written to disk */
			reply->packet_len = sizeof(struct wal_reply);
			reply->epoch = epoch;
			if (reply->row_count > 0)
				reply->lsn = request->rows[reply->row_count - 1]->lsn;

			rows_confirmed -= reply->row_count;

			say_debug("reply[%i] rows:%i LSN:%"PRIi64,
				  i, reply->row_count, reply->lsn);
		}

		if (flush(fd, requests, request_count) < 0) {
			/* parent is dead, exit quetly */
			result = EX_OK;
			goto exit;
		}

		fiber_gc();
	}
exit:
	[writer->current_wal free];
	writer->current_wal = nil;
	return result;
}

@implementation XLogWriter

static void
wal_disk_writer_input_dispatch(ev_io *ev, int __attribute__((unused)) events)
{
	XLogWriter *self = ev->data;
	struct netmsg_io *io = container_of(ev, struct netmsg_io, in);
	struct tbuf *rbuf = &io->rbuf;

	static struct wal_reply err_reply;
	ssize_t r;
	do
		r = rbuf_recv(io, 64 * 1024);
	while (tbuf_free(&io->rbuf) == 0);

	if (r == 0)
		panic("WAL writer connection EOF");
	if (r < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
			say_syserror("%s: recv", __func__);
			panic("WAL writer connection read error");
		}
	}

	while (rbuf_len(io) > sizeof(u32)) {
		int len = *(u32 *)rbuf->ptr;
		if (rbuf_len(io) < len)
			break;

		struct wal_pack *pack = TAILQ_FIRST(&self->wal_queue);
		struct wal_reply *reply = rbuf->ptr;
		assert(pack->seq >= reply->seq);

		say_debug("%s: => rows:%i LSN:%"PRIi64" => %"PRIi64, __func__,
			  reply->row_count, self->lsn, reply->lsn);

		if (reply->row_count > 0) { /* success or partial success */
			self->lsn = reply->lsn;
			if (pack->seq == reply->seq)
				resume(pack->fiber, reply);
		} else {
			say_warn("WAL writer returned error status");
		}

		if (reply->epoch > self->epoch) {
			struct wal_pack *tmp;
			TAILQ_FOREACH_REVERSE_SAFE(pack, &self->wal_queue, wal_pack_tailq, link, tmp) {
				assert(pack->epoch == self->epoch);
				resume(pack->fiber, &err_reply); /* row_count == 0 => error */
			}
			self->epoch = reply->epoch;
		} else if (reply->epoch < self->epoch){
			assert(reply->row_count == 0);
		}
		rbuf_ltrim(io, len);
	}

	netmsg_pool_ctx_gc(&self->ctx);
}

- (i64) lsn { return lsn; }
- (const struct child *) wal_writer { return &wal_writer; };

void optimistic_write(ev_prepare *ev, int events __attribute__((unused)))
{
	XLogWriter *self = container_of(ev, XLogWriter, prepare);
	struct netmsg_io *io = self->io;
	if (io->wbuf.bytes > 0) {
		ssize_t r;
		do r = netmsg_writev(io->fd, &io->wbuf);
		while (r > 0);

		if (r < 0) {
			say_syswarn("writev() to %s failed, closing connection",
				    net_fd_name(io->fd));
			[io close];
			return;
		}
	}

	if (io->wbuf.bytes > 0)
		ev_io_start(&io->out);
	else
		ev_io_stop(&io->out);
}

- (id)
init_lsn:(i64)init_lsn
   state:(id<RecoveryState>)state_
{
	assert(init_lsn > 0);
#if CFG_object_space
	assert(cfg.object_space == NULL || [[state_ shard:0] scn] > 0);
#endif
	TAILQ_INIT(&wal_queue);
	lsn = init_lsn;
	state = state_;

	if (cfg.rows_per_wal <= 4)
		panic("inacceptable value of 'rows_per_wal'");

	say_info("Configuring WAL writer LSN:%"PRIi64, lsn);

	struct wal_disk_writer_conf *conf = xcalloc(1, sizeof(*conf));
	conf->lsn = lsn;
	wal_writer = spawn_child("wal_writer", wal_disk_writer, -1, conf, sizeof(*conf));
	if (wal_writer.pid < 0)
		panic("unable to start WAL writer");
	io = [netmsg_io alloc];
	netmsg_pool_ctx_init(&ctx, "wal_writer", 4 * 1024 * 1024);
	netmsg_io_init(io, &ctx, wal_writer.fd);
	ev_init(&io->in, wal_disk_writer_input_dispatch);
	io->in.data = self;
	ev_set_priority(&io->in, 1);
	ev_set_priority(&io->out, 1);
	ev_io_start(&io->in);
	ev_prepare_init(&self->prepare, optimistic_write);
	ev_set_priority(&self->prepare, -1);
	ev_prepare_start(&self->prepare);
	return self;
}


void
wal_pack_prepare(XLogWriter *w, struct wal_pack *pack)
{
	TAILQ_INSERT_TAIL(&w->wal_queue, pack, link);
	pack->fiber = fiber;
	pack->seq = w->seq++;
	pack->epoch = w->epoch;
	pack->shard_id = -1;
	pack->row_count = 0;

	if (unlikely(w->io == NULL)) {
		pack->netmsg = NULL;
		return;
	}
	pack->netmsg = &w->io->wbuf;
	pack->request = net_add_alloc(pack->netmsg, sizeof(*pack->request));
	pack->request->packet_len = sizeof(*pack->request);
	pack->request->magic = 0xba0babed;
	pack->request->seq = pack->seq;
	pack->request->epoch = pack->epoch;
	pack->request->row_count = 0;
}

u32
wal_pack_append_row(struct wal_pack *pack, struct row_v12 *row)
{
	assert(pack->row_count <= WAL_PACK_MAX);
	assert(pack->shard_id == -1 || pack->shard_id == row->shard_id);
	pack->shard_id = row->shard_id;

	pack->row_count++;
	if (unlikely(pack->netmsg == NULL))
		goto exit;

	int row_len = sizeof(*row) + row->len;
	pack->request->packet_len += row_len;
	if (row_len < 512) {
		pack->row = net_add_alloc(pack->netmsg, row_len);
		memcpy(pack->row, row, row_len);
	} else {
		pack->row = row;
		net_add_iov(pack->netmsg, row, row_len);
	}
exit:
	return WAL_PACK_MAX - pack->row_count;
}

void
wal_pack_append_data(struct wal_pack *pack, const void *data, size_t len)
{
	if (unlikely(pack->netmsg == NULL))
		return;
	pack->request->packet_len += len;
	pack->row->len += len;
	if (len < 512)
		net_add_iov_dup(pack->netmsg, data, len);
	else
		net_add_iov(pack->netmsg, data, len);
}

- (struct wal_reply *)
wal_pack_submit
{
	struct wal_pack *pack = TAILQ_LAST(&wal_queue, wal_pack_tailq);
	pack->request->row_count = pack->row_count;
	say_debug("submit WAL request seq:%"PRIi64, pack->seq);
	struct wal_reply *reply = yield();
	TAILQ_REMOVE(&wal_queue, pack, link);
	return reply;
}
@end

@implementation DummyXLogWriter
- (id)
init_lsn:(i64)init_lsn
   state:(id<RecoveryState>)state_
{
	(void)state_;
	TAILQ_INIT(&wal_queue);
	lsn = init_lsn;
	return self;
}

- (struct wal_reply *)
wal_pack_submit
{
	struct wal_pack *pack = TAILQ_LAST(&wal_queue, wal_pack_tailq);
	TAILQ_REMOVE(&wal_queue, pack, link);
	static struct wal_reply reply;
	reply.row_count = pack->row_count;
	lsn += pack->row_count;
	return &reply;
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
		if (shard == nil)
			continue;
		if (i == 0 && shard->dummy)
			legacy_mode = 1;

		scn[i] = [shard scn];
		total_rows += [[shard executor] snapshot_estimate];
	}

	snap = [snap_dir open_for_write:[state lsn]];
	if (snap == nil) {
		say_syserror("can't open snap for writing");
		return -1;
	}
	[(XLog12 *)snap write_header_scn:scn];
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

		u32 run_crc_log = [state shard:0]->run_crc;
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
				 scn:snap_scn tag:snap_initial] == NULL)
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
				 scn:snap_scn tag:snap_initial] == NULL)
		{
			say_error("unable write initial row");
			return -1;
		}

		for (int i = 0; i < MAX_SHARD; i++) {
			Shard<Shard> *shard = [state shard:i];
			if (shard == nil)
				continue;

			struct row_v12 *header = [shard creator_row];
			if ([snap append_row:header data:header->data] == NULL)
			{
				say_error("unable write initial row");
				return -1;
			}

			if ([[shard executor] snapshot_write_rows:snap] < 0)
				return -1;

			char dummy[2] = { 0 };
			if ([snap append_row:dummy len:sizeof(dummy)
				       shard:shard tag:shard_final] == NULL)
				return -1;
		}
	}

	const char end[] = "END";
	if ([snap append_row:end len:strlen(end) scn:snap_scn tag:snap_final] == NULL) {
		say_error("unable write final row");
		return -1;
	}

	if ([snap rows] == 0) /* initial snapshot in compat mode has no rows */
		[snap append_successful:1]; /* -[XLog close] won't rename empty .inprogress, trick it */

	if ([snap flush] == -1) {
		say_syserror("snap flush failed");
		return -1;
	}

	if ([snap write_eof_marker] == -1) {
		say_syserror("snap close failed");
		return -1;
	}

	if ([snap inprogress_rename] == -1) {
		say_syserror("snap inprogress rename failed");
		return -1;
	}

	[snap free];
	say_info("done");
	return 0;
}


@end

register_source();
