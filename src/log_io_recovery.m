/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016, 2017 Yuriy Vostrikov
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
#import <log_io.h>
#import <palloc.h>
#import <say.h>
#import <pickle.h>
#import <tbuf.h>
#import <net_io.h>
#import <assoc.h>
#import <paxos.h>
#import <shard.h>
#import <cfg/defs.h>

#include <third_party/crc32.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sysexits.h>

static struct iproto_service *recovery_service = NULL;
i64 fold_scn = 0;

static void recovery_iproto_ignore(void);
static void recovery_iproto(void);


bool
our_shard(const struct shard_op *sop)
{
	for (int i = 0; i < nelem(sop->peer); i++)
		if (strcmp(sop->peer[i], cfg.hostname) == 0)
			return true;
	return false;
}

struct octopus_cfg_peer *
cfg_peer_by_name(const char *name)
{
	for (struct octopus_cfg_peer **c = cfg.peer; *c; c++)
		if (strcmp(name, (*c)->name) == 0)
			return *c;
	return NULL;
}

const struct sockaddr_in *
peer_addr(const char *name, enum port_type port_type)
{
	static struct sockaddr_in sin;
	struct octopus_cfg_peer *c = cfg_peer_by_name(name);

	if (c == NULL)
		return NULL;
	if (atosin(c->addr, &sin) == -1)
		return NULL;
	switch (port_type) {
	case PORT_PRIMARY:
		break;
	case PORT_REPLICATION:
		if (c->replication_port > 0)
			sin.sin_port = htons(c->replication_port);
		break;
	}
	return &sin;
}

void
update_rt(int shard_id, Shard<Shard> *shard, const char *master_name)
{
	static struct msg_void_ptr msg;
	// FIXME: do a broadcast after change local destinations
	struct shard_route *route = &shard_rt[shard_id];
	static struct netmsg_pool_ctx ctx = { .cfg = {.name = "proxy_pool"},
					      .limit = 64 * 1024 };

	const struct sockaddr_in *addr = NULL;
	if (master_name && strcmp(master_name, "<dummy_addr>") != 0) {
		addr = peer_addr(master_name, PORT_PRIMARY);
		if (!addr) {
			say_error("Unknown peer %s, ignoring shard %i route update", master_name, shard_id);
			return;
		}
	}

	route->shard = shard;
	route->proxy = NULL;

	if (master_name) {
		if (!addr) {
			assert(shard->dummy);
			route->proxy = (void *)0x1; // FIXME: dummy struct;
			goto exit;
		}
		route->proxy = iproto_remote_add_peer(NULL, addr, &ctx); // will check for existing connect
	}
exit:
	if (shard == nil || shard->loading)
		return;

	if (cfg.hostname && strcmp(shard->peer[0], cfg.hostname) == 0) {
		shard_log("route update, force notify", shard_id);
		if (msg.link.tqe_prev == NULL)
			mbox_put(&recovery->rt_notify_mbox, &msg, link);
	}
}

void
update_rt_notify(va_list ap __attribute__((unused)))
{
	int sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock == -1) {
		say_syserror("socket");
		return;
	}

	int peer_count = 0;
	for (struct octopus_cfg_peer **p = cfg.peer; *p; p++)
		if (strcmp((*p)->name, cfg.hostname) != 0)
			peer_count++;

	struct sockaddr_in *buf = calloc(peer_count + 1, sizeof(*buf));
	struct sockaddr_in *paddr = buf;
	for (struct octopus_cfg_peer **p = cfg.peer; *p; p++) {
		if (strcmp((*p)->name, cfg.hostname) == 0)
			continue;
		const struct sockaddr_in *addr = peer_addr((*p)->name, PORT_PRIMARY);
		if (!addr || addr->sin_family == AF_UNSPEC) {
			say_error("bad peer addr %s", (*p)->name);
			continue;
		}
		*paddr++ = *addr;
	}
	paddr->sin_family = AF_UNSPEC;

	for (;;) {
		mbox_timedwait(&recovery->rt_notify_mbox, 1, 1);
		mbox_clear(&recovery->rt_notify_mbox);

		if (recovery->writer == nil)
			continue;

		for (int i = 0; i < nelem(shard_rt); i++) {
			if (shard_rt[i].shard == nil ||
			    shard_rt[i].shard->executor == nil ||
			    shard_rt[i].lock.locked)
				continue;

			id<Shard> shard = shard_rt[i].shard;
			char hostname[16];
			strncpy(hostname, cfg.hostname, 15);
			i64 scn = [shard scn];
			struct shard_op *sop = [shard snapshot_header];
			struct iproto header = { .msg_code = MSG_SHARD,
						 .shard_id = [shard id],
						 .data_len = sizeof(hostname) +
							     sizeof(scn) +
							     sizeof(*sop)};
			struct iovec iov[] = { { &header, sizeof(header) },
					       { hostname, 16 },
					       { &scn, sizeof(scn) },
					       { sop, sizeof(*sop) } };
			struct msghdr msg = (struct msghdr){
				.msg_namelen = sizeof(paddr[0]),
				.msg_iov = iov,
				.msg_iovlen = nelem(iov)
			};

			for (paddr = buf; paddr->sin_family != AF_UNSPEC; paddr++) {
				msg.msg_name = paddr;
				if (sendmsg(sock, &msg, 0) < 0)
					say_syserror("sendmsg");
			}
		}
	}
}

static struct shard_op *
validate_sop(struct netmsg_head *wbuf, const struct iproto *req, void *data, int len)
{
#define sop_err(...) ({							\
		if (wbuf) iproto_error_fmt(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, __VA_ARGS__); \
		say_error(__VA_ARGS__);					\
		})

	if (len < sizeof(struct shard_op)) {
		sop_err("sop: packet len is too small");
		return NULL;
	}
	struct shard_op *sop = data;
	if (sop->ver != 0) {
		sop_err("sop: bad version");
		return NULL;
	}
	if (!cfg.hostname || !cfg.peer || !cfg.peer[0]) {
		sop_err("sop: cfg.hostname or cfg.peer unconfigured");
		return NULL;
	}

	if (objc_lookUpClass(sop->mod_name) == Nil) {
		sop_err("bad mod name '%s'", sop->mod_name);
		return NULL;
	}

	if (strlen(sop->peer[0]) == 0) {
		sop_err("sop: empty master");
		return NULL;
	}

	for (int i = 0; i < nelem(sop->peer); i++) {
		if (sop->peer[i][15] != 0) {
			sop_err("sop: bad peer");
			return NULL;
		}
		if (strlen(sop->peer[i]) == 0)
			continue;
		if (!cfg_peer_by_name(sop->peer[i])) {
			sop_err("sop: unknown peer '%s'", sop->peer[i]);
			return NULL;
		}
		for (int j = 0; j < i; j++) {
			if (strcmp(sop->peer[i], sop->peer[j]) == 0) {
				sop_err("sop: duplicate peer '%s'", sop->peer[i]);
				return NULL;
			}
		}
	}

	if (sop->type == SHARD_TYPE_PART) {
		if (strcmp(sop->peer[0], cfg.hostname) == 0) {
			sop_err("sop: partial shard, bad master");
			return NULL;
		}
		if (strcmp(sop->peer[1], cfg.hostname) != 0) {
			sop_err("sop: partial shard, bad replica");
			return NULL;
		}
		for (int i = 2; i < nelem(sop->peer); i++)
			if (strlen(sop->peer[i]) != 0) {
				sop_err("sop: partial shard, bad replica");
				return NULL;
			}
	}

	if (sop->type != SHARD_TYPE_PAXOS &&
	    sop->type != SHARD_TYPE_POR &&
	    sop->type != SHARD_TYPE_PART)
	{
		sop_err("sop: invalid shard type %i", sop->type);
		return NULL;
	}
	return sop;
}


/// Recovery

static void pending_snapshot(ev_timer *w, int events __attribute__((unused)));

@implementation Recovery
- (id)
init
{
	[super init];
	mbox_init(&run_crc_mbox);
	mbox_init(&rt_notify_mbox);

	reader = [[XLogReader alloc] init_recovery:self];
	snap_writer = [[SnapWriter alloc] init_state:self];

	ev_init(&snapshot_timer, pending_snapshot);
	return self;
}

- (Shard<Shard> *) shard:(unsigned)shard_id
{
	assert(shard_id < nelem(shard_rt));
	return shard_rt[shard_id].shard;
}

- (i64) lsn {
	if (unlikely(initial_snap)) return 1;
	if (writer) return [writer lsn];
	if (reader) return [reader lsn];
	return -1;
}

- (id<XLogWriter>) writer { return writer; }

- (Shard<Shard> *)
shard_alloc:(char)type
{
	switch (type) {
	case SHARD_TYPE_PAXOS:
		return [Paxos alloc];
	case SHARD_TYPE_POR:
	case SHARD_TYPE_PART:
		return [POR alloc];
	default:
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad shard type");
	}
}

- (Shard<Shard> *)
shard_create:(int)shard_id scn:(i64)scn sop:(const struct shard_op *)sop
{
	assert(sop->ver == 0);
	assert(shard_rt[shard_id].shard == nil);
	Shard<Shard> *shard = [self shard_alloc:sop->type];
	[shard init_id:shard_id scn:scn sop:sop];
	[shard set_executor:[[objc_lookUpClass(sop->mod_name) alloc] init]];
	update_rt(shard->id, shard, NULL);
	shard_log("shard_create", shard->id);

	return shard;
}

- (Shard<Shard> *)
shard_create_dummy:(const struct row_v12 *)row
{
	i64 scn = 1;
	u32 row_count = 0;
	u32 run_crc = 0;
	if (row) {
		scn = row->scn;
		if (row->len > sizeof(u32) * 2) {
			// when loadding from v11 snapshot row has no row_count & row_crc
			struct tbuf buf = TBUF(row->data, row->len, NULL);
			row_count = read_u32(&buf);
			run_crc = read_u32(&buf);
		}
	}

	struct shard_op op = { .ver = 0,
			       .type = SHARD_TYPE_POR,
			       .row_count = row_count,
			       .run_crc_log = run_crc };
	strncpy(op.mod_name, [default_exec_class name], 15);

	struct feeder_param feeder;
	enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
	bool is_master = !fid_err && feeder.addr.sin_family == AF_UNSPEC;
	Shard<Shard> *shard = [self shard_alloc:op.type];
	shard->dummy = true;
	[shard init_id:0 scn:scn sop:&op];
	[shard set_executor:[[default_exec_class alloc] init]];
	update_rt(shard->id, shard, is_master ? NULL : "<dummy_addr>");
	shard_log("shard_create_dummy", shard->id);
	return shard;
}

- (void)
shard_alter_type:(Shard<Shard> **)shard sop:(struct shard_op *)sop
{
	Shard *old_shard = *shard,
	      *new_shard = [self shard_alloc:sop->type];
	id executor = old_shard->executor;
	[new_shard init_id:old_shard->id scn:old_shard->scn sop:sop];
	new_shard->loading = old_shard->loading;
	[new_shard set_executor:executor];
	*shard = new_shard;
	[*shard alter:sop];
	old_shard->executor = nil;
	[old_shard release];
}

- (void)
shard_op_create:(int)shard_id sop:(struct shard_op *)sop
{
	Shard<Shard> *shard;
	struct wal_reply *reply;
	shard = [self shard_create:shard_id scn:1 sop:sop];
	reply = [writer submit:sop len:sizeof(*sop)
			   tag:shard_create shard_id:shard_id];
	if (reply->row_count != 1) {
		[shard release];
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
	}
	[shard wal_final_row];
	assert(shard_rt[shard->id].shard == shard);
}


- (void)
shard_op_alter_peer:(Shard<Shard> *)shard sop:(struct shard_op *)sop
{
	if ([shard submit:sop len:sizeof(*sop) tag:shard_alter] != 1)
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

	[shard alter:sop];
}

- (void)
shard_op_alter_type:(Shard<Shard> *)shard type:(char)type
{
	struct shard_op *sop = [shard snapshot_header];
	sop->type = type;
	if ([shard submit:sop len:sizeof(*sop) tag:shard_alter] != 1)
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

	[self shard_alter_type:&shard sop:sop];
	shard_log("shard_alter_type", shard->id);
}


- (void)
recover_row:(struct row_v12 *)r
{
	assert(r->shard_id < nelem(shard_rt));
	Shard<Shard> *shard;
	if (shard_rt[0].shard && shard_rt[0].shard->dummy)
		shard = shard_rt[0].shard;
	else
		shard = shard_rt[r->shard_id].shard;
	int old_ushard = fiber->ushard;
	static int state = -1;
	if (shard) {
		if (r->scn <= shard->scn && shard->snap_loaded && !shard->dummy) {
			say_debug("%s: skip LSN:%"PRIi64" SCN:%"PRIi64" tag:%s",
				  __func__, r->lsn, r->scn, xlog_tag_to_a(r->tag));

			return;
		}

		fiber->ushard = shard->id;
	}
	@try {
		say_debug("%s: LSN:%"PRIi64" SCN:%"PRIi64" tag:%s",
			  __func__, r->lsn, r->scn, xlog_tag_to_a(r->tag));
		if (r->len)
			say_debug3("	%s", tbuf_to_hex(&TBUF(r->data, r->len, fiber->pool)));

		int tag = r->tag & TAG_MASK;
		switch (tag) {
		case snap_initial:
			if (r->scn != -1) /* no sharding */
				shard = [self shard_create_dummy:r];
			state = snap_initial;
			break;
		case snap_final:
			state = snap_final;
			if ([self shard:0] && [self shard:0]->dummy)
				[[self shard:0] recover_row:r];
			return;
		case wal_final:
			assert(false);
		case shard_create:
			if (cfg.hostname == NULL)
				panic("cfg.hostname is missing");
			assert(shard == nil);
			struct shard_op *sop = (struct shard_op *)r->data;
			if (our_shard(sop) || remote_loading) {
				shard = [self shard_create:r->shard_id scn:r->scn sop:sop];
				if (sop->type == SHARD_TYPE_PART)
					[(POR *)shard set_remote_scn:r];
				if (state == snap_final)
					shard->snap_loaded = true;
			} else {
				say_error("shard %i will be ignored", r->shard_id);
			}
			return;
		case shard_alter: {
			if (cfg.hostname == NULL)
				panic("cfg.hostname is missing");
			struct shard_op *sop = (struct shard_op *)r->data;
			if (our_shard(sop) && shard) {
				struct shard_op *old = [shard snapshot_header];

				if (sop->type != old->type)
					[self shard_alter_type:&shard sop:sop];
			}
		}
		default:
			break;
		}

		[shard recover_row:r];

		if (unlikely(fold_scn)) {
			if (r->scn == fold_scn && (r->tag & ~TAG_MASK) == TAG_WAL) {
				if ([(id)[shard executor] respondsTo:@selector(snapshot_fold)])
					exit([(id)[shard executor] snapshot_fold]);
				exit([snap_writer snapshot_write]);
			}
		}
	}
	@catch (Error *e) {
		say_error("Recovery: %s at %s:%i\n%s", e->reason, e->file, e->line,
				e->backtrace);
		struct tbuf *out = tbuf_alloc(fiber->pool);
		[[shard executor] print:r into:out];
		say_error("Failed row: %.*s\n", tbuf_len(out), (char *)out->ptr);

		@throw;
	}
	@finally {
		say_debug2("%s: => recovery LSN:%"PRIi64, __func__, [self lsn]);
		fiber->ushard = old_ushard;
	}
}


- (i64)
load_from_local
{
	if (fold_scn)  {
		/* select snapshot before desired scn */
		XLog *snap = [snap_dir find_with_scn:fold_scn shard:0];
		[reader load_full:snap];
		say_error("unable to find record with SCN:%"PRIi64, fold_scn);
		exit(EX_OSFILE);
	}

	i64 local_lsn = [reader load_full:nil];
	title(NULL);
	return local_lsn;
}

- (int)
load_from_remote
{
	int count = 0;
	struct feeder_param feeder;
	XLogRemoteReader *remote_reader = [[XLogRemoteReader alloc] init_recovery:self];
	remote_loading = true;
#if CFG_object_space
	if (cfg.object_space) {
 		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		assert (!fid_err && feeder.addr.sin_family != AF_UNSPEC);
		count = [remote_reader load_from_remote:&feeder];
	} else
#endif
	for (struct octopus_cfg_peer **p = cfg.peer; cfg.peer && *p; p++) {
		if (strcmp((*p)->name, cfg.hostname) == 0)
			continue;

		feeder = (struct feeder_param){ .ver = 2,
						.addr = *peer_addr((*p)->name, PORT_REPLICATION),
						.filter = {.type = FILTER_TYPE_C,
							   .name = "shard" } };
		count += [remote_reader load_from_remote:&feeder];
	}
	remote_loading = false;
	[remote_reader free];
	return count;
}

void
wal_lock(va_list ap __attribute__((unused)))
{
	while ([wal_dir lock] != 0)
		fiber_sleep(1);

	[recovery enable_local_writes];
}

- (void)
simple:(struct iproto_service *)service
{
	recovery_service = service;
	recovery_iproto_ignore();

	i64 local_lsn = [self load_from_local];

#if CFG_object_space
	if (local_lsn == 0 && cfg.object_space) {
		struct feeder_param feeder;
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		if (fid_err || feeder.addr.sin_family == AF_UNSPEC) {
			say_error("unable to find initial snapshot");
			say_info("don't you forget to initialize "
				 "storage with --init-storage switch?");
			exit(EX_USAGE);
		}
	}
#else
	(void)local_lsn;
#endif

	if (cfg.local_hot_standby) {
		[reader hot_standby];
		for (int i = 0; i < MAX_SHARD; i++)
			[[self shard:i] wal_final_row];
		fiber_create("wal_lock", wal_lock, self);
	} else {
		[self enable_local_writes];
	}
}

static int
same_dir(XLogDir *a, XLogDir *b)
{
	struct stat sta, stb;
	if ([a stat:&sta] == 0 && [a stat:&stb] == 0)
		return sta.st_ino == stb.st_ino;
	else
		return strcmp(a->dirname, b->dirname) == 0;
}

- (void)
lock
{
	if ([wal_dir lock] < 0)
		panic_syserror("Can't lock wal_dir:%s", wal_dir->dirname);

	if (!same_dir(wal_dir, snap_dir)) {
		if ([snap_dir lock] < 0)
			panic_syserror("Can't lock snap_dir:%s", snap_dir->dirname);
	}
	say_info("WAL dir exclusive lock acquired");
}

- (void)
enable_local_writes
{
	[self lock];
	i64 reader_lsn = [reader recover_finalize];
	[reader free];
	reader = nil;

	i64 writer_lsn = reader_lsn;
	if (reader_lsn == 0) {
		int count = [self load_from_remote];
		if (count < 1) {
			say_error("unable to pull initial snapshot");
			exit(1);
		}
		writer_lsn = 1;
#if CFG_object_space
		if (cfg.object_space) {
			assert([[self shard:0] scn] > 0);
			if (cfg.sync_scn_with_lsn)
				writer_lsn = [[self shard:0] scn];
		}
#endif
	}

	[self configure_wal_writer:writer_lsn];

	if (reader_lsn == 0) {
		say_debug("Saving initial replica snapshot LSN:%"PRIi64, writer_lsn);
		/* don't wait for snapshot. our goal to be replica as fast as possible */
		fiber_create("snapshot", fork_and_snapshot);
	}

	for (int i = 0; i < MAX_SHARD; i++) {
		Shard *shard = [self shard:i];
		if (shard && [shard our_shard])
			[shard enable_local_writes];
		else
			[shard release];
	}

	recovery_iproto();
}

static void
run_crc_writer(va_list ap)
{
	ev_tstamp submit_tstamp = ev_now(),
			  delay = va_arg(ap, ev_tstamp);
	for (;;) {
		mbox_wait(&recovery->run_crc_mbox);
		mbox_clear(&recovery->run_crc_mbox);

		if (ev_now() - submit_tstamp < delay)
			continue;

		@try {
			for (int i = 0; i < MAX_SHARD; i++) {
				id<Shard> shard = [recovery shard:i];
				if (shard == nil)
					continue;
				if ([shard is_replica])
					continue;
				while ([shard submit_run_crc] < 0)
					fiber_sleep(0.1);
			}
		}
		@catch (Error *e) {
			say_warn("run_crc submit failed, [%s reason:\"%s\"] at %s:%d",
				 [[e class] name], e->reason, e->file, e->line);
			[e release];
		}

		submit_tstamp = ev_now();
		fiber_gc();
	}
}

static void
nop_hb_writer(va_list ap)
{
	ev_tstamp delay = va_arg(ap, ev_tstamp);
	char body[2] = {0};

	for (;;) {
		fiber_sleep(delay);

		for (int i = 0; i < MAX_SHARD; i++) {
			Shard<Shard> *shard = [recovery shard:i];
			if (shard == nil || shard->loading)
				continue;
			if ([shard is_replica])
				continue;

			[shard submit:body len:nelem(body) tag:nop];
		}
	}
}


- (void)
configure_wal_writer:(i64)lsn
{
	if (fold_scn)
		return;

	if (cfg.wal_writer_inbox_size == 0) {
		writer = [[DummyXLogWriter alloc] init_lsn:lsn];
		return;
	}

	writer = [[XLogWriter alloc] init_lsn:lsn
					state:self];

	if (cfg.run_crc_delay > 0)
		fiber_create("run_crc", run_crc_writer, cfg.run_crc_delay);

	if (cfg.nop_hb_delay > 0)
		fiber_create("nop_hb", nop_hb_writer, cfg.nop_hb_delay);
}

- (int)
write_initial_state
{
	initial_snap = true;
	return [snap_writer snapshot_write];
}

- (void)
request_snapshot
{
	static ev_tstamp delay;
	if (delay == 0)
		delay = getenv("OCTOPUS_TEST") ? 0.1 : 60;
	ev_timer_stop(&snapshot_timer);
	ev_timer_set(&snapshot_timer, delay, 0.);
	ev_timer_start(&snapshot_timer);
}

- (int)
fork_and_snapshot
{
	pid_t p;
	int status;
	i64 lsn;

	assert(fiber != sched);

	if ([self lsn] <= 0) {
		say_error("can't save snapshot: LSN is unknown");
		return -1;
	}

	wlock(&snapshot_lock);
	lsn = [self lsn];
	p = oct_fork();
	wunlock(&snapshot_lock);
	switch (p) {
	case -1:
		say_syserror("fork");
		return -1;

	case 0: /* child, the dumper */
		current_module = NULL;
		fiber->name = "dumper";
		title("(%" PRIu32 ")", getppid());
		fiber_destroy_all();
		palloc_unmap_unused();
		close_all_xcpt(3, stderrfd, sayfd, snap_dir->fd);

		int fd = open("/proc/self/oom_score_adj", O_WRONLY);
		if (fd) {
			int res _unused_ = write(fd, "900\n", 4);
			close(fd);
		}
		int r = [snap_writer snapshot_write];

#ifdef COVERAGE
		__gcov_flush();
#endif
		_exit(r != 0 ? errno : 0);

	default: /* parent, wait for child */
		snapshot_running = true;
		status = wait_for_child(p);
		snapshot_running = false;
		if (status == 0)
			last_snapshot_lsn = lsn;
		return status;
	}
}

void
fork_and_snapshot(va_list ap __attribute__((unused)))
{
	if ([recovery fork_and_snapshot] != 0)
		[recovery request_snapshot];
}

static void
pending_snapshot(ev_timer *w, int events __attribute__((unused)))
{
	if (recovery->snapshot_running || [recovery lsn] == recovery->last_snapshot_lsn ||
	    [recovery lsn] == 1) {
		ev_timer_set(w, 1, 0.);
		ev_timer_start(w);
	} else {
		fiber_create("snapshot/deadline", fork_and_snapshot);
	}
}

- (const char *)
status
{
	Shard<Shard> *shard = [self shard:0];
	if (shard && shard->dummy) // legacy mode
		return [shard status];
	return cfg.hostname;
}


static int
peer_idx(const struct shard_op *sop, const char *peer)
{
	for (int i = 0; i < nelem(sop->peer); i++)
		if (memcmp(sop->peer[i], peer, 16) == 0)
			return i;
	return -1;
}

void
iproto_shard_cb(struct netmsg_head *wbuf, struct iproto *req)
{
	struct tbuf data = TBUF(req->data, req->data_len, NULL);
	int version = read_u8(&data);
	int cmd = read_u8(&data);
	int shard_id = req->shard_id;

	if (version != 1)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad version");
	if (shard_id > nelem(shard_rt))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "shard_id too big");

	struct shard_route *route = shard_rt + shard_id;

	char *peer;
	struct shard_op *sop;
	int i;

	if (cmd == 0) {
		if (route->shard || route->proxy) // FIXME: race
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "shard already exists");

		if (shard_rt[0].shard && shard_rt[0].shard->dummy)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't create shards while dummy shard exists");

		sop = &(struct shard_op){ .type = read_u8(&data) };
		strncpy(sop->mod_name, [recovery->default_exec_class name], 16);

		switch (sop->type) {
		case SHARD_TYPE_POR:
			strcpy(sop->peer[0], cfg.hostname);
			for (i = 1; i < nelem(sop->peer); i++)
				strcpy(sop->peer[i], read_bytes(&data, 16));
			break;
		case SHARD_TYPE_PAXOS:
			strcpy(sop->peer[0], cfg.hostname);
			for (i = 1; i < 3; i++)
				strcpy(sop->peer[i], read_bytes(&data, 16));
			break;
		case SHARD_TYPE_PART:
			strcpy(sop->peer[0], read_bytes(&data, 16));
			strcpy(sop->peer[1], cfg.hostname);
			break;
		default:
			iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "bad shard type %i", sop->type);
		}
		for (i = 0; i < nelem(sop->peer); i++) {
			if (*sop->peer[i] &&
			    peer_addr(sop->peer[i], PORT_PRIMARY) == NULL)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unknown peer");
		}

		if (validate_sop(wbuf, req, sop, sizeof(*sop))) {
			[recovery shard_op_create:shard_id sop:sop];
			iproto_reply_small(wbuf, req, ERR_CODE_OK);
		}
		return;
	}

	if (route->proxy) {
		struct iproto_ingress *ingress = container_of(wbuf, struct iproto_ingress, wbuf);
		iproto_proxy_send(route->proxy, ingress, MSG_IPROXY, req, NULL, 0);
		return;
	}

	Shard<Shard> *shard = route->shard;

	if (!shard)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "no such shard");
	if (shard->loading)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "shard is loading");
	if (cmd !=2 && strcmp(shard->peer[0], cfg.hostname) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "not master");
	sop = [shard snapshot_header];

	if (cmd == 6) { /* change type */
		char type = read_u8(&data);

		if (sop->type == SHARD_TYPE_POR && type == SHARD_TYPE_PAXOS) {
			if (peer_idx(sop, (char[16]){0}) != 3)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad peer count");
		} else if (sop->type == SHARD_TYPE_POR && type == SHARD_TYPE_PART) {
			if (peer_idx(sop, (char[16]){0}) != 2)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad peer count");
		} else
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unsuported type change");
		[recovery shard_op_alter_type:shard type:type];
		iproto_reply_small(wbuf, req, ERR_CODE_OK);
		return;
	}

	if (sop->type != SHARD_TYPE_POR && !(sop->type == SHARD_TYPE_PART && cmd == 5))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unsupported");

	switch (cmd) {
	case 1: /* delete */
		if (peer_idx(sop, (char[16]){0}) != 1)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't delete shard with replicas");
		update_rt(shard->id, nil, NULL);
		[shard release];
		[recovery request_snapshot]; // FIXME: do a proper WAL write
		break;
	case 2: /* upgrade dummy */
		if (!shard->dummy)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "not dummy");
		if (!cfg.hostname)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "hostname is not configured");

		shard->dummy = false;
		strcpy(shard->peer[0], cfg.hostname);
		[recovery request_snapshot];
		break;
	case 3: /* add peer */
		peer = read_bytes(&data, 16);
		if (peer_addr(peer, PORT_PRIMARY) == NULL)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unknown peer");
		if (peer_idx(sop, peer) == -1) {
			i = peer_idx(sop, (char[16]){0});
			if (i == -1)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "too many peers for shard");
			memcpy(sop->peer[i], peer, 16);
			[recovery shard_op_alter_peer:shard sop:sop];
		}
		break;
	case 4: /* remove peer */
		peer = read_bytes(&data, 16);
		i = peer_idx(sop, peer);
		if (i != -1) {
			for (; i < nelem(sop->peer) - 1; i++)
				memcpy(sop->peer[i], sop->peer[i + 1], 16);
			memset(sop->peer[nelem(sop->peer) - 1], 0, 16);
			[recovery shard_op_alter_peer:shard sop:sop];
		}
		break;
	case 5: /* set master */
		peer = read_bytes(&data, 16);
		i = peer_idx(sop, peer);
		if (i == -1)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "shard has no such peer");
		char tmp[16];
		memcpy(tmp, sop->peer[0], 16);
		memcpy(sop->peer[0], sop->peer[i], 16);
		memcpy(sop->peer[i], tmp, 16);
		[recovery shard_op_alter_peer:shard sop:sop];
		break;
	default:
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unknown cmd");
	}
	iproto_reply_small(wbuf, req, ERR_CODE_OK);
}

static void
iproto_shard_rt_cb(struct netmsg_head *wbuf, struct iproto *req)
{
	struct iproto_retcode *reply = iproto_reply(wbuf, req, 0);

	int sz = sizeof(struct sockaddr_in);
	struct sockaddr_in none = { .sin_family = AF_UNSPEC };
	char version = 0x01;

	net_add_iov_dup(wbuf, &version, sizeof(version));
	for (int i = 0; i < nelem(shard_rt); i++) {
		struct shard_route *e = &shard_rt[i];
		if (e->shard == nil && e->proxy == NULL)
			net_add_iov_dup(wbuf, &none, sz);
		else if (e->proxy)
			net_add_iov_dup(wbuf, &e->proxy->ts.daddr, sz);
		else
			net_add_iov_dup(wbuf, peer_addr(e->shard->peer[0], PORT_PRIMARY), sz);
	};

	iproto_reply_fixup(wbuf, reply);
}
static void
iproto_shard_load_aux(va_list ap)
{
	int shard_id = va_arg(ap, int);
	struct shard_op sop = *va_arg(ap, struct shard_op *);

	Shard<Shard> *shard = nil;
	struct shard_route *route = shard_rt + shard_id;
	@try {
		wlock(&route->lock);
		assert(route->shard == nil);
		shard = [recovery shard_create:shard_id scn:-1 sop:&sop];
		[shard load_from_remote];
		if (shard->executor)
			[recovery request_snapshot];
		else
			[shard release];
	}
	@finally {
		wunlock(&route->lock);
	}
}

static void
iproto_shard_udpcb(const char *buf, ssize_t len, void *data __attribute__((unused)))
{
	struct iproto *req = (void *)buf;
	if (len < sizeof(struct iproto) || sizeof(*req) + req->data_len != len)
		return;
	if (req->data_len != 16 + sizeof(i64) + sizeof(struct shard_op))
		return;

	int shard_id = req->shard_id;
	struct shard_route *route = &shard_rt[shard_id];
	Shard<Shard> *shard = route->shard;
	const char *peer_name = (char *)req->data;
	i64 scn = *(i64 *)(req->data + 16);
	struct shard_op *sop = validate_sop(NULL, NULL,
					    req->data + 16 + sizeof(i64),
					    req->data_len - 16 - sizeof(i64));
	if (sop == NULL)
		return;

	if (route->lock.locked) {
		say_debug("ignore route update, shard %i loading", shard_id);
		return;
	}

	if (shard == nil) {
		if (!our_shard(sop))
			update_rt(shard_id, nil, sop->peer[0]);
		else
			fiber_create("load shard", iproto_shard_load_aux, shard_id, sop);
		return;
	}

	if (!our_shard(sop)) {
		// we must get shard_alter via replication
		say_warn("route update from %s: shard %i master %s, FIXME ignoring delete request",
			 peer_name, shard_id, sop->peer[0]);
		return;
	}
	if (scn > [shard scn] && strcmp(shard->peer[0], sop->peer[0]) != 0)
	{
		say_warn("route update from %s: shard %i master %s, FIXME ignoring alter request",
			 peer_name, shard_id, sop->peer[0]);
		return;
	}
}

void
set_recovery_service(struct iproto_service *service)
{
	assert(recovery_service == NULL);
	recovery_service = service;
}

static void
recovery_iproto(void)
{
	if (recovery_service == NULL)
		return;

	if (cfg.peer && *cfg.peer && cfg.hostname) {
		fiber_create("route_recv", udp_server,
			     recovery_service->addr, iproto_shard_udpcb, NULL, NULL);
		fiber_create("udpate_rt_notify", update_rt_notify);
		service_register_iproto(recovery_service, MSG_SHARD, iproto_shard_cb, IPROTO_LOCAL|IPROTO_WLOCK);
		service_register_iproto(recovery_service, MSG_SHARD_RT, iproto_shard_rt_cb, IPROTO_LOCAL);
		paxos_service(recovery_service);
	}
}

static void
iproto_ignore(struct netmsg_head *h __attribute__((unused)),
	      struct iproto *r __attribute__((unused)))
{
}

static void
recovery_iproto_ignore()
{
	if (recovery_service == NULL)
		return;
	if (cfg.peer && *cfg.peer && cfg.hostname && cfg_peer_by_name(cfg.hostname)) {
		service_register_iproto(recovery_service, MSG_SHARD, iproto_ignore, IPROTO_LOCAL|IPROTO_NONBLOCK);
		service_register_iproto(recovery_service, LEADER_PROPOSE, iproto_ignore, IPROTO_LOCAL|IPROTO_NONBLOCK);
		service_register_iproto(recovery_service, PREPARE, iproto_ignore, IPROTO_LOCAL|IPROTO_NONBLOCK);
		service_register_iproto(recovery_service, ACCEPT, iproto_ignore, IPROTO_LOCAL|IPROTO_NONBLOCK);
		service_register_iproto(recovery_service, DECIDE, iproto_ignore, IPROTO_LOCAL|IPROTO_NONBLOCK);
	} else {
		say_info("usharding disabled (cfg.peer of cfg.hostname is bad or missing)");
	}
}

- (void)
shard_info:(struct tbuf *)buf
{
	for (int i = 0; i < nelem(shard_rt); i++) {
		const struct shard_route *rt = shard_rt + i;
		if (rt->shard || rt->proxy) {
			tbuf_printf(buf, "%i: {", i);
			route_info(rt, buf);
			tbuf_printf(buf, "}\r\n");
		}
	}
}
@end


register_source();
