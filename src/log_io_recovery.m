/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

static struct iproto_service *recovery_service;
i64 fold_scn = 0;

static void recovery_iproto_ignore(void);
static void recovery_iproto(void);

static void shard_log(const char *msg, int shard_id);

struct row_v12 *
dummy_row(i64 lsn, i64 scn, u16 tag)
{
	struct row_v12 *r = palloc(fiber->pool, sizeof(struct row_v12));

	r->lsn = lsn;
	r->scn = scn;
	r->tm = ev_now();
	r->tag = tag;
	r->cookie = default_cookie;
	r->len = 0;
	r->data_crc32c = crc32c(0, (unsigned char *)"", 0);
	r->header_crc32c = crc32c(0, (unsigned char *)r + sizeof(r->header_crc32c),
				  sizeof(*r) - sizeof(r->header_crc32c));
	return r;
}

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
	static struct palloc_pool *proxy_pool;
	if (!proxy_pool)
		proxy_pool = palloc_create_pool((struct palloc_config){.name = "proxy_to_primary"});

	const struct sockaddr_in *addr = NULL;
	if (master_name && strcmp(master_name, "<dummy_addr>") != 0) {
		addr = peer_addr(master_name, PORT_PRIMARY);
		if (!addr) {
			say_error("Unknown peer %s, ignoring shard %i route update", master_name, shard_id);
			return;
		}
		memcpy(route->master_name, master_name, 16);
	} else {
		route->master_name[0] = 0;
	}

	route->shard = shard;
	route->proxy = NULL;

	if (master_name) {
		if (!addr) {
			assert(shard->dummy);
			route->proxy = (void *)0x1; // FIXME: dummy struct;
			goto exit;
		}
		route->proxy = iproto_remote_add_peer(NULL, addr, proxy_pool); // will check for existing connect
		route->proxy->ts.name = "proxy_to_primary";
	}
exit:
	if (shard != nil) {
		if (shard->loading)
			return;
		if (cfg.hostname &&
		    strcmp(shard->peer[0], cfg.hostname) == 0) {
			shard_log("route update, force notify", shard_id);
			if (msg.link.tqe_prev == NULL)
				mbox_put(&recovery->rt_notify_mbox, &msg, link);
		}
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
			if (shard_rt[i].shard == nil)
				continue;

			id<Shard> shard = shard_rt[i].shard;
			char hostname[16];
			strncpy(hostname, cfg.hostname, 16);
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
validate_sop(void *data, int len)
{
	struct shard_op *sop = data;
	if (len < sizeof(struct shard_op)) {
		say_error("sop: packet len is too small");
		return NULL;
	}
	if (sop->ver != 0) {
		say_error("sop: bad version");
		return NULL;
	}
	if (!cfg.hostname || !cfg.peer || !cfg.peer[0]) {
		say_error("sop: cfg.hostname or cfg.peer unconfigured");
		return NULL;
	}

	if (objc_lookUpClass(sop->mod_name) == Nil) {
		say_error("bad mod name '%s'", sop->mod_name);
		return NULL;
	}

	if (strlen(sop->peer[0]) == 0) {
		say_error("sop: empty master");
		return NULL;
	}

	for (int i = 0; i < nelem(sop->peer); i++) {
		if (sop->peer[i][15] != 0) {
			say_error("sop: bad peer");
			return NULL;
		}
		if (strlen(sop->peer[i]) == 0)
			continue;
		if (!cfg_peer_by_name(sop->peer[i])) {
			say_error("sop: unknown peer '%s'", sop->peer[i]);
			return NULL;
		}
	}
	if (sop->type != SHARD_TYPE_PAXOS && sop->type != SHARD_TYPE_POR) {
		say_error("sop: invalide shard type %i", sop->type);
		return NULL;
	}
	return sop;
}


@implementation Shard
- (int) id { return self->id; }
- (i64) scn { return scn; }
- (id<Executor>) executor { return executor; }
- (ev_tstamp) lag { return lag; }
- (ev_tstamp) last_update_tstamp { return last_update_tstamp; }

- (const char *)status { return status_buf; }
- (ev_tstamp) run_crc_lag { return run_crc_lag(&run_crc_state); }
- (const char *) run_crc_status { return run_crc_status(&run_crc_state); }
- (u32) run_crc_log { return run_crc_log; }


static void
shard_info(Shard *shard, struct tbuf *buf)
{
	tbuf_printf(buf, "SCN: %"PRIi64", type: %s, ", [shard scn], [[shard class] name]);
	tbuf_printf(buf, "peer: [%s", shard->peer[0]);
	for (int i = 1; i < nelem(shard->peer) && shard->peer[i][0]; i++)
		tbuf_printf(buf, ", %s", shard->peer[i]);
	tbuf_printf(buf, "]");
}

static void
route_info(const struct shard_route *route, struct tbuf *buf)
{
	tbuf_printf(buf, "mode: ");
	if (route->shard == nil && route->proxy == nil) {
		tbuf_printf(buf, "NONE");
	} else if (route->shard && route->shard->loading) {
		tbuf_printf(buf, "LOADING");
	} else if (route->shard && route->proxy) {
		tbuf_printf(buf, "PARTIAL_PROXY");
	} else if (route->proxy) {
		tbuf_printf(buf, "PROXY");
	} else {
		tbuf_printf(buf, "LOCAL");
	}

	if (route->shard && !route->shard->loading) {
		tbuf_printf(buf, ", ");
		shard_info(route->shard, buf);
	}

	if (route->proxy && route->proxy != (void *)0x1)
		tbuf_printf(buf, ", proxy_addr: '%s/%s'",
			    route->proxy->ts.name, sintoa(&route->proxy->ts.daddr));
}

static void
shard_log(const char *msg, int shard_id)
{
	int old_ushard = fiber->ushard;
	fiber->ushard = shard_id;

	struct tbuf *buf = tbuf_alloc(fiber->pool);
	route_info(shard_rt + shard_id, buf);
	tbuf_printf(buf, " => %s", msg);
	say_info("META %s", (char *)buf->ptr);
	fiber->ushard = old_ushard;
}


- (id) free
{
	[(id)executor free];
	update_rt(self->id, nil, peer[0]);
	shard_log("removed", self->id);

	return [super free];
}

- (id) init_id:(int)shard_id
	   scn:(i64)scn_
	   sop:(const struct shard_op *)sop
{
	[super init];
	loading = true;
	self->id = shard_id;
	scn = scn_;
	run_crc_log = sop->run_crc_log;

	for (int i = 0; i < nelem(sop->peer); i++) {
		if (strlen(sop->peer[i]) == 0)
			break;

		strncpy(peer[i], sop->peer[i], 16); // FIXME: это данные из сети

		assert(cfg.hostname); // cfg.hostname может быть пустым только при создании dummy шарда.
	}

	if (objc_lookUpClass(sop->mod_name) == Nil)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "bad mod name '%s'", sop->mod_name);

	executor = [[objc_lookUpClass(sop->mod_name) alloc] init_shard:self];

	shard_log("init", self->id);
	return self;
}

- (void)
adjust_route
{
	abort();
}

- (void)
alter_peers:(struct shard_op *)sop
{
	for (int i = 0; i < nelem(sop->peer); i++)
		strncpy(peer[i], sop->peer[i], 16);
	[self adjust_route];
}

- (struct shard_op *)
snapshot_header
{
	static struct shard_op op;
	op = (struct shard_op){ .ver = 0,
				.type = strcmp("POR", [[self class] name]) != 0,
				.tm = ev_now(),
				.row_count = [executor snapshot_estimate],
				.run_crc_log = run_crc_log };
	strncpy(op.mod_name, [[(id)executor class] name], 16);
	for (int i = 0; i < nelem(peer) && peer[i]; i++)
		strncpy(op.peer[i], peer[i], 16);
	return &op;
}

- (const struct row_v12 *)
snapshot_write_header:(XLog *)snap
{
	struct shard_op *sop = [self snapshot_header];
	return [snap append_row:sop len:sizeof(*sop)
			  shard:self tag:shard_create|TAG_SYS];
}

- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	(void)data; (void)len; (void)tag;
	abort();
}

- (int)
submit_run_crc
{
	struct tbuf *b = tbuf_alloc(fiber->pool);
	tbuf_append(b, &scn, sizeof(scn));
	tbuf_append(b, &run_crc_log, sizeof(run_crc_log));
	typeof(run_crc_log) run_crc_mod = 0;
	tbuf_append(b, &run_crc_mod, sizeof(run_crc_mod));

	return [self submit:b->ptr len:tbuf_len(b) tag:run_crc|TAG_SYS];
}


- (void)
status_update:(const char *)fmt, ...
{
	char buf[sizeof(status_buf)];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (strcmp(buf, status_buf) == 0)
		return;

	shard_log(buf, self->id);

	strncpy(status_buf, buf, sizeof(status_buf));
	title(NULL);

	[executor status_changed];
}

- (bool)
our_shard
{
	if (loading || dummy)
		return 1;
	for (int i = 0; i < nelem(peer); i++)
		if (strcmp(peer[i], cfg.hostname) == 0)
			return 1;
	return 0;
}

- (void)
wal_final_row
{
	loading = false;

	if (![self our_shard]) {
		[self free];
		return;
	}
	[self adjust_route];
	[executor wal_final_row];
}

- (void)
enable_local_writes
{
	[self wal_final_row];
}

@end

/// Recovery

@implementation Recovery
- (id)
init
{
	[super init];
	mbox_init(&run_crc_mbox);
	mbox_init(&rt_notify_mbox);

	reader = [[XLogReader alloc] init_recovery:self];
	snap_writer = [[SnapWriter alloc] init_state:self];

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
shard_add:(int)shard_id scn:(i64)scn sop:(const struct shard_op *)sop
{
	assert(sop->ver == 0);
	assert(shard_rt[shard_id].shard == nil);
	Shard<Shard> *shard;
	switch (sop->type) {
	case SHARD_TYPE_PAXOS:
		shard = [Paxos alloc];
		break;
	case SHARD_TYPE_POR:
		shard = [POR alloc];
		break;
	default:
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad shard type");
	}
	if (sop->peer[0][0] == 0)
		shard->dummy = true;
	[shard init_id:shard_id scn:scn sop:sop];
	update_rt(shard_id, shard, NULL);
	return shard;
}

- (Shard<Shard> *)
shard_add_dummy:(const struct row_v12 *)row
{
	i64 scn = 1;
	u32 row_count = 0;
	u32 run_crc = 0;
	if (row) {
		scn = row->scn;
		if (row->len > sizeof(u32) * 2) {
			// when loadding from v11 snapshow row has no row_count & row_crc
			struct tbuf buf = TBUF(row->data, row->len, NULL);
			row_count = read_u32(&buf);
			run_crc = read_u32(&buf);
		}
	}

	struct shard_op op = { .ver = 0,
			       .type = SHARD_TYPE_POR,
			       .row_count = row_count,
			       .run_crc_log = run_crc };
	strncpy(op.mod_name, [default_exec_class name], 16);

	Shard<Shard> *shard = [self shard_add:0 scn:scn sop:&op];
	return shard;
}

- (void)
shard_create:(int)shard_id sop:(struct shard_op *)sop
{
	Shard<Shard> *shard;
	struct wal_reply *reply;
	shard = [self shard_add:shard_id scn:1 sop:sop];
	reply = [writer submit:sop len:sizeof(*sop)
			   tag:shard_create|TAG_SYS shard_id:shard_id];
	if (reply->row_count != 1) {
		[shard free];
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
	}
	[shard wal_final_row];
	assert(shard_rt[shard->id].shard == shard);
}

- (void)
shard_load:(int)shard_id sop:(struct shard_op *)sop
{
	struct shard_op sop_ = *sop;
	sop = &sop_;
	Shard<Shard> *shard;
	assert(shard_rt[shard_id].shard == nil);
	shard = [self shard_add:shard_id scn:-1 sop:sop];
	[shard load_from_remote];
	[shard wal_final_row];
	if ([self shard:shard_id] == nil)
		return;

	extern int allow_snap_overwrite;
	allow_snap_overwrite = 1;
	if ([self fork_and_snapshot] != 0)
		say_error("Can't save snapshot"); // FIXME
	allow_snap_overwrite = 0;
}

- (void)
shard_alter:(Shard<Shard> *)shard sop:(struct shard_op *)sop
{
	if ((sop->type == SHARD_TYPE_PAXOS && [shard class] != [Paxos class]) ||
	    (sop->type == SHARD_TYPE_POR   && [shard class] != [POR class]))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't change shard type");

	struct shard_op *new_sop = [shard snapshot_header];
	for (int i = 0; i < nelem(sop->peer); i++)
		strncpy(new_sop->peer[i], sop->peer[i], 16);
	if ([shard submit:new_sop len:sizeof(*new_sop) tag:shard_alter|TAG_SYS] != 1)
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

	[shard alter_peers:new_sop];
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
	if (shard) {
		if (r->scn >= shard->scn && !shard->dummy)
			return;

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
				shard = [self shard_add_dummy:r];
		case snap_final:
			return;
		case wal_final:
			assert(false);
		case shard_create:
			assert(shard == nil);
			struct shard_op *sop = (struct shard_op *)r->data;
			if (our_shard(sop))
				shard = [self shard_add:r->shard_id scn:r->scn sop:sop];
			else
				say_error("shard %i will be ignored", r->shard_id);
			return;
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
		printf("Failed row: %.*s\n", tbuf_len(out), (char *)out->ptr);

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
		snap_lsn = [snap_dir containg_scn:fold_scn]; /* select snapshot before desired scn */
		[reader load_from_local:0];
		say_error("unable to find record with SCN:%"PRIi64, fold_scn);
		exit(EX_OSFILE);
	}

	i64 local_lsn = [reader load_from_local:0];
	title(NULL);
	return local_lsn;
}

- (int)
load_from_remote
{
	int count = 0;
	struct feeder_param feeder;
	XLogRemoteReader *remote_reader = [[XLogRemoteReader alloc] init_recovery:self];
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

		feeder = (struct feeder_param){ .ver = 1,
						.filter = { .type = FILTER_TYPE_ID } };
		feeder.addr = *peer_addr((*p)->name, PORT_REPLICATION);
		count += [remote_reader load_from_remote:&feeder];
	}
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

static void
validate_cfg()
{
	if (!cfg.peer)
		return;
	assert(cfg.hostname);
}

- (void)
simple:(struct iproto_service *)service
{
	recovery_service = service;
	recovery_iproto_ignore();

	validate_cfg();
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
		[reader local_hot_standby];
		fiber_create("wal_lock", wal_lock, self);

		for (int i = 0; i < MAX_SHARD; i++)
			[[self shard:i] wal_final_row];
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

	recovery_iproto();

	for (int i = 0; i < MAX_SHARD; i++)
		[[self shard:i] enable_local_writes];
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
			id<Shard> shard = [recovery shard:i];
			if (shard == nil)
				continue;
			if ([shard is_replica])
				continue;

			[shard submit:body len:nelem(body) tag:nop|TAG_SYS];
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
#if CFG_object_space
	if (cfg.object_space)
		[self shard_add_dummy:NULL];
#endif
	return [snap_writer snapshot_write];
}

void
fork_and_snapshot(va_list ap __attribute__((unused)))
{
	[recovery fork_and_snapshot];
}

- (int)
fork_and_snapshot
{
	pid_t p;

	assert(fiber != sched);

	if ([self lsn] <= 0) {
		say_error("can't save snapshot: LSN is unknown");
		return -1;
	}

	wlock(&snapshot_lock);
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
		return wait_for_child(p);
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


static void
iproto_shard_cb(struct netmsg_head *wbuf, struct iproto *req, void *arg __attribute__((unused)))
{
	int shard_id = req->shard_id;
	struct shard_op *sop = validate_sop(req->data, req->data_len);

	if (!sop || req->shard_id > nelem(shard_rt)) {
		iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "bad request");
		return;
	}

	struct shard_route *route = shard_rt + shard_id;
	Shard<Shard> *shard = route->shard;

	if (route->proxy) {
		struct iproto_ingress *ingress = container_of(wbuf, struct iproto_ingress, wbuf);
		iproto_proxy_send(route->proxy, ingress, MSG_IPROXY, req, NULL, 0);
		return;
	}

	bool new_master = strncmp(sop->peer[0], cfg.hostname, 16) == 0,
	     old_master = shard && strncmp(shard->peer[0], cfg.hostname, 16) == 0;

	if (!our_shard(sop) || !(new_master || old_master)) {
		iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "not my shard");
		return;
	}

	if (route->shard) {
		if (route->shard->loading) {
			iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "shard is loading");
			return;
		}
		if (shard->dummy) {
			assert(shard_id == 0);
			assert(strcmp(cfg.hostname, sop->peer[0]) == 0);
		}
		[recovery shard_alter:shard sop:sop];
		if (shard->dummy)
			shard->dummy = false;
	} else {
		[recovery shard_create:shard_id sop:sop];

	}

	iproto_reply_small(wbuf, req, ERR_CODE_OK);
}

static void
iproto_shard_load_aux(va_list ap)
{
	int shard_id = va_arg(ap, int);
	struct shard_op *sop = va_arg(ap, struct shard_op *);

	void *tmp = palloc(fiber->pool, sizeof(*sop));
	memcpy(tmp, sop, sizeof(*sop));

	@try {
		[recovery shard_load:shard_id sop:tmp];
	}
	@catch (Error *e) {
		say_error("Failed to load shard, [%s reason:\"%s\"] at %s:%d",
			 [[e class] name], e->reason, e->file, e->line);
		[e release];
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
	struct shard_op *sop = validate_sop(req->data + 16 + sizeof(i64),
					    req->data_len - 16 - sizeof(i64));
	if (sop == NULL)
		return;

	if (shard && shard->loading) {
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
	if (ev_now() - sop->tm > 4 && scn > [shard scn] &&
	    strcmp(shard->peer[0], sop->peer[0]) != 0)
	{
		say_warn("route update from %s: shard %i master %s, FIXME ignoring alter request",
			 peer_name, shard_id, sop->peer[0]);
		return;
	}
}

const char *
iproto_shard_luacb(struct iproto *req __attribute__((unused)))
{
	return "error";
}

static void
recovery_iproto(void)
{
	if (cfg.peer && *cfg.peer && cfg.hostname) {
		fiber_create("route_recv", udp_server,
			     recovery_service->addr, iproto_shard_udpcb, NULL, NULL);
		fiber_create("udpate_rt_notify", update_rt_notify);
	}
	service_register_iproto(recovery_service, MSG_SHARD, iproto_shard_cb, IPROTO_LOCAL);
	paxos_service(recovery_service);
}

static void
iproto_ignore(struct netmsg_head *h __attribute__((unused)),
	      struct iproto *r __attribute__((unused)),
	      void *arg __attribute__((unused)))
{
}

static void
recovery_iproto_ignore()
{
	service_register_iproto(recovery_service, MSG_SHARD, iproto_ignore, IPROTO_NONBLOCK);
	service_register_iproto(recovery_service, LEADER_PROPOSE, iproto_ignore, IPROTO_NONBLOCK);
	service_register_iproto(recovery_service, PREPARE, iproto_ignore, IPROTO_NONBLOCK);
	service_register_iproto(recovery_service, ACCEPT, iproto_ignore, IPROTO_NONBLOCK);
	service_register_iproto(recovery_service, DECIDE, iproto_ignore, IPROTO_NONBLOCK);
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


static void
hexdump(struct tbuf *out, u16 tag __attribute__((unused)), struct tbuf *row)
{
	tbuf_printf(out, "%s", tbuf_to_hex(row));
}

void
print_row(struct tbuf *buf, const struct row_v12 *row,
	  void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row))
{
	struct tbuf row_data = TBUF(row->data, row->len, fiber->pool);

	int tag = row->tag & TAG_MASK;
	int tag_type = row->tag & ~TAG_MASK;
	int inner_tag;
	u64 ballot;
	u32 value_len;
	bool print_shard_id = getenv("PRINT_SHARD_ID") != NULL;

	tbuf_printf(buf, "lsn:%" PRIi64, row->lsn);
	if (print_shard_id && row->scn != -1)
		tbuf_printf(buf, " shard:%i", row->shard_id);

	tbuf_printf(buf, " scn:%" PRIi64 " tm:%.3f t:%s ",
		    row->scn, row->tm,
		    xlog_tag_to_a(row->tag));

	if (!print_shard_id)
		tbuf_printf(buf, "%s ", sintoa((void *)&row->cookie));

	if (!handler)
		handler = hexdump;

	if (tag_type != TAG_SYS) {
		handler(buf, row->tag, &TBUF(row->data, row->len, fiber->pool));
		return;
	}

	switch (tag) {
	case snap_initial:
		if (tbuf_len(&row_data) == sizeof(u32) * 3) {
			u32 count = read_u32(&row_data);
			u32 log = read_u32(&row_data);
			u32 mod = read_u32(&row_data);
			tbuf_printf(buf, "count:%u run_crc_log:0x%08x run_crc_mod:0x%08x",
				    count, log, mod);
		} else if (row->scn == -1) {
			u8 ver = read_u8(&row_data);
			u32 count = read_u32(&row_data);
			u32 flags = read_u32(&row_data);
			tbuf_printf(buf, "ver:%i count:%u flags:0x%08x", ver, count, flags);
		} else {
			tbuf_printf(buf, "unknow format");
		}

		break;
	case run_crc: {
		i64 scn = -1;
		if (tbuf_len(&row_data) == sizeof(i64) + 2 * sizeof(u32))
			scn = read_u64(&row_data);
		u32 log = read_u32(&row_data);
		(void)read_u32(&row_data); /* ignore run_crc_mod */
		tbuf_printf(buf, "SCN:%"PRIi64 " log:0x%08x", scn, log);
		break;
	}
	case shard_alter:
	case shard_create: {
		int ver = read_u8(&row_data);
		if (ver != 0) {
			tbuf_printf(buf, "unknow version: %i", ver);
			break;
		}

		int type = read_u8(&row_data);
		i64 tm = read_u64(&row_data);
		u32 estimated_row_count = read_u32(&row_data);
		u32 run_crc = read_u32(&row_data);
		const char *mod_name = read_bytes(&row_data, 16);

		switch (tag & TAG_MASK) {
		case shard_create: tbuf_printf(buf, "SHARD_CREATE"); break;
		case shard_alter: tbuf_printf(buf, "SHARD_ALTER"); break;
		default: assert(false);
		}
		tbuf_printf(buf, " shard_id:%i  tm:%"PRIi64" %s %s",
			    row->shard_id, tm, type == 0 ? "POR" : "PAXOS", mod_name);
		tbuf_printf(buf, " count:%i run_crc:0x%08x", estimated_row_count, run_crc);
		tbuf_printf(buf, " master:%s", (const char *)read_bytes(&row_data, 16));
		while (tbuf_len(&row_data) > 0) {
			const char *str = read_bytes(&row_data, 16);
			if (strlen(str))
			    tbuf_printf(buf, " repl:%s", str);
		}

		break;
	}
	case snap_final:
	case nop:
		break;

	case paxos_promise:
	case paxos_nop:
		ballot = read_u64(&row_data);
		tbuf_printf(buf, "ballot:%"PRIx64, ballot);
		break;
	case paxos_accept:
		ballot = read_u64(&row_data);
		inner_tag = read_u16(&row_data);
		value_len = read_u32(&row_data);
		(void)value_len;
		assert(value_len == tbuf_len(&row_data));
		tbuf_printf(buf, "ballot:%"PRIx64" it:%s ", ballot, xlog_tag_to_a(inner_tag));

		switch(inner_tag & TAG_MASK) {
		case run_crc: {
			i64 scn = read_u64(&row_data);
			u32 log = read_u32(&row_data);
			(void)read_u32(&row_data); /* ignore run_crc_mod */
			tbuf_printf(buf, "SCN:%"PRIi64 " log:0x%08x", scn, log);
			break;
		}
		case nop:
			break;
		default:
			handler(buf, inner_tag, &row_data);
			break;
		}
		break;
	default:
		handler(buf, row->tag, &TBUF(row->data, row->len, fiber->pool));
	}
}


register_source();
