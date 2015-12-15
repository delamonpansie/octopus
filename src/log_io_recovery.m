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

Class paxos = Nil;

i64 fold_scn = 0;

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

static struct octopus_cfg_peer *
cfg_peer_by_name(const char *name)
{
	for (struct octopus_cfg_peer **c = cfg.peer; *c; c++)
		if (strcmp(name, (*c)->name) == 0)
			return *c;
	return NULL;
}

const struct sockaddr_in *
shard_addr(const char *name, enum port_type port_type)
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
shard_feeder(const char *name, struct feeder_param *feeder)
{
	*feeder = (struct feeder_param){ .ver = 1,
					 .filter = { .type = FILTER_TYPE_ID } };
	memcpy(&feeder->addr, shard_addr(name, PORT_REPLICATION), sizeof(feeder->addr));
}

void
update_rt(int shard_id, enum shard_mode mode, Shard<Shard> *shard, const char *master_name)
{
	// FIXME: do a broadcast after change local destinations
	struct shard_route *route = &shard_rt[shard_id];
	static struct palloc_pool *proxy_pool;
	if (!proxy_pool)
		proxy_pool = palloc_create_pool((struct palloc_config){.name = "proxy_to_primary"});

	const struct sockaddr_in *addr = NULL;
	if (master_name) {
		addr = shard_addr(master_name, PORT_PRIMARY);
		if (!addr) {
			say_error("Unknown peer %s, ignoring shard %i route update", master_name, shard_id);
			return;
		}
	}

	route->shard = shard;
	route->executor = [shard executor];
	route->mode = mode;
	route->proxy = NULL;

	if (mode == SHARD_MODE_PARTIAL_PROXY || mode == SHARD_MODE_PROXY) {
		if (!master_name) {
			assert(shard->dummy);
			return;
		}
		assert(master_name);
		if (route->proxy && strcmp(route->proxy->ts.name, master_name) == 0) /* already proxying */
			return;
		route->proxy = iproto_remote_add_peer(NULL, addr, proxy_pool);
		route->proxy->ts.name = master_name;
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

	int cnt = 0;
	static struct sockaddr_in peer_addr[16];
	for (struct octopus_cfg_peer **p = cfg.peer; *p; p++) {
		if (strcmp((*p)->name, cfg.hostname) == 0)
			continue;
		if (cnt == nelem(peer_addr)) {
			say_error("to many peers");
			break;
		}
		const struct sockaddr_in *addr = shard_addr((*p)->name, PORT_PRIMARY);
		if (!addr || addr->sin_family == AF_UNSPEC) {
			say_error("bad peer addr %s", (*p)->name);
			continue;
		}
		peer_addr[cnt++] = *addr;
	}

	for (;;) {
		fiber_sleep(1);

		for (int i = 0; i < nelem(shard_rt); i++) {
			if (shard_rt[i].mode != SHARD_MODE_LOCAL &&
			    shard_rt[i].mode != SHARD_MODE_PARTIAL_PROXY)
				continue;

			id<Shard> shard = shard_rt[i].shard;
			char hostname[16];
			strncpy(hostname, cfg.hostname, 16);
			i64 scn = [shard scn];
			struct shard_op *sop = [shard snapshot_header];
			sop->op |= 0x80; /* mark as rt update */
			struct iproto header = { .msg_code = MSG_SHARD,
						 .shard_id = [shard id],
						 .data_len = 16 + sizeof(scn) + sizeof(*sop)};
			struct iovec iov[] = { { &header, sizeof(header) },
					       { hostname, 16 },
					       { &scn, sizeof(scn) },
					       { sop, sizeof(*sop) } };
			struct msghdr msg = (struct msghdr){
				.msg_namelen = sizeof(peer_addr[0]),
				.msg_iov = iov,
				.msg_iovlen = nelem(iov)
			};
			for (int j = 0; j < cnt; j++) {
				msg.msg_name = &peer_addr[j];
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

	if (strlen(sop->peer[0]) == 0) {
		say_syserror("sop: empty master");
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
	int op = sop->op & ~ 0xc0;
	if (op != 0 && op != 1) {
		say_error("sop: invalid op 0x%02x", sop->op);
		return NULL;
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
shard_log(const char *msg, Shard *shard)
{
	struct shard_route *route = &shard_rt[shard->id];
	struct tbuf *buf = tbuf_alloc(fiber->pool);
	tbuf_printf(buf, "shard:%i ", shard->id);

	tbuf_printf(buf, "SCN:%"PRIi64" %s %s ", [shard scn],
		    [[shard class] name], [[(id)[shard executor] class] name]);

	tbuf_printf(buf, "peer:{%s", shard->peer[0]);
	for (int i = 1; i < 5; i++)
		if (shard->peer[i][0])
			tbuf_printf(buf, ", %s", shard->peer[i]);
	tbuf_printf(buf, "} ");

	switch (route->mode) {
	case SHARD_MODE_NONE:
		tbuf_printf(buf, "NONE");
		break;
	case SHARD_MODE_LOADING:
		tbuf_printf(buf, "LOADING");
		break;
	case SHARD_MODE_PARTIAL_PROXY:
		tbuf_printf(buf, "PARTIAL_");
	case SHARD_MODE_PROXY:
		tbuf_printf(buf, "PROXY");
		if (route->proxy)
			tbuf_printf(buf, ": %s,%s",
				    route->proxy->ts.name, sintoa(&route->proxy->ts.daddr));
		else
			tbuf_printf(buf, ": legacy mode");
		break;
	case SHARD_MODE_LOCAL:
		tbuf_printf(buf, "LOCAL");
		break;
	}
	tbuf_printf(buf, " => %s", msg);
	say_info("META %s", (char *)buf->ptr);
}


- (id) free
{
	[(id)executor free];
	update_rt(self->id, SHARD_MODE_NONE, nil, NULL);
	return [super free];
}

- (id) init_id:(int)shard_id
	   scn:(i64)scn_
      recovery:(Recovery *)recovery_
	   sop:(const struct shard_op *)sop
{
	[super init];
	self->id = shard_id;
	recovery = recovery_;
	scn = scn_;
	run_crc_log = sop->run_crc_log;

	for (int i = 0; i < nelem(sop->peer); i++) {
		if (strlen(sop->peer[i]) == 0)
			break;

		strncpy(peer[i], sop->peer[i], 16); // FIXME: это данные из сети

		assert(cfg.hostname); // cfg.hostname может быть пустым только при создании dummy шарда.
	}

	assert(dummy || our_shard(sop));
	if (objc_lookUpClass(sop->mod_name) == Nil)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad mod name");

	executor = [[objc_lookUpClass(sop->mod_name) alloc] init_shard:self];

	shard_log("init", self);
	return self;
}

- (void)
adjust_route
{
	abort();
}

- (void)
reload_from:(const char *)name
{
	(void)name;
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
				.op = 0,
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
			  shard:self tag:shard_tag|TAG_SYS];
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

	if (strcmp(buf, status_buf) == 0 && shard_rt[self->id].mode == old_mode)
		return;

	shard_log(buf, self);

	strncpy(status_buf, buf, sizeof(status_buf));
	title(NULL);

	if (shard_rt[self->id].mode == old_mode)
		return;

	old_mode = shard_rt[self->id].mode;
	[executor status_changed];
}

- (void)
wal_final_row
{
	[self adjust_route];
	[executor wal_final_row];
}

@end

/// Recovery

@implementation Recovery
- (id)
init
{
	[super init];
	mbox_init(&run_crc_mbox);
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
	assert(sop->ver == 0 && (sop->op == 0 || sop->op == 0x80));
	assert(shard_rt[shard_id].shard == nil);
	Shard<Shard> *shard;
	switch (sop->type) {
	case SHARD_TYPE_PAXOS:
		shard = [paxos alloc];
		break;
	case SHARD_TYPE_POR:
		shard = [POR alloc];
		break;
	default:
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad shard type");
	}
	if (sop->peer[0][0] == 0)
		shard->dummy = true;
	[shard init_id:shard_id scn:scn recovery:self sop:sop];
	update_rt(shard_id, SHARD_MODE_LOADING, shard, NULL);
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
			       .op = 0,
			       .type = SHARD_TYPE_POR,
			       .row_count = row_count,
			       .run_crc_log = run_crc };
	strncpy(op.mod_name, [default_exec_class name], 16);

	return [self shard_add:0 scn:scn sop:&op];
}

- (void)
shard_create:(int)shard_id sop:(struct shard_op *)sop
{
	Shard<Shard> *shard;
	struct wal_reply *reply;
	shard = [self shard_add:shard_id scn:1 sop:sop];
	reply = [writer submit:sop len:sizeof(*sop)
			   tag:shard_tag|TAG_SYS shard_id:shard_id];
	if (reply->row_count != 1) {
		[shard free];
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
	}
	[shard adjust_route];
	assert(shard_rt[shard->id].shard == shard);
}

- (void)
shard_load:(int)shard_id sop:(struct shard_op *)sop
{
	struct shard_op sop_ = *sop;
	sop = &sop_;
	Shard<Shard> *shard;
	struct wal_reply *reply;
	assert(shard_rt[shard_id].shard == nil);
	shard = [self shard_add:shard_id scn:-1 sop:sop];
	[shard load_from_remote];
	reply = [writer submit:sop len:sizeof(*sop)
			   tag:shard_tag|TAG_SYS shard_id:shard->id];
	if (reply->row_count != 1 ||
	    [self fork_and_snapshot] != 0)
	{
		[shard free];
		return;
	}
	[shard adjust_route];
}

- (void)
shard_alter:(Shard<Shard> *)shard sop:(struct shard_op *)sop
{
	struct shard_op *new_sop = [shard snapshot_header];
	for (int i = 0; i < nelem(sop->peer); i++)
		strncpy(new_sop->peer[i], sop->peer[i], 16);
	if ([shard submit:new_sop len:sizeof(*new_sop) tag:shard_tag|TAG_SYS] != 1)
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

	[shard alter_peers:sop];
}

- (void)
recover_row:(struct row_v12 *)r
{
	assert(r->shard_id < nelem(shard_rt));
	id<Shard> shard;
	if (shard_rt[0].shard && shard_rt[0].shard->dummy)
		shard = shard_rt[0].shard;
	else
		shard = shard_rt[r->shard_id].shard;

	@try {
		say_debug("%s: LSN:%"PRIi64" SCN:%"PRIi64" tag:%s",
			  __func__, r->lsn, r->scn, xlog_tag_to_a(r->tag));
		if (r->len)
			say_debug2("	%s", tbuf_to_hex(&TBUF(r->data, r->len, fiber->pool)));

		if (unlikely((r->tag & ~TAG_MASK) == TAG_SYS)) {
			int tag = r->tag & TAG_MASK;
			switch (tag) {
			case snap_initial:
				if (r->scn != -1) /* no sharding */
					shard = [self shard_add_dummy:r];
			case snap_final:
				return;
			case shard_tag:
				if (shard == nil) {
					struct shard_op *sop = (struct shard_op *)r->data;
					shard = [self shard_add:r->shard_id scn:r->scn sop:sop];
				}
				break;
			default:
				break;
			}
		}

		if (unlikely(r->tag & TAG_MASK) == wal_final)
			return;

		if (unlikely(shard == nil))
			raise_fmt("shard %i is not configured", r->shard_id);

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

	/* loading is faster until wal_final_row called because service is not yet initialized and
	   only pk indexes must be updated. remote feeder will send wal_final_row then all remote
	   rows are read */

	for (int i = 0; i < MAX_SHARD; i++) {
		Shard *shard = shard_rt[i].shard;
		if (shard != nil && (shard->dummy || strcmp(cfg.hostname, shard->peer[0]) == 0))
			[shard wal_final_row];
	}
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
		memcpy(&feeder.addr, shard_addr((*p)->name, PORT_REPLICATION), sizeof(feeder.addr));
		count = [remote_reader load_from_remote:&feeder];
		if (count >= 0)
			break;
	}
	[remote_reader free];
	return count;
}

void
wal_lock(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);

	while ([wal_dir lock] != 0)
		fiber_sleep(1);

	[r enable_local_writes];
}

static void
validate_cfg()
{
	if (!cfg.peer)
		return;
	assert(cfg.hostname);
}

- (void)
simple
{
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

	for (int i = 0; i < MAX_SHARD; i++)
		[[self shard:i] adjust_route];

	if (cfg.peer && *cfg.peer && cfg.hostname)
		fiber_create("udpate_rt_notify", update_rt_notify);
}

static void
run_crc_writer(va_list ap)
{
	Recovery *recovery = va_arg(ap, Recovery *);
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
	Recovery *recovery = va_arg(ap, Recovery *);
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

	if (!cfg.io_compat && cfg.run_crc_delay > 0)
		fiber_create("run_crc", run_crc_writer, self, cfg.run_crc_delay);

	if (!cfg.io_compat && cfg.nop_hb_delay > 0)
		fiber_create("nop_hb", nop_hb_writer, self, cfg.nop_hb_delay);
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
	extern Recovery *recovery;
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
		close_all_xcpt(2, stderrfd, sayfd);

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
iproto_shard_cb_aux(va_list ap)
{
	int shard_id = va_arg(ap, int);
	struct shard_route *route = va_arg(ap, struct shard_route *);
	struct shard_op *sop = va_arg(ap, struct shard_op *);
	Shard<Shard> *shard = route->shard;

	void *tmp = palloc(fiber->pool, sizeof(*sop));
	memcpy(tmp, sop, sizeof(*sop));
	sop = tmp;

	extern Recovery *recovery;
	switch (route->mode) {
	case SHARD_MODE_LOCAL:
		if (shard->dummy) {
			assert(shard_id == 0);
			assert(strcmp(cfg.hostname, sop->peer[0]) == 0);
		}
		[recovery shard_alter:shard sop:sop];
		if (shard->dummy)
			shard->dummy = false;
		break;
	case SHARD_MODE_NONE:
		[recovery shard_create:shard_id sop:sop];
		break;
	case SHARD_MODE_PROXY:
	case SHARD_MODE_PARTIAL_PROXY:
	case SHARD_MODE_LOADING:
		abort();
	}
}

static void
iproto_shard_cb(struct netmsg_head *wbuf, struct iproto *req, void *arg __attribute__((unused)))
{
	int shard_id = req->shard_id;
	struct shard_route *route = &shard_rt[shard_id];
	Shard<Shard> *shard = route->shard;
	struct shard_op *sop = validate_sop(req->data, req->data_len);

	if (!sop || (sop->op & 0x80)) {
		iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "bad request");
		return;
	}
	if (route->mode == SHARD_MODE_PROXY || route->mode == SHARD_MODE_PARTIAL_PROXY) {
		if (sop->op & 0x40) {
			iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "route loop");
		} else {
			sop->op |= 0x40;
			struct iproto_ingress *ingress = container_of(wbuf, struct iproto_ingress, wbuf);
			iproto_proxy_send(route->proxy, ingress, req, NULL, 0);
		}
		return;
	}
	bool new_master = strncmp(sop->peer[0], cfg.hostname, 16) == 0,
	     old_master = shard && strncmp(shard->peer[0], cfg.hostname, 16) == 0;

	if (!our_shard(sop) || !(new_master || old_master)) {
		iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "not my shard");
		return;
	}

	switch (route->mode) {
	case SHARD_MODE_LOADING:
		iproto_error(wbuf, req, ERR_CODE_ILLEGAL_PARAMS, "shard is loading");
		return;
	case SHARD_MODE_PARTIAL_PROXY:
	case SHARD_MODE_PROXY:
		assert(false);
	case SHARD_MODE_NONE:
	case SHARD_MODE_LOCAL:
		break;
	}
	sop->op &= ~0x40;

	say_warn("route %p, mode %i", route, route->mode);
	fiber_create("load/alter shard", iproto_shard_cb_aux, shard_id, route, sop);
	while (route->mode == SHARD_MODE_NONE || route->mode == SHARD_MODE_LOADING)
		fiber_sleep(0.1);
	iproto_reply_small(wbuf, req, ERR_CODE_OK);
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
	if (sop == NULL || (sop->op & 0x80) == 0) // bad packet or not rt_update
		return;

	if (route->mode == SHARD_MODE_LOADING) {
		say_debug("ignore route update, shard loading");
		return;
	}

	if (!our_shard(sop)) {
		assert (shard == nil || !shard->dummy);

		[shard free];
		update_rt(shard_id, SHARD_MODE_PROXY, nil, sop->peer[0]);
		return;
	}

	if (shard == nil || ev_now() - sop->tm > 4) {
		if (shard) {
			if (scn < [shard scn]) // ignore stale update
				return;

			// FIXME: what to do if shard is as (stale) master ?
			[shard reload_from:peer_name];
		} else {
			assert(route->mode == SHARD_MODE_NONE);
			fiber_create("load/alter shard", iproto_shard_cb_aux, shard_id, route, sop);
		}
	}
}

const char *
iproto_shard_luacb(struct iproto *req __attribute__((unused)))
{
	return "error";
}

+ (void)
service:(struct iproto_service *)s
{
	if (cfg.wal_writer_inbox_size == 0)
		return;
	fiber_create("route_recv", udp_server, s->addr, iproto_shard_udpcb, NULL, NULL);
	service_register_iproto(s, MSG_SHARD, iproto_shard_cb, 0);
	paxos = objc_lookUpClass("Paxos");
	[paxos service:s];
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
	case shard_tag: {
		int ver = read_u8(&row_data);
		if (ver != 0) {
			tbuf_printf(buf, "unknow version: %i", ver);
			break;
		}

		int op = read_u8(&row_data);
		int type = read_u8(&row_data);
		i64 tm = read_u64(&row_data);
		u32 estimated_row_count = read_u32(&row_data);
		u32 run_crc = read_u32(&row_data);
		const char *mod_name = read_bytes(&row_data, 16);

		switch (op) {
		case 0: tbuf_printf(buf, "SHARD_CREATE"); break;
		case 1: tbuf_printf(buf, "SHARD_ALTER"); break;
		case 0x80 :tbuf_printf(buf, "SHARD_LOAD"); break;
		default: tbuf_printf(buf, "UNKNOWN"); break;
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
