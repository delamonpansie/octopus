/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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
#import <log_io.h>
#import <shard.h>
#import <say.h>
#import <tbuf.h>
#import <paxos.h>
#import <cfg/defs.h>


@implementation Shard
- (int) id { return self->id; }
- (i64) scn { return scn; }
- (id<Executor>) executor { return executor; }
- (ev_tstamp) lag { return lag; }
- (ev_tstamp) last_update_tstamp { return last_update_tstamp; }

- (const char *) name { return [[self class] name]; }
- (const char *) status { return status_buf; }
- (ev_tstamp) run_crc_lag { return run_crc_lag(&run_crc_state); }
- (const char *) run_crc_status { return run_crc_status(&run_crc_state); }
- (u32) run_crc_log { return run_crc_log; }

- (id) retain { rc++; return self; }
- (void) release { if (--rc == 0) [self free]; }

static void
shard_info(Shard *shard, struct tbuf *buf)
{
	tbuf_printf(buf, "SCN: %"PRIi64", type: %s, ", [shard scn], [shard name]);
	tbuf_printf(buf, "peer: [%s", shard->peer[0]);
	for (int i = 1; i < nelem(shard->peer) && shard->peer[i][0]; i++)
		tbuf_printf(buf, ", %s", shard->peer[i]);
	tbuf_printf(buf, "]");
}

void
route_info(const struct shard_route *route, struct tbuf *buf)
{
	tbuf_printf(buf, "mode: ");
	if (route->shard == nil && route->proxy == nil) {
		tbuf_printf(buf, "NONE");
	} else if (route->shard && route->shard->loading) {
		tbuf_printf(buf, "LOADING");
	} else if (route->shard && route->shard->dummy) {
		tbuf_printf(buf, "LEGACY");
	} else if (route->shard && route->shard->executor == nil) {
		tbuf_printf(buf, "INVALID");
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
		tbuf_printf(buf, ", proxy_addr: '%s'", net_sin_name(&route->proxy->ts.daddr));
}

void
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

- (id)
free
{
	[(id)executor free];
	if (shard_rt[self->id].shard == self) {
		const char *master = peer[0];
		if (strcmp(master, cfg.hostname) == 0)
			master = NULL;
		update_rt(self->id, nil, master);
		shard_log("removed", self->id);
	}
	return [super free];
}

- (void)
set_executor:(id)executor_
{
	executor = executor_;
	[executor set_shard:self];
}

- (id)
init_id:(int)shard_id scn:(i64)scn_ sop:(const struct shard_op *)sop
{
	[super init];
	rc = 1;
	loading = true;
	self->id = shard_id;
	scn = scn_;
	run_crc_log = sop->run_crc_log;
	type = sop->type;

	for (int i = 0; i < nelem(sop->peer); i++) {
		if (strlen(sop->peer[i]) == 0)
			break;

		strncpy(peer[i], sop->peer[i], 16); // FIXME: это данные из сети

		assert(self->dummy || cfg.hostname); // cfg.hostname может быть пустым только при создании dummy шарда.
	}

	if (objc_lookUpClass(sop->mod_name) == Nil)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "bad mod name '%s'", sop->mod_name);

	return self;
}

- (void)
adjust_route
{
	abort();
}

- (void)
alter:(struct shard_op *)sop
{
	for (int i = 0; i < nelem(sop->peer); i++)
		strncpy(peer[i], sop->peer[i], 16);
	if (!loading)
		[self adjust_route];
}

- (struct shard_op *)
snapshot_header
{
	static struct shard_op op;
	op = (struct shard_op){ .ver = 0,
				.type = type,
				.row_count = [executor snapshot_estimate],
				.run_crc_log = run_crc_log };
	strncpy(op.mod_name, [[(id)executor class] name], 15);
	for (int i = 0; i < nelem(peer) && peer[i]; i++)
		strncpy(op.peer[i], peer[i], 15);
	return &op;
}

- (const struct row_v12 *)
snapshot_write_header:(XLog *)snap
{
	struct shard_op *sop = [self snapshot_header];
	return [snap append_row:sop len:sizeof(*sop)
			  shard:self tag:shard_create];
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
update_run_crc:(const struct wal_reply *)reply
{
	for (int i = 0; i < reply->crc_count; i++) {
		run_crc_log = reply->row_crc[i].value;
		run_crc_record(&run_crc_state, reply->row_crc[i]);
	}
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
	status_buf[sizeof(status_buf)-1] = '\0';
	title(NULL);

	[executor status_changed];
}

- (bool)
our_shard
{
	if (dummy)
		return 1;
	for (int i = 0; i < nelem(peer); i++)
		if (strcmp(peer[i], cfg.hostname) == 0)
			return 1;
	return 0;
}

- (void)
wal_final_row
{
	if (loading) {
		loading = false;

		if (![self our_shard]) {
			[self release];
			return;
		}
		[self adjust_route];
		[executor wal_final_row];
		shard_log("wal_final_row", self->id);
	}
}

- (void)
enable_local_writes
{
	[self wal_final_row];
}

- (void)
fill_feeder_param:(struct feeder_param *)feeder peer:(int)i
{
	void *filter_arg = feeder->filter.arg;
	*feeder = (struct feeder_param){
		.ver = 2,
		.addr = *peer_addr(peer[i], PORT_REPLICATION),
		.filter = {.type = FILTER_TYPE_C,
			   .name = "shard",
			   .arg = filter_arg,
			   .arglen = 1 + sprintf(filter_arg, "%i", self->id) }
	};
}

- (i64)
handshake_scn
{
	return scn + 1;
}

- (void)
load_from_remote
{
	char feeder_param_arg[16];
	struct feeder_param feeder = { .filter = { .arg = feeder_param_arg } };
	XLogRemoteReader *reader = [[XLogRemoteReader alloc] init_recovery:(id)self];
	[self fill_feeder_param:&feeder peer:0];
	int ret = [reader load_from_remote:&feeder];
	[reader free];
	if (ret < 0) {
		[(id)executor free];
		executor = nil;
	}
}

- (int)
prepare_remote_row:(struct row_v12 *)row offt:(int)offt
{
	(void)row;
	(void)offt;
	return 1;
}
@end


@implementation DefaultExecutor
- (id)
init
{
	return self;
}

- (void)
set_shard:(Shard<Shard> *)shard_
{
	shard = shard_;
}

- (u32)
snapshot_estimate
{
	return 0;
}
- (void)
wal_final_row
{
}

- (void)
status_changed
{
}

- (void)
print:(const struct row_v12 *)row into:(struct tbuf *)buf
{
	tbuf_printf(buf, "%s: row=%p", __func__, row);
}
@end

register_source();
