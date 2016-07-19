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
#import <fiber.h>
#import <say.h>
#import <pickle.h>
#import <tbuf.h>
#import <shard.h>

#include <third_party/crc32.h>
#include <string.h>

@implementation POR
- (id)
init_id:(int)shard_id
    scn:(i64)scn_
    sop:(const struct shard_op *)sop
{
	[super init_id:shard_id scn:scn_ sop:sop];
	feeder.filter.arg = feeder_param_arg;
	if (type == SHARD_TYPE_PART)
		partial_replica = true;
	return self;
}

- (const char *)
name
{
	if (partial_replica)
		return "POR/PART";
	else
		return "POR";
}

- (id) free
{
	if (remote)
		[remote abort_and_free];
	return [super free];
}

- (const struct row_v12 *)
snapshot_write_header:(XLog *)snap
{
	struct shard_op *sop = [self snapshot_header];
	struct row_v12 row = { .scn = scn,
				.tm = ev_now(),
				.tag = shard_create|TAG_SYS,
				.shard_id = self->id,
			       .len = sizeof(*sop) };
	if (partial_replica) {
		assert(sop->type == SHARD_TYPE_PART);
		memcpy(row.remote_scn, &remote_scn, 6);
	}
	return [snap append_row:&row data:sop];
}

- (i64)
handshake_scn
{
	if (partial_replica) {
		assert(remote_scn > 0);
		return remote_scn + 1;
	}
	return [super handshake_scn];
}


- (void)
set_feeder:(struct feeder_param*)new
{
	/* legacy */
	if (shard_rt[self->id].shard && shard_rt[self->id].proxy)
		[remote set_feeder:new];
}

- (void)
fill_feeder_param:(struct feeder_param *)param peer:(int)i
{

	[super fill_feeder_param:param peer:i];
	if (partial_replica) {
		assert(i == 0); /* only master has valid SCN */
		param->filter.type = FILTER_TYPE_LUA;
		param->filter.name = "partial";
	}
}

- (bool)
master
{
	if (dummy) {
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		return fid_err || feeder.addr.sin_family == AF_UNSPEC;
	}
	return strcmp(cfg.hostname, peer[0]) == 0;
}

- (void)
set_remote_scn:(const struct row_v12 *)row
{
	memcpy(&remote_scn, row->remote_scn, 6);
}

static void partial_replica_load(va_list ap);

- (void)
remote_hot_standby
{
	assert(![self master]);

	if (partial_replica && remote_scn == 0) {
		fiber_create("replica_load", partial_replica_load, self);
		return;
	}

	if (dummy) {
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		if (fid_err) panic("wrong feeder conf");
	} else {
		[self fill_feeder_param:&feeder peer:0];
	}
	if (remote == nil) {
		[self status_update:"hot_standby/%s/init", net_sin_name(&feeder.addr)];
		remote = [[XLogReplica alloc] init_shard:self];
		[remote hot_standby:&feeder];
	} else {
		[remote set_feeder:&feeder];
	}
}

static void
partial_replica_load(va_list ap)
{
	POR *shard = va_arg(ap, POR *);
	shard->partial_replica_loading = true;
	[shard load_from_remote];
	shard->partial_replica_loading = false;
	if (shard->remote_scn == 0) {
		[(id)shard->executor free];
		shard->executor = nil;
	}
	if (shard->executor != nil) {
		[recovery fork_and_snapshot];
		[shard remote_hot_standby];
	}
}

- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	static unsigned count;
	static struct msg_void_ptr msg;

	if (shard_rt[self->id].proxy) {
		say_warn("not master");
		return 0;
	}

	if (recovery->writer == nil) {
		say_warn("local writes disabled");
		return 0;
	}

	if (++count % 32 == 0 && msg.link.tqe_prev == NULL)
	 	mbox_put(&recovery->run_crc_mbox, &msg, link);

	struct wal_reply *reply = [recovery->writer submit:data len:len tag:tag shard_id:self->id];
	if (reply->row_count) {
		scn = reply->scn;
		[self update_run_crc:reply];
	}
	return reply->row_count;
}


- (void)
adjust_route
{
	if ([self master]) {
		update_rt(self->id, self, NULL);
		[self status_update:"primary"];
		struct feeder_param empty = { .addr = { .sin_family = AF_UNSPEC } };
		[remote set_feeder:&empty];
	} else {
		update_rt(self->id, self, dummy ? "<dummy_addr>" : peer[0]);
		if (recovery->writer)
			[self remote_hot_standby];
		else
			[self status_update:"hot_standby/local"];
	}
}

- (void)
enable_local_writes
{
	if ([self master])
		[self wal_final_row];
	else
		[self remote_hot_standby];
}

- (void)
recover_row:(struct row_v12 *)row
{
	assert(row->scn <= 0 ||
	       (row->tag & TAG_MASK) == snap_initial ||
	       (row->tag & TAG_MASK) == snap_final ||
	       row->shard_id == self->id);
	int old_ushard = fiber->ushard;
	fiber->ushard = self->id;

	if (!partial_replica_loading) {
		if (unlikely(cfg.sync_scn_with_lsn && dummy && row->lsn != row->scn))
			panic("LSN != SCN : %"PRIi64 " != %"PRIi64, row->lsn, row->scn);

		if (unlikely(row->scn - scn != 1 && (row->tag & ~TAG_MASK) == TAG_WAL &&
			     cfg.panic_on_scn_gap))
			panic("SCN sequence has gap after %"PRIi64 " -> %"PRIi64, scn, row->scn);


		// calculate run_crc _before_ calling executor: it may change row
		if (scn_changer(row->tag))
			run_crc_calc(&run_crc_log, row->tag, row->data, row->len);
	}

	switch (row->tag & TAG_MASK) {
	case snap_initial:
	case snap_final:
		if (dummy || partial_replica)
			snap_loaded = true;
		break;
	case run_crc:
		if (cfg.ignore_run_crc || partial_replica)
			break;

		if (row->len != sizeof(i64) + sizeof(u32) * 2)
			break;

		run_crc_verify(&run_crc_state, &TBUF(row->data, row->len, NULL));
		break;
	case nop:
		break;
	case wal_final:
		assert(false);
	case shard_create:
		break;
	case shard_alter:
		[self alter:(struct shard_op *)row->data];
		if (!loading && ![self our_shard]) {
			[self free];
			fiber->ushard = old_ushard;
			return;
		}
		break;
	case shard_final:
		snap_loaded = true;
		break;
	default:
		[executor apply:&TBUF(row->data, row->len, fiber->pool) tag:row->tag];
		break;
	}

	if (!partial_replica_loading) {
		last_update_tstamp = ev_now();
		lag = last_update_tstamp - row->tm;

		if (scn_changer(row->tag)) {
			run_crc_record(&run_crc_state, (struct run_crc_hist){ .scn = row->scn, .value = run_crc_log });
			scn = row->scn;
			if (partial_replica)
				memcpy(&remote_scn, row->remote_scn, 6);
		}
	} else {
		if (scn_changer(row->tag))
			memcpy(&remote_scn, row->remote_scn, 6);
	}
	fiber->ushard = old_ushard;
}

- (int)
prepare_remote_row:(struct row_v12 *)row offt:(int)offt
{
	if (partial_replica) {
		if (row->scn <= remote_scn)
			return 0;
		memcpy(&row->remote_scn, &row->scn, 6);
		row->scn = scn + 1 + offt;
	}

	return !(snap_loaded && row->scn <= scn);
}

- (bool)
is_replica
{
	if (shard_rt[self->id].proxy)
		return 1;
	if (recovery->writer == nil)
		return 1;
	return 0;
}

@end


register_source();
