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
run_crc:(u32)run_crc_
    sop:(const struct shard_op *)sop
{
	[super init_id:shard_id scn:scn_ run_crc:run_crc_ sop:sop];
	wet_scn = scn;
	wet_run_crc = run_crc;
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

- (struct row_v12 *)
creator_row
{
	struct row_v12 *row = [super creator_row];
	if (partial_replica)
		memcpy(row->remote_scn, &remote_scn, 6);
	return row;
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
	if (shard_rt[self->id].proxy) {
		say_warn("not master");
		return 0;
	}

	if (recovery->writer == nil) {
		say_warn("local writes disabled");
		return 0;
	}

	u32 saved_crc = wet_run_crc;
	i64 saved_scn = wet_scn;
	wet_scn++;
	wet_run_crc = crc32c(wet_run_crc, data, len);
	say_debug("%s: SCN: %"PRIi64" -> %"PRIi64" CRC: 0x%08x -> 0x%08x",
		  __func__, saved_scn, wet_scn, saved_crc, wet_run_crc);
	struct row_v12 row = { .scn = wet_scn,
			       .tag = tag,
			       .shard_id = self->id,
			       .run_crc = wet_run_crc };
	struct wal_pack pack;
	wal_pack_prepare(recovery->writer, &pack);
	wal_pack_append_row(&pack, &row);
	wal_pack_append_data(&pack, data, len);
	struct wal_reply *reply = [recovery->writer wal_pack_submit];
	if (reply->row_count == 1) {
		scn = row.scn;
		run_crc = row.run_crc;
	} else  {
		assert(wet_scn - 1 == saved_scn);
		wet_scn = saved_scn;
		wet_run_crc = saved_crc;
	}
	return reply->row_count;
}


- (void)
adjust_route
{
	if ([self master] && recovery->writer) {
		update_rt(self->id, self, NULL, 0);
		[self status_update:"primary"];
		struct feeder_param empty = { .addr = { .sin_family = AF_UNSPEC } };
		[remote set_feeder:&empty];
	} else {
		update_rt(self->id, self, dummy ? "<dummy_addr>" : peer[0], 0);
		if (recovery->writer)
			[self remote_hot_standby];
		else
			[self status_update:"hot_standby/local"];
	}
}

- (void)
enable_local_writes
{
	if ([self master]) {
		if (self->loading)
			[self wal_final_row];
		else
			[self adjust_route]; /* в случае local/hot_standby [wal_final_row]
						будет вызван до вызван [enable_local_writes] */
	} else {
		[self remote_hot_standby];
	}
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

	switch (row->tag & TAG_MASK) {
	case snap_initial:
		break;
	case snap_final:
		snap_loaded = true;
		if (!dummy)
			goto exit;

		if (dummy && [(id)executor respondsTo:@selector(snap_final_row)])
			[(id)executor snap_final_row];

		if (scn != -1) /* уже был shard_final и выставил правильный scn */
			goto exit;
                // fallthrough
	case shard_final:
		assert(scn == -1 || scn == row->scn);
		scn = wet_scn = row->scn;
		run_crc = wet_run_crc = row->run_crc;
		goto exit;
	case nop:
		break;
	case wal_final:
		assert(false);
	case shard_create:
		assert(scn == -1 || scn == row->scn);
		scn = wet_scn = row->scn;
		run_crc = wet_run_crc = row->run_crc;
		goto exit;
	case shard_alter:
		[self alter:(struct shard_op *)row->data];
		if (!loading && ![self our_shard]) {
			[self free];
			goto exit;
		}
		break;
	default:
		[executor apply:&TBUF(row->data, row->len, fiber->pool) tag:row->tag];
		break;
	}

	int tag_type = row->tag & ~TAG_MASK;
	if (tag_type == TAG_SNAP) /* no run_crc & scn in snap rows */
		goto exit;

	if (partial_replica_loading) {
		if (row->scn > 0)
			memcpy(&remote_scn, row->remote_scn, 6);
		goto exit;
	}

	last_update_tstamp = ev_now();
	lag = last_update_tstamp - row->tm;

	if (unlikely(cfg.sync_scn_with_lsn && dummy && row->lsn != row->scn && row->scn >= 0))
		panic("LSN != SCN : %"PRIi64 " != %"PRIi64, row->lsn, row->scn);

	if (unlikely(row->scn - scn != 1 && tag_type == TAG_WAL && cfg.panic_on_scn_gap))
		panic("SCN sequence has gap after %"PRIi64 " -> %"PRIi64, scn, row->scn);

	u32 crc = crc32c(run_crc, row->data, row->len);
	say_debug("%s: LSN:%"PRIi64" SCN: %"PRIi64" -> %"PRIi64" CRC: 0x%08x -> 0x%08x(0x%08x)",
		  __func__, row->lsn, scn, row->scn, run_crc, row->run_crc, crc);

	if (crc != row->run_crc) {
		say_warn("LSN:%"PRIi64" crc mismatch 0x%08x <> 0x%08x",
			 row->lsn, crc, row->run_crc);
		run_crc_status = "error";
	}
	scn = wet_scn = row->scn;
	run_crc = wet_run_crc = crc;

	if (partial_replica)
		memcpy(&remote_scn, row->remote_scn, 6);

exit:
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

@end


register_source();
