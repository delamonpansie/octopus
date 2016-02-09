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
#import <log_io.h>
#import <fiber.h>
#import <say.h>
#import <pickle.h>
#import <tbuf.h>
#import <shard.h>

#include <third_party/crc32.h>
#include <string.h>

@implementation POR
- (i64) scn { return scn; }

- (id) free
{
	if (remote)
		remote->shard = nil;
	return [super free];
}

- (void)
set_feeder:(struct feeder_param*)new
{
	/* legacy */
	if (shard_rt[self->id].shard && shard_rt[self->id].proxy)
		[remote set_feeder:new];
}

- (struct feeder_param *)
feeder
{
	static struct feeder_param feeder = { .ver = 1,
					      .filter = {.type = FILTER_TYPE_ID }};
	if (dummy) {
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		if (fid_err) panic("wrong feeder conf");
		return &feeder;
	}
	feeder.addr = *peer_addr(peer[0], PORT_REPLICATION);
	return &feeder;
}

- (void)
load_from_remote
{
	XLogRemoteReader *reader = [[XLogRemoteReader alloc] init_recovery:self];
	struct feeder_param feeder = { .ver = 1,
				       .filter = { .type = FILTER_TYPE_ID } };
	for (int i = 0; i < nelem(peer); i++) {
		if (strcmp(peer[i], cfg.hostname) == 0)
			continue;

		feeder.addr = *peer_addr(peer[i], PORT_REPLICATION);
		if ([reader load_from_remote:&feeder] >= 0)
			break;
	}
	[reader free];
}

- (bool)
master
{
	if (dummy) {
		struct feeder_param feeder;
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		return fid_err || feeder.addr.sin_family == AF_UNSPEC;
	}
	return strcmp(cfg.hostname, peer[0]) == 0;
}

- (void)
remote_hot_standby
{
	assert(![self master]);
	struct feeder_param feeder = { .ver = 1,
				       .filter = { .type = FILTER_TYPE_ID } };
	if (dummy) {
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		if (fid_err) panic("wrong feeder conf");
	} else {
		feeder.addr = *peer_addr(peer[0], PORT_REPLICATION);
	}
	if (remote == nil) {
		remote = [[XLogReplica alloc] init_shard:self];
		[remote hot_standby:&feeder];
	} else {
		[remote set_feeder:&feeder];
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
		for (int i = 0; i < reply->crc_count; i++) {
			run_crc_log = reply->row_crc[i].value;
			run_crc_record(&run_crc_state, reply->row_crc[i]);
		}
	}
	return reply->row_count;
}

- (void)
adjust_route
{
	if (loading)
		return;

	if ([self master]) {
		update_rt(self->id, self, NULL);
		[self status_update:"primary"];
		struct feeder_param empty = { .addr = { .sin_family = AF_UNSPEC } };
		[remote set_feeder:&empty];
	} else {
		const char *master = dummy ? "<dummy_addr>" : peer[0];
		update_rt(self->id, self, master);
		// writer == nil => local_hot_standby
		if (recovery->writer) {
			[self status_update:"hot_standby/%s/init", master];
			[self remote_hot_standby];
		}
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
	// calculate run_crc _before_ calling executor: it may change row
	if (unlikely(cfg.sync_scn_with_lsn && dummy && row->lsn != row->scn))
		panic("LSN != SCN : %"PRIi64 " != %"PRIi64, row->lsn, row->scn);

	if (scn_changer(row->tag))
		run_crc_calc(&run_crc_log, row->tag, row->data, row->len);

	switch (row->tag & TAG_MASK) {
	case snap_initial:
	case snap_final:
		break;
	case run_crc:
		if (cfg.ignore_run_crc)
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
		[self alter_peers:(struct shard_op *)row->data];
		if (![self our_shard]) {
			[self free];
			return;
		}
		break;
	default:
		[executor apply:&TBUF(row->data, row->len, fiber->pool) tag:row->tag];
		break;
	}

	last_update_tstamp = ev_now();
	lag = last_update_tstamp - row->tm;

	if (scn_changer(row->tag)) {
		run_crc_record(&run_crc_state, (struct run_crc_hist){ .scn = row->scn, .value = run_crc_log });
		scn = row->scn;
	}
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
