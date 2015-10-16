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

- (bool) feeder_changed:(struct feeder_param*)new { return [remote feeder_changed:new]; }
- (struct feeder_param *)
feeder
{
	static struct feeder_param feeder = { .ver = 1,
					      .filter.type = FILTER_TYPE_ID };
	if (dummy) {
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		if (fid_err) panic("wrong feeder conf");
		return &feeder;
	}
	memcpy(&feeder.addr, shard_addr(peer[0], PORT_REPLICATION), sizeof(feeder.addr));
	return &feeder;
}

- (void)
load_from_remote
{
	XLogRemoteReader *reader = [[XLogRemoteReader alloc] init_recovery:self];
	[reader load_from_remote:[self feeder]];
	[reader free];
}

- (void)
remote_hot_standby
{
	remote = [[XLogReplica alloc] init_shard:self];
	[remote hot_standby:[self feeder] writer:[recovery writer]];
}

- (bool)
standalone
{
	if (dummy) {
		struct feeder_param feeder;
		enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
		return fid_err || feeder.addr.sin_family == AF_UNSPEC;
	}
	return strcmp(cfg.hostname, peer[0]) == 0;
}

- (void)
start
{
	if ([self standalone])
		[self status_update:PRIMARY fmt:"primary"];
	else
		[self remote_hot_standby];
}

- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	static unsigned count;
	static struct msg_void_ptr msg;

	if (cfg.wal_writer_inbox_size == 0) {
		scn++;
		return 1;
	}

	if (recovery->writer == nil) {
		say_warn("local writes disabled");
		return 0;
	}

	if (status == REMOTE_STANDBY) {
		say_warn("remote hot standby");
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
	const struct sockaddr_in *master_addr = shard_addr(peer[0], PORT_PRIMARY);
	if (peer[0] == NULL || strlen(peer[0]) == 0) {
		assert(false); // А когда вообще такое может быть? Если перед улаением, то правильней занулить роут в [free]
		update_rt(self->id, SHARD_MODE_NONE, nil, NULL);
		return;
	}

	if (strcmp(peer[0], cfg.hostname) == 0) {
		update_rt(self->id, SHARD_MODE_LOCAL, self, NULL);
		[self status_update:PRIMARY fmt:""];
		return;
	}

	enum shard_mode mode = SHARD_MODE_PROXY;
	for (int i = 0; i < nelem(peer) && peer[i]; i++)
		if (strcmp(peer[i], cfg.hostname) == 0) {
			mode = SHARD_MODE_PARTIAL_PROXY;
			break;
		}
	[self status_update:REMOTE_STANDBY fmt:"replicating from %s", peer[0]];
	update_rt(self->id, mode, self, master_addr);
}

- (void)
recover_row_sys:(struct row_v12 *)r
{
	int tag = r->tag & TAG_MASK;
	struct tbuf row_data = TBUF(r->data, r->len, NULL);

	switch (tag) {
	case snap_initial:
	case snap_final:
		break;
	case run_crc:
		if (cfg.ignore_run_crc)
			break;

		if (r->len != sizeof(i64) + sizeof(u32) * 2)
			break;

		run_crc_verify(r, &run_crc_state, &row_data);
		break;
	case nop:
		break;
	case shard_tag:
		[self alter_peers:r];
		break;
	default:
		say_warn("%s row ignored", xlog_tag_to_a(r->tag));
		break;
	}
}

- (void)
recover_row:(struct row_v12 *)row
{
	// calculate run_crc _before_ calling executor: it may change row
	if (scn_changer(row->tag))
		run_crc_calc(&run_crc_log, row->tag, row->data, row->len);

	if ((row->tag & ~TAG_MASK) != TAG_SYS)
		[executor apply:&TBUF(row->data, row->len, fiber->pool) tag:row->tag];
	else
		[self recover_row_sys:row];
	last_update_tstamp = ev_now();
	lag = last_update_tstamp - row->tm;

	if (scn_changer(row->tag)) {
		run_crc_record(&run_crc_state, (struct run_crc_hist){ .scn = row->scn, .value = run_crc_log });
		assert(cfg.sync_scn_with_lsn == 0 || row->scn - self->scn == 1);
		scn = row->scn;

		if (row->tag == (shard_tag|TAG_SYS)) {
			for (int i = 0; i < nelem(peer) && peer[i]; i++)
				if (strcmp(peer[i], cfg.hostname) == 0)
					return;
			[self free];
		}
	}
}

- (bool)
is_replica
{
	if (cfg.wal_writer_inbox_size == 0)
		return 0;
	return recovery->writer == nil || status != PRIMARY;
}
@end


register_source();
