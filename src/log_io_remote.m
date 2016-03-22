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
#import <say.h>
#import <fiber.h>
#import <objc.h>

#include <assert.h>


@implementation XLogRemoteReader

- (id) init_recovery:(id<RecoverRow>)recovery_
{
	[super init];
	recovery = recovery_;
	return self;
}

- (int)
recover_row_stream:(id<XLogPullerAsync>)puller
{
	int count = 0;
	for (;;) {
		struct row_v12 *row;
		[puller recv_row];
		while ((row = [puller fetch_row])) {
			if ((row->tag & TAG_MASK) == wal_final) {
				if ([(id)recovery respondsTo:@selector(wal_final_row)])
					[(id)recovery wal_final_row];
				return count;
			}

			[recovery recover_row:row];
			count++;
		}
		fiber_gc();
	}
}

- (int)
load_from_remote:(struct feeder_param *)param
{
	XLogPuller *puller = nil;
	say_info("initial loading from WAL feeder %s", sintoa(&param->addr));
	assert(fiber != sched); /* load_from_remote expects being called from fiber */
	@try {
		puller = [[XLogPuller alloc] init];
		[puller feeder_param:param];

		int i = 5;
		while (i-- > 0) {
			if ([puller handshake:0] > 0)
				break;
			fiber_sleep(1);
		}
		if (i <= 0) {
			say_error("feeder handshake failed: %s", [puller error]);
			return -1;
		}

		zero_io_collect_interval();
		return [self recover_row_stream:puller];
	}
	@finally {
		[puller free]; //FIXME: do not drop connection after initial loading
		unzero_io_collect_interval();
	}
}

@end

@implementation XLogReplica

- (id) init_shard:(id<Shard>)shard_
{
	[super init];
	mbox_init(&mbox);
	shard = shard_;
	return self;
}

- (id)
free
{
	[remote_puller free];
	return [super free];
}

- (i64) handshake_scn { return [shard scn]; }

- (void)
status:(const char *)status reason:(const char *)reason
{
	if (strcmp(status, "unconfigured") == 0) {
		// assert(local_writes);
		// [shard status_update:"primary"];
		return;
	}
	if (strcmp(status, "configured") == 0) {
		say_info("configured remote hot standby, WAL feeder %s", reason);
		return;
	}

	[shard status_update:"hot_standby/%s/%s%s%s",
	       sintoa(&feeder.addr), status, reason ? ":" : "", reason ?: ""];
	if (strcmp([shard status], "fail") == 0)
		say_error("replication failure: %s", reason);
}


- (void)
set_feeder:(struct feeder_param*)new
{
	static struct msg_void_ptr msg;

	if (feeder_param_eq(&feeder, new))
		return;

	free(feeder.filter.name);
	free(feeder.filter.arg);
	feeder = *new;
	if (feeder.filter.name) {
		feeder.filter.name = strdup(feeder.filter.name);
	}
	if (feeder.filter.arg) {
		feeder.filter.arg = xmalloc(feeder.filter.arglen);
		memcpy(feeder.filter.arg, new->filter.arg, feeder.filter.arglen);
	}

	[remote_puller abort_recv];
	if ([self feeder_addr_configured]) {
		[self status:"configured" reason:sintoa(&feeder.addr)];
		if (msg.link.tqe_prev == NULL)
			mbox_put(&mbox, &msg, link);
	}
}

- (struct sockaddr_in)
feeder_addr
{
	return feeder.addr;
}

- (bool)
feeder_addr_configured
{
	return feeder.addr.sin_family != AF_UNSPEC;
}

/* replicate remote rows: apply and save to local WAL
   throws exceptions on failure */

- (int)
replicate_row_stream:(id<XLogPullerAsync>)puller
{
	struct row_v12 *row, *final_row = NULL, *rows[WAL_PACK_MAX];
	/* TODO: use designated palloc_pool */
	say_debug("%s: scn:%"PRIi64, __func__, [shard scn]);
	assert(recovery->writer != nil);
	assert([recovery->writer lsn] > 0);

	const i64 shard_id = [shard id];
	bool dummy_writer = [DummyXLogWriter class] == [(id)recovery->writer class];
	i64 min_scn = [shard scn];
	int pack_rows = 0;

	/* old version doesn's send wal_final_tag for us. */
	if ([puller version] == 11)
		[shard wal_final_row];

	[puller recv_row];

	while ((row = [puller fetch_row])) {
		int tag = row->tag & TAG_MASK;

		if (tag == wal_final) {
			final_row = row;
			break;
		}

		if (row->shard_id != shard_id)
			continue;

		/* TODO: apply filter on feeder side */
		/* filter out all paxos rows
		   these rows define non shared/non replicated state */
		if (tag == paxos_promise ||
		    tag == paxos_accept ||
		    tag == paxos_nop)
			continue;

		if (row->scn <= min_scn)
			continue;

		rows[pack_rows++] = row;
		if (pack_rows == WAL_PACK_MAX || tag == shard_alter)
			break;
	}

	if (pack_rows > 0) {
		rlock(&recovery->snapshot_lock);
#ifndef NDEBUG
		i64 pack_min_scn = rows[0]->scn,
		    pack_max_scn = rows[pack_rows - 1]->scn;
#endif
		assert(!cfg.sync_scn_with_lsn || [shard scn] == pack_min_scn - 1);
		@try {
			for (int j = 0; j < pack_rows; j++) {
				row = rows[j]; /* this pointer required for catch below */
				[shard recover_row:row];
			}
		}
		@catch (Error *e) {
			panic("Replication failure: %s at %s:%i"
			      " remote row LSN:%"PRIi64 " SCN:%"PRIi64,
			      e->reason, e->file, e->line,
			      row->lsn, row->scn);
			[e release];
		}

		if (dummy_writer) {
			[(id)recovery->writer incr_lsn:pack_rows];
		} else {
			int confirmed = 0;
			while (confirmed != pack_rows) {
				struct wal_pack pack;

				wal_pack_prepare(recovery->writer, &pack);
				for (int i = confirmed; i < pack_rows; i++) {
					rows[i]->lsn = 0;
					wal_pack_append_row(&pack, rows[i]);
				}

				struct wal_reply *reply = [recovery->writer wal_pack_submit];
				confirmed += reply->row_count;
				if (confirmed != pack_rows) {
					say_warn("WAL write failed confirmed:%i != sent:%i",
						 confirmed, pack_rows);
					fiber_sleep(0.05);
				}
			}
		}

		if (shard == nil)
			return 2;

		assert([shard scn] == pack_max_scn);
		runlock(&recovery->snapshot_lock);
	}

	fiber_gc();

	if (final_row) {
		[shard wal_final_row];
		return 1;
	}

	return 0;
}

- (void)
connect_loop
{
	ev_tstamp reconnect_delay = 0.1;
	bool warning_said = false;

	remote_puller = [[XLogPuller alloc] init];
again:
	mbox_wait(&mbox);
	mbox_clear(&mbox);

	[self status:"connect" reason:NULL];
	do {
		[remote_puller feeder_param:&feeder];

		if ([remote_puller handshake:[self handshake_scn]] <= 0) {
			/* no more WAL rows in near future, notify module about that */
			[shard wal_final_row];

			if (shard == nil) /* [shard wal_final_row] may delete shard */
				goto close;
			if (!warning_said) {
				[self status:"fail" reason:[remote_puller error]];
				say_warn("feeder handshake failed: %s", [remote_puller error]);
				say_info("will retry every %.2f second", reconnect_delay);
				warning_said = true;
			}
			goto sleep;
		}
		warning_said = false;

		@try {
			[self status:"ok" reason:NULL];
			for (;;) {
				[self replicate_row_stream:remote_puller];
				if (shard == nil)
					goto close;
			}
		}
		@catch (Error *e) {
			[remote_puller close];
			if ([self feeder_addr_configured])
				[self status:"fail" reason:e->reason];
			[e release];
		}
	sleep:
		fiber_gc();
		fiber_sleep(reconnect_delay);
	} while ([self feeder_addr_configured]);

	[self status:"unconfigured" reason:NULL];

	goto again;
close:
	say_info("close");
	[self free];

}

static void
hot_standby(va_list ap)
{
	XLogReplica *r = va_arg(ap, XLogReplica *);
	[r connect_loop];
}

- (void)
hot_standby:(struct feeder_param*)feeder_
{
	assert(recovery->writer != nil);
	[self set_feeder:feeder_];
	fiber_create("remote_hot_standby", hot_standby, self);
}

- (void)
abort_and_free
{
	[remote_puller abort_recv];
	shard = nil; // connect_loop() will exit if shard is nil
}

@end

register_source();
