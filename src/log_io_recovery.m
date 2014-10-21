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
#import <objc.h>
#import <log_io.h>
#import <palloc.h>
#import <say.h>
#import <pickle.h>
#import <tbuf.h>
#import <net_io.h>
#import <assoc.h>
#import <paxos.h>

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


#if HAVE_OBJC_RUNTIME_H
#include <objc/runtime.h>
#elif HAVE_OBJC_OBJC_API_H
#include <objc/objc-api.h>
#define objc_lookUpClass objc_lookup_class
#endif

i64 fold_scn = 0;

@implementation Recovery
+ (id)
alloc
{
	if (strcmp([[self class] name], "Recovery") != 0) /* break recursion */
	    goto ours;

	if (fold_scn > 0)
		return [FoldRecovery alloc];

	if (cfg.wal_writer_inbox_size == 0)
		return [NoWALRecovery alloc];

#ifdef PAXOS
	if (cfg.paxos_enabled)
		return [PaxosRecovery alloc];
#endif

ours:
	return [super alloc];
}

- (const char *) status { return status_buf; }
- (ev_tstamp) lag { return lag; }
- (ev_tstamp) last_update_tstamp { return last_update_tstamp; }

- (struct row_v12 *)
dummy_row_lsn:(i64)lsn_ scn:(i64)scn_ tag:(u16)tag
{
	struct row_v12 *r = palloc(fiber->pool, sizeof(struct row_v12));

	r->lsn = lsn_;
	r->scn = scn_;
	r->tm = ev_now();
	r->tag = tag;
	r->cookie = default_cookie;
	r->len = 0;
	r->data_crc32c = crc32c(0, (unsigned char *)"", 0);
	r->header_crc32c = crc32c(0, (unsigned char *)r + sizeof(r->header_crc32c),
				  sizeof(*r) - sizeof(r->header_crc32c));
	return r;
}


- (void)
verify_run_crc:(struct tbuf *)buf
{
	i64 scn_of_crc = read_u64(buf);
	u32 log = read_u32(buf);
	read_u32(buf); /* ignore run_crc_mod */

	struct crc_hist *h = NULL;
	for (unsigned i = crc_hist_i, j = 0; j < nelem(crc_hist); j++, i--) {
		struct crc_hist *p = &crc_hist[i % nelem(crc_hist)];
		if (p->scn == scn_of_crc) {
			h = p;
			break;
		}
	}

	if (!h) {
		say_warn("unable to track run_crc: crc history too short"
			 " SCN:%"PRIi64" CRC_SCN:%"PRIi64, scn, scn_of_crc);
		return;
	}

	if (h->log != log) {
		run_crc_log_mismatch |= 1;
		say_error("run_crc_log mismatch: SCN:%"PRIi64" saved:0x%08x computed:0x%08x",
			  scn_of_crc, log, h->log);
	} else {
		say_info("run_crc verified SCN:%"PRIi64, h->scn);
	}
	run_crc_verify_tstamp = ev_now();
}

- (void)
apply_sys:(const struct row_v12 *)r
{
	int tag = r->tag & TAG_MASK;

	switch (tag) {
	case snap_initial:
		if (r->len == sizeof(u32) * 3) { /* not a dummy row */
			struct tbuf buf = TBUF(r->data, r->len, NULL);
			estimated_snap_rows = read_u32(&buf);
			run_crc_log = read_u32(&buf);
			(void)read_u32(&buf); /* ignore run_crc_mod */
		}
			/* set initial lsn & scn, otherwise gap check below will fail */
		lsn = r->lsn;
		scn = r->scn;
		say_debug("%s: run_crc_log: 0x%x", __func__, run_crc_log);
		break;
	case snap_skip_scn:
		assert(r->len > 0 && r->len % sizeof(u64) == 0);
		char *ptr = malloc(r->len);
		memcpy(ptr, r->data, r->len);
		skip_scn = TBUF(ptr, r->len, (void *)ptr); /* NB: backing storage is malloc! */
		next_skip_scn = read_u64(&skip_scn);
		break;
	case run_crc:
		if (cfg.ignore_run_crc)
			break;

		if (r->len != sizeof(i64) + sizeof(u32) * 2)
			break;

		[self verify_run_crc:&TBUF(r->data, r->len, NULL)];
		break;
	}
}

- (void)
recover_row:(struct row_v12 *)r
{
	int tag = r->tag & TAG_MASK;
	int tag_type = r->tag & ~TAG_MASK;

	@try {
		say_debug("%s: LSN:%"PRIi64" SCN:%"PRIi64" tag:%s",
			  __func__, r->lsn, r->scn, xlog_tag_to_a(r->tag));
		say_debug2("	%s", tbuf_to_hex(&TBUF(r->data, r->len, fiber->pool)));

		if (++recovered_rows % 100000 == 0) {
			if (estimated_snap_rows && recovered_rows <= estimated_snap_rows) {
				float pct = 100. * recovered_rows / estimated_snap_rows;
				say_info("%.1fM/%.2f%% rows recovered",
					 recovered_rows / 1000000., pct);
				title("loading %.2f%%", pct);
			} else {
				say_info("%.1fM rows recovered", recovered_rows / 1000000.);
			}
		}

		if (tag_type == TAG_WAL)
			run_crc_log = crc32c(run_crc_log, r->data, r->len);

		if (r->scn == next_skip_scn) {
			say_info("skip SCN:%"PRIi64 " tag:%s", next_skip_scn, xlog_tag_to_a(r->tag));

			/* there are multiply rows with same SCN in paxos mode.
			   the last one is WAL row */
			if (tag_type == TAG_WAL)
				next_skip_scn = tbuf_len(&skip_scn) > 0 ?
						read_u64(&skip_scn) : 0;
		} else {
			[self apply:&TBUF(r->data, r->len, fiber->pool) tag:r->tag];
			if (tag_type == TAG_SYS)
				[self apply_sys:r];
		}

		/* note: it's to late to raise here: txn is already commited */
		if (unlikely(r->lsn - lsn > 1 && cfg.panic_on_lsn_gap))
			panic("LSN sequence has gap after %"PRIi64 " -> %"PRIi64, lsn, r->lsn);

		if (cfg.sync_scn_with_lsn && r->lsn != r->scn)
			panic("out of sync SCN:%"PRIi64 " != LSN:%"PRIi64, r->scn, r->lsn);

		lsn = r->lsn;
		last_update_tstamp = ev_now();
		lag = last_update_tstamp - r->tm;

		if (scn_changer(r->tag) || tag == snap_final) {
			if (unlikely(tag != snap_final && r->scn - scn != 1 &&
				     cfg.panic_on_scn_gap && [[self class] name] == [Recovery name]))
				panic("non consecutive SCN %"PRIi64 " -> %"PRIi64, scn, r->scn);

			scn = r->scn;
			say_debug("save crc_hist SCN:%"PRIi64" log:0x%08x", scn, run_crc_log);
			crc_hist[++crc_hist_i % nelem(crc_hist)] =
				(struct crc_hist){ scn, run_crc_log };
		}
	}
	@catch (Error *e) {
		say_error("Recovery: %s at %s:%i\n%s", e->reason, e->file, e->line,
				e->backtrace);
		struct tbuf *out = tbuf_alloc(fiber->pool);
		print_gen_row(out, r, self->print_row);
		printf("Failed row: %.*s\n", tbuf_len(out), (char *)out->ptr);

		@throw;
	}
	@finally {
		say_debug("%s: => LSN:%"PRIi64" SCN:%"PRIi64, __func__, lsn, scn);
	}
}

- (void)
recover_row_stream:(id<XLogPuller>)stream
{
	@try {
		int row_count = 0;
		struct row_v12 *row;
		palloc_register_cut_point(fiber->pool);

		while ((row = [stream fetch_row])) {
			if (row->lsn > lsn ||
			    row->tag == (snap_initial|TAG_SYS) ||
			    row->tag == (snap_final|TAG_SYS) ||
			    (row->tag & ~TAG_MASK) == TAG_SNAP)
				break;
		}

		for (; row; row = [stream fetch_row]) {
			[self recover_row:row];

			if (row_count++ > 1024) {
				palloc_cutoff(fiber->pool);
				palloc_register_cut_point(fiber->pool);
				row_count = 0;
			}
		}
	}
	@finally {
		palloc_cutoff(fiber->pool);
	}
}


- (void)
wal_final_row
{
	/* recovery of empty local_hot_standby & remote_hot_standby replica done in reverse:
	   first: primary port bound & service initialized (and proctitle set)
	   second: pull rows from remote (ans proctitle set to "loading %xx.yy")
	   in order to avoid stuck proctitle set it after every pull done,
	   not after service initialization */
	[self status_changed];
}

- (i64)
snap_lsn
{
	return [snap_dir greatest_lsn];
}

- (i64)
recover_snap
{
	XLog *snap = nil;

	@try {
		palloc_register_cut_point(fiber->pool);

		i64 snap_lsn = [self snap_lsn];
		if (snap_lsn == -1)
			raise("snap_dir reading failed");

		if (snap_lsn < 1)
			return 0;

		snap = [snap_dir open_for_read:snap_lsn];
		if (snap == nil)
			raise("can't find/open snapshot");

		say_info("recover from `%s'", snap->filename);

		bool legacy_snap = ![snap isKindOf:[XLog12 class]];

		if (legacy_snap && !cfg.sync_scn_with_lsn)
			panic("sync_scn_with_lsn is required when loading from v11 snapshots");

		if (legacy_snap)
			[self recover_row:[self dummy_row_lsn:snap_lsn scn:snap_lsn tag:snap_initial|TAG_SYS]];

		[self recover_row_stream:snap];

		/* old v11 snapshot, scn == lsn from filename */
		if (legacy_snap)
			[self recover_row:[self dummy_row_lsn:snap_lsn scn:snap_lsn tag:snap_final|TAG_SYS]];

		if (![snap eof])
			raise("unable to fully read snapshot");
	}
	@finally {
		palloc_cutoff(fiber->pool);
		[snap close];
		snap = nil;
	}
	say_info("snapshot recovered, lsn:%"PRIi64 " scn:%"PRIi64, lsn, scn);
	return lsn;
}

- (XLog *)
next_wal
{
	return [wal_dir open_for_read:lsn + 1];
}

/*
 * this function will not close r->current_wal if recovery was successful
 */
- (void)
recover_remaining_wals
{
	say_debug("%s: lsn:%"PRIi64, __func__, lsn);
	i64 wal_greatest_lsn = [wal_dir greatest_lsn];
	if (wal_greatest_lsn == -1)
		raise("wal_dir reading failed");

	/* if the caller already opened WAL for us, recover from it first */
	if (current_wal != nil) {
		say_debug("%s: current_wal:%s", __func__, current_wal->filename);
		goto recover_current_wal;
	}

	while (lsn < wal_greatest_lsn) {
		if (current_wal != nil) {
                        say_warn("wal `%s' wasn't correctly closed", current_wal->filename);
                        [current_wal close];
                        current_wal = nil;
		}

		current_wal = [self next_wal];
		if (current_wal == nil) /* either no more WALs or current one is broken */
			break;

		say_info("recover from `%s'", current_wal->filename);
	recover_current_wal:
		[self recover_row_stream:current_wal];

		if ([current_wal eof]) {
			say_info("done `%s' lsn:%"PRIi64" scn:%"PRIi64,
				 current_wal->filename, lsn, scn);

			[current_wal close];
			current_wal = nil;
		}
		fiber_gc();
	}
	fiber_gc();

	/* empty WAL or borken header encountered: unable to parse remaining WALs */
	if (wal_greatest_lsn > lsn)
		raise("not all WALs have been successfully read! "
		      "greatest_lsn:%"PRIi64" lsn:%"PRIi64" diff:%"PRIi64,
		      wal_greatest_lsn, lsn, wal_greatest_lsn - lsn);
}


- (i64)
load_from_local
{
	say_info("local recovery start");
	[self status_update:LOADING fmt:"loading/local"];

	if (lsn == 0) {
		[self recover_snap];
		if (lsn == 0)
			return 0;

		/*
		 * just after snapshot recovery current_wal isn't known
		 * so find wal which contains record with next lsn
		 */
		current_wal = [wal_dir containg_lsn:lsn + 1];
	}

	if (current_wal != nil)
		say_info("recover from `%s'", current_wal->filename);
	[self recover_remaining_wals];
	say_info("wals recovered, lsn:%"PRIi64" scn:%"PRIi64, lsn, scn);

	/* loading is faster until wal_final_row called because service is not yet initialized and
	   only pk indexes must be updated. remote feeder will send wal_final_row then all remote
	   rows are read */
	if (![self feeder_addr_configured])
		[self wal_final_row];

	if (last_wal_lsn && last_wal_lsn < lsn)
		raise("Snapshot LSN is greater then last WAL LSN");
	return lsn;
}

void
wal_lock(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);

	while ([r->wal_dir lock] != 0)
		fiber_sleep(1);

	[r enable_local_writes];
}

- (void)
local_hot_standby
{
	[self recover_follow:cfg.wal_dir_rescan_delay]; /* FIXME: make this conf */
	[self status_update:LOCAL_STANDBY fmt:"hot_standby/local"];

	fiber_create("wal_lock", wal_lock, self);
}

static void follow_file(ev_stat *, int);

static void
follow_dir(ev_timer *w, int events __attribute__((unused)))
{
	Recovery *r = w->data;
	static int tick = 5;
	if (r->current_wal && tick-- > 0)
		return;
	tick = 5;
	[r recover_remaining_wals];
	[r->current_wal follow:follow_file data:r];
}

static void
follow_file(ev_stat *w, int events __attribute__((unused)))
{
	Recovery *r = w->data;
	[r recover_row_stream:r->current_wal];
	if ([r->current_wal eof]) {
		say_info("done `%s' LSN:%"PRIi64" SCN:%"PRIi64,
			 r->current_wal->filename, [r lsn], [r scn]);
		[r->current_wal close];
		r->current_wal = nil;
		follow_dir((ev_timer *)w, 0);
		return;
	}
}

- (void)
recover_follow:(ev_tstamp)wal_dir_rescan_delay
{
	ev_timer_init(&wal_timer, follow_dir,
		      wal_dir_rescan_delay / 5, wal_dir_rescan_delay / 5);
	ev_timer_start(&wal_timer);
	if (current_wal != nil)
		[current_wal follow:follow_file data:self];
}

- (void)
recover_finalize
{
	ev_timer_stop(&wal_timer);
	/* [currert_wal follow] cb will be stopped by [current_wal close] */

	[self recover_remaining_wals];

	if (current_wal != nil)
                say_warn("wal `%s' wasn't correctly closed", current_wal->filename);

        [current_wal close];
        current_wal = nil;

	free(skip_scn.pool);
	skip_scn = TBUF(NULL, 0, NULL);
}

- (void)
simple
{
	i64 local_lsn = [self load_from_local];
	if (local_lsn == 0) {
		if (![self feeder_addr_configured]) {
			say_error("unable to find initial snapshot");
			say_info("don't you forget to initialize "
				 "storage with --init-storage switch?");
			exit(EX_USAGE);

		}
	}
	if (cfg.local_hot_standby)
		[self local_hot_standby];
	else
		[self enable_local_writes];
}

- (void)
pull_snapshot:(id<XLogPullerAsync>)puller
{
	for (;;) {
		struct row_v12 *row;
		[puller recv_row];

		while ((row = [puller fetch_row])) {
			int tag = row->tag & TAG_MASK;
			int tag_type = row->tag & ~TAG_MASK;

			if (tag_type == TAG_SNAP || tag == snap_initial || tag == snap_final) {
				[self recover_row:row];
				if (tag == snap_final)
					return;
			} else {
				raise("unexpected tag %s", xlog_tag_to_a(row->tag));
			}
		}
		fiber_gc();
	}
}

- (int)
pull_wal:(id<XLogPullerAsync>)puller
{
	struct row_v12 *row, *final_row = NULL, *rows[WAL_PACK_MAX];
	/* TODO: use designated palloc_pool */
	say_debug("%s: scn:%"PRIi64, __func__, scn);

	int pack_rows = 0;

	[puller recv_row];

	while ((row = [puller fetch_row])) {
		int tag = row->tag & TAG_MASK;

		/* TODO: apply filter on feeder side */
		/* filter out all paxos rows
		   these rows define non shared/non replicated state */
		if (tag == paxos_prepare ||
		    tag == paxos_promise ||
		    tag == paxos_propose ||
		    tag == paxos_accept ||
		    tag == paxos_nop)
			continue;

		if (tag == wal_final) {
			final_row = row;
			break;
		}

		if (row->scn <= scn)
			continue;

		if (cfg.io_compat && tag == run_crc)
			continue;

		rows[pack_rows++] = row;
		if (pack_rows == WAL_PACK_MAX)
			break;
	}

	if (pack_rows > 0) {
		/* we'r use our own lsn numbering */
		for (int j = 0; j < pack_rows; j++)
			rows[j]->lsn = lsn + 1 + j;

		if (cfg.io_compat) {
			for (int j = 0; j < pack_rows; j++) {
				u16 tag = rows[j]->tag & TAG_MASK;
				u16 tag_type = rows[j]->tag & ~TAG_MASK;

				if (tag_type != TAG_WAL)
					continue;

				switch (tag) {
				case wal_data:
				case wal_final:
					continue;
				default:
					panic("can't replicate from non io_compat master");
				}
			}
		}
#ifndef NDEBUG
		i64 pack_min_scn = rows[0]->scn,
		    pack_max_scn = rows[pack_rows - 1]->scn,
		    pack_max_lsn = rows[pack_rows - 1]->lsn;
#endif
		assert(!cfg.sync_scn_with_lsn || scn == pack_min_scn - 1);
		@try {
			for (int j = 0; j < pack_rows; j++) {
				row = rows[j]; /* this pointer required for catch below */
				[self recover_row:row];
			}
		}
		@catch (Error *e) {
			panic("Replication failure: %s at %s:%i"
			      " remote row LSN:%"PRIi64 " SCN:%"PRIi64, /* FIXME: here we primting "fixed" LSN */
			      e->reason, e->file, e->line,
			      row->lsn, row->scn);
		}

		int confirmed = 0;
		while (confirmed != pack_rows) {
			struct wal_pack pack;

			if (!wal_pack_prepare(self, &pack)) {
				fiber_sleep(0.1);
				continue;
			}
			for (int i = confirmed; i < pack_rows; i++)
				wal_pack_append_row(&pack, rows[i]);

			confirmed += [self wal_pack_submit];
			if (confirmed != pack_rows) {
				say_warn("WAL write failed confirmed:%i != sent:%i",
					 confirmed, pack_rows);
				fiber_sleep(0.05);
			}
		}

		assert(scn == pack_max_scn);
		assert(lsn == pack_max_lsn);
	}

	fiber_gc();

	if (final_row) {
		[self wal_final_row];
		return 1;
	}

	return 0;
}

- (int)
load_from_remote
{
	int ret = [self load_from_remote:&feeder];
	if (ret < 0)
		panic("unable to pull initial snapshot");
	return ret;
}

- (int)
load_from_remote:(struct feeder_param *)remote
{
	XLogPuller *puller = nil;

	@try {
		puller = [[objc_lookUpClass("XLogPuller") alloc] init];
		[puller feeder_param:remote];

		int i = 5;
		while (i-- > 0) {
			if ([puller handshake:scn] > 0)
				break;
			fiber_sleep(1);
		}
		if (i <= 0) {
			say_error("feeder handshake failed: %s", [puller error]);
			return -1;
		}


		zero_io_collect_interval();

		[self pull_snapshot:puller];
		[self configure_wal_writer];

		/* don't wait for snapshot. our goal to be replica as fast as possible */
		if (getenv("SYNC_DUMP") == NULL)
			[[self snap_writer] snapshot:false];
		else
			[[self snap_writer] snapshot_write];

		/* old version doesn's send wal_final_tag for us. */
		if ([puller version] == 11)
			[self wal_final_row];

		while ([self pull_wal:puller] != 1);
	}
	@finally {
		[puller free];
		unzero_io_collect_interval();
	}
	return 0;
}

- (void)
pull_from_remote:(id<XLogPullerAsync>)puller
{
	assert([self lsn] > 0);
	for (;;)
		[self pull_wal:puller];
}

static void
hot_standby_status(Recovery *r, const char *status, const char *reason)
{
	[r status_update:REMOTE_STANDBY fmt:"hot_standby/%s/%s%s%s",
	   sintoa(&r->feeder.addr), status, reason ? ":" : "", reason ?: ""];
	if (strcmp([r status], "fail") == 0)
		say_error("replication failure: %s", reason);
}

void
remote_hot_standby(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);
	ev_tstamp reconnect_delay = 0.1;
	bool warning_said = false;

	r->remote_puller = [[objc_lookUpClass("XLogPuller") alloc] init];
again:
	while (![r feeder_addr_configured])
		fiber_sleep(reconnect_delay);

	hot_standby_status(r, "connect", NULL);
	do {
		[r->remote_puller feeder_param: &r->feeder];

		assert([r scn] > 0); /* snapshot must be loaded */
		i64 scn = [r scn] + 1; /* start recover from next scn */

		if ([r->remote_puller handshake:scn] <= 0) {
			/* no more WAL rows in near future, notify module about that */
			[r wal_final_row];

			if (!warning_said) {
				hot_standby_status(r, "fail", [r->remote_puller error]);
				say_warn("feeder handshake failed: %s", [r->remote_puller error]);
				say_info("will retry every %.2f second", reconnect_delay);
				warning_said = true;
			}
			goto sleep;
		}
		warning_said = false;

		@try {
			hot_standby_status(r, "ok", NULL);
			[r pull_from_remote:r->remote_puller];
		}
		@catch (Error *e) {
			[r->remote_puller close];
			hot_standby_status(r, "fail", e->reason);
		}
	sleep:
		fiber_gc();
		fiber_sleep(reconnect_delay);
	} while ([r feeder_addr_configured]);

	assert(r->local_writes);
	[r status_update:PRIMARY fmt:"primary"];

	goto again;
}


- (bool)
feeder_changed:(struct feeder_param*)new
{
	if (feeder_param_eq(&feeder, new) != true) {
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
		if ([self feeder_addr_configured])
			say_info("configured remote hot standby, WAL feeder %s", sintoa(&feeder.addr));
		return true;
	}
	return false;
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
		panic("Can't lock wal_dir:%s", wal_dir->dirname);

	if (!same_dir(wal_dir, snap_dir)) {
		if ([snap_dir lock] < 0)
			panic("Can't lock snap_dir:%s", snap_dir->dirname);
	}
}

- (void)
enable_local_writes
{
	[self lock];
	[self recover_finalize];
	local_writes = true;

	if (lsn == 0) {
		assert([self feeder_addr_configured]);
		say_info("initial loading from WAL feeder %s", sintoa(&feeder.addr));
		assert(fiber != &sched); /* load_from_remote expects being called from fiber */
		[self load_from_remote];
	} else {
		[self configure_wal_writer];
	}

	fiber_create("remote_hot_standby", remote_hot_standby, self);

	if ([self feeder_addr_configured])
		say_info("configured remote hot standby, WAL feeder %s", sintoa(&feeder.addr));
	else
		[self status_update:PRIMARY fmt:"primary"];
}

- (bool)
is_replica
{
	if (!local_writes)
		return true;
	if ([self feeder_addr_configured])
		return true;
	return false;
}

- (void)
check_replica
{
	if ([self is_replica])
		raise("replica is readonly");
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

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
{
	snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
	wal_dir = [[WALDir alloc] init_dirname:wal_dirname];
	snap_dir->recovery = wal_dir->recovery = self;
	wal_timer.data = self;

	return self;
}

- (ev_tstamp)
run_crc_lag
{
	return ev_now() - run_crc_verify_tstamp;
}

- (const char *)
run_crc_status
{
	if (run_crc_log_mismatch && run_crc_mod_mismatch)
		return "ALL_CRC_MISMATCH";
	if (run_crc_log_mismatch)
		return "LOG_CRC_MISMATCH";
	if (run_crc_mod_mismatch)
		return "MOD_CRC_MISMATCH";
	return "ok";
}

static void
run_crc_writer(va_list ap)
{
	Recovery *recovery = va_arg(ap, Recovery *);
	ev_tstamp submit_tstamp = ev_now(), delay = va_arg(ap, ev_tstamp);
	i64 lsn = [recovery lsn];
	for (;;) {
		fiber_sleep(1.);
		if ([recovery is_replica])
			continue;
		if ([recovery lsn] - lsn < 32)
			continue;
		if (ev_now() - submit_tstamp < delay)
			continue;

		@try {
			while ([recovery submit_run_crc] < 0)
				fiber_sleep(0.1);
		}
		@catch (Error *e) {
			say_warn("run_crc submit failed, [%s reason:\"%s\"] at %s:%d",
				 [[e class] name], e->reason, e->file, e->line);
		}

		submit_tstamp = ev_now();
		lsn = [recovery lsn];
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
		if ([recovery is_replica])
			continue;

		[recovery submit:body len:nelem(body) tag:nop|TAG_SYS];
	}
}

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
	rows_per_wal:(int)wal_rows_per_file
	feeder_param:(struct feeder_param*)feeder_
               flags:(int)flags
{
	/* Recovery object is never released */

	snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
	wal_dir = [[WALDir alloc] init_dirname:wal_dirname];
	memset(&feeder, 0, sizeof(feeder));

	wal_timer.data = self;
	wal_dir->recovery = snap_dir->recovery = self;

	(void)flags;
	if (wal_rows_per_file <= 4)
		panic("inacceptable value of 'rows_per_file'");

	wal_dir->rows_per_file = wal_rows_per_file;
	wal_dir->fsync_delay = cfg.wal_fsync_delay;

	if (feeder_ != NULL)
		[self feeder_changed: feeder_];

	return self;
}

- (void)
configure_wal_writer
{
	[self init_wal_dir: wal_dir];

	if (!cfg.io_compat && cfg.run_crc_delay > 0)
		fiber_create("run_crc", run_crc_writer, self, cfg.run_crc_delay);

	if (!cfg.io_compat && cfg.nop_hb_delay > 0)
		fiber_create("nop_hb", nop_hb_writer, self, cfg.nop_hb_delay);
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

- (void)
status_update:(enum recovery_status)new_status fmt:(const char *)fmt, ...
{
	prev_status = status;
	status = new_status;

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(status_buf, sizeof(status_buf), fmt, ap);
	va_end(ap);

	if (prev_status != status) {
		say_info("recovery status: %s", status_buf);
		[self status_changed];
	}
}

- (void)
status_changed
{
}

- (SnapWriter *)
snap_writer
{
	if (snap_writer)
		return snap_writer;
	snap_writer = [[SnapWriter alloc] init_state:self snap_dir:snap_dir];
	return snap_writer;
}

- (int)
write_initial_state
{
	lsn = scn = 1;
	return [[self snap_writer] snapshot_write];
}
@end


static void
hexdump(struct tbuf *out, u16 tag __attribute__((unused)), struct tbuf *row)
{
	tbuf_printf(out, "%s", tbuf_to_hex(row));
}

void
print_gen_row(struct tbuf *out, const struct row_v12 *row,
	      void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row))
{
	if (handler == NULL)
		handler = hexdump;

	tbuf_printf(out, "lsn:%" PRIi64 " scn:%" PRIi64 " tm:%.3f t:%s %s ",
		    row->lsn, row->scn, row->tm,
		    xlog_tag_to_a(row->tag),
		    sintoa((void *)&row->cookie));

	struct tbuf row_data = TBUF(row->data, row->len, fiber->pool);

	int tag = row->tag & TAG_MASK;
	switch (tag) {
	case snap_initial:
		if (tbuf_len(&row_data) == sizeof(u32) * 3) {
			u32 count = read_u32(&row_data);
			u32 log = read_u32(&row_data);
			u32 mod = read_u32(&row_data);
			tbuf_printf(out, "count:%u run_crc_log:0x%08x run_crc_mod:0x%08x",
				    count, log, mod);
		}
		break;
	case snap_skip_scn:
		while (tbuf_len(&row_data) > 0)
			tbuf_printf(out, "%"PRIi64" ", read_u64(&row_data));
		break;
	case run_crc: {
		i64 scn = -1;
		if (tbuf_len(&row_data) == sizeof(i64) + 2 * sizeof(u32))
			scn = read_u64(&row_data);
		u32 log = read_u32(&row_data);
		(void)read_u32(&row_data); /* ignore run_crc_mod */
		tbuf_printf(out, "SCN:%"PRIi64 " log:0x%08x", scn, log);
		break;
	}
	case nop:
		break;
#ifdef PAXOS
	case paxos_prepare:
	case paxos_promise:
	case paxos_propose:
	case paxos_accept:
		paxos_print(out, handler, row);
		break;
#endif
	default:
		handler(out, row->tag, &row_data);
	}
}

/* this little hole shouldn't be used too much */
int
read_log(const char *filename, void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row))
{
	XLog *l;
	const struct row_v12 *row;
	int row_count = 0;
	l = [XLog open_for_read_filename:filename dir:NULL];
	if (l == nil) {
		say_syserror("unable to open filename `%s'", filename);
		return -1;
	}

	palloc_register_cut_point(fiber->pool);
	while ((row = [l fetch_row])) {
		struct tbuf *out = tbuf_alloc(fiber->pool);
		print_gen_row(out, row, handler);
		printf("%.*s\n", tbuf_len(out), (char *)out->ptr);

		if (row_count++ > 1024) {
			palloc_cutoff(fiber->pool);
			palloc_register_cut_point(fiber->pool);
			row_count = 0;
		}
	}
	palloc_cutoff(fiber->pool);

	if (![l eof]) {
		say_error("binary log `%s' wasn't correctly closed", filename);
		return -1;
	}
	return 0;
}

@implementation NoWALRecovery
- (id)
init_snap_dir:(const char *)snap_dirname
      wal_dir:(const char *)wal_dirname
{
	[super init];
        snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
        wal_dir = [[WALDir alloc] init_dirname:wal_dirname];
	return self;
}

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
        rows_per_wal:(int)wal_rows_per_file
	feeder_param:(struct feeder_param *)feeder_param_
               flags:(int)flags
{
	(void)wal_rows_per_file;
	(void)feeder_param_;
	(void)flags;
	return [self init_snap_dir:snap_dirname wal_dir:wal_dirname];
}


- (void)
configure_wal_writer
{
}


- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	(void)data; (void)len; (void)tag;
	scn++;
	lsn++;
	return 1;
}

@end

@implementation FoldRecovery

- (i64)
snap_lsn
{
	return [snap_dir containg_scn:fold_scn];
}

- (void)
recover_row:(struct row_v12 *)r
{
	[super recover_row:r];

	if (r->scn == fold_scn && (r->tag & ~TAG_MASK) == TAG_WAL) {
		if ([self respondsTo:@selector(snapshot_fold)])
			exit([self snapshot_fold]);
		exit([[self snap_writer] snapshot_write]);
	}
}

- (void)
wal_final_row
{
	say_error("unable to find record with SCN:%"PRIi64, fold_scn);
	exit(EX_OSFILE);
}

@end

register_source();
