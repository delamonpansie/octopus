/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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
#import <object.h>
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
#include <unistd.h>

@implementation Recovery

- (const char *) status { return status; }
- (ev_tstamp) lag { return lag; }
- (ev_tstamp) last_update_tstamp { return last_update_tstamp; }

- (i64) scn { return scn; }
- (void) set_scn:(i64)scn_ { scn = scn_; }

- (void)
initial
{
	[self set_lsn:1];
	[self set_scn:1];
}

/* this little hole shouldn't be used too much */
int
read_log(const char *filename, void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row))
{
	XLog *l;
	struct tbuf *row;
	XLogDir *dir;

	if (strstr(filename, ".xlog")) {
                dir = [[WALDir alloc] init_dirname:NULL];
	} else if (strstr(filename, ".snap")) {
                dir = [[SnapDir alloc] init_dirname:NULL];
	} else {
		say_error("don't know what how to read `%s'", filename);
		return -1;
	}

	l = [dir open_for_read_filename:filename];
	if (l == nil) {
		say_syserror("unable to open filename `%s'", filename);
		return -1;
	}
	fiber->pool = l->pool;
	while ((row = [l fetch_row])) {
		struct tbuf *out = tbuf_alloc(l->pool);
		struct row_v12 *v12 = row_v12(row);

		tbuf_printf(out, "lsn:%" PRIi64 " scn:%" PRIi64 " tm:%.3f t:%s %s ",
			    v12->lsn, v12->scn, v12->tm, xlog_tag_to_a(v12->tag),
			    sintoa((void *)&v12->cookie));

		struct tbuf row_data = TBUF(v12->data, v12->len, NULL);

		switch (v12->tag) {
		case snap_initial_tag:
			if (tbuf_len(&row_data) == sizeof(u32) * 3) {
				u32 count = read_u32(&row_data);
				u32 log = read_u32(&row_data);
				u32 mod = read_u32(&row_data);
				tbuf_printf(out, "count:%u run_crc_log:0x%08x run_crc_mod:0x%08x",
					    count, log, mod);
			}
			break;
		case snap_tag:
		case wal_tag:
			handler(out, v12->tag, &row_data);
			break;
		case run_crc: {
			u32 log = read_u32(&row_data);
			u32 mod = read_u32(&row_data);
			tbuf_printf(out, "log:0x%08x mod:0x%08x", log, mod);
			break;
		}
		case nop:
			break;
		case paxos_prepare:
		case paxos_promise:
		case paxos_propose:
		case paxos_accept:
			paxos_print(out, handler, row);
			break;
		default:
			tbuf_printf(out, "UNKNOWN");
		}
		printf("%.*s\n", tbuf_len(out), (char *)out->ptr);
		prelease_after(l->pool, 128 * 1024);
	}

	if (![l eof]) {
		say_error("binary log `%s' wasn't correctly closed", filename);
		return -1;
	}
	return 0;
}

- (struct tbuf *)
dummy_row_lsn:(i64)lsn_ scn:(i64)scn_ tag:(u16)tag
{
	struct tbuf *b = tbuf_alloc(fiber->pool);
	tbuf_ensure(b, sizeof(struct row_v12));
	tbuf_append(b, NULL, sizeof(struct row_v12));

	row_v12(b)->lsn = lsn_;
	row_v12(b)->scn = scn_;
	row_v12(b)->tm = ev_now();
	row_v12(b)->tag = tag;
	row_v12(b)->cookie = default_cookie;
	row_v12(b)->len = 0;
	row_v12(b)->data_crc32c = crc32c(0, (unsigned char *)"", 0);
	row_v12(b)->header_crc32c =
		crc32c(0, (unsigned char *)row_v12(b) + sizeof(row_v12(b)->header_crc32c),
		       sizeof(row_v12(b)) - sizeof(row_v12(b)->header_crc32c));
	return b;
}

- (void)
apply_row:(struct tbuf *)row tag:(u16)tag
{
	(void)row;
	(void)tag;
	panic("%s must be specilized in subclass", __func__);
}

- (void)
recover_row:(struct tbuf *)row
{
	i64 row_lsn = row_v12(row)->lsn;
	i64 row_scn = row_v12(row)->scn;
	u16 tag = row_v12(row)->tag;
	ev_tstamp tm = row_v12(row)->tm;

	/* FIXME: temporary hack */
	if (cfg.io12_hack && row_lsn > 0)
		row_scn = row_v12(row)->scn = row_lsn;

	@try {
		say_debug("%s: lsn:%"PRIi64" scn:%"PRIi64" tag:%s",
			  __func__, row_v12(row)->lsn, row_scn, xlog_tag_to_a(tag));

		if (row_lsn > 0 &&
		    tag != snap_final_tag &&
		    tag != snap_initial_tag &&
		    tag != snap_tag &&
		    row_lsn != lsn + 1)
		{
			if (!cfg.io_compat)
				raise("lsn sequence has gap after %"PRIi64 " -> %"PRIi64,
				      lsn, row_lsn);
			else
				say_warn("lsn sequence has gap after %"PRIi64 " -> %"PRIi64,
					 lsn, row_lsn);
		}

		if (row_lsn > 0) {
			say_debug("%s: lsn %"PRIi64" => %"PRIi64, __func__, lsn, row_lsn);
			lsn = row_lsn;
		}

		if (++processed_rows % 100000 == 0) {
			if (estimated_snap_rows && processed_rows <= estimated_snap_rows) {
				float pct = 100. * processed_rows / estimated_snap_rows;
				say_info("%.1fM/%.2f%% rows processed",
					 processed_rows / 1000000., pct);
				set_proc_title("loading %.2f%%", pct);
			} else {
				say_info("%.1fM rows processed", processed_rows / 1000000.);
			}
		}

		tbuf_ltrim(row, sizeof(struct row_v12)); /* drop header */

		/* since apply_row may change contents of `row' header,
		   make a clone for crc calulation */
		struct tbuf row_clone = TBUF(row->ptr, tbuf_len(row), NULL);

		[self apply_row:row tag:tag];

		switch (tag) {
		case wal_tag:
			assert(row_scn > 0);
			assert(row_scn == scn + 1);
			scn = row_scn; /* each wal_tag row represent a single atomic change */
			lag = ev_now() - tm;
			last_update_tstamp = ev_now();
			run_crc_log = crc32c(run_crc_log, row_clone.ptr, tbuf_len(&row_clone));
			break;
		case snap_initial_tag:
			if (tbuf_len(&row_clone) == sizeof(u32) * 3) { /* not a dummy row */
				estimated_snap_rows = read_u32(&row_clone);
				run_crc_log = read_u32(&row_clone);
				run_crc_mod = read_u32(&row_clone);
			}
			say_debug("%s: run_crc_log/mod: 0x%x/0x%x", __func__, run_crc_log, run_crc_mod);
			break;
		/* remove case snap_tag with io12_hack */
		case snap_tag:
			scn = row_scn;
			break;
		case snap_final_tag:
			assert(row_scn > 0);
			scn = row_scn;
			break;
		case nop:
			lag = ev_now() - tm;
			last_update_tstamp = ev_now();
			scn = row_scn;
			break;
		case run_crc:
			assert(row_scn == scn + 1);
			u32 log = read_u32(&row_clone);
			u32 mod = read_u32(&row_clone);
			if (run_crc_log != log) {
				run_crc_log_mismatch |= 1;
				say_crit("run_crc_log mismatch: saved:0x%08x computed:0x%08x",
					 log, run_crc_log);
			}
			if (run_crc_mod != mod) {
				run_crc_mod_mismatch |= 1;
				say_crit("run_crc_mod mismatch: saved:0x%08x computed:0x%08x",
					 mod, run_crc_mod);
			}
			say_debug("%s: verified run_crc_log:0x%08x run_crc_mod:0x%08x", __func__, log, mod);

			scn = row_scn;
			lag = ev_now() - tm;
			last_update_tstamp = run_crc_verify_tstamp = ev_now();
			break;
		default:
			break;
		}
	}
	@catch (Error *e) {
		say_error("Recovery: %s at %s:%i", e->reason, e->file, e->line);
		@throw;
	}
}

- (void)
wal_final_row
{
}

- (i64)
recover_snap
{
	XLog *snap = nil;
	struct tbuf *row;

	struct palloc_pool *saved_pool = fiber->pool;
	@try {
		i64 snap_lsn = [snap_dir greatest_lsn];
		if (snap_lsn == -1)
			raise("snap_dir reading failed");

		if (snap_lsn < 1)
			return 0;

		snap = [snap_dir open_for_read:snap_lsn];
		if (snap == nil)
			raise("can't find/open snapshot");

		say_info("recover from `%s'", snap->filename);

		fiber->pool = snap->pool;

		if ([snap isKindOf:[XLog11 class]])
			[self recover_row:[self dummy_row_lsn:0
							  scn:0
							  tag:snap_initial_tag]];
		while ((row = [snap fetch_row])) {
			if (unlikely(row_v12(row)->tag == snap_final_tag)) {
				[self recover_row:row];
				continue;
			}
			[self recover_row:row];
			prelease_after(snap->pool, 128 * 1024);
		}

		/* old v11 snapshot, scn == lsn from filename */
		if ([snap isKindOf:[XLog11 class]])
			[self recover_row:[self dummy_row_lsn:snap_lsn
							  scn:snap_lsn
							  tag:snap_final_tag]];

		if (![snap eof])
			raise("unable to fully read snapshot");
	}
	@finally {
		fiber->pool = saved_pool;
		[snap close];
		snap = nil;
	}
	say_info("snapshot recovered, lsn:%"PRIi64 " scn:%"PRIi64, lsn, scn);
	return lsn;
}

- (void)
recover_wal:(id<XLogPuller>)l
{
	struct tbuf *row = NULL;

	struct palloc_pool *saved_pool = fiber->pool;
	fiber->pool = [l pool];
	@try {
		while ((row = [l fetch_row])) {
			if (row_v12(row)->lsn > lsn) {
				last_wal_lsn = row_v12(row)->lsn;
				[self recover_row:row];
			}
			prelease_after(fiber->pool, 128 * 1024);
		}
	}
	@finally {
		fiber->pool = saved_pool;
	}
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
                        say_warn("wal `%s' wasn't correctly closed, lsn:%"PRIi64" scn:%"PRIi64,
				 current_wal->filename, lsn, scn);
                        [current_wal close];
                        current_wal = nil;
		}

		current_wal = [self next_wal];
		if (current_wal == nil)
			break;
		if (!current_wal->valid) /* unable to read & parse header */
			break;

		say_info("recover from `%s'", current_wal->filename);
	recover_current_wal:
		[self recover_wal:current_wal];
		if ([current_wal rows] == 0) /* either broken wal or empty inprogress */
			break;

		if ([current_wal eof]) {
			say_info("done `%s' lsn:%"PRIi64" scn:%"PRIi64,
				 current_wal->filename, lsn, scn);

			[current_wal close];
			current_wal = nil;
		}
		fiber_gc();
	}
	fiber_gc();

	/*
	 * It's not a fatal error when last WAL is empty,
	 * but if it's in the middle then we lost some logs.
	 */
	if (wal_greatest_lsn > lsn + 1)
		raise("not all WALs have been successfully read! "
		      "greatest_lsn:%"PRIi64" lsn:%"PRIi64" diff:%"PRIi64,
		      wal_greatest_lsn, lsn, wal_greatest_lsn - lsn);
}

- (i64)
recover_cont
{
	if (current_wal != nil)
		say_info("recover from `%s'", current_wal->filename);

	[self recover_remaining_wals];
	say_info("wals recovered, lsn:%"PRIi64" scn:%"PRIi64, lsn, scn);

	[self recover_follow:cfg.wal_dir_rescan_delay]; /* FIXME: make this conf */
	strcpy(status, "hot_standby/local");

	/* all curently readable wal rows were read, notify about that */
	if (feeder_addr == NULL || cfg.local_hot_standby)
		[self wal_final_row];

	return lsn;
}

- (i64)
recover_start
{
	say_info("local recovery start");
	[self recover_snap];
	if (scn == 0)
		return 0;
	/*
	 * just after snapshot recovery current_wal isn't known
	 * so find wal which contains record with next lsn
	 */
	current_wal = [wal_dir containg_lsn:lsn + 1];
	[self recover_cont];
	if (last_wal_lsn && last_wal_lsn < lsn)
		raise("Snapshot LSN is greater then last WAL LSN");
	return lsn;
}


static void follow_file(ev_stat *, int);

static void
follow_dir(ev_timer *w, int events __attribute__((unused)))
{
	Recovery *r = w->data;
	[r recover_remaining_wals];

	if (r->current_wal == nil)
		return;

	if (r->current_wal->inprogress && [r->current_wal rows] > 1)
		[r->current_wal reset_inprogress];

	[r->current_wal follow:follow_file];
}

static void
follow_file(ev_stat *w, int events __attribute__((unused)))
{
	Recovery *r = w->data;
	[r recover_wal:r->current_wal];
	if ([r->current_wal eof]) {
		say_info("done `%s' lsn:%"PRIi64" scn:%"PRIi64,
			 r->current_wal->filename, r->lsn, [r scn]);
		[r->current_wal close];
		r->current_wal = nil;
		follow_dir((ev_timer *)w, 0);
		return;
	}

	if (r->current_wal->inprogress && [r->current_wal rows] > 1) {
		[r->current_wal reset_inprogress];
		[r->current_wal follow:follow_file];
	}
}

- (void)
recover_follow:(ev_tstamp)wal_dir_rescan_delay
{
	ev_timer_init(&wal_timer, follow_dir,
		      wal_dir_rescan_delay, wal_dir_rescan_delay);
	ev_timer_start(&wal_timer);
	if (current_wal != nil)
		[current_wal follow:follow_file];
}

- (void)
recover_finalize
{
	ev_timer_stop(&wal_timer);
	if (current_wal != nil)
		ev_stat_stop(&current_wal->stat);

	[self recover_remaining_wals];

	if (current_wal != nil && current_wal->inprogress) {
		if ([current_wal rows] < 1) {
			say_warn("%s: Removed broken WAL %s", __func__, current_wal->filename);
			[current_wal inprogress_unlink];
			[current_wal close];
			current_wal = nil;
		}
	}

	if (current_wal != nil)
                say_warn("wal `%s' wasn't correctly closed", current_wal->filename);

        [current_wal close];
        current_wal = nil;
}


static void
pull_snapshot(Recovery *r, id<XLogPuller> puller)
{
	struct tbuf *row;
	for (;;) {
		while ((row = [puller fetch_row])) {
			switch (row_v12(row)->tag) {
			case snap_initial_tag:
			case snap_tag:
				[r recover_row:row];
				break;
			case snap_final_tag:
				[r recover_row:row];
				[r configure_wal_writer];
				say_debug("saving snapshot");
				if (save_snapshot(NULL, 0) != 0)
					raise("replication failure: failed save snapshot");
				return;
			default:
				raise("unexpected tag %i/%s",
				      row_v12(row)->tag, xlog_tag_to_a(row_v12(row)->tag));
			}

		}
		fiber_gc();
	}
}

static void
pull_wal(Recovery *r, XLogPuller *puller, int exit_on_eof)
{
	struct tbuf *row, *special_row = NULL, *rows[WAL_PACK_MAX];
	struct wal_pack *pack;
	/* TODO: use designated palloc_pool */
	say_debug("%s: scn:%"PRIi64, __func__, [r scn]);

	for (;;) {
		int pack_rows = 0;
		while ((row = [puller fetch_row])) {
			if (row_v12(row)->tag == wal_final_tag) {
				special_row = row;
				break;
			}

			if (row_v12(row)->tag != wal_tag &&
			    row_v12(row)->tag != run_crc &&
			    row_v12(row)->tag != nop)
				continue;

			if (cfg.io12_hack && row_v12(row)->lsn > 0)
				row_v12(row)->scn = row_v12(row)->lsn;

			if (row_v12(row)->scn <= [r scn])
				continue;

			if (cfg.io_compat) {
				if (row_v12(row)->tag == run_crc)
					continue;
			}

			rows[pack_rows++] = row;
			if (pack_rows == WAL_PACK_MAX)
				break;
		}

		if (pack_rows > 0) {
			i64 pack_min_scn = row_v12(rows[0])->scn,
			    pack_max_scn = row_v12(rows[pack_rows - 1])->scn;

			assert([r scn] == pack_min_scn - 1);
			@try {
				for (int j = 0; j < pack_rows; j++) {
					row = rows[j];
					[r recover_row:tbuf_clone(fiber->pool, row)];
				}
			}
			@catch (id e) {
				panic("Replication failure: remote row LSN:%"PRIi64 " SCN:%"PRIi64,
				      row_v12(row)->lsn, row_v12(row)->scn);
			}

			int confirmed = 0;
			while (confirmed != pack_rows) {
				pack = [r wal_pack_prepare];
				for (int i = confirmed; i < pack_rows; i++) {
					row = rows[i];
					[r wal_pack_append:pack
						      data:row_v12(row)->data
						       len:row_v12(row)->len
						       scn:row_v12(row)->scn
						       tag:row_v12(row)->tag
						    cookie:row_v12(row)->cookie];
				}
				confirmed += [r wal_pack_submit];
				if (confirmed != pack_rows) {
					say_warn("WAL write failed confirmed:%i != sent:%i",
						 confirmed, pack_rows);
					fiber_sleep(0.05);
				}
			}
			assert([r scn] == pack_max_scn);
		}

		if (special_row) {
			[r wal_final_row];
			special_row = NULL;
			if (exit_on_eof)
				return;
		}

		fiber_gc();
	}
}

- (void)
recover_follow_remote:(struct sockaddr_in *)addr exit_on_eof:(int)exit_on_eof
{
	for (;;) {
		XLogPuller *puller = nil;
		@try {
			const char *err;
			bool warning_said = false;
			i64 want_scn = scn, remote_scn = 0;
			puller = [[XLogPuller alloc] init_addr:addr];
			if (want_scn > 0) {
				want_scn -= 1024;
				if (want_scn < 1)
					want_scn = 1;
			}
			while ((remote_scn = [puller handshake:want_scn err:&err]) < 0) {
				if (exit_on_eof)
					return;

				/* no more WAL rows in near future, notify module about that */
				[self wal_final_row];

				ev_tstamp reconnect_delay = 0.5;
				if (!warning_said) {
					say_error("%s", err);
					say_info("will retry every %.2f second", reconnect_delay);
					warning_said = true;
				}
				fiber_sleep(reconnect_delay);
			}

			if (lsn == 0)
				pull_snapshot(self, puller);

			if ([puller version] == 11)
				[self wal_final_row];

			pull_wal(self, puller, exit_on_eof);
			if (exit_on_eof)
				return;
		}
		@catch (Error *e) {
			say_error("replication failure: %s", e->reason);
			fiber_sleep(1);
			fiber_gc();
		}
		@finally {
			[puller free];
			puller = nil;
		}
	}
}

static void
pull_from_remote_trampoline(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);
	struct sockaddr_in *addr = va_arg(ap, struct sockaddr_in *);
	[r recover_follow_remote:addr exit_on_eof:false];
}

- (struct fiber *)
recover_follow_remote_async:(struct sockaddr_in *)addr;
{
	char *name = malloc(64);
	snprintf(name, 64, "remote_hot_standby/%s", sintoa(addr));

	remote_puller = fiber_create(name, pull_from_remote_trampoline, self, addr);
	if (remote_puller == NULL) {
		free(name);
		return NULL;
	}

	return remote_puller;
}

- (void)
enable_local_writes
{
	say_debug("%s", __func__);
	[self recover_finalize];
	local_writes = true;

	if (feeder_addr != NULL) {
		if (lsn > 0) /* we're already have some xlogs and recovered from them */
			[self configure_wal_writer];

		struct sockaddr_in *sin = malloc(sizeof(*sin));
		if (atosin(feeder_addr, sin) == -1 || sin->sin_addr.s_addr == INADDR_ANY)
			panic("bad feeder addr: `%s'", feeder_addr);

		if ([self recover_follow_remote_async:sin] == NULL)
			panic("unable to start remote hot standby fiber");

		say_info("starting remote hot standby");
		snprintf(status, sizeof(status), "hot_standby/%s", feeder_addr);
	} else {
		[self configure_wal_writer];
		say_info("I am primary");
		strcpy(status, "primary");
	}
}

- (bool)
is_replica
{
	if (!local_writes)
		return true;
	if (feeder_addr != NULL)
		return true;
	return false;
}

- (int)
submit:(void *)data len:(u32)data_len scn:(i64)scn_ tag:(u16)tag
{
	if ([self is_replica])
		raise("replica is readonly");

	return [super submit:data len:data_len scn:scn_ tag:tag];
}

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
{
	snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
	wal_dir = [[WALDir alloc] init_dirname:wal_dirname];

	snap_dir->writer = self;
	wal_dir->writer = self;
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

		submit_tstamp = ev_now();
		lsn = [recovery lsn];
		[recovery submit_run_crc];
	}
}

static void
nop_hb_writer(va_list ap)
{
	Recovery *recovery = va_arg(ap, Recovery *);
	ev_tstamp submit_tstamp = ev_now(), delay = va_arg(ap, ev_tstamp);
	char body[2] = {0};

	for (;;) {
		fiber_sleep(delay);
		if ([recovery is_replica])
			continue;

		submit_tstamp = ev_now();
		[recovery submit:body len:nelem(body) scn:0 tag:nop];
	}
}


- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
        rows_per_wal:(int)wal_rows_per_file
	 feeder_addr:(const char *)feeder_addr_
         fsync_delay:(double)wal_fsync_delay
       run_crc_delay:(double)run_crc_delay
	nop_hb_delay:(double)nop_hb_delay
               flags:(int)flags
  snap_io_rate_limit:(int)snap_io_rate_limit_
{
	/* Recovery object is never released */

        snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
        wal_dir = [[WALDir alloc] init_dirname:wal_dirname];

	snap_dir->writer = self;
	wal_dir->writer = self;
	wal_timer.data = self;

	if ((flags & RECOVER_READONLY) == 0) {
		if (wal_rows_per_file <= 4)
			panic("inacceptable value of 'rows_per_file'");

		wal_dir->rows_per_file = wal_rows_per_file;
		wal_dir->fsync_delay = wal_fsync_delay;
		snap_io_rate_limit = snap_io_rate_limit_ * 1024 * 1024;

		struct fiber *wal_out = fiber_create("wal_writer/output_flusher", service_output_flusher);
		struct fiber *wal_in = fiber_create("wal_writer/input_dispatcher",
							wal_disk_writer_input_dispatch);
		wal_writer = spawn_child("wal_writer", wal_in, wal_out, wal_disk_writer, self);
		if (!wal_writer)
			panic("unable to start WAL writer");

		ev_set_priority(&wal_writer->c->in, 1);
		ev_set_priority(&wal_writer->c->out, 1);
		ev_io_start(&wal_writer->c->in);

		if (!cfg.io_compat && run_crc_delay > 0)
			fiber_create("run_crc", run_crc_writer, self, run_crc_delay);

		if (!cfg.io_compat && nop_hb_delay > 0)
			fiber_create("nop_hb", nop_hb_writer, self, nop_hb_delay);
	}

	if (feeder_addr_ != NULL) {
		feeder_addr = feeder_addr_;
		say_crit("configuring remote hot standby, WAL feeder %s", feeder_addr);
	}

	return self;
}

@end

register_source();
