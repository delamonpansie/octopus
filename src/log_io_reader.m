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
#import <palloc.h>
#import <say.h>
#import <fiber.h>
#import <log_io.h>

@implementation XLogReader
- (i64) lsn { return lsn; }

- (id)
init_recovery:(Recovery *)recovery_
{
	recovery = recovery_;
	wal_timer.data = self;
	return self;
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
			[recovery recover_row:row];

			if (unlikely(row->lsn - lsn > 1 && cfg.panic_on_lsn_gap))
				panic("LSN sequence has gap after %"PRIi64 " -> %"PRIi64, lsn, row->lsn);
			lsn = row->lsn;

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

i64 snap_lsn = 0;
- (i64)
recover_snap
{
	XLog *snap = nil;

	@try {
		palloc_register_cut_point(fiber->pool);

		if (snap_lsn == 0)
			snap_lsn = [snap_dir greatest_lsn];
		if (snap_lsn == -1)
			raise_fmt("snap_dir reading failed");

		if (snap_lsn < 1)
			return 0;

		snap = [snap_dir open_for_read:snap_lsn];
		if (snap == nil)
			raise_fmt("can't find/open snapshot");

		say_info("recover from `%s'", snap->filename);

		bool legacy_snap = ![snap isKindOf:[XLog12 class]];

		if (legacy_snap && !cfg.sync_scn_with_lsn)
			panic("sync_scn_with_lsn is required when loading from v11 snapshots");

		lsn = snap_lsn;

		if (legacy_snap)
			[recovery recover_row:dummy_row(snap_lsn, snap_lsn, snap_initial|TAG_SYS)];

		[self recover_row_stream:snap];

		/* old v11 snapshot, scn == lsn from filename */
		if (legacy_snap)
			[recovery recover_row:dummy_row(snap_lsn, snap_lsn, snap_final|TAG_SYS)];

		if (![snap eof])
			raise_fmt("unable to fully read snapshot");

		say_info("close `%s' LSN:%"PRIi64, snap->filename, lsn);
	}
	@finally {
		palloc_cutoff(fiber->pool);
		[snap close];
		snap = nil;
		snap_lsn = 0;
	}
	say_info("snapshot recovered, LSN:%"PRIi64, lsn);
	return lsn;
}

- (void)
close_current_wal
{
	if (current_wal == nil)
		return;

	if (![current_wal eof])
		say_warn("WAL `%s' wasn't correctly closed", current_wal->filename);
	say_info("close `%s' LSN:%"PRIi64, current_wal->filename, lsn);
	[current_wal close];
	current_wal = nil;
}

- (XLog *)
open_next_wal
{
	[self close_current_wal];

	current_wal = [wal_dir open_for_read:lsn + 1];
	if (current_wal != nil)
		say_info("recover from `%s'", current_wal->filename);
	return current_wal;
}

/*
 * this function will not close current_wal if recovery was successful
 */
- (void)
recover_remaining_wals
{
	assert(lsn > 0);
	say_debug("%s: LSN:%"PRIi64, __func__, lsn);
	i64 wal_greatest_lsn = [wal_dir greatest_lsn];
	if (wal_greatest_lsn == -1)
		raise_fmt("wal_dir reading failed");

	/* if the caller already opened WAL for us, recover from it first */
	if (current_wal != nil) {
		say_debug("%s: current_wal:%s", __func__, current_wal->filename);
		[self recover_row_stream:current_wal];
	}

	while (lsn < wal_greatest_lsn) {
		if ([self open_next_wal] == nil) /* either no more WALs or current one is broken */
			break;

		[self recover_row_stream:current_wal];
	}
	fiber_gc();

	/* empty WAL or borken header encountered: unable to parse remaining WALs */
	if (wal_greatest_lsn > lsn)
		raise_fmt("not all WALs have been successfully read "
			  "greatest_LSN:%"PRIi64" LSN:%"PRIi64" diff:%"PRIi64,
			  wal_greatest_lsn, lsn, wal_greatest_lsn - lsn);
}


- (i64)
load_from_local:(i64)initial_lsn
{
	say_debug("%s: initial_LSN:%li", __func__, initial_lsn);

	if ([wal_dir greatest_lsn] == 0 && [snap_dir greatest_lsn] == 0) {
		say_info("local state is empty: no snapshot and xlog found");
		return 0;
	}

	say_info("local recovery start");
	if ([recovery respondsTo:@selector(status_update:fmt:)])
		[recovery status_update:LOADING fmt:"loading/local"];

	i64 snap_lsn;
	if (initial_lsn == 0) {
		snap_lsn = [self recover_snap];
		assert(lsn > 0);

		/*
		 * just after snapshot recovery current_wal isn't known
		 * so find wal which contains record with _next_ lsn
		 */
		current_wal = [wal_dir containg_lsn:lsn + 1];
	} else {
		assert(initial_lsn > 1);
		lsn = initial_lsn - 1; /* since initial_lsn is > 1, lsn is >= 1
					  valid lsn is vital for [recover_follow]: [open_next_wal] is relies on valid LSN */
		current_wal = [wal_dir containg_lsn:initial_lsn];
		say_info("unable to find WAL with LSN:%li, greatest_LSN:%li", initial_lsn, [wal_dir greatest_lsn]);
		if (current_wal == nil)
			return 0;
	}

	if (current_wal != nil)
		say_info("recover from `%s'", current_wal->filename);
	[self recover_remaining_wals];
	say_info("WALs recovered, LSN:%"PRIi64, lsn);

	if (snap_lsn == lsn &&
	    current_wal != nil && /* loading from standalone snapshot is a special case: usefull for debugging */
	    [current_wal last_read_lsn] < snap_lsn)
		raise_fmt("last WAL is missing or truncated: snapshot LSN:%li > last WAL row LSN:%li",
			  snap_lsn, [current_wal last_read_lsn]);

	return lsn;
}


static void follow_file(ev_stat *, int);

static void
follow_dir(ev_timer *w, int events __attribute__((unused)))
{
	XLogReader *reader = w->data;
	static int tick = 5;
	/* reread directory 5 times faster if current_wal is unknown.
	   e.g. avoid feeder start delay after initial loading */
	if (reader->current_wal && tick-- > 0)
		return;
	tick = 5;
	say_debug2("%s: current_wal:%s", __func__, reader->current_wal ? reader->current_wal->filename : NULL);
	[reader recover_remaining_wals];
	[reader->current_wal follow:follow_file data:reader];
}

static void
follow_file(ev_stat *w, int events __attribute__((unused)))
{
	XLogReader *reader = w->data;
	say_debug2("%s: current_wal:%s", __func__, reader->current_wal ? reader->current_wal->filename : NULL);
	[reader recover_row_stream:reader->current_wal];
	if ([reader->current_wal eof]) {
		say_info("done `%s' LSN:%"PRIi64,
			 reader->current_wal->filename, [reader lsn]);
		[reader close_current_wal];
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


- (i64)
recover_finalize
{
	ev_timer_stop(&wal_timer);
	/* [currert_wal follow] cb will be stopped by [current_wal close] called by [self close_current_wal] */

	if (lsn > 0) {
		[self recover_remaining_wals];
		[self close_current_wal];
	} else {
		assert(current_wal == nil);
	}
	return lsn;
}

- (void)
local_hot_standby
{
	[self recover_follow:cfg.wal_dir_rescan_delay];
	if ([recovery respondsTo:@selector(status_update:fmt:)])
		[recovery status_update:LOCAL_STANDBY fmt:"hot_standby/local"];
}

- (id)
free
{
	ev_timer_stop(&wal_timer);
	[self close_current_wal];
	return [super free];
}

@end

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

register_source();
