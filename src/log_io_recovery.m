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

@implementation Recovery
+ (id)
alloc
{
	if (strcmp([[self class] name], "Recovery") != 0) /* break recursion */
	    goto ours;

#ifdef PAXOS
	if (cfg.paxos_enabled)
		return [PaxosRecovery alloc];
#endif

ours:
	return [super alloc];
}

- (id)
init
{
	[super init];
	mbox_init(&run_crc_mbox);
	reader = [[XLogReader alloc] init_recovery:self];
	return self;
}

- (id<RecoveryClient>) client { return client; }
- (const char *) status { return status_buf; }
- (ev_tstamp) lag { return lag; }
- (ev_tstamp) last_update_tstamp { return last_update_tstamp; }

- (i64) lsn {
	if (unlikely(initial_snap)) return 1;
	if (writer) return [writer lsn];
	if (reader) return [reader lsn];
	return -1;
}
- (i64) scn { return scn; }
- (u32) run_crc_log { return run_crc_log; }
- (bool) local_writes { return local_writes; }
- (void) update_state_rci:(const struct row_commit_info *)rci count:(int)count
{
	for (int i = 0; i < count; i++, rci++) {
		if (cfg.sync_scn_with_lsn && rci->lsn != rci->scn)
			panic("out ouf sync SCN:%"PRIi64 " != LSN:%"PRIi64,
			      rci->scn, rci->lsn);

		/* only TAG_WAL rows affect scn & run_crc */
		if (scn_changer(rci->tag)) {
			scn = rci->scn;
			run_crc_log = rci->run_crc;
			run_crc_record(&run_crc_state, rci->tag, rci->scn, rci->run_crc);
		}
	}
}
- (void) update_state_r:(const struct row_v12 *)r
{
	if (cfg.sync_scn_with_lsn && r->lsn != r->scn)
		panic("out of sync SCN:%"PRIi64 " != LSN:%"PRIi64, r->scn, r->lsn);

	last_update_tstamp = ev_now();
	lag = last_update_tstamp - r->tm;

	int tag = r->tag & TAG_MASK;

	if (scn_changer(r->tag) || tag == snap_final) {
		if (unlikely(tag != snap_final && r->scn - scn != 1 &&
			     cfg.panic_on_scn_gap && [[self class] name] == [Recovery name]))
			panic("non consecutive SCN %"PRIi64 " -> %"PRIi64, scn, r->scn);

		scn = r->scn;
		run_crc_record(&run_crc_state, r->tag, scn, run_crc_log);
	}
}

- (XLogWriter *) writer { return writer; }
- (const struct child *) wal_writer { return [[self writer] wal_writer]; }

- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	static unsigned count;
	static struct msg_void_ptr msg;
	if (++count % 32 == 0 && msg.link.tqe_prev == NULL)
		mbox_put(&run_crc_mbox, &msg, link);

	if (cfg.wal_writer_inbox_size == 0) {
		scn++;
		return 1;
	}

	if (!local_writes || status == REMOTE_STANDBY) {
		say_warn("local writes disabled");
		return 0;
	}

	return [writer submit:data len:len tag:tag];
}


- (void)
apply_sys:(const struct row_v12 *)r
{
	int tag = r->tag & TAG_MASK;

	switch (tag) {
	case snap_initial:
		assert(snap_lsn == 0 || r->lsn == snap_lsn);
		if (r->len == sizeof(u32) * 3) { /* not a dummy row */
			struct tbuf buf = TBUF(r->data, r->len, NULL);
			estimated_snap_rows = read_u32(&buf);
			run_crc_log = read_u32(&buf);
			(void)read_u32(&buf); /* ignore run_crc_mod */
		}
			/* set initial lsn & scn, otherwise gap check below will fail */
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

		run_crc_verify(&run_crc_state, &TBUF(r->data, r->len, NULL));
		break;
	}
}

- (void)
recover_row:(struct row_v12 *)r
{
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

		run_crc_calc(&run_crc_log, r->tag, r->data, r->len);

		if (r->scn == next_skip_scn) {
			say_info("skip SCN:%"PRIi64 " tag:%s", next_skip_scn, xlog_tag_to_a(r->tag));

			/* there are multiply rows with same SCN in paxos mode.
			   the last one is WAL row */
			if (tag_type == TAG_WAL)
				next_skip_scn = tbuf_len(&skip_scn) > 0 ?
						read_u64(&skip_scn) : 0;
		} else {
			[client apply:&TBUF(r->data, r->len, fiber->pool) tag:r->tag];
			if (tag_type == TAG_SYS)
				[self apply_sys:r];
		}

		[self update_state_r:r];

		if (unlikely(fold_scn)) {
			if (r->scn == fold_scn && (r->tag & ~TAG_MASK) == TAG_WAL) {
				if ([(id)client respondsTo:@selector(snapshot_fold)])
					exit([(id)client snapshot_fold]);
				exit([snap_writer snapshot_write]);
			}
		}
	}
	@catch (Error *e) {
		say_error("Recovery: %s at %s:%i\n%s", e->reason, e->file, e->line,
				e->backtrace);
		struct tbuf *out = tbuf_alloc(fiber->pool);
		[client print:r into:out];
		printf("Failed row: %.*s\n", tbuf_len(out), (char *)out->ptr);

		@throw;
	}
	@finally {
		say_debug("%s: => LSN:%"PRIi64" SCN:%"PRIi64, __func__, [self lsn], scn);
	}
}


- (void)
wal_final_row
{
	if (unlikely(fold_scn)) {
		say_error("unable to find record with SCN:%"PRIi64, fold_scn);
		exit(EX_OSFILE);
	}
	[client wal_final_row];
	title(NULL);
}

- (i64)
load_from_local
{
	if (fold_scn)
		snap_lsn = [snap_dir containg_scn:fold_scn]; /* select snapshot before desired scn */
	i64 local_lsn = [reader load_from_local:0];
	/* loading is faster until wal_final_row called because service is not yet initialized and
	   only pk indexes must be updated. remote feeder will send wal_final_row then all remote
	   rows are read */
	if (![remote feeder_addr_configured])
		[self wal_final_row];
	return local_lsn;
}

void
wal_lock(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);

	while ([wal_dir lock] != 0)
		fiber_sleep(1);

	[r enable_local_writes];
}

- (void)
simple
{
	i64 local_lsn = [self load_from_local];
	if (local_lsn == 0) {
		if (![remote feeder_addr_configured]) {
			say_error("unable to find initial snapshot");
			say_info("don't you forget to initialize "
				 "storage with --init-storage switch?");
			exit(EX_USAGE);

		}
	}
	if (cfg.local_hot_standby) {
		[reader local_hot_standby];
		fiber_create("wal_lock", wal_lock, self);
	} else {
		[self enable_local_writes];
	}
}

- (bool)
feeder_changed:(struct feeder_param*)new
{
	return [remote feeder_changed:new];
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
}

- (void)
enable_local_writes
{
	[self lock];
	i64 reader_lsn = [reader recover_finalize];
	[reader free];
	reader = nil;

	free(skip_scn.pool);
	skip_scn = TBUF(NULL, 0, NULL);
	local_writes = true;


	i64 writer_lsn = reader_lsn;
	if (reader_lsn == 0) {
		assert([remote feeder_addr_configured]);

		i64 remote_scn = [remote load_from_remote];
		if (remote_scn <= 0)
			raise_fmt("unable to pull initial snapshot");
		if (cfg.sync_scn_with_lsn)
			writer_lsn = remote_scn;
		else
			writer_lsn = 1;
	}

	[self configure_wal_writer:writer_lsn];

	if (reader_lsn == 0) {
		say_debug("Saving initial replica snapshot LSN:%"PRIi64, reader_lsn);
		/* don't wait for snapshot. our goal to be replica as fast as possible */
		[self fork_and_snapshot:(getenv("SYNC_DUMP") == NULL)];
	}
	[remote hot_standby];

	if (![remote feeder_addr_configured])
		[self status_update:PRIMARY fmt:"primary"];
}

- (struct sockaddr_in *)
primary_addr
{
	static struct sockaddr_in addr;
	if (local_writes || ![remote feeder_addr_configured] || !cfg.wal_feeder_primary_port)
		return NULL;

	addr = [remote feeder_addr];
	addr.sin_port = htons(cfg.wal_feeder_primary_port);
	return &addr;
}

- (bool)
is_replica
{
	if (!local_writes)
		return true;
	if ([remote feeder_addr_configured])
		return true;
	return false;
}

- (void)
check_replica
{
	if ([self is_replica])
		raise_fmt("replica is readonly");
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

- (ev_tstamp)
run_crc_lag
{
	return run_crc_lag(&run_crc_state);
}

- (const char *)
run_crc_status
{
	return run_crc_status(&run_crc_state);
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

		if ([recovery is_replica])
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

- (id) init_feeder_param:(struct feeder_param*)feeder_
{
	/* Recovery object is never released */
	[self init];
	snap_writer = [[SnapWriter alloc] init_state:self];
	remote = [[XLogReplica alloc] init_recovery:self
					     feeder:feeder_];

	return self;
}

- (void)
configure_wal_writer:(i64)lsn
{
	assert(reader == nil || [reader lsn] > 0);
	if (cfg.wal_writer_inbox_size == 0 || fold_scn)
		return;

	writer = [[XLogWriter alloc] init_lsn:lsn
					state:self];

	if (!cfg.io_compat && cfg.run_crc_delay > 0)
		fiber_create("run_crc", run_crc_writer, self, cfg.run_crc_delay);

	if (!cfg.io_compat && cfg.nop_hb_delay > 0)
		fiber_create("nop_hb", nop_hb_writer, self, cfg.nop_hb_delay);
}


- (void)
status_update:(enum recovery_status)new_status fmt:(const char *)fmt, ...
{
	char buf[sizeof(status_buf)];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (strcmp(buf, status_buf) == 0 && new_status == status)
		return;

	say_info("recovery status: %i %s", new_status, buf);
	strncpy(status_buf, buf, sizeof(status_buf));
	title(NULL);

	if (new_status == status)
		return;

	prev_status = status;
	status = new_status;
	[self status_changed];
}

- (void)
status_changed
{
	[client status_changed];
}

- (void)
set_client:(id<RecoveryClient>)obj
{
	client = obj;
}


- (int)
write_initial_state
{
	initial_snap = true;
	scn = 1;
	return [snap_writer snapshot_write];
}

- (int)
fork_and_snapshot:(bool)wait
{
	pid_t p;

	switch ((p = tnt_fork())) {
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
			write(fd, "900\n", 4);
			close(fd);
		}
		int r = [snap_writer snapshot_write];

#ifdef COVERAGE
		__gcov_flush();
#endif
		_exit(r != 0 ? errno : 0);

	default: /* parent, may wait for child */
		return wait ? wait_for_child(p) : 0;
	}
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

	tbuf_printf(buf, "lsn:%" PRIi64 " scn:%" PRIi64 " tm:%.3f t:%s %s ",
		    row->lsn, row->scn, row->tm,
		    xlog_tag_to_a(row->tag),
		    sintoa((void *)&row->cookie));

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
		}
		break;
	case snap_skip_scn:
		while (tbuf_len(&row_data) > 0)
			tbuf_printf(buf, "%"PRIi64" ", read_u64(&row_data));
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
	case snap_final:
	case nop:
		break;

	case paxos_prepare:
	case paxos_promise:
	case paxos_nop:
		ballot = read_u64(&row_data);
		tbuf_printf(buf, "ballot:%"PRIi64, ballot);
		break;
	case paxos_propose:
	case paxos_accept:
		ballot = read_u64(&row_data);
		inner_tag = read_u16(&row_data);
		value_len = read_u32(&row_data);
		(void)value_len;
		assert(value_len == tbuf_len(&row_data));
		tbuf_printf(buf, "ballot:%"PRIi64" it:%s ", ballot, xlog_tag_to_a(inner_tag));

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
		hexdump(buf, row->tag, &row_data);
	}
}


register_source();
