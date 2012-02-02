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

#include <third_party/crc32.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <poll.h>
#include <sysexits.h>
#include <netinet/in.h>
#include <arpa/inet.h>

@implementation Recovery

- (i64)lsn { return lsn; }
- (i64)scn { return scn; }
- (const char *)status { return status; };
- (ev_tstamp)lag { return lag; };
- (ev_tstamp)last_update_tstamp { return last_update_tstamp; };
- (struct child *)wal_writer { return wal_writer; };


- (void)
initial_lsn:(i64)new_lsn
{
        lsn = new_lsn;
}


/* this little hole shouldn't be used too much */
int
read_log(const char *filename, row_handler *xlog_handler, row_handler *snap_handler, void *state)
{
	XLog *l;
	struct tbuf *row;
	row_handler *h = NULL;


	if (strstr(filename, ".xlog")) {
                XLogDir *dir = [[WALDir alloc] init_dirname:NULL];
		l = [dir open_for_read_filename:filename];
		h = xlog_handler;
	} else if (strstr(filename, ".snap")) {
                XLogDir *dir = [[SnapDir alloc] init_dirname:NULL];
                l = [dir open_for_read_filename:filename];
		h = snap_handler;
	} else {
		say_error("don't know what how to read `%s'", filename);
		return -1;
	}

	fiber->pool = l->pool;
	while ((row = [l next_row])) {
		h(state, row);
		prelease_after(l->pool, 128 * 1024);
	}

	if (!l->eof) {
		say_error("binary log `%s' wasn't correctly closed", filename);
		return -1;
	}
	return 0;
}


- (void)
validate_row:(struct tbuf *)row
{
	u16 tag = row_v12(row)->tag;
	if (tag == wal_tag && row_v12(row)->lsn != lsn + 1)
		raise("lsn sequence has gap after %"PRIi64 " -> %"PRIi64,
		      lsn, row_v12(row)->lsn);
}

- (struct tbuf *)
dummy_row_lsn:(i64)lsn_ tag:(u16)tag
{
	struct tbuf *b = tbuf_alloc(fiber->pool);
	tbuf_ensure(b, sizeof(struct row_v12));
	b->len = sizeof(struct row_v12);

	row_v12(b)->scn = scn;
	row_v12(b)->lsn = lsn_;
	row_v12(b)->tm = ev_now();
	row_v12(b)->tag = tag;
	row_v12(b)->cookie = default_cookie;
	row_v12(b)->len = 0;

	return b;
}


- (i64)
recover_snap
{
	XLog *snap = nil;
	struct tbuf *row;
	i64 max_snap_lsn;

	struct palloc_pool *saved_pool = fiber->pool;
	@try {
		max_snap_lsn = [snap_dir greatest_lsn];
		if (max_snap_lsn == -1)
			raise("snap_dir reading failed");

		if (max_snap_lsn < 1)
			return 0;

		snap = [snap_dir open_for_read:max_snap_lsn];
		if (snap == nil)
			raise("can't find/open snapshot");

		say_info("recover from `%s'", snap->filename);

		fiber->pool = snap->pool;

		if ([snap isKindOf:[XLog11 class]])
			[self recover_row:[self dummy_row_lsn:max_snap_lsn
							  tag:snap_initial_tag]];

		while ((row = [snap next_row])) {
			[self validate_row:row];
			[self recover_row:row];
			prelease_after(snap->pool, 128 * 1024);
		}

		if (!snap->eof)
			raise("unable to fully read snapshot");

		[self recover_row:[self dummy_row_lsn:lsn tag:snap_final_tag]];

		say_info("snapshot recovered, lsn:%" PRIi64, lsn);
	}
	@finally {
		fiber->pool = saved_pool;
		[snap close];
		snap = nil;
	}
	return lsn;
}

- (void)
recover_wal:(XLog *)l
{
	struct tbuf *row = NULL;

	struct palloc_pool *saved_pool = fiber->pool;
	fiber->pool = l->pool;
	@try {

		while ((row = [l next_row])) {
			if (row_v12(row)->lsn <= lsn) {
				say_debug("skipping too young row");
				continue;
			}

			/*  after -[recover_row] has returned, row may be modified, do not use it */
			[self validate_row:row];
			[self recover_row:row];

			prelease_after(l->pool, 128 * 1024);
		}
	}
	@finally {
		fiber->pool = saved_pool;
	}
	say_debug("after recover wal:%s lsn:%"PRIi64, l->filename, lsn);
}

/*
 * this function will not close r->current_wal if recovery was successful
 */
- (void)
recover_remaining_wals
{
	XLog *next_wal;
	i64 current_lsn, wal_greatest_lsn;

	current_lsn = lsn + 1;
	wal_greatest_lsn = [wal_dir greatest_lsn];

	if (wal_greatest_lsn == -1)
		raise("wal_dir reading failed");

	/* if the caller already opened WAL for us, recover from it first */
	if (current_wal != nil)
		goto recover_current_wal;

	while (lsn < wal_greatest_lsn) {
		if (current_wal != nil) {
                        say_warn("wal `%s' wasn't correctly closed", current_wal->filename);
                        [current_wal close];
                        current_wal = nil;
		}


		/* FIXME: lsn + 1 */
		current_lsn = lsn + 1;
                next_wal = [wal_dir open_for_read:current_lsn];

		if (next_wal == nil)
			break;

		assert(current_wal == nil);
		current_wal = next_wal;

		if (!current_wal->valid) /* broken or yet to be written inprogress. will decide later  */
			break;

		say_info("recover from `%s'", current_wal->filename);

	recover_current_wal:
		[self recover_wal:current_wal];
		if (current_wal->eof) {
			say_info("done `%s' lsn:%" PRIi64,
				 current_wal->filename, lsn);
			[current_wal close];
			current_wal = nil;
		}
	}

	/*
	 * It's not a fatal error when last WAL is empty, but if
	 * we lost some logs it is a fatal error.
	 */
	if (wal_greatest_lsn > lsn + 1)
		raise("not all WALs have been successfully read greatest_lsn:%"PRIi64
		      "lsn:%"PRIi64, wal_greatest_lsn, lsn);


}

- (i64)
recover_local:(i64)start_lsn
{
	/*
	 * if caller set confirmed_lsn to non zero value, snapshot recovery
	 * will be skipped, but WAL reading still happens
	 */

	say_info("local recovery start");
	if (start_lsn == 0) {
		[self recover_snap];
		if (lsn == 0)
			return 0;
	} else {
		/*
		 * note, that recovery start with lsn _NEXT_ to confirmed one
		 */
		lsn = start_lsn - 1;
	}

	/*
	 * just after snapshot recovery current_wal isn't known
	 * so find wal which contains record with next lsn
	 */
	if (current_wal == nil) {
		i64 next_lsn = lsn + 1;
		i64 wal_start_lsn = [wal_dir find_file_containg_lsn:next_lsn];
		if (next_lsn != wal_start_lsn && wal_start_lsn > 0) {
			current_wal = [wal_dir open_for_read:wal_start_lsn];
			if (current_wal == nil)
				raise("unable to open WAL %s",
				      [wal_dir format_filename:wal_start_lsn in_progress:false]);
		}
	}

	[self recover_remaining_wals];
	[self recover_follow:cfg.wal_dir_rescan_delay]; /* FIXME: make this conf */
	/* feeder will send his own 'wal_final_tag' */
	if (feeder_addr == NULL)
		[self recover_row:[self dummy_row_lsn:lsn tag:wal_final_tag]];
	say_info("wals recovered, lsn: %" PRIi64, lsn);
	strcpy(status, "hot_standby/local");

	return lsn;
}

static void recover_follow_file(ev_stat *w, int events __attribute__((unused)));

static void
recover_follow_dir(ev_timer *w, int events __attribute__((unused)))
{
	Recovery *r = w->data;
	[r recover_remaining_wals];

	if (r->current_wal == nil)
		return;

	if (r->current_wal->inprogress && r->current_wal->rows > 1)
		[r->current_wal reset_inprogress];

	[r->current_wal follow:recover_follow_file];
}

static void
recover_follow_file(ev_stat *w, int events __attribute__((unused)))
{
	Recovery *r = w->data;
	[r recover_wal:r->current_wal];
	if (r->current_wal->eof) {
		say_info("done `%s' lsn:%" PRIi64, r->current_wal->filename, r->lsn);
		[r->current_wal close];
		r->current_wal = nil;
		recover_follow_dir((ev_timer *)w, 0);
		return;
	}

	if (r->current_wal->inprogress && r->current_wal->rows > 0) {
		[r->current_wal reset_inprogress];
		[r->current_wal follow:recover_follow_file];
	}
}

- (void)
recover_follow:(ev_tstamp)wal_dir_rescan_delay
{
	ev_timer_init(&wal_timer, recover_follow_dir,
		      wal_dir_rescan_delay, wal_dir_rescan_delay);
	ev_timer_start(&wal_timer);
	if (current_wal != nil)
		[current_wal follow:recover_follow_file];
}

- (void)
recover_finalize
{
	ev_timer_stop(&wal_timer);
	if (current_wal != nil)
		ev_stat_stop(&current_wal->stat);

	[self recover_remaining_wals];

	if (current_wal != nil && current_wal->inprogress) {
		if (current_wal->rows < 1) {
			[current_wal inprogress_unlink];
			[current_wal close];
			current_wal = nil;
		} else {
			assert(current_wal->rows == 1);
			if ([current_wal inprogress_rename] != 0)
				panic("can't rename 'inprogress' wal");
		}
	}

	if (current_wal != nil)
                say_warn("wal `%s' wasn't correctly closed", current_wal->filename);

        [current_wal close];
        current_wal = nil;
}

- (u32)
remote_handshake:(struct sockaddr_in *)addr conn:(struct conn *)c
{
	bool warning_said = false;
	const int reconnect_delay = 1;
	const char *err = NULL;
	u32 version;

	i64 initial_lsn = 0;

	if (lsn > 0)
		initial_lsn = lsn + 1;

	do {
		if ((c->fd = tcp_connect(addr, NULL, 0)) < 0) {
			err = "can't connect to feeder";
			goto err;
		}

		if (conn_write(c, &initial_lsn, sizeof(initial_lsn)) != sizeof(initial_lsn)) {
			err = "can't write initial lsn";
			goto err;
		}

		if (conn_read(c, &version, sizeof(version)) != sizeof(version)) {
			err = "can't read version";
			goto err;
		}

		if (version != default_version && version != version_11) {
			err = "unknown remote version";
			goto err;
		}

		say_crit("succefully connected to feeder");
		say_crit("starting remote recovery from lsn:%" PRIi64, initial_lsn);
		break;

	err:
		if (err != NULL && !warning_said) {
			say_info("%s", err);
			say_info("will retry every %i second", reconnect_delay);
			warning_said = true;
		}
		conn_close(c);
		fiber_sleep(reconnect_delay);
	} while (c->fd < 0);

	return version;
}

- (void)
handle_remote_row:(struct tbuf *)row
{
	struct tbuf *row_data;

	u16 tag = row_v12(row)->tag;
	u64 row_cookie = row_v12(row)->cookie;
	/* save row data since wal_row_handler may clobber it */
	row_data = tbuf_alloc(fiber->pool);
	tbuf_append(row_data, row_v12(row)->data, row_v12(row)->len);

	[self recover_row:row];

	if (tag == snap_final_tag) {
		say_debug("saving snapshot");
		if (save_snapshot(NULL, 0) != 0)
			raise("replication failure: failed save snapshot");
		[self configure_wal_writer];
	} else if (tag == wal_tag) {
		if ([self wal_request_write:row_data tag:tag cookie:row_cookie] == 0)
			raise("replication failure: can't write row to WAL");
	}
}


static bool
contains_full_row_v12(const struct tbuf *b)
{
	return tbuf_len(b) >= sizeof(struct row_v12) &&
		tbuf_len(b) >= sizeof(struct row_v12) + row_v12(b)->len;
}

static bool
contains_full_row_v11(const struct tbuf *b)
{
	return tbuf_len(b) >= sizeof(struct _row_v11) &&
		tbuf_len(b) >= sizeof(struct _row_v11) + _row_v11(b)->len;
}

static struct tbuf *
pull_row(struct conn *c, u32 version)
{
	struct tbuf *row;

	switch (version) {
	case 12:
		while (!contains_full_row_v12(c->rbuf))
			if (conn_readahead(c, sizeof(struct row_v12)) <= 0)
				raise("unexpected eof");

		row = tbuf_split(c->rbuf, sizeof(struct row_v12) + row_v12(c->rbuf)->len);
		row->pool = c->rbuf->pool; /* FIXME: this is cludge */
		return row;
	case 11:
		while (!contains_full_row_v11(c->rbuf))
			if (conn_readahead(c, sizeof(struct _row_v11)) <= 0)
				raise("unexpected eof");
		row = tbuf_split(c->rbuf, sizeof(struct _row_v11) + _row_v11(c->rbuf)->len);
		row->pool = c->rbuf->pool;
		return convert_row_v11_to_v12(row);
	default:
		raise("version mismatch");
	}
}

static void
pull_from_remote(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);
	struct sockaddr_in *addr = va_arg(ap, struct sockaddr_in *);
	struct tbuf *row;
	struct conn c;
	u32 version = 0;

	conn_init(&c, fiber->pool, -1, REF_STATIC);
	palloc_register_gc_root(fiber->pool, &c, conn_gc);

	for (;;) {
		@try {
			if (c.fd < 0)
				version = [r remote_handshake:addr conn:&c];

			row = pull_row(&c, version);

			r->lag = ev_now() - row_v12(row)->tm;
			r->last_update_tstamp = ev_now();

			[r handle_remote_row:row];
		}
		@catch (Error *e) {
			say_error("replication failure: %s", e->reason);
			conn_close(&c);
			fiber_sleep(1);
		}
		fiber_gc();
	}
}

- (struct fiber *)
recover_follow_remote
{
	char *name;
	name = malloc(64);
	snprintf(name, 64, "remote_hot_standby/%s:%i", feeder_ipaddr, feeder_port);

	remote_puller = fiber_create(name, pull_from_remote, self, feeder_addr);
	if (remote_puller == NULL) {
		free(name);
		return NULL;
	}

	return remote_puller;
}

- (void)
enable_local_writes
{
	[self recover_finalize];
	local_writes = true;

	if (lsn > 0)
		[self configure_wal_writer];

	if (feeder_addr != NULL) {
		say_info("starting remote hot standby");
		snprintf(status, sizeof(status), "hot_standby/%s:%i",
			 feeder_ipaddr, feeder_port);

		[self recover_follow_remote];
	} else {
		say_info("I am primary");
		strcpy(status, "primary");
	}
}



- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
{
        snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
        wal_dir = [[WALDir alloc] init_dirname:wal_dirname];

        snap_dir->recovery_state = self;
        wal_dir->recovery_state = self;

	wal_timer.data = self;

	return self;
}

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
        rows_per_wal:(int)wal_rows_per_file
       feeder_ipaddr:(const char *)feeder_ipaddr_
	 feeder_port:(u16)feeder_port_
         fsync_delay:(double)wal_fsync_delay
          inbox_size:(int)inbox_size
               flags:(int)flags
  snap_io_rate_limit:(int)snap_io_rate_limit_
{
        snap_dir = [[SnapDir alloc] init_dirname:snap_dirname];
        wal_dir = [[WALDir alloc] init_dirname:wal_dirname];

        snap_dir->recovery_state = self;
        wal_dir->recovery_state = self;

	wal_timer.data = self;

	if ((flags & RECOVER_READONLY) == 0) {
		if (wal_rows_per_file <= 4)
			panic("inacceptable value of 'rows_per_file'");

		wal_dir->rows_per_file = wal_rows_per_file;
		wal_dir->fsync_delay = wal_fsync_delay;
		snap_io_rate_limit = snap_io_rate_limit_ * 1024 * 1024;

		wal_writer = spawn_child("wal_writer", inbox_size, wal_disk_writer, self);
	}

	if (feeder_ipaddr_ != NULL) {
		feeder_ipaddr = feeder_ipaddr_;
		feeder_port = feeder_port_;

		say_crit("configuring remote hot standby, WAL feeder %s:%i", feeder_ipaddr, feeder_port);

		feeder_addr = calloc(1, sizeof(*feeder_addr));
		feeder_addr->sin_family = AF_INET;
		if (inet_aton(feeder_ipaddr, &feeder_addr->sin_addr) < 0)
			panic("inet_aton: %s", feeder_ipaddr);

		feeder_addr->sin_port = htons(feeder_port);
	}

	return self;
}

@end
