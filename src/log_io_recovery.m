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
	FILE *fd;

	if ((fd = fopen(filename, "r")) == NULL) {
		say_syserror("fopen(%s)", filename);
		return -1;
	}

	if (strstr(filename, ".xlog")) {
                XLogDir *dir = [[WALDir alloc] init_dirname:NULL];
		l = [dir open_for_read_filename:filename fd:fd lsn:0];
		h = xlog_handler;
	} else if (strstr(filename, ".snap")) {
                XLogDir *dir = [[SnapDir alloc] init_dirname:NULL];
		l = [dir open_for_read_filename:filename fd:fd lsn:0];
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
	if (tag == wal_tag && row_v12(row)->lsn != lsn + 1) {
		if (!cfg.io_compat)
			raise("lsn sequence has gap after %"PRIi64 " -> %"PRIi64,
			      lsn, row_v12(row)->lsn);
		else
			say_warn("lsn sequence has gap after %"PRIi64 " -> %"PRIi64,
				 lsn, row_v12(row)->lsn);
	}
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
	row_v12(b)->data_crc32c = crc32c(0, (unsigned char *)"", 0);
	row_v12(b)->header_crc32c = crc32c(0,
					   (unsigned char *)row_v12(b) + sizeof(row_v12(b)->header_crc32c),
					   sizeof(row_v12(b)) - sizeof(row_v12(b)->header_crc32c));
	return b;
}

- (void)
recover_row:(struct tbuf *)row
{
	i64 row_lsn = row_v12(row)->lsn;
	u16 tag = row_v12(row)->tag;
	ev_tstamp tm = row_v12(row)->tm;

	@try {
		recover_row(row);
		switch (tag) {
		case wal_tag:
			lsn = row_lsn;
			lag = ev_now() - tm;
			last_update_tstamp = ev_now();
			break;
		case snap_initial_tag:
			lsn = 0;
			break;
		case snap_final_tag:
			lsn = row_lsn;
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

		[self recover_row:[self dummy_row_lsn:max_snap_lsn tag:snap_final_tag]];

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
			if (row_v12(row)->lsn > lsn) {
				[self validate_row:row];
				[self recover_row:row];
			}

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
                        say_warn("wal `%s' wasn't correctly closed, lsn:%"PRIi64,
				 current_wal->filename, lsn);
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
		if ([current_wal rows] == 0) /* probably broken wal */
			break;

		if (current_wal->eof) {
			say_info("done `%s' lsn:%" PRIi64,
				 current_wal->filename, lsn);
			[current_wal close];
			current_wal = nil;
		}
		fiber_gc();
	}

	/*
	 * It's not a fatal error when last WAL is empty, but if
	 * we lost some logs it is a fatal error.
	 */
	if (wal_greatest_lsn > lsn + 1)
		raise("not all WALs have been successfully read! "
		      "greatest_lsn:%"PRIi64" lsn:%"PRIi64" diff:%"PRIi64,
		      wal_greatest_lsn, lsn, wal_greatest_lsn - lsn);
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
		i64 wal_start_lsn = [wal_dir find_file_containg_lsn:(lsn + 1)];
		if (lsn + 1 != wal_start_lsn && wal_start_lsn > 0) {
			current_wal = [wal_dir open_for_read:wal_start_lsn];
			if (current_wal == nil)
				raise("unable to open WAL %s",
				      [wal_dir format_filename:wal_start_lsn]);
			say_info("recover from `%s'", current_wal->filename);
		}
	}

	[self recover_remaining_wals];
	[self recover_follow:cfg.wal_dir_rescan_delay]; /* FIXME: make this conf */
	/* all curently readable wal rows were read, notify about that */
	if (feeder_addr == NULL || cfg.local_hot_standby)
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

	if (r->current_wal->inprogress && [r->current_wal rows] > 1)
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

	if (r->current_wal->inprogress && [r->current_wal rows] > 1) {
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
		if ([current_wal rows] < 1) {
			[current_wal inprogress_unlink];
			[current_wal close];
			current_wal = nil;
		} else {
			assert([current_wal rows] == 1);
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

static void
pull(struct conn *c, u32 version)
{
	switch (version) {
	case 12:
		while (!contains_full_row_v12(c->rbuf))
			if (conn_readahead(c, sizeof(struct row_v12)) <= 0)
				raise("unexpected eof");
		break;
	case 11:
		while (!contains_full_row_v11(c->rbuf))
			if (conn_readahead(c, sizeof(struct _row_v11)) <= 0)
				raise("unexpected eof");
		break;
	default:
		raise("unexpected version: %i", version);
	}
}

static struct tbuf *
fetch_row(struct conn *c, u32 version)
{
	struct tbuf *row;
	u32 data_crc;

	switch (version) {
	case 12:
		if (!contains_full_row_v12(c->rbuf))
			return NULL;

		row = tbuf_split(c->rbuf, sizeof(struct row_v12) + row_v12(c->rbuf)->len);
		row->pool = c->rbuf->pool; /* FIXME: this is cludge */

		data_crc = crc32c(0, row_v12(row)->data, row_v12(row)->len);
		if (row_v12(row)->data_crc32c != data_crc)
			raise("data crc32c mismatch");

		return row;
	case 11:
		if (!contains_full_row_v11(c->rbuf))
			return NULL;

		row = tbuf_split(c->rbuf, sizeof(struct _row_v11) + _row_v11(c->rbuf)->len);
		row->pool = c->rbuf->pool;

		data_crc = crc32c(0, _row_v11(row)->data, _row_v11(row)->len);
		if (_row_v11(row)->data_crc32c != data_crc)
			raise("data crc32c mismatch");

		return convert_row_v11_to_v12(row);
	default:
		raise("unexpected version: %i", version);
	}
}

static void
pull_snapshot(Recovery *r, struct conn *c, u32 version)
{
	struct tbuf *row;
	for (;;) {
		pull(c, version);
		while ((row = fetch_row(c, version))) {
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
				raise("unexpected tag %i", row_v12(row)->tag);
			}
		}
		fiber_gc();
	}
}

static void
pull_wal(Recovery *r, struct conn *c, u32 version)
{
	struct tbuf *row, *special_row = NULL, *rows[WAL_PACK_MAX], *pack;
	int pack_rows = 0;

	/* TODO: use designated palloc_pool */
	for (;;) {
		pull(c, version);

		pack_rows = 0;
		pack = [r wal_pack_prepare];
		while ((row = fetch_row(c, version))) {
			if (row_v12(row)->tag != wal_tag) {
				special_row = row;
				break;
			}

			rows[pack_rows++] = row;
			if ([r wal_pack_append:pack
					  data:row_v12(row)->data
					   len:row_v12(row)->len
					   tag:row_v12(row)->tag
					cookie:row_v12(row)->cookie] == 0)
				break;
		}

		if (pack_rows > 0) {
			int confirmed = [r wal_pack_submit:pack];
			for (int j = 0; j < confirmed; j++)
				[r recover_row:rows[j]];

			if (confirmed != pack_rows)
				raise("wal write failed confirmed:%i < sent:%i",
				      confirmed, pack_rows);
		}

		if (special_row) {
			[r recover_row:special_row];
			special_row = NULL;
		}

		fiber_gc();
	}
}

static void
pull_from_remote(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);
	struct sockaddr_in *addr = va_arg(ap, struct sockaddr_in *);
	struct conn c;
	u32 version = 0;

	conn_init(&c, fiber->pool, -1, REF_STATIC);
	palloc_register_gc_root(fiber->pool, &c, conn_gc);


	for (;;) {
		@try {
			if (c.fd < 0)
				version = [r remote_handshake:addr conn:&c];

			if ([r lsn] == 0)
				pull_snapshot(r, &c, version);
			else {
				if (version == 11)
					[r recover_row:[r dummy_row_lsn:[r lsn] tag:wal_final_tag]];
				pull_wal(r, &c, version);
			}


		}
		@catch (Error *e) {
			say_error("replication failure: %s", e->reason);
			conn_close(&c);
			fiber_sleep(1);
			fiber_gc();
		}
	}
}

- (struct fiber *)
recover_follow_remote
{
	char *name;
	name = malloc(64);
	snprintf(name, 64, "remote_hot_standby/%s", feeder_addr);

	remote_puller = fiber_create(name, pull_from_remote, self, feeder);
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

	if (feeder_addr != NULL) {
		if (lsn > 0) /* we'll fetch remote snapshot first */
			[self configure_wal_writer];

		[self recover_follow_remote];

		say_info("starting remote hot standby");
		snprintf(status, sizeof(status), "hot_standby/%s", feeder_addr);
	} else {
		[self configure_wal_writer];
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

void
input_dispatch(va_list ap)
{
	struct conn *c = va_arg(ap, struct conn *);
	u32 data_len, fid;

	for (;;) {
		if (tbuf_len(c->rbuf) < sizeof(u32) * 2) {
			if (conn_readahead(c, sizeof(u32) * 2) <= 0)
				panic("child is dead");
		}

		data_len = read_u32(c->rbuf);
		fid = read_u32(c->rbuf);

		if (tbuf_len(c->rbuf) < data_len) {
			if (conn_readahead(c, data_len) < 0)
				panic("child is dead");
		}

		resume(fid2fiber(fid), read_bytes(c->rbuf, data_len));

		if (palloc_allocated(fiber->pool) > 4 * 1024 * 1024)
			palloc_gc(c->pool);
	}
}

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
	 recover_row:(void (*)(struct tbuf *))recover_row_
        rows_per_wal:(int)wal_rows_per_file
	 feeder_addr:(const char *)feeder_addr_
         fsync_delay:(double)wal_fsync_delay
               flags:(int)flags
  snap_io_rate_limit:(int)snap_io_rate_limit_
{
	/* Recovery object is never released */

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

		wal_writer = spawn_child("wal_writer", wal_disk_writer, self);

		ev_io_init(&wal_writer->c->out,
			   (void *)fiber_create("wal_writer/output_flusher", service_output_flusher),
			   wal_writer->c->fd, EV_WRITE);
		fiber_create("wal_writer/input_dispatcher", input_dispatch, wal_writer->c);
	}

	recover_row = recover_row_;

	if (feeder_addr_ != NULL) {
		feeder_addr = feeder_addr_;

		say_crit("configuring remote hot standby, WAL feeder %s", feeder_addr);

		feeder = malloc(sizeof(struct sockaddr_in));
		if (atosin(feeder_addr, feeder) == -1 || feeder->sin_addr.s_addr == INADDR_ANY)
			panic("bad feeder_addr: `%s'", feeder_addr);

	}

	return self;
}

@end
