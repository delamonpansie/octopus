/*
 * Copyright (C) 2010, 2011 Mail.RU
 * Copyright (C) 2010, 2011 Yuriy Vostrikov
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

struct log_io_iter {
	struct tarantool_coro coro;
	XLog *log;
	void *data;
	int io_rate_limit;
};

static void
iter_open(XLog *l, struct log_io_iter *i, void (*iterator) (struct log_io_iter * i))
{
	memset(i, 0, sizeof(*i));
	i->log = l;
	tarantool_coro_create(&i->coro, (void *)iterator, i);
}


@implementation Recovery
- (void)
confirm_lsn:(i64)new_lsn
{
	assert(confirmed_lsn <= lsn);

	if (confirmed_lsn < new_lsn) {
		if (confirmed_lsn + 1 != new_lsn)
			say_warn("non consecutive lsn, last confirmed:%" PRIi64
				 " new:%" PRIi64 " diff: %" PRIi64,
				 confirmed_lsn, new_lsn, new_lsn - confirmed_lsn);
		confirmed_lsn = new_lsn;
	} else {
		say_warn("lsn double confirmed:%" PRIi64, confirmed_lsn);
	}
}

- (i64)
next_lsn
{
        lsn++;
	return lsn;
}

- (void)
init_lsn:(i64)new_lsn
{
        lsn = new_lsn;
        [self confirm_lsn:new_lsn];
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
recover_row:(struct tbuf *)row
{
	if (tbuf_len(row) < sizeof(struct row_v11) + sizeof(u16))
		raise("row is too short");

	u16 tag = *(u16 *)row_v11(row)->data;
	if (tag == wal_tag && row_v11(row)->lsn != lsn + 1)
		raise("lsn sequence has gap after %"PRIi64 " -> %"PRIi64,
		      lsn, row_v11(row)->lsn);

	if (tag == snap_final_tag || tag == wal_tag)
		lsn = row_v11(row)->lsn;
}

- (i64)
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
recover_snap
{
	XLog *snap = nil;
	struct tbuf *row;
	i64 max_snap_lsn;

	struct palloc_pool *saved_pool = fiber->pool;
	@try {
		max_snap_lsn = [snap_dir greatest_lsn];

		if (max_snap_lsn < 1) {
			say_crit("don't you forget to initialize "
				 "storage with --init-storage switch?");
			_exit(1);
		}

		snap = [snap_dir open_for_read:max_snap_lsn];
		if (snap == nil)
			raise("can't find/open snapshot");

		say_info("recover from `%s'", snap->filename);

		fiber->pool = snap->pool;
		while ((row = [snap next_row])) {
			[self recover_row:row];
			prelease_after(snap->pool, 128 * 1024);
		}

		if (!snap->eof)
			raise("unable to fully read snapshot");

		lsn = confirmed_lsn = max_snap_lsn;
		say_info("snapshot recovered, confirmed lsn:%" PRIi64, confirmed_lsn);
	}
	@finally {
		fiber->pool = saved_pool;
		[snap close];
		snap = nil;
	}
}

- (void)
recover_wal:(XLog *)l
{
	struct tbuf *row = NULL;

	struct palloc_pool *saved_pool = fiber->pool;
	fiber->pool = l->pool;
	@try {

		while ((row = [l next_row])) {
			i64 row_lsn = row_v11(row)->lsn;
			if (row_lsn <= confirmed_lsn) {
				say_debug("skipping too young row");
				continue;
			}

			/*  after -[recover_row] has returned, row may be modified, do not use it */
			[self recover_row:row];
			[self init_lsn:row_lsn];
			prelease_after(l->pool, 128 * 1024);
		}
	}
	@finally {
		fiber->pool = saved_pool;
	}
}

/*
 * this function will not close r->current_wal if recovery was successful
 */
- (void)
recover_remaining_wals
{
	XLog *next_wal;
	i64 current_lsn, wal_greatest_lsn;

	current_lsn = confirmed_lsn + 1;
	wal_greatest_lsn = [wal_dir greatest_lsn];

	/* if the caller already opened WAL for us, recover from it first */
	if (current_wal != nil)
		goto recover_current_wal;

	while (confirmed_lsn < wal_greatest_lsn) {
		if (current_wal != nil) {
                        say_warn("wal `%s' wasn't correctly closed", current_wal->filename);
                        [current_wal close];
                        current_wal = nil;
		}

		current_lsn = confirmed_lsn + 1;	/* TODO: find better way looking for next xlog */

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
			say_info("done `%s' confirmed_lsn:%" PRIi64,
				 current_wal->filename, confirmed_lsn);
			[current_wal close];
			current_wal = nil;
		}
	}

	/*
	 * It's not a fatal error when last WAL is empty, but if
	 * we lost some logs it is a fatal error.
	 */
	if (wal_greatest_lsn > confirmed_lsn + 1)
		raise("not all WALs have been successfully read");

}

- (void)
recover:(i64)start_lsn
{
	/*
	 * if caller set confirmed_lsn to non zero value, snapshot recovery
	 * will be skipped, but WAL reading still happens
	 */

	say_info("recovery start");
	if (start_lsn == 0) {
		[self recover_snap];
	} else {
		/*
		 * note, that recovery start with lsn _NEXT_ to confirmed one
		 */
		lsn = confirmed_lsn = start_lsn - 1;
	}

	/*
	 * just after snapshot recovery current_wal isn't known
	 * so find wal which contains record with next lsn
	 */
	if (current_wal == nil) {
		i64 next_lsn = confirmed_lsn + 1;
		i64 wal_start_lsn = [wal_dir find_file_containg_lsn:next_lsn];
		if (next_lsn != wal_start_lsn && wal_start_lsn > 0) {
			current_wal = [wal_dir open_for_read:wal_start_lsn];
			if (current_wal == nil)
				raise("unable to open WAL %s",
				      [wal_dir format_filename:wal_start_lsn in_progress:false]);
		}
	}

	[self recover_remaining_wals];
	say_info("wals recovered, confirmed lsn: %" PRIi64, confirmed_lsn);
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
		say_info("done `%s' confirmed_lsn:%" PRIi64, r->current_wal->filename, r->confirmed_lsn);
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

	if (current_wal != nil && current_wal->rows < 1)
		raise("unable to read any valid row from %s", current_wal->filename);

	if (current_wal != nil)
                say_warn("wal `%s' wasn't correctly closed", current_wal->filename);

        [current_wal close];
        current_wal = nil;
}

static struct wal_write_request *
wal_write_request(const struct tbuf *t)
{
	return t->data;
}

- (bool)
wal_request_write:(struct tbuf *)row tag:(u16)tag cookie:(u64)row_cookie lsn:(i64)row_lsn
{
	struct tbuf *m = tbuf_alloc(wal_writer->out->pool);
	struct msg *a;
	u32 len = tbuf_len(row) + sizeof(tag) + sizeof(row_cookie) + sizeof(struct wal_write_request);

	say_debug("wal_write lsn=%" PRIi64, row_lsn);
	tbuf_ensure(m, sizeof(u32) * 2 +
		    sizeof(struct wal_write_request) +
		    sizeof(tag) + sizeof(row_cookie) + tbuf_len(row));

	/* sock2inbox header */
	tbuf_append(m, &len, sizeof(u32));
	tbuf_append(m, &fiber->fid, sizeof(u32));

	/* wal request header */
	len -= sizeof(struct wal_write_request);
	tbuf_append(m, &row_lsn, sizeof(i64));
	tbuf_append(m, &len, sizeof(u32));

	/* wal row */
	tbuf_append(m, &tag, sizeof(tag));
	tbuf_append(m, &row_cookie, sizeof(row_cookie));
	tbuf_append(m, row->data, tbuf_len(row));

	if (write_inbox(wal_writer->out, m) == false) {
		say_warn("wal writer inbox is full");
		return false;
	}
	a = read_inbox();

	u32 reply = read_u32(a->msg);
	say_debug("wal_write reply=%" PRIu32, reply);
	if (reply != 0)
		say_warn("wal writer returned error status");
	return reply == 0;
}

- (struct tbuf *)
wal_write_row:(struct tbuf *)t
{
	static XLog *wal_to_close = nil;
	struct tbuf *reply, *header;
	u32 result = 0;

	reply = tbuf_alloc(t->pool);

	if (current_wal == nil)
		/* Open WAL with '.inprogress' suffix. */
		current_wal = [wal_dir open_for_write:wal_write_request(t)->lsn
					  saved_errno:NULL];
        if (current_wal == nil) {
                say_error("can't open wal");
                goto fail;
        }

        if (current_wal->rows == 1) {
		/* rename wal after first successfull write to name without inprogress suffix*/
		if ([current_wal inprogress_rename] != 0) {
			say_error("can't rename inprogress wal");
			goto fail;
		}
	}

	if (wal_to_close != nil) {
		[wal_to_close close];
		wal_to_close = nil;
	}

	if (fwrite(&marker, sizeof(marker), 1, current_wal->fd) != 1) {
		say_syserror("can't write marker to wal");
		goto fail;
	}

	header = tbuf_alloc(t->pool);
	tbuf_ensure(header, sizeof(struct row_v11));
	header->len = sizeof(struct row_v11);

	row_v11(header)->lsn = wal_write_request(t)->lsn;
	row_v11(header)->tm = ev_now();
	row_v11(header)->len = wal_write_request(t)->len;
	row_v11(header)->data_crc32c =
		crc32c(0, wal_write_request(t)->data, wal_write_request(t)->len);
	row_v11(header)->header_crc32c =
		crc32c(0, header->data + field_sizeof(struct row_v11, header_crc32c),
		       sizeof(struct row_v11) - field_sizeof(struct row_v11, header_crc32c));

	if (fwrite(header->data, tbuf_len(header), 1, current_wal->fd) != 1) {
		say_syserror("can't write row header to wal");
		goto fail;
	}

	if (fwrite(wal_write_request(t)->data, wal_write_request(t)->len, 1, current_wal->fd) != 1) {
		say_syserror("can't write row data to wal");
		goto fail;
	}

	current_wal->rows++;
	if (current_wal->dir->rows_per_file <= current_wal->rows ||
	    (wal_write_request(t)->lsn + 1) % current_wal->dir->rows_per_file == 0)
	{
		wal_to_close = current_wal;
		current_wal = nil;
	}

	tbuf_append(reply, &result, sizeof(result));
	return reply;

      fail:
	result = 1;
	tbuf_append(reply, &result, sizeof(result));
	return reply;
}

static int
wal_disk_writer(int fd, void *state)
{
	Recovery *rcvr = state;
	struct tbuf *request, *reply, *wbuf, *rbuf;
	int result = EXIT_FAILURE;
	ev_tstamp last_flush = 0;

	rbuf = tbuf_alloca(fiber->pool);
	palloc_register_gc_root(fiber->pool, (void *)rbuf, tbuf_gc);

	for (;;) {
	reread:
		tbuf_ensure(rbuf, 16 * 1024);
		ssize_t r = recv(fd, rbuf->data + tbuf_len(rbuf),
				 rbuf->size - tbuf_len(rbuf), 0);
		if (r < 0 && (errno == EINTR))
			goto reread;
		else if (r < 0) {
			say_syserror("recv");
			result = EX_OSERR;
			break;
		} else if (r == 0) {
			result = EX_OK;
			break;
		}

		rbuf->len += r;

		wbuf = tbuf_alloc(fiber->pool);

		/* we're not running inside ev_loop, so update ev_now manually */
		ev_now_update();

		while (tbuf_len(rbuf) > sizeof(u32) * 2) {
			size_t packet_size = sizeof(u32) * 2 + *(u32 *)rbuf->data;
			if (tbuf_len(rbuf) < packet_size) {
				if (tbuf_len(wbuf) == 0) {
					tbuf_ensure(rbuf, packet_size);
					goto reread;
				} else {
					break;
				}
			}

			u32 data_len = read_u32(rbuf);
			u32 fid = read_u32(rbuf);
			void *data = read_bytes(rbuf, data_len);

			request = tbuf_alloc_fixed(fiber->pool, data, data_len);
			request->pool = fiber->pool;

			reply = [rcvr wal_write_row: request];
			u32 len = tbuf_len(reply);
			tbuf_append(wbuf, &len, sizeof(u32));
			tbuf_append(wbuf, &fid, sizeof(fid));
			tbuf_append(wbuf, reply->data, len);
		}

		if (rcvr->current_wal != nil) {
			/* flush stdio buffer to keep feeder in sync */
			if (fflush(rcvr->current_wal->fd) < 0) {
				say_syserror("can't flush wal");
				result = EX_OSERR;
				break;
			}

			float fsync_delay = rcvr->current_wal->dir->fsync_delay;
			if (fsync_delay == 0 || ev_now() - last_flush >= fsync_delay)
			{
				if ([rcvr->current_wal flush] < 0) {
					say_syserror("can't flush wal");
					result = EX_OSERR;
					break;
				}
				last_flush = ev_now();
			}
		}

		while (tbuf_len(wbuf) > 0) {
			ssize_t r = write(fd, wbuf->data, tbuf_len(wbuf));
			if (r < 0) {
				if (errno == EINTR)
					continue;
				result = EX_OK;
				break;
			}
			wbuf->data += r;
			wbuf->len -= r;
		}

		fiber_gc();
	}

	[rcvr->current_wal close];
	rcvr->current_wal = nil;
	return result;
}

- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
        rows_per_wal:(int)wal_rows_per_file
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

	return self;
}


static void
write_rows(struct log_io_iter *i)
{
	XLog *l = i->log;
	struct tbuf *data;
	struct row_v11 row;

	row.lsn = *(i64 *)i->data;

	for (;;) {
		coro_transfer(&i->coro.ctx, &fiber->coro.ctx);
		data = i->data;

		row.tm = ev_now();
		row.len = tbuf_len(data);
		row.data_crc32c = crc32c(0, data->data, tbuf_len(data));
		row.header_crc32c = crc32c(0, (unsigned char *)&row + sizeof(row.header_crc32c),
					   sizeof(row) - sizeof(row.header_crc32c));

		if (fwrite(&marker, sizeof(marker), 1, l->fd) != 1)
			panic("fwrite");
		if (fwrite(&row, sizeof(row), 1, l->fd) != 1)
			panic("fwrite");
		if (fwrite(data->data, tbuf_len(data), 1, l->fd) != 1)
			panic("fwrite");

		prelease_after(fiber->pool, 128 * 1024);
	}
}

void
snapshot_write_row(struct log_io_iter *i, u16 tag, u64 cookie, struct tbuf *row)
{
	static int rows;
	static int bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;
	struct tbuf *wal_row = tbuf_alloc(fiber->pool);

	tbuf_append(wal_row, &tag, sizeof(tag));
	tbuf_append(wal_row, &cookie, sizeof(cookie));
	tbuf_append(wal_row, row->data, tbuf_len(row));

	i->data = wal_row;
	if (i->io_rate_limit > 0) {
		if (last == 0) {
			ev_now_update();
			last = ev_now();
		}

		bytes += tbuf_len(row) + sizeof(struct row_v11);

		while (bytes >= i->io_rate_limit) {
			[i->log flush];

			ev_now_update();
			elapsed = ev_now() - last;
			if (elapsed < 1)
				usleep(((1 - elapsed) * 1000000));

			ev_now_update();
			last = ev_now();
			bytes -= i->io_rate_limit;
		}
	}
	coro_transfer(&fiber->coro.ctx, &i->coro.ctx);
	if (++rows % 100000 == 0)
		say_crit("%.1fM rows written", rows / 1000000.);
}

- (void)
snapshot_save:(void (*)(struct log_io_iter *))callback
{
	struct log_io_iter i;
        XLog *snap;
	const char *final_filename;
	int saved_errno;

	memset(&i, 0, sizeof(i));

        snap = [snap_dir open_for_write:confirmed_lsn saved_errno:&saved_errno];
	if (snap == nil)
		panic_status(saved_errno, "can't open snap for writing");

	iter_open(snap, &i, write_rows);

	i.data = &lsn;
	coro_transfer(&fiber->coro.ctx, &i.coro.ctx);

	if (snap_io_rate_limit > 0)
		i.io_rate_limit = snap_io_rate_limit;

	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */

	final_filename = [snap final_filename];

	say_info("saving snapshot `%s'", final_filename);
	callback(&i);

	struct tbuf *empty = tbuf_alloc(fiber->pool);
	snapshot_write_row(&i, snap_final_tag, 0, empty);

	if (fsync(fileno(snap->fd)) < 0)
		panic("fsync");

	if (link(snap->filename, final_filename) == -1)
		panic_status(errno, "can't create hard link to snapshot");

	if (unlink(snap->filename) == -1)
		say_syserror("can't unlink 'inprogress' snapshot");

	[snap close];
	snap = nil;
	say_info("done");
}

static struct tbuf *
remote_row_reader_v11(struct conn *c)
{
	struct tbuf *m;
	for (;;) {
		if (tbuf_len(c->rbuf) >= sizeof(struct row_v11) &&
		    tbuf_len(c->rbuf) >= sizeof(struct row_v11) + row_v11(c->rbuf)->len)
		{
			m = tbuf_split(c->rbuf, sizeof(struct row_v11) + row_v11(c->rbuf)->len);
			m->pool = c->rbuf->pool; /* FIXME: this is cludge */
			say_debug("read %" PRIu32 " bytes: %s", tbuf_len(m), tbuf_to_hex(m));
			return m;
		}

		if (conn_readahead(c, sizeof(struct row_v11)) <= 0) {
			say_error("unexpected eof reading row header");
			return NULL;
		}
	}
}

static struct tbuf *
remote_read_row(struct conn *c, struct sockaddr_in *addr, i64 initial_lsn)
{
	struct tbuf *row;
	bool warning_said = false;
	const int reconnect_delay = 1;
	const char *err = NULL;
	u32 version;

	for (;;) {
		if (c->fd < 0) {
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

			if (version != default_version) {
				err = "remote version mismatch";
				goto err;
			}

			say_crit("succefully connected to feeder");
			say_crit("starting remote recovery from lsn:%" PRIi64, initial_lsn);
			warning_said = false;
			err = NULL;
		}

		row = remote_row_reader_v11(c);
		if (row == NULL) {
			err = "can't read row";
			goto err;
		}

		return row;

	      err:
		if (err != NULL && !warning_said) {
			say_info("%s", err);
			say_info("will retry every %i second", reconnect_delay);
			warning_said = true;
		}
		conn_close(c);
		fiber_sleep(reconnect_delay);
	}
}

- (void)
handle_remote_row:(struct tbuf *)row
{
	struct tbuf *row_data;
	i64 row_lsn = row_v11(row)->lsn;
	u16 tag;

	/* save row data since wal_row_handler may clobber it */
	row_data = tbuf_alloc(row->pool);
	tbuf_append(row_data, row_v11(row)->data, row_v11(row)->len);

	[self recover_row:row];

	tag = read_u16(row_data);
	(void)read_u64(row_data); /* drop the cookie */

	if ([self wal_request_write:row_data tag:tag cookie:cookie lsn:row_lsn] == false)
		raise("replication failure: can't write row to WAL");

	[self init_lsn:row_lsn];
}

static void
pull_from_remote(va_list ap)
{
	Recovery *r = va_arg(ap, Recovery *);
	struct sockaddr_in *addr = va_arg(ap, struct sockaddr_in *);
	struct tbuf *row;
	struct conn *c = conn_create(fiber->pool, -1);

	for (;;) {
		@try {
			row = remote_read_row(c, addr, r->confirmed_lsn + 1);
			r->recovery_lag = ev_now() - row_v11(row)->tm;
			r->recovery_last_update_tstamp = ev_now();

			[r handle_remote_row:row];
		}
		@catch (Error *e) {
			say_error("replication failure: %s", e->reason);
			conn_close(c);
			fiber_sleep(1);
		}
		fiber_gc();
	}
}

- (struct fiber *)
recover_follow_remote:(char *)ip_addr port:(int)port
{
	char *name;
	struct fiber *f;
	struct in_addr server;
	struct sockaddr_in *addr;
	Recovery *h;

	say_crit("initializing remote hot standby, WAL feeder %s:%i", ip_addr, port);

	if (inet_aton(ip_addr, &server) < 0) {
		say_syserror("inet_aton: %s", ip_addr);
		return NULL;
	}

	addr = calloc(1, sizeof(*addr));
	addr->sin_family = AF_INET;
	memcpy(&addr->sin_addr.s_addr, &server, sizeof(server));
	addr->sin_port = htons(port);

	h = malloc(sizeof(Recovery *));
	h = self;
	memcpy(&self->cookie, &addr, MIN(sizeof(self->cookie), sizeof(addr)));

	name = malloc(64);
	snprintf(name, 64, "remote_hot_standby/%s:%i", ip_addr, port);
	f = fiber_create(name, -1, pull_from_remote, h, addr);
	if (f == NULL) {
		free(name);
		free(addr);
		free(h);
		return NULL;
	}

	fiber_wake(f, NULL);
	return f;
}

@end
