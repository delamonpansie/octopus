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

#import <config.h>
#import <fiber.h>
#import <tarantool_ev.h>
#import <tbuf.h>
#import <util.h>

#include <objc/Object.h>

#include <stdio.h>
#include <limits.h>


#define RECOVER_READONLY 1
#define ROW_EOF (void *)1

extern const u16 wal_tag, snap_tag, snap_final_tag;
extern const u64 default_cookie;
extern const u32 default_version;
extern const u32 marker, eof_marker;
extern const char *inprogress_suffix;

const char *xlog_tag_to_a(u16 tag);

@class Recovery;
typedef int (row_handler) (Recovery *, struct tbuf *);
typedef void (follow_cb)(ev_stat *w, int events);

@interface XLogDir: Object {
@public
	size_t rows_per_file;
	double fsync_delay;
	bool recover_from_inprogress;

	const char *filetype;
	const char *suffix;
	const char *dirname;

	Recovery *recovery_state;
};
- (id) init_dirname:(const char *)dirname_;
- (id) open_for_read_filename:(const char *)filename;
- (id) open_for_read:(i64)lsn;
- (id) open_for_write:(i64)lsn saved_errno:(int *)saved_errno;
- (i64) greatest_lsn;
- (const char *) format_filename:(i64)lsn in_progress:(bool)in_progress;
- (i64) find_file_containg_lsn:(i64)target_lsn;
@end

@interface SnapDir: XLogDir
@end

@interface WALDir: XLogDir
@end

@interface XLog: Object {
@public
        char filename[PATH_MAX + 1];
	FILE *fd;
	void *vbuf;
	ev_stat stat;

        XLogDir *dir;

	struct palloc_pool *pool;
	enum log_mode {
		LOG_READ,
		LOG_WRITE
	} mode;
	size_t rows;

	bool valid, eof, inprogress;

	size_t bytes_written, offset;
}
- init_filename:(const char *)filename fd:(FILE *)fd dir:(XLogDir *)dir;
- (const char *)final_filename;
- (void) follow:(follow_cb *)cb;
- (void) reset_inprogress;
- (int) inprogress_rename;
- (int) inprogress_unlink;
- (int) read_header;
- (int) write_header;
- (struct tbuf *)next_row;
- (int) flush;
- (int) close;
@end

@interface Recovery: Object {
@public
	i64 lsn, confirmed_lsn;

	XLog *current_wal;	/* the WAL we'r currently reading/writing from/to */
	struct child *wal_writer;

        XLogDir *wal_dir, *snap_dir;

	ev_timer wal_timer;
	ev_tstamp recovery_lag, recovery_last_update_tstamp;

	int snap_io_rate_limit;
	u64 cookie;
}
- (i64) next_lsn;
- (void) init_lsn:(i64)new_lsn;
- (void) confirm_lsn:(i64)lsn;
- (bool) wal_request_write:(struct tbuf *)row tag:(u16)tag cookie:(u64)cookie lsn:(i64)lsn;
- (struct tbuf *) wal_write_row:(struct tbuf *)t;
- (void) recover_finalize;
- (struct fiber *) recover_follow_remote:(char *)ipaddr port:(int)port;
- (void) recover:(i64)lsn;
- (void) recover_follow:(ev_tstamp)delay;
- (void) recover_finalize;
- (void) snapshot_save:(void (*)(struct log_io_iter *))f;
- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir
        rows_per_wal:(int)rows_per_wal
         fsync_delay:(double)wal_fsync_delay
          inbox_size:(int)inbox_size
               flags:(int)flags
  snap_io_rate_limit:(int)snap_io_rate_limit;
@end

@interface Recovery (row)
/* recover_row will be presented by most recent format of data
   XLog reader is responsible of converting data from old format */
- (void) recover_row:(struct tbuf *)row;
@end

struct wal_write_request {
	i64 lsn;
	u32 len;
	u8 data[];
} __attribute__((packed));

struct row_v11 {
	u32 header_crc32c;
	i64 lsn;
	double tm;
	u32 len;
	u32 data_crc32c;
	u8 data[];
} __attribute__((packed));

static inline struct row_v11 *row_v11(const struct tbuf *t)
{
	return (struct row_v11 *)t->data;
}


int read_log(const char *filename,
	     row_handler xlog_handler, row_handler snap_handler, void *state);

struct log_io_iter;
void snapshot_write_row(struct log_io_iter *i, u16 tag, u64 cookie, struct tbuf *row);
