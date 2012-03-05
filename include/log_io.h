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

#import <config.h>
#import <fiber.h>
#import <tarantool_ev.h>
#import <tbuf.h>
#import <util.h>

#include <objc/Object.h>

#include <stdio.h>
#include <limits.h>


#define RECOVER_READONLY 1
#define WAL_PACK_MAX 1024

enum { snap_initial_tag = 1,
       snap_tag,
       wal_tag,
       snap_final_tag,
       wal_final_tag };

extern const u64 default_cookie;
extern const u32 default_version, version_11;
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
- (id) open_for_read_filename:(const char *)filename fd:(FILE *)fd lsn:(i64)lsn;
- (id) open_for_read:(i64)lsn;
- (id) open_for_write:(i64)lsn;
- (i64) greatest_lsn;
- (const char *) format_filename:(i64)lsn suffix:(const char *)extra_suffix;
- (const char *) format_filename:(i64)lsn;
- (i64) find_file_containg_lsn:(i64)target_lsn;
@end

@interface SnapDir: XLogDir
@end

@interface WALDir: XLogDir
@end

@interface XLog: Object {
	size_t rows, wet_rows;
@public
	char *filename;
	FILE *fd;
	void *vbuf;
	ev_stat stat;

	XLogDir *dir;
	i64 next_lsn;

	struct palloc_pool *pool;
	enum log_mode {
		LOG_READ,
		LOG_WRITE
	} mode;

	bool valid, eof, inprogress, no_wet;

	size_t bytes_written;
	off_t offset, wet_rows_offset[WAL_PACK_MAX * 8];
}
- (XLog *) init_filename:(const char *)filename_
		      fd:(FILE *)fd_
		    mode:(int)mode
		     dir:(XLogDir *)dir_
		     lsn:(i64)lsn_;
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
- (size_t) rows;
- (size_t)wet_rows_offset_available;
- (int) append_row:(const void *)data len:(u32)data_len tag:(u16)tag cookie:(u64)cookie;
- (i64) confirm_write;
@end

struct tbuf *convert_row_v11_to_v12(struct tbuf *orig);
@interface XLog11: XLog
@end

@interface XLog12: XLog
@end

@interface Recovery: Object {
	struct child *wal_writer;
	i64 lsn, scn;
	ev_tstamp lag, last_update_tstamp;
	char status[64];
	XLog *current_wal;	/* the WAL we'r currently reading/writing from/to */
	XLog *wal_to_close;

	struct sockaddr_in *feeder;

        XLogDir *wal_dir, *snap_dir;

	ev_timer wal_timer;

	int snap_io_rate_limit;

	bool local_writes;
	struct fiber *remote_puller;
	const char *feeder_addr;
}

- (i64)lsn;
- (i64)scn;
- (const char *)status;
- (ev_tstamp)lag;
- (ev_tstamp)last_update_tstamp;
- (struct child *)wal_writer;

- (void) initial_lsn:(i64)new_lsn;
- (void) recover_finalize;
- (i64) recover_local:(i64)lsn;
- (void) recover_follow:(ev_tstamp)delay;
- (void) enable_local_writes;
- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir
        rows_per_wal:(int)rows_per_wal
       feeder_addr:(const char *)feeder_addr
         fsync_delay:(double)wal_fsync_delay
               flags:(int)flags
  snap_io_rate_limit:(int)snap_io_rate_limit;
@end

@interface Recovery (row)
/* recover_row will be presented by most recent format of data
   XLog reader is responsible of converting data from old format */
- (void) recover_row:(struct tbuf *)row;
@end



int wal_disk_writer(int fd, void *state);
void snapshot_write_row(XLog *l, u16 tag, struct tbuf *row);

@interface Recovery (writers)
- (void) submit_change:(struct tbuf *)change;
- (void) configure_wal_writer;
- (struct tbuf *) wal_pack_prepare;
- (void) wal_pack_append:(struct tbuf *)m
		    data:(void *)data
		     len:(u32)data_len
		     tag:(u16)tag
		  cookie:(u64)cookie;
- (int) wal_pack_submit:(struct tbuf *)m;
- (void) snapshot_save:(void (*)(XLog *))callback;
@end

struct _row_v11 {
	u32 header_crc32c;
	i64 lsn;
	double tm;
	u32 len;
	u32 data_crc32c;
	u8 data[];
} __attribute__((packed));

struct row_v12 {
	u32 header_crc32c;
	i64 lsn;
	i64 scn;
	u16 tag;
	u64 cookie;
	double tm;
	u32 len;
	u32 data_crc32c;
	u8 data[];
} __attribute__((packed));

static inline struct _row_v11 *_row_v11(const struct tbuf *t)
{
	return (struct _row_v11 *)t->data;
}

static inline struct row_v12 *row_v12(const struct tbuf *t)
{
	return (struct row_v12 *)t->data;
}


int read_log(const char *filename,
	     row_handler xlog_handler, row_handler snap_handler, void *state);
