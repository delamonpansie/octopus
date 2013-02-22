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
#import <net_io.h>
#import <object.h>

#include <stdio.h>
#include <limits.h>


#define RECOVER_READONLY 1
#define WAL_PACK_MAX 1024

enum { snap_initial_tag = 1,
       snap_tag,
       wal_tag,
       snap_final_tag,
       wal_final_tag,
       run_crc,
       nop,
       snap_skip_scn,
       paxos_prepare,
       paxos_promise,
       paxos_propose,
       paxos_accept,
       paxos_nop
};

static inline bool dummy_tag(int tag) /* dummy row tag */
{
	return tag == wal_final_tag;
}

extern const u64 default_cookie;
extern const u32 default_version, version_11;
extern const u32 marker, eof_marker;
extern const char *inprogress_suffix;

const char *xlog_tag_to_a(u16 tag);

struct tbuf;

@protocol XLogPuller
- (struct row_v12 *) fetch_row;
- (u32) version;
- (bool) eof;
- (int) close;
- (struct palloc_pool *) pool;
@end

@protocol XLogPullerAsync <XLogPuller>
- (ssize_t) recv;
- (void) abort_recv; /* abort running recv asynchronously */
@end

@class XLog;
@class XLogWriter;
@class Recovery;
typedef void (follow_cb)(ev_stat *w, int events);

@interface XLogDir: Object {
@public
	size_t rows_per_file;
	double fsync_delay;
	bool recover_from_inprogress;

	const char *filetype;
	const char *suffix;
	const char *dirname;

	XLogWriter *writer;
};
- (id) init_dirname:(const char *)dirname_;
- (XLog *) open_for_read_filename:(const char *)filename;
- (XLog *) open_for_read:(i64)lsn;
- (XLog *) open_for_write:(i64)lsn scn:(i64)scn;
- (i64) greatest_lsn;
- (const char *) format_filename:(i64)lsn suffix:(const char *)extra_suffix;
- (const char *) format_filename:(i64)lsn;
- (XLog *) containg_lsn:(i64)target_lsn;
- (i64) containg_scn:(i64)target_scn;
@end

@interface SnapDir: XLogDir
@end

@interface WALDir: XLogDir
@end

@interface XLog: Object <XLogPuller> {
	size_t rows, wet_rows;
	bool eof;
@public
	char *filename;
	FILE *fd;
	void *vbuf;
	ev_stat stat;

	XLogDir *dir;
	XLogWriter *writer;
	i64 next_lsn;

	struct palloc_pool *pool;
	enum log_mode {
		LOG_READ,
		LOG_WRITE
	} mode;

	bool valid, inprogress, no_wet;

	size_t bytes_written;
	off_t offset, wet_rows_offset[WAL_PACK_MAX * 8];

#if HAVE_SYNC_FILE_RANGE
	size_t sync_bytes;
	off_t sync_offset;
#endif
}
- (XLog *) init_filename:(const char *)filename_
		      fd:(FILE *)fd_
		     dir:(XLogDir *)dir_;
- (const char *)final_filename;
- (void) follow:(follow_cb *)cb;
- (void) reset_inprogress;
- (int) inprogress_rename;
- (int) inprogress_unlink;
- (int) read_header;
- (int) write_header;
- (int) flush;
- (void) fadvise_dont_need;
- (size_t) rows;
- (size_t)wet_rows_offset_available;
- (i64) append_row:(const void *)data len:(u32)data_len scn:(i64)scn tag:(u16)tag cookie:(u64)cookie;
- (i64) append_row:(const void *)data len:(u32)data_len scn:(i64)scn tag:(u16)tag;
- (void) configure_for_write:(i64)lsn;
- (i64) confirm_write;
@end

struct tbuf *convert_row_v11_to_v12(struct tbuf *orig);
@interface XLog11: XLog
@end

@interface XLog12: XLog {
	i64 next_scn;
}
- (void) configure_for_write:(i64)lsn next_scn:(i64)scn;
@end


@interface XLogWriter: Object {
	i64 lsn, scn, last_scn;
	struct child *wal_writer;
	XLogDir *wal_dir, *snap_dir;
	bool local_writes;
	XLog *wal_to_close;
	ev_timer wal_timer;
@public
	XLog *current_wal;	/* the WAL we'r currently reading/writing from/to */
	int snap_io_rate_limit;
	u32 run_crc_log, run_crc_mod;
}

- (i64) scn;
- (void) set_scn:(i64)scn;
- (i64) lsn;

- (struct child *) wal_writer;
- (void) configure_wal_writer;

- (struct wal_pack *) wal_pack_prepare;
- (u32) wal_pack_append:(struct wal_pack *)pack
		   data:(const void *)data
		    len:(u32)data_len
		    scn:(i64)scn
		    tag:(u16)tag
		 cookie:(u64)cookie;
- (int) wal_pack_submit;
- (int) wal_pack_submit_x; // FIXME: hack
- (int) wal_row_submit:(const void *)data len:(u32)len scn:(i64)scn tag:(u16)tag;

- (int) snapshot:(bool)sync;
- (int) snapshot_write;
- (int) snapshot_initial;
- (u32) snapshot_estimate;
- (int) snapshot_write_rows:(XLog *)snap;
@end

@interface XLogWriter (Fold)
- (int) snapshot_fold;
@end

@interface XLogPuller: Object <XLogPuller, XLogPullerAsync> {
	struct conn c;
	struct sockaddr_in addr;
	u32 version;
	bool abort;
	struct fiber *in_recv;
}

- (ssize_t) recv;
- (void) abort_recv;

- (XLogPuller *) init;
- (XLogPuller *) init_addr:(struct sockaddr_in *)addr_;
- (int) handshake:(i64)scn err:(const char **)err_ptr;
- (int) handshake:(struct sockaddr_in *)addr_ scn:(i64)scn;
- (int) handshake:(struct sockaddr_in *)addr_ scn:(i64)scn err:(const char **)err_ptr;
@end

@interface Recovery: XLogWriter {
	i64 last_wal_lsn;
	ev_tstamp lag, last_update_tstamp, run_crc_verify_tstamp;
	char status[64];

	struct mhash_t *pending_row;

	XLogPuller *remote_puller;
	const char *feeder_addr;

	bool run_crc_log_mismatch, run_crc_mod_mismatch;
	u32 processed_rows, estimated_snap_rows;

	struct crc_hist { i64 scn; u32 log; u32 mod; } crc_hist[512]; /* should be larger then cfg.wal_writer_inbox_size */
	unsigned crc_hist_i;

	i64 next_skip_scn;
	struct tbuf skip_scn;
}

- (const char *) status;
- (ev_tstamp) lag;
- (ev_tstamp) last_update_tstamp;
- (ev_tstamp) run_crc_lag;
- (const char *) run_crc_status;

- (void) apply:(struct tbuf *)op tag:(u16)tag;
- (void) apply_row:(const struct row_v12 *)row;
- (void) recover_row:(const struct row_v12 *)row;
- (void) recover_finalize;
- (i64) recover_start;
- (void) recover_follow:(ev_tstamp)delay;
- (i64) recover_snap;
- (i64) recover_cont;
- (void) wal_final_row;
- (int) recover_follow_remote:(XLogPuller *)puller exit_on_eof:(int)exit_on_eof;
- (void) enable_local_writes;
- (bool) is_replica;

- (void) feeder_change_from:(const char *)old to:(const char *)new;

- (int) submit:(const void *)data len:(u32)len;
- (int) submit:(const void *)data len:(u32)len tag:(u16)tag;
- (int) submit_run_crc;

- (const struct row_v12 *)dummy_row_lsn:(i64)lsn_ scn:(i64)scn_ tag:(u16)tag;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir
        rows_per_wal:(int)rows_per_wal
	 feeder_addr:(const char *)feeder_addr
	 fsync_delay:(double)wal_fsync_delay
       run_crc_delay:(double)run_crc_delay
	nop_hb_delay:(double)nop_hb_delay
               flags:(int)flags
  snap_io_rate_limit:(int)snap_io_rate_limit;
@end


i64 fold_scn;
@interface FoldRecovery: Recovery
- (id) init_snap_dir:(const char *)snap_dirname
	     wal_dir:(const char *)wal_dirname;
@end

@interface NoWALRecovery: Recovery
@end

int wal_disk_writer(int fd, void *state);
void wal_disk_writer_input_dispatch(va_list ap __attribute__((unused)));
int snapshot_write_row(XLog *l, u16 tag, struct tbuf *row);

struct replication_handshake {
		u32 ver;
		i64 scn;
		char filter[32];
} __attribute__((packed));

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
	return (struct _row_v11 *)t->ptr;
}

static inline struct row_v12 *row_v12(const struct tbuf *t)
{
	return (struct row_v12 *)t->ptr;
}


int read_log(const char *filename, void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row));

void print_gen_row(struct tbuf *out, const struct row_v12 *row,
		   void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row));
