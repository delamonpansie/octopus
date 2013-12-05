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

#ifndef LOG_IO_H
#define LOG_IO_H

#include <util.h>
#include <net_io.h>
#include <objc.h>

#include <stdio.h>
#include <limits.h>


#define RECOVER_READONLY 1
#define WAL_PACK_MAX 1024

/* despite having type encoding tag must be unique */

enum { snap_initial_tag = 1,  /* SNAP */
       snap_tag,              /* SNAP */
       wal_tag,               /* WAL */
       snap_final_tag,        /* SNAP */
       wal_final_tag,         /* WAL */
       run_crc,               /* WAL */
       nop,                   /* WAL */
       snap_skip_scn,         /* SNAP */
       paxos_prepare,         /* SYS */
       paxos_promise,         /* SYS */
       paxos_propose,         /* SYS */
       paxos_accept,          /* SYS */
       paxos_nop,             /* SYS */

       user_tag = 32
};


/* two highest bit in tag encode tag type:
   00 - invalid
   01 - snap
   10 - wal
   11 - system wal */

#define TAG_MASK 0x3fff
#define TAG_SIZE 14
#define TAG_SNAP 0x4000
#define TAG_WAL 0x8000
#define TAG_SYS 0xc000

static inline bool dummy_tag(int tag) /* dummy row tag */
{
	return (tag & TAG_MASK) == wal_final_tag;
}

static inline u16 fix_tag(u16 tag)
{
	switch (tag) {
	case snap_initial_tag:	return tag | TAG_SNAP;
	case snap_tag:		return tag | TAG_SNAP;
	case wal_tag:		return tag | TAG_WAL;
	case snap_final_tag:	return tag | TAG_SNAP;
	case wal_final_tag:	return tag | TAG_WAL;
	case run_crc:		return tag | TAG_WAL;
	case nop:		return tag | TAG_WAL;
	case snap_skip_scn:	return tag | TAG_SNAP;
	case paxos_prepare:	return tag | TAG_SYS;
	case paxos_promise:	return tag | TAG_SYS;
	case paxos_propose:	return tag | TAG_SYS;
	case paxos_accept:	return tag | TAG_SYS;
	case paxos_nop:		return tag | TAG_SYS;
	default:
		assert(tag & ~TAG_MASK);
		return tag;
	}
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

	const char *filetype;
	const char *suffix;
	const char *dirname;
};
- (id) init_dirname:(const char *)dirname_;
- (XLog *) open_for_read:(i64)lsn;
- (XLog *) open_for_write:(i64)lsn scn:(i64)scn;
- (i64) greatest_lsn;
- (XLog *) containg_lsn:(i64)target_lsn;
- (i64) containg_scn:(i64)target_scn;
@end

@interface SnapDir: XLogDir
@end

@interface WALDir: XLogDir
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
	u8 data[0];
} __attribute__((packed));

@interface XLog: Object <XLogPuller> {
	size_t rows, wet_rows;
	bool eof;
@public
	char *filename;
	FILE *fd;
	void *vbuf;
	ev_stat stat;

	XLogDir *dir;
	i64 next_lsn;

	enum log_mode {
		LOG_READ,
		LOG_WRITE
	} mode;

	bool no_wet, inprogress;

	size_t bytes_written;
	off_t offset, wet_rows_offset[WAL_PACK_MAX * 8];

#if HAVE_SYNC_FILE_RANGE
	size_t sync_bytes;
	off_t sync_offset;
#endif
}
+ (XLog *) open_for_read_filename:(const char *)filename
			      dir:(XLogDir *)dir;

- (void) follow:(follow_cb *)cb data:(void *)data;
- (int) inprogress_rename;
- (int) read_header;
- (int) write_header;
- (int) flush;
- (void) fadvise_dont_need;
- (size_t) rows;
- (size_t) wet_rows_offset_available;
- (const struct row_v12 *) append_row:(const void *)data len:(u32)data_len scn:(i64)scn
				  tag:(u16)tag cookie:(u64)cookie;
- (const struct row_v12 *) append_row:(const void *)data len:(u32)data_len scn:(i64)scn tag:(u16)tag;
- (const struct row_v12 *) append_row:(struct row_v12 *)row data:(const void *)data;
- (i64) confirm_write;
- (void) append_successful:(size_t)bytes;
@end

struct tbuf *convert_row_v11_to_v12(struct tbuf *orig);
@interface XLog11: XLog
@end

@interface XLog12: XLog {
@public
	i64 next_scn;
}
@end

@protocol Txn
- (void) prepare:(struct row_v12 *)row data:(const void *)data;
- (void) commit;
- (void) rollback;
- (void) append:(struct wal_pack *)pack;
- (struct row_v12 *)row;
@end

struct wal_pack {
	struct netmsg_head *netmsg;
	u32 packet_len;
	u32 row_count;
	struct fiber *sender;
	u32 fid;
} __attribute__((packed));

struct wal_reply {
	u32 packet_len;
	u32 row_count;
	struct fiber *sender;
	u32 fid;
	i64 lsn;
	i64 scn;
	u32 run_crc;
} __attribute__((packed));


int wal_pack_prepare(XLogWriter *r, struct wal_pack *);
u32 wal_pack_append_row(struct wal_pack *pack, struct row_v12 *row);
void wal_pack_append_data(struct wal_pack *pack, struct row_v12 *row,
			  const void *data, size_t len);

@protocol RecoveryState
- (i64) lsn;
- (i64) scn;
- (u32) run_crc_log;
@end

@interface SnapWriter: Object {
	id<RecoveryState> state;
	XLogDir *snap_dir;
}
- (id) init_state:(id<RecoveryState>)state snap_dir:(XLogDir *)snap_dir;
- (int) snapshot:(bool)sync;
- (int) snapshot_write;
- (u32) snapshot_estimate;
- (int) snapshot_write_rows:(XLog *)snap;
@end

@interface XLogWriter: Object <RecoveryState> {
	i64 lsn, scn, last_scn;
	XLogDir *wal_dir, *snap_dir;
	ev_timer wal_timer;
	bool configured;
	SnapWriter *snap_writer;
@public
	bool local_writes;
	struct child *wal_writer;
	XLog *current_wal;	/* the WAL we'r currently reading/writing from/to */

	u32 run_crc_log;
	struct crc_hist { i64 scn; u32 log; } crc_hist[512]; /* should be larger than
								cfg.wal_writer_inbox_size */
	unsigned crc_hist_i;
}

- (i64) scn;
- (void) set_scn:(i64)scn;
- (i64) lsn;

- (struct child *) wal_writer;
- (void) configure_wal_writer;

- (int) wal_pack_submit;

/* entry points: modules should call this */
- (int) submit:(id<Txn>)txn;
- (int) submit:(const void *)data len:(u32)len tag:(u16)tag;

- (SnapWriter *) snap_writer;
- (int) write_initial_state;
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
- (void) set_addr:(struct sockaddr_in *)addr_;

/* returns -1 in case of handshake failure. puller is closed.  */
- (int) handshake:(i64)scn err:(const char **)err_ptr;
- (int) handshake:(struct sockaddr_in *)addr_ scn:(i64)scn;
- (int) handshake:(struct sockaddr_in *)addr_ scn:(i64)scn err:(const char **)err_ptr;
@end

@interface Recovery: XLogWriter {
	i64 last_wal_lsn;
	ev_tstamp lag, last_update_tstamp, run_crc_verify_tstamp;
	char status[64];

	XLogPuller *remote_puller;
	const char *feeder_addr;

	bool run_crc_log_mismatch, run_crc_mod_mismatch;
	u32 processed_rows, estimated_snap_rows;

	i64 next_skip_scn;
	struct tbuf skip_scn;

@public
	Class txn_class;
}

- (const char *) status;
- (ev_tstamp) lag;
- (ev_tstamp) last_update_tstamp;
- (ev_tstamp) run_crc_lag;
- (const char *) run_crc_status;

- (void) recover_row:(struct row_v12 *)row;
- (void) verify_run_crc:(struct tbuf *)buf;
- (void) recover_finalize;
- (i64) recover_start;
- (void) recover_follow:(ev_tstamp)delay;
- (i64) recover_snap;
- (i64) recover_cont;
- (void) wal_final_row;
/* pull_wal & load_from_remote throws exceptions on failure */
- (int) pull_wal:(id<XLogPullerAsync>)puller;
- (int) load_from_remote:(XLogPuller *)puller;
- (void) enable_local_writes;
- (bool) is_replica;
- (void) check_replica;

- (void) feeder_change_from:(const char *)old to:(const char *)new;

- (int) submit_run_crc;

- (struct row_v12 *)dummy_row_lsn:(i64)lsn_ scn:(i64)scn_ tag:(u16)tag;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir
        rows_per_wal:(int)rows_per_wal
	 feeder_addr:(const char *)feeder_addr
               flags:(int)flags
	   txn_class:(Class)txn_class;
@end

@interface Recovery (Deprecated)
- (void) apply:(struct tbuf *)op tag:(u16)tag;
@end

@interface FoldRecovery: Recovery
i64 fold_scn;
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

#endif
