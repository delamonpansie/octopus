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

enum { snap_initial = 1,
       snap_data,
       wal_data,
       snap_final,
       wal_final,
       run_crc,
       nop,
       snap_skip_scn,
       paxos_prepare,
       paxos_promise,
       paxos_propose,
       paxos_accept,
       paxos_nop,

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

static inline bool scn_changer(int tag)
{
	int tag_type = tag & ~TAG_MASK;
	tag &= TAG_MASK;
	return tag_type == TAG_WAL || tag == nop || tag == run_crc;
}

static inline bool dummy_tag(int tag) /* dummy row tag */
{
	return (tag & TAG_MASK) == wal_final;
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
	int fd;
@public
	size_t rows_per_file;
	double fsync_delay;

	const char *filetype;
	const char *suffix;
	const char *dirname;
	Recovery *recovery;
};
- (id) init_dirname:(const char *)dirname_;
- (XLog *) open_for_read:(i64)lsn;
- (XLog *) open_for_write:(i64)lsn scn:(i64)scn;
- (i64) greatest_lsn;
- (XLog *) containg_lsn:(i64)target_lsn;
- (i64) containg_scn:(i64)target_scn;
- (int) lock;
- (int) stat:(struct stat *)buf;
@end

@interface SnapDir: XLogDir
@end

@interface WALDir: XLogDir
@end

struct _row_v04 {
	i64 lsn;
	u16 type;
	u32 len;
	u8 data[];
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
	u8 data[0];
} __attribute__((packed));

typedef struct marker_desc {
	u64 marker, eof;
	off_t size, eof_size;
} marker_desc_t;

@interface XLog: Object <XLogPuller> {
	size_t rows, wet_rows;
	bool eof;

#if HAVE_SYNC_FILE_RANGE
	size_t sync_bytes;
	off_t sync_offset;
#endif
	void *vbuf;
	ev_stat stat;

	FILE *fd;
@public
	char *filename;

	XLogDir *dir;
	i64 next_lsn;

	enum log_mode {
		LOG_READ,
		LOG_WRITE
	} mode;

	bool no_wet, inprogress;

	size_t bytes_written;
	off_t offset, wet_rows_offset[WAL_PACK_MAX * 8];
}
+ (XLog *) open_for_read_filename:(const char *)filename
			      dir:(XLogDir *)dir;
+ (void) register_version4: (Class)xlog;
+ (void) register_version3: (Class)xlog;

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
void fixup_row_v12(struct row_v12 *);
u16 fix_tag_v2(u16 tag);

@interface XLog04: XLog
@end

@interface XLog03Template: XLog
@end

@interface XLog11: XLog
@end

@interface XLog12: XLog {
@public
	i64 next_scn;
}
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

#define replication_handshake_base_fields \
	u32 ver; \
	i64 scn; \
	char filter[32]
struct replication_handshake_base {
	replication_handshake_base_fields;
} __attribute__((packed));
#define REPLICATION_FILTER_NAME_LEN field_sizeof(struct replication_handshake_base, filter)

#define replication_handshake_v1 replication_handshake_base

struct replication_handshake_v2 {
	replication_handshake_base_fields;
	u32 filter_type;
	u32 filter_arglen;
	char filter_arg[];
} __attribute__((packed));

struct feeder_filter {
	u32 type;
	u32 arglen;
	char *name;
	void *arg;
};

struct feeder_param {
	struct sockaddr_in addr;
	u32 ver;
	struct feeder_filter filter;
};

static inline bool
feeder_param_eq(struct feeder_param *this, struct feeder_param *that)
{
	bool equal =
		this->ver == that->ver &&
		this->addr.sin_family == that->addr.sin_family &&
		this->addr.sin_addr.s_addr == that->addr.sin_addr.s_addr &&
		this->addr.sin_port == that->addr.sin_port &&
		this->filter.type == that->filter.type &&
		this->filter.arglen == that->filter.arglen;
	if (!equal) return false;
	bool this_name_empty = this->filter.name == NULL || strlen(this->filter.name) == 0;
	bool that_name_empty = that->filter.name == NULL || strlen(that->filter.name) == 0;
	equal = (this_name_empty && that_name_empty) ||
		(!this_name_empty && !that_name_empty &&
		 strcmp(this->filter.name, that->filter.name) == 0);
	if (!equal) return false;
	equal = this->filter.arglen == 0 ||
		memcmp(this->filter.arg, that->filter.arg, this->filter.arglen) == 0;
	return equal;
}

enum feeder_cfg_e {
	FEEDER_CFG_OK = 0,
	FEEDER_CFG_BAD_ADDR = 1,
	FEEDER_CFG_BAD_FILTER = 2,
	FEEDER_CFG_BAD_VERSION = 4,
};
enum feeder_cfg_e feeder_param_fill_from_cfg(struct feeder_param *param, struct octopus_cfg *cfg);
bool feeder_param_set_addr(struct feeder_param *param, const char *addr);


enum {
	FILTER_TYPE_ID  = 0,
	FILTER_TYPE_LUA = 1,
	FILTER_TYPE_C   = 2,
	FILTER_TYPE_MAX = 3
};


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
- (int) submit:(const void *)data len:(u32)len tag:(u16)tag;

- (SnapWriter *) snap_writer;
- (int) write_initial_state;
@end

@interface XLogWriter (Fold)
- (int) snapshot_fold;
@end

@interface XLogPuller: Object <XLogPuller, XLogPullerAsync> {
	struct conn c;
	u32 version;
	bool abort;
	struct fiber *in_recv;
	struct feeder_param *feeder;
	char errbuf[64];
}

- (ssize_t) recv;
- (void) abort_recv;

- (XLogPuller *) init;
- (XLogPuller *) init:(struct feeder_param*)_feeder;
- (void) feeder_param:(struct feeder_param*)_feeder;
/* returns -1 in case of handshake failure. puller is closed.  */
- (int) handshake:(i64)scn;
- (const char *)error;
@end

enum recovery_status { LOADING = 1, PRIMARY, STANDBY };
@interface Recovery: XLogWriter {
@public
	i64 last_wal_lsn;
	ev_tstamp lag, last_update_tstamp, run_crc_verify_tstamp;
	enum recovery_status status, prev_status;
	char status_buf[64];

	XLogPuller *remote_puller;
	struct feeder_param feeder;

	bool run_crc_log_mismatch, run_crc_mod_mismatch;
	i64 recovered_rows;
	u32 estimated_snap_rows;

	i64 next_skip_scn;
	struct tbuf skip_scn;
}

- (const char *) status;
- (ev_tstamp) lag;
- (ev_tstamp) last_update_tstamp;
- (ev_tstamp) run_crc_lag;
- (const char *) run_crc_status;

- (void) simple;
- (void) lock; /* lock wal_dir & snap_dir */

- (void) recover_row:(struct row_v12 *)row;
- (void) verify_run_crc:(struct tbuf *)buf;

- (i64) recover_snap;
- (void) recover_remaining_wals;

- (i64) load_from_local; /* load from local snap+wal */
- (int) load_from_remote; /* fetch and load snap+wal from feeder. doesn't persist anything */

- (void) local_hot_standby;
- (void) recover_follow:(ev_tstamp)delay;
- (void) recover_finalize;

- (void) wal_final_row;
/* pull_wal & load_from_remote throws exceptions on failure */
- (int) pull_wal:(id<XLogPullerAsync>)puller;
- (void) pull_from_remote:(id<XLogPullerAsync>)puller;
- (void) enable_local_writes;
- (bool) is_replica;
- (void) check_replica;

- (int) submit_run_crc;

- (struct row_v12 *)dummy_row_lsn:(i64)lsn_ scn:(i64)scn_ tag:(u16)tag;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir;

- (id) init_snap_dir:(const char *)snap_dir
             wal_dir:(const char *)wal_dir
        rows_per_wal:(int)rows_per_wal
	feeder_param:(struct feeder_param*)feeder_
               flags:(int)flags;

- (struct sockaddr_in) feeder_addr;
- (bool) feeder_addr_configured;
- (bool) feeder_changed:(struct feeder_param*)new;

- (void) status_update:(enum recovery_status)s fmt:(const char *)fmt, ...;
- (void) status_changed;
@end

@interface Recovery (Deprecated)
- (void) apply:(struct tbuf *)op tag:(u16)tag;
@end


@interface NoWALRecovery: Recovery
- (id) init_snap_dir:(const char *)snap_dirname
	     wal_dir:(const char *)wal_dirname;
@end

@interface FoldRecovery: NoWALRecovery
i64 fold_scn;
@end

int wal_disk_writer(int fd, void *state);
void wal_disk_writer_input_dispatch(va_list ap __attribute__((unused)));
int snapshot_write_row(XLog *l, u16 tag, struct tbuf *row);

void remote_hot_standby(va_list ap);

static inline struct _row_v04 *_row_v04(const struct tbuf *t)
{
	return (struct _row_v04 *)t->ptr;
}

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
