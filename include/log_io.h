/*
 * Copyright (C) 2010-2016 Mail.RU
 * Copyright (C) 2010-2017, 2020, 2021 Yury Vostrikov
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
#import <spawn_child.h>
#import <mbox.h>
#import <shard.h>

#include <stdio.h>
#include <limits.h>


#define RECOVER_READONLY 1
#define WAL_PACK_MAX 1024

/* despite having type encoding tag must be unique */

enum row_tag {
	snap_initial = 1,
	snap_data,
	wal_data,
	snap_final,
	wal_final,
	run_crc,
	nop,
	raft_append,
	raft_commit,
	raft_vote,
	shard_create,
	shard_alter,
	shard_final,
	tlv,

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
	return (tag & TAG_MASK) == wal_final;
}


extern const u32 default_version, version_11;
extern const u32 marker, eof_marker;
extern const char *inprogress_suffix;

const char *xlog_tag_to_a(u16 tag);

@class XLog;
@class XLogWriter;
@class Recovery;
@protocol Shard;
@class Shard;

extern Recovery *recovery;

@protocol XLogPuller
- (struct row_v12 *) fetch_row;
- (u32) version;
- (bool) eof;
- (int) close;
@end

@protocol XLogPullerAsync <XLogPuller>
- (ssize_t) recv;
- (void) abort_recv; /* abort running recv asynchronously */
- (ssize_t)recv_row;
@end

typedef void (follow_cb)(ev_stat *w, int events);

struct XLogDirRS;
extern int xlog_dir_fd(struct XLogDirRS *);

@interface XLogDir: Object {
	Class xlog_class;
@public
	const char *filetype;
	const char *suffix;
	const char *dirname;
	struct XLogDirRS *rs_dir;
};
- (id) init_dirname:(const char *)dirname_;
- (XLog *) open_for_read:(i64)lsn;
- (XLog *) open_for_write:(i64)lsn;
- (XLog *) find_with_lsn:(i64)lsn;
- (XLog *) find_with_scn:(i64)scn shard:(int)shard_id;
- (i64) greatest_lsn;
- (int) lock;
- (int) sync;
@end

@interface SnapDir: XLogDir
@end

@interface WALDir: XLogDir
@end

extern XLogDir *wal_dir, *snap_dir;

struct row_v12 {
	u32 header_crc32c;
	i64 lsn;
	i64 scn;
	u16 tag;

	u16 shard_id;

	union {
		u8 remote_scn[6];
		struct {
			u32 run_crc;
		} __attribute__((packed));
	} __attribute__((packed));

	double tm;
	u32 len;
	u32 data_crc32c;
	u8 data[0];
} __attribute__((packed));
struct row_v12 *dummy_row(i64 lsn, i64 scn, u16 tag);

typedef struct marker_desc {
	u64 marker, eof;
	off_t size, eof_size;
} marker_desc_t;

@interface XLog: Object <XLogPuller> {
	size_t rows, wet_rows;
	bool eof, header_written;

#if HAVE_SYNC_FILE_RANGE
	size_t sync_bytes;
	off_t sync_offset;
#endif
	void *vbuf;
	ev_stat stat;

	FILE *fd;
	i64 last_read_lsn;
	u16 tag_mask;
@public
	char *filename;

	XLogDir *dir;
	i64 lsn, next_lsn;

	enum log_mode {
		LOG_READ,
		LOG_WRITE
	} mode;

	bool no_wet, inprogress;

	size_t bytes_written, wet_rows_offset_size;
	off_t offset, alloced, *wet_rows_offset;
}
+ (XLog *) open_for_read_filename:(const char *)filename
			      dir:(XLogDir *)dir;

- (void) follow:(follow_cb *)cb data:(void *)data;
- (int) inprogress_rename;
- (int) read_header;
- (void) write_header;
- (int) flush;
- (void) fadvise_dont_need;
- (size_t) rows;
- (i64) last_read_lsn;
- (const struct row_v12 *) append_row:(const void *)data len:(u32)data_len scn:(i64)scn tag:(u16)tag;
- (const struct row_v12 *) append_row:(const void *)data len:(u32)data_len shard:(Shard *)shard tag:(u16)tag;
- (const struct row_v12 *) append_row:(struct row_v12 *)row data:(const void *)data;

- (i64) confirm_write;
- (void) append_successful:(size_t)bytes;
- (int) fileno;
- (int) write_eof_marker;
- (marker_desc_t) marker_desc;
@end

@interface XLog12: XLog
- (void) write_header_scn:(const i64 *)scn;
@end

struct wal_pack {
	struct netmsg_head *netmsg;
	struct row_v12 *row;
	struct wal_request *request;
	TAILQ_ENTRY(wal_pack) link;
	struct Fiber *fiber;
	i64 epoch, seq;
	int shard_id;
	int row_count;
};

struct wal_request {
	u32 packet_len;
	u32 row_count;
	u32 magic;
	i64 seq;
	i64 epoch;
} __attribute__((packed));

struct wal_reply {
	u32 packet_len;
	u32 row_count;
	i64 seq, epoch, lsn;
} __attribute__((packed));


void wal_pack_prepare(XLogWriter *r, struct wal_pack *);
u32 wal_pack_append_row(struct wal_pack *pack, struct row_v12 *row);
void wal_pack_append_data(struct wal_pack *pack, const void *data, size_t len);


struct shard_op {
	u8 ver;
	u8 type;
	u32 row_count;
	char mod_name[16];
	char peer[5][16];
	u16 aux_len;
	char aux[0];
} __attribute__((packed));

bool our_shard(const struct shard_op *sop);

@protocol Executor
- (id) init;
- (void) set_shard:(Shard<Shard> *)obj;
- (void) apply:(struct tbuf *)data tag:(u16)tag;
- (void) wal_final_row;
- (void) status_changed;
- (void) print:(const struct row_v12 *)row into:(struct tbuf *)buf;
- (u32) snapshot_estimate;
- (int) snapshot_write_rows:(XLog *)snap;
@end

@protocol RecoveryState
- (i64) lsn;
- (Shard<Shard> *) shard:(unsigned)shard_id;
@end

@protocol RecoverRow
- (void) recover_row:(struct row_v12 *)row;
@end

@interface XLogReader : Object {
	i64 lsn;
	id<RecoverRow> recovery;
	XLog *current_wal;
	ev_timer wal_timer;
}
- (id) init_recovery:(id<RecoverRow>)recovery;
- (i64) lsn;

- (i64) load_full:(XLog *)preferred_snap;
- (i64) load_incr:(XLog *)initial_xlog;
- (void) hot_standby;

- (void) recover_follow:(ev_tstamp)wal_dir_rescan_delay;
- (i64) recover_snap;
- (void) recover_remaining_wals;
- (i64) recover_finalize;
@end

@interface SnapWriter: Object {
	id<RecoveryState> state;
}
- (id) init_state:(id<RecoveryState>)state;
- (int) snapshot_write;
@end

@protocol XLogWriter
- (i64) lsn;
- (struct wal_reply *) wal_pack_submit;
@end


@interface XLogWriter: Object <XLogWriter> {
	i64 lsn;
	id<RecoveryState> state;
	struct child wal_writer;
	struct netmsg_io *io;
	ev_prepare prepare;
	struct netmsg_pool_ctx ctx;
@public
	i64 epoch, seq;
	TAILQ_HEAD(wal_pack_tailq, wal_pack) wal_queue;
}
- (id) init_lsn:(i64)lsn
	  state:(id<RecoveryState>)state;

- (const struct child *) wal_writer;
@end

@interface DummyXLogWriter: XLogWriter
@end

@interface XLogPuller: Object <XLogPuller, XLogPullerAsync> {
	int fd;
	struct tbuf rbuf;

	u32 version;
	bool abort;
	struct Fiber *in_recv;
	struct feeder_param *feeder;
	char errbuf[64];
}

- (ssize_t) recv;
- (void) abort_recv;

- (id) init;
- (id) init:(struct feeder_param*)_feeder;

- (void) feeder_param:(struct feeder_param*)_feeder;
/* returns -1 in case of handshake failure. puller is closed.  */
- (int) handshake:(i64)scn;
- (const char *)error;
@end

#define REPLICATION_FILTER_NAME_LEN 32
#define replication_handshake_base_fields \
	u32 ver; \
	i64 scn; \
	char filter[REPLICATION_FILTER_NAME_LEN]

struct replication_handshake_base {
	replication_handshake_base_fields;
} __attribute__((packed));

#define replication_handshake_v1 replication_handshake_base

struct replication_handshake_v2 {
	replication_handshake_base_fields;
	u32 filter_type;
	u32 filter_arglen;
	char filter_arg[];
} __attribute__((packed));

struct feeder_param {
	struct sockaddr_in addr;
	u32 ver;
	struct feeder_filter {
		u32 type;
		u32 arglen;
		char *name;
		void *arg;
	} filter;
};

enum feeder_cfg_e {
	FEEDER_CFG_OK = 0,
	FEEDER_CFG_BAD_ADDR = 1,
	FEEDER_CFG_BAD_FILTER = 2,
	FEEDER_CFG_BAD_VERSION = 4,
};
enum feeder_cfg_e feeder_param_fill_from_cfg(struct feeder_param *param, struct octopus_cfg *cfg);
bool feeder_param_set_addr(struct feeder_param *param, const char *addr);
bool feeder_param_eq(struct feeder_param *this, struct feeder_param *that);

enum feeder_filter_type {
	FILTER_TYPE_ID  = 0,
	FILTER_TYPE_LUA = 1,
	FILTER_TYPE_C   = 2,
	FILTER_TYPE_MAX = 3
};

@interface XLogRemoteReader : Object {
	XLogPuller *remote_puller;
	id<RecoverRow> recovery;
}
- (id) init_recovery:(id<RecoverRow>)recovery_;
- (int) load_from_remote:(struct feeder_param *)remote; /* throws exceptions on failure */

@end

@interface XLogReplica : Object {
	struct feeder_param feeder;
	XLogPuller *remote_puller;
	struct mbox_void_ptr mbox;
@public
	Shard<Shard> *shard;
}
- (id)init_shard:(id<Shard>)shard;
- (struct sockaddr_in) feeder_addr;
- (bool) feeder_addr_configured;
- (void) set_feeder:(struct feeder_param*)new;
- (void) hot_standby:(struct feeder_param*)feeder_;
- (void) abort_and_free;
@end


@protocol Shard <RecoverRow>
- (int) id;
- (i64) scn;
- (id<Executor>)executor;
- (ev_tstamp) last_update_tstamp;
- (ev_tstamp) lag;

- (const char *) run_crc_status;

- (void) load_from_remote;

- (int) submit:(const void *)data len:(u32)len tag:(u16)tag;

- (const char *) status;
- (void) status_update:(const char *)fmt, ...;
- (bool) is_replica;

- (void) adjust_route;
- (struct shard_op *)shard_op;
- (struct row_v12 *)creator_row;
@end

@interface POR: Shard <Shard,RecoverRow> {
	XLogReplica *remote;
	bool partial_replica, partial_replica_loading;
	i64 wet_scn, remote_scn;
	u32 wet_run_crc;
	struct feeder_param feeder;
	char feeder_param_arg[32];
}
- (void) set_remote_scn:(const struct row_v12 *)row;
@end

@interface Recovery: Object <RecoveryState, RecoverRow> {
	XLogReader *reader;
	bool initial_snap, remote_loading;

	SnapWriter *snap_writer;
	ev_timer snapshot_timer;
	bool snapshot_running;
	i64 last_snapshot_lsn;
@public
	id<XLogWriter> writer;
	struct rwlock snapshot_lock;
	struct mbox_void_ptr rt_notify_mbox;
	Class default_exec_class;
}
- (i64) lsn;
- (id<XLogWriter>)writer;

- (void) simple:(struct iproto_service *)service;
- (void) lock; /* lock wal_dir & snap_dir */

- (void) configure_wal_writer:(i64)lsn;

- (i64) load_from_local; /* load from local snap+wal */
- (void) enable_local_writes;

- (void) shard_info:(struct tbuf *)buf;
- (int) write_initial_state;
- (int) fork_and_snapshot;
void fork_and_snapshot(va_list ap);

- (Shard<Shard> *) shard_create_dummy:(const struct row_v12 *)row;
@end
/* this method were exported for non-sharded configuration
 * (ie when only dummy shard #0)
 * It should be called in [executor wal_final_row] to setup
 * iproto_service for primary port, so some management
 * service commands will be registered */
void set_recovery_service(struct iproto_service *service);

@interface Recovery (Deprecated)
- (void) apply:(struct tbuf *)op tag:(u16)tag;
@end

@interface Recovery (Fold)
- (int) snapshot_fold;
@end
extern i64 fold_scn;

static inline struct row_v12 *row_v12(const struct tbuf *t)
{
	return (struct row_v12 *)t->ptr;
}


int read_log(const char *filename, void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row));


void print_row(struct tbuf *out, const struct row_v12 *row,
	       void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row));

struct octopus_cfg_peer *cfg_peer_by_name(const char *name);

@interface DefaultExecutor : Object {
@public
	Shard<Shard> *shard;
}
- (id) init;
- (void) set_shard:(Shard<Shard> *)shard_;
- (u32) snapshot_estimate;
- (void) wal_final_row;
- (void) status_changed;
- (void) print:(const struct row_v12 *)row into:(struct tbuf *)buf;
@end

@interface DefaultExecutor (SnapFinal)
- (void)snap_final_row;
@end

#endif
