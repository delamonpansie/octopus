/*
 * Copyright (C) 2015, 2016 Mail.RU
 * Copyright (C) 2015, 2016 Yuriy Vostrikov
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

#ifndef SHARD_H
#define SHARD_H

#include <util.h>
#import <run_crc.h>

#define MAX_SHARD 4096

@class XLog;
@protocol Shard;
@protocol Executor;

@interface Shard: Object {
	ev_tstamp last_update_tstamp, lag;
	u32 run_crc_log;
	struct run_crc run_crc_state;
	char status_buf[64];
	char type;
@public
	int id, rc;
	id<Executor> executor;
	i64 scn;
	bool dummy, loading, snap_loaded;
	char peer[5][16];
}
- (id) init_id:(int)shard_id scn:(i64)scn_ sop:(const struct shard_op *)sop;

- (int) id;
- (i64) scn;
- (id<Executor>)executor;
- (ev_tstamp) run_crc_lag;
- (const char *) run_crc_status;
- (u32) run_crc_log;
- (int) submit_run_crc;
- (void) status_update:(const char *)fmt, ...;
- (const char *)status;

- (ev_tstamp) lag;
- (ev_tstamp) last_update_tstamp;

- (struct shard_op *)snapshot_header;
- (const struct row_v12 *)snapshot_write_header:(XLog *)snap;

- (void) alter:(struct shard_op *)sop;
- (void) wal_final_row;
- (void) enable_local_writes;
- (bool) our_shard;

- (void) set_executor:(id)executor_;

- (void) fill_feeder_param:(struct feeder_param *)feeder peer:(int)i;
- (i64) handshake_scn;
- (void) load_from_remote;
- (int) prepare_remote_row:(struct row_v12 *)row offt:(int)offt;
- (void) update_run_crc:(const struct wal_reply *)reply;

@end

enum shard_type { SHARD_TYPE_POR = 0, SHARD_TYPE_PART = 2 } ;

struct shard_route {
	Shard<Shard> *shard;
	struct iproto_egress *proxy;
	struct rwlock lock;
};

struct shard_conf {
	int id;
	const char *mod_name;
	enum shard_type type;
	const struct feeder_param *feeder_param;
};

struct shard_route shard_rt[MAX_SHARD];

void update_rt(int shard_id, Shard<Shard> *shard, const char *peer_name);

enum port_type { PORT_PRIMARY, PORT_REPLICATION };
const struct sockaddr_in *peer_addr(const char *name, enum port_type port_type);

void shard_log(const char *msg, int shard_id);
void route_info(const struct shard_route *route, struct tbuf *buf);

#endif
