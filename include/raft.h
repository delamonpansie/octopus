/*
 * Copyright (C) 2016 Mail.RU
 * Copyright (C) 2016 Yury Vostrikov
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

#ifndef RAFT_H
#define RAFT_H

#include <iproto.h>
#include <third_party/tree.h>
#import <log_io.h>

#define RAFT_CODE(_)					\
	_(RAFT_REQUEST_VOTE, 0xffa1)			\
	_(RAFT_APPEND_ENTRIES, 0xffa2)			\
	_(RAFT_PULL_ENTRIES, 0xffa3)

enum raft_msg_code ENUM_INITIALIZER(RAFT_CODE);

@interface Raft: Shard <Shard> {
@public
	struct iproto_egress_list remotes;
	struct Fiber *wal_dumper, *elector;
	i64 run_crc_scn;
	bool wal_dumper_idle;
	int leader_id, peer_id, remote_count;

	TAILQ_HEAD(log_tailq, log_entry) log;
	struct log_entry *commited;

	enum { FOLLOWER, CANDIDATE, LEADER } role;
	i64 term;
	i64 leader_commit;
	i64 last_wal_commit, last_wal_append ;
	ev_tstamp election_deadline;
	int voted_for, nop_commited;
	struct Fiber *catchup[5];
	struct iproto_egress *egress[5];
	struct iproto_mbox mbox;
}

@end

void raft_service(struct iproto_service *s);
#endif
