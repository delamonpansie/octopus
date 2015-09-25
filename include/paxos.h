/*
 * Copyright (C) 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

#ifndef PAXOS_H
#define PAXOS_H

#include <iproto.h>
#include <third_party/tree.h>
#import <log_io.h>

struct paxos_peer;
struct proposal;
RB_HEAD(ptree, proposal);

@class Recovery;
@class XLogWriter;
@class XLog;
@protocol RecoveryClient;

@interface Paxos: Shard <Shard> {
@public
	SLIST_HEAD(paxos_group, paxos_peer) group;
	struct iproto_egress_list paxos_remotes;
	struct Fiber *proposer_fiber;
	struct Fiber *output_flusher, *reply_reader, *follower, *wal_dumper;
	i64 app_scn, max_scn, run_crc_scn;
	bool wal_dumper_busy;
	int leader_id, self_id;

	struct ptree proposals;
}

struct iproto_service;
+ (void) service:(struct iproto_service *)s;

@end

struct sockaddr_in *paxos_leader_primary_addr(Paxos *paxos);
struct feeder_param *paxos_peer_feeder(Paxos *paxos, int id);

enum proposal_flags { P_APPLIED	= 0x01,
		      P_WALED = 0x02 };

struct proposal {
	const i64 scn;
	u64 ballot;
	u32 flags;
	u32 value_len; /* must be same type with msg_paxos->value_len */
	u8 *value;
	u16 tag;
	ev_tstamp delay, tstamp;
	struct Fiber *waiter;
	RB_ENTRY(proposal) link;
};

struct proposal *proposal(Paxos *r, i64 scn);
void proposal_update_ballot(struct proposal *p, u64 ballot);
void proposal_update_value(struct proposal *p, u32 value_len, const char *value, u16 tag);
void proposal_mark_applied(Paxos *r, struct proposal *p);

int paxos_submit(Paxos *paxos, const void *data, u32 len, u16 tag);


#endif
