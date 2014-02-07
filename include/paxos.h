/*
 * Copyright (C) 2011, 2012, 2013 Mail.RU
 * Copyright (C) 2011, 2012, 2013 Yuriy Vostrikov
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

struct paxos_peer;
struct proposal;
RB_HEAD(ptree, proposal);

@interface PaxosRecovery: Recovery {
@public
	SLIST_HEAD(paxos_group, paxos_peer) group;
	struct iproto_group remotes;
	struct fiber *proposer_fiber;
	struct fiber *output_flusher, *reply_reader, *follower, *wal_dumper;
	struct palloc_pool *pool;
	i64 app_scn, max_scn;
	bool wal_dumper_busy;
	struct ptree proposals;
	struct service service;
}
- (int) submit:(const void *)data len:(u32)data_len scn:(i64)scn_ tag:(u16)tag;
- (i64) next_scn;
@end

void paxos_print(struct tbuf *out,
		 void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row),
		 const struct row_v12 *row);

#endif
