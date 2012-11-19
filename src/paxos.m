/*
 * Copyright (C) 2011, 2012 Mail.RU
 * Copyright (C) 2011, 2012 Yuriy Vostrikov
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
#import <assoc.h>
#import <salloc.h>
#import <net_io.h>
#import <log_io.h>
#import <palloc.h>
#import <say.h>
#import <fiber.h>
#import <paxos.h>
#import <iproto.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PAXOS_CODE(_)				\
	_(NACK,	0xf0)				\
	_(LEADER_PROPOSE, 0xf1)			\
	_(LEADER_ACK, 0xf2)			\
	_(LEADER_NACK, 0xf3)			\
	_(PREPARE, 0xf4)			\
	_(PROMISE, 0xf5)			\
	_(ACCEPT, 0xf6)				\
	_(ACCEPTED, 0xf7)			\
	_(DECIDE, 0xf8)				\
	_(QUERY, 0xf9)				\
	_(STALE, 0xfa)

ENUM(paxos_msg_code, PAXOS_CODE);
STRS(paxos_msg_code, PAXOS_CODE);

struct paxos_peer {
	struct iproto_peer iproto;
	int id;
	const char *name;
	struct sockaddr_in primary_addr, feeder_addr;
	SLIST_ENTRY(paxos_peer) link;
};

struct paxos_peer *
make_paxos_peer(int id, const char *name, const char *addr,
		short primary_port, short feeder_port)
{
	struct paxos_peer *p = calloc(1, sizeof(*p));

	p->id = id;
	p->name = name;
	if (init_iproto_peer(&p->iproto, id, name, addr) == -1) {
		free(p);
		return NULL;
	}
	p->primary_addr = p->iproto.addr;
	p->primary_addr.sin_port = htons(primary_port);
	p->feeder_addr = p->iproto.addr;
	p->feeder_addr.sin_port = htons(feeder_port);
	return p;
}

struct paxos_peer *
paxos_peer(PaxosRecovery *r, int id)
{
	struct paxos_peer *p;
	SLIST_FOREACH(p, &r->group, link)
		if (p->id == id)
			return p;
	return NULL;
}

struct msg_leader {
	struct iproto msg;
	i16 leader_id;
	ev_tstamp expire;
} __attribute__((packed));


struct msg_paxos {
	struct iproto header;
	i64 scn;
	u64 ballot;
	u16 tag;
	u32 value_len;
	char value[];
} __attribute__((packed));

struct proposal {
	const i64 scn;
	u64 ballot;
	u32 flags;
	u32 value_len; /* must be same type with msg_paxos->value_len */
	char *value;
	u16 tag;
	struct fiber *waiter;
	ev_tstamp delay;
	RB_ENTRY(proposal) link;
};

#define P_DECIDED 1
#define P_APPLIED 2
#define P_CLOSED 4

/*
  1. P_CLOSED: Continious from initial SCN up to [Recovery lsn]. There are no gaps. Proposal may be P_CLOSED
             only after being P_DECIDED.
  2. P_APPLIED: Proposal my be P_APPLIED only after being P_DECIDED.
              Leader can apply proposal out of order because it may assume theirs independece (this
              follows from the following observation: caller of [Recovery submit] already took all
	      neccecary locks.)
              Accpetors(non leader) _must_ apply proposals in order because they cannot prove
	      proposal's independece.
  3. P_DECIDED: no ordering enforced.
 */


struct slab_cache proposal_cache;
static int
proposal_cmp(const struct proposal *a, const struct proposal *b)
{
	return (a->scn < b->scn) ? -1 : (a->scn > b->scn);
}
#ifndef __unused
#define __unused    __attribute__((__unused__))
#endif
RB_GENERATE_STATIC(ptree, proposal, link, proposal_cmp)

static int leader_id, self_id;
static ev_tstamp leadership_expire;
static const ev_tstamp leader_lease_interval = 0.5;
static const ev_tstamp paxos_default_timeout = 0.2;

struct service *mesh_service;

static struct service *input_service;

extern void title(const char *fmt, ...); /* FIXME: hack */

static bool
paxos_leader()
{
	return leader_id >= 0 && leader_id == self_id;
}

static void
paxos_broadcast(PaxosRecovery *r, enum paxos_msg_code code, ev_tstamp timeout,
		i64 scn, u64 ballot, const char *value, u32 value_len, u16 tag)
{
	struct msg_paxos msg = { .header = { .data_len = sizeof(msg) - sizeof(struct iproto),
					     .sync = 0,
					     .msg_code = code },
				 .scn = scn,
				 .ballot = ballot,
				 .tag = tag,
				 .value_len = value_len };

	struct iproto_req *req = req_make(paxos_msg_code_strs[code], r->quorum, timeout,
					  &msg.header, value, value_len);
	say_debug("%s: > %s sync:%u ballot:%"PRIu64" SCN:%"PRIi64" timeout:%.2f"
		  " value_len:%i value:%s",
		  __func__, paxos_msg_code_strs[code], req->header->sync, ballot, scn, timeout,
		  value_len, tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

	broadcast(&r->remotes, req);
}

static void
paxos_reply(struct conn *c, const struct msg_paxos *req, enum paxos_msg_code code,
	    u64 ballot, const struct proposal *p)
{
	if (c->state == CLOSED)
		return;

	struct msg_paxos *msg = p0alloc(c->pool, sizeof(*msg));
	msg->header = (struct iproto){ code,
				       sizeof(*msg) - sizeof(struct iproto) + (p ? p->value_len : 0),
				       req->header.sync };
	msg->scn = req->scn;
	msg->ballot = ballot ?: req->ballot;
	struct netmsg *m = netmsg_tail(&c->out_messages);

	if (p) {
		msg->value_len = p->value_len;
		msg->tag = p->tag;

		net_add_iov(&m, msg, sizeof(*msg));
		if (p->value_len)
			net_add_iov_dup(&m, p->value, p->value_len);
	} else {
		net_add_iov(&m, msg, sizeof(*msg));
	}

	say_debug("%s: > %s sync:%i SCN:%"PRIi64" ballot:%"PRIu64, __func__,
		  paxos_msg_code_strs[code], msg->header.sync, msg->scn, msg->ballot);

	assert(c->state != CLOSED);
	ev_io_start(&c->out);
}

static void leader_catchup(PaxosRecovery *r);

static void
notify_leadership_change(PaxosRecovery *r)
{
	static int prev_leader = -1;
	if (leader_id < 0) {
		if (prev_leader != leader_id)
			say_info("leader unknown");
		title("paxos_slave");
	} else if (!paxos_leader()) {
		if (prev_leader != leader_id)
			say_info("leader is %s", paxos_peer(r, leader_id)->name);
		title("paxos_slave");
	} else if (paxos_leader()) {
		if (prev_leader != leader_id) {
			say_info("I am leader");
			leader_catchup(r);
		}
		title("paxos_leader");
	}
	prev_leader = leader_id;
}

static void
propose_leadership(va_list ap)
{
	PaxosRecovery *pr = va_arg(ap, PaxosRecovery *);

	struct msg_leader leader_propose = { .msg = { .data_len = sizeof(leader_propose) - sizeof(struct iproto),
						      .sync = 0,
						      .msg_code = LEADER_PROPOSE },
					     .leader_id = self_id };
	fiber_sleep(0.3); /* wait connections to be up */
	for (;;) {
		if (ev_now() > leadership_expire)
			leader_id = -1;

		if (leader_id < 0) {
			fiber_sleep(drand(leader_lease_interval * 0.1));
		} else {
			if (!paxos_leader())
				fiber_sleep(leadership_expire + leader_lease_interval * .01 - ev_now());
			else
				fiber_sleep(leadership_expire - leader_lease_interval * .1 - ev_now());
		}

		if (leader_id >= 0 && !paxos_leader())
			continue;

		say_debug("%s: ELECTIONS expired:%.2f leader:%i", __func__,
			  leadership_expire - ev_now(), leader_id);
		leader_propose.expire = ev_now() + leader_lease_interval;
		broadcast(&pr->remotes, req_make("leader_propose", 1, 1.0,
						 &leader_propose.msg, NULL, 0));
		struct iproto_req *r = yield();

		int votes = 0;
		ev_tstamp nack_leadership_expire = 0;
		int nack_leader_id = -1;
		FOREACH_REPLY(r, reply) {
			if (reply->msg_code == LEADER_ACK) {
				votes++;
			} else {
				assert(reply->msg_code == LEADER_NACK);
				struct msg_leader *msg = (struct msg_leader *)reply;
				nack_leadership_expire = msg->expire;
				nack_leader_id = msg->leader_id;
			}
		}
		if (votes >= r->quorum) {
			say_debug("%s: quorum reached", __func__);
			leadership_expire = leader_propose.expire;
			leader_id = self_id;
		} else {
			if (nack_leader_id != -1) {
				say_debug("%s: nack leader_id:%i", __func__, nack_leader_id);
				leadership_expire = nack_leadership_expire;
				leader_id = nack_leader_id;
			} else {
				say_debug("%s: no quorum", __func__);
			}
		}
		req_release(r);

		notify_leadership_change(pr);
	}
}

static struct proposal *
find_proposal(PaxosRecovery *r, i64 scn)
{
	return RB_FIND(ptree, &r->proposals, &(struct proposal){ .scn = scn });
}

static void
update_proposal_ballot(struct proposal *p, u64 ballot)
{
	say_debug("%s: SCN:%"PRIi64" ballot:%"PRIu64, __func__, p->scn, ballot);
	assert(p->ballot <= ballot);
	p->ballot = ballot;
}

static void
update_proposal_value(struct proposal *p, u32 value_len, const char *value, u16 tag)
{
	say_debug("%s: SCN:%"PRIi64" value_len:%i", __func__, p->scn, value_len);

	if (p->value_len != value_len) {
		assert(p->value_len == 0 && value_len > 0); /* value never goes empty */
		assert((p->flags & P_DECIDED) == 0); /* P_DECIDED is immutable */
		assert(p->tag == 0 || p->tag == tag);
		if (value_len > p->value_len) {
			if (p->value)
			 	sfree(p->value);
			p->value = salloc(value_len);
		}
		p->value_len = value_len;
		p->tag = tag;
	}
	memcpy(p->value, value, value_len);
}

static struct proposal *
create_proposal(PaxosRecovery *r, i64 scn)
{
	say_debug("%s: SCN:%"PRIi64, __func__, scn);

	struct proposal *p = slab_cache_alloc(&proposal_cache);
	struct proposal ini = { .scn = scn, .delay = paxos_default_timeout };
	memcpy(p, &ini, sizeof(*p));
	RB_INSERT(ptree, &r->proposals, p);
	if (r->max_scn < scn)
		r->max_scn = scn;

	return p;
}

static struct proposal *
proposal(PaxosRecovery *r, i64 scn)
{
	return find_proposal(r, scn) ?: create_proposal(r, scn);
}

static struct proposal *
prev_proposal(PaxosRecovery *r, struct proposal *p)
{
	return RB_PREV(ptree, &r->proposals, p) ?: create_proposal(r, p->scn - 1);
}

static void
delete_proposal(PaxosRecovery *r, struct proposal *p)
{
	RB_REMOVE(ptree, &r->proposals, p);
	sfree(p->value);
	slab_cache_free(&proposal_cache, p);
}

static void
expire_proposal(PaxosRecovery *r)
{
	struct proposal *p, *tmp;
	i64 scn = [r scn];

	RB_FOREACH_SAFE(p, ptree, &r->proposals, tmp) {
		if ((p->flags & P_CLOSED) == 0)
			break;

		if (p->scn > scn - 1024)
			break;

		delete_proposal(r, p);
	}
}

static void
promise(PaxosRecovery *r, struct proposal *p, struct conn *c, struct msg_paxos *req)
{
	u64 old_ballot = p->ballot;
	if ([r wal_row_submit:&req->ballot len:sizeof(req->ballot) scn:req->scn tag:paxos_promise] != 1)
		return;

	if (p->ballot != old_ballot) {
		say_warn("concurent update of ballot SCN:%"PRIi64, p->scn);
		if (p->ballot > req->ballot)
			return;
	}

	update_proposal_ballot(p, req->ballot);
	paxos_reply(c, req, PROMISE, old_ballot, p);
}

static void
accepted(PaxosRecovery *r, struct proposal *p, struct conn *c, struct msg_paxos *req)
{
	assert(req->scn == p->scn);
	assert(p->ballot <= req->ballot);
	assert(req->value_len > 0);

	struct tbuf *x = tbuf_alloc(fiber->pool);
	tbuf_append(x, &req->ballot, sizeof(req->ballot));
	tbuf_append(x, &req->tag, sizeof(req->tag));
	tbuf_append(x, &req->value_len, sizeof(req->value_len));
	tbuf_append(x, req->value, req->value_len);

	if ([r wal_row_submit:x->ptr len:tbuf_len(x) scn:req->scn tag:paxos_accept] != 1)
		return;

	update_proposal_ballot(p, req->ballot);
	update_proposal_value(p, req->value_len, req->value, req->tag);
	paxos_reply(c, req, ACCEPTED, 0, NULL);
}

static struct iproto_req *
prepare(PaxosRecovery *r, struct proposal *p, u64 ballot)
{
	if ([r wal_row_submit:&ballot len:sizeof(ballot) scn:p->scn tag:paxos_prepare] != 1)
		panic("give up");
	update_proposal_ballot(p, ballot);
	paxos_broadcast(r, PREPARE, p->delay, p->scn, p->ballot, NULL, 0, 0);
	return yield();
}

static struct iproto_req *
propose(PaxosRecovery *r, struct proposal *p)
{
	assert(p->value_len > 0);
	struct tbuf *m = tbuf_alloc(fiber->pool);
	tbuf_append(m, &p->ballot, sizeof(p->ballot));
	tbuf_append(m, &p->tag, sizeof(p->tag));
	tbuf_append(m, &p->value_len, sizeof(p->value_len));
	tbuf_append(m, p->value, p->value_len);
	if ([r wal_row_submit:m->ptr len:tbuf_len(m) scn:p->scn tag:paxos_propose] != 1)
		panic("give up");
	paxos_broadcast(r, ACCEPT, p->delay, p->scn, p->ballot, p->value, p->value_len, p->tag);
	return yield();
}

static void
decide(PaxosRecovery *r, struct proposal *p)
{
	paxos_broadcast(r, DECIDE, 0, p->scn, p->ballot, p->value, p->value_len, p->tag);
	p->flags |= P_DECIDED;
}

static void
nack(struct conn *c, struct msg_paxos *req, u64 ballot)
{
	paxos_reply(c, req, NACK, ballot, NULL);
}

static void
decided(struct conn *c, struct msg_paxos *req, struct proposal *p)
{
	paxos_reply(c, req, DECIDE, p->ballot, p);
}

static void
maybe_wake_dumper(PaxosRecovery *r, struct proposal *p)
{
	if (p->scn - [r scn] < 8)
		return;
	if (r->wal_dumper_busy)
		return;
	r->wal_dumper_busy = true;
	fiber_wake(r->wal_dumper, NULL);
}

static void
learn(PaxosRecovery *r, struct proposal *p)
{
	if (!p || p->scn <= [r scn] || p->flags & P_APPLIED)
		return;

	if ((p->flags & P_DECIDED) == 0 || p->scn != [r scn] + 1)
		return;


	say_debug("%s: SCN:%"PRIi64" value_len:%i %s", __func__, p->scn,
		  p->value_len, tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));

	assert(p->flags & P_DECIDED);
	assert(p->scn == [r scn] + 1);

	[r apply:&TBUF(p->value, p->value_len, NULL) tag:p->tag]; /* FIXME: what to do if this fails ? */
	p->flags |= P_APPLIED;

	learn(r, RB_NEXT(ptree, &r->proposals, p));
}


static void
learner(PaxosRecovery *r, struct iproto *msg)
{
	struct msg_paxos *mp = (struct msg_paxos *)msg;
	struct proposal *p = proposal(r, mp->scn);

	say_debug("%s: < sync:%i type:DECIDE SCN:%"PRIi64" ballot:%"PRIu64" tag:%s value_len:%i %s", __func__,
		  msg->sync, mp->scn, mp->ballot, xlog_tag_to_a(mp->tag), mp->value_len,
		  tbuf_to_hex(&TBUF(mp->value, mp->value_len, fiber->pool)));

	if (p->flags & P_DECIDED) {
		assert(memcmp(mp->value, p->value, MIN(mp->value_len, p->value_len)) == 0);
		assert(p->tag == mp->tag);
		return;
	}

	if (p->ballot > mp->ballot) {
		say_warn("ignoring stale DECIDE");
		return;
	}

	update_proposal_ballot(p, mp->ballot);
	update_proposal_value(p, mp->value_len, mp->value, mp->tag);
	p->flags |= P_DECIDED;

	maybe_wake_dumper(r, p);
	learn(r, p);
}

static void
acceptor(PaxosRecovery *r, struct conn *c, struct iproto *msg)
{
	struct msg_paxos *mp = (struct msg_paxos *)msg;
	struct proposal *p = proposal(r, mp->scn);

	if (p->flags & P_DECIDED) {
		/* if we already knew the value, notify current leader immediately */
		decided(c, mp, p);
	} else if (p->ballot > mp->ballot) {
		nack(c, mp, p->ballot);
	} else {
		assert(p->ballot <= mp->ballot);
		say_debug("%s: < type:%s sync:%i SCN:%"PRIi64" ballot:%"PRIu64" value_len:%i value:%s",
			  __func__,
			  paxos_msg_code_strs[msg->msg_code], msg->sync, mp->scn, mp->ballot,
			  mp->value_len, tbuf_to_hex(&TBUF(mp->value, mp->value_len, fiber->pool)));
		switch (msg->msg_code) {
		case PREPARE:
			assert(p->ballot <= mp->ballot);
			promise(r, p, c, mp);
			break;
		case ACCEPT:
			accepted(r, p, c, mp);
			break;
		default:
			say_error("%s: < unexpected msg type: %s", __func__, paxos_msg_code_strs[msg->msg_code]);
			break;
		}
	}
}

static int
run_protocol(PaxosRecovery *r, i64 scn, char *value, size_t value_len, u16 tag)
{
	struct proposal *p = proposal(r, scn);

	struct iproto_req *rsp;
	int votes, evalue = 0;

	/* phase 1 */
	const int quorum = 1; /* not counting myself */
	u64 ballot = 0, min_ballot = 0;

	goto start;
retry:
	p->delay *= 1.5;
	if (p->delay > 1)
		p->delay = 1;

start:
	if (!paxos_leader()) { /* FIXME: leadership is required only for leading SCN */
		say_debug("not a leader, givin up");
		return 0;
	}

	if (p->flags & P_DECIDED)
		goto decide;

	min_ballot = MAX(min_ballot, p->ballot);
	do {
		ballot >>= 8;
		ballot++;
		ballot <<= 8;
		ballot |= self_id & 0xff;
	} while (ballot < min_ballot);

	assert(p);

	rsp = prepare(r, p, ballot);
	if (rsp == NULL)
		goto retry;

	struct msg_paxos *max = NULL;
	votes = 0;
	FOREACH_REPLY(rsp, reply) {
		struct msg_paxos *mp = (struct msg_paxos *)reply;
		switch(reply->msg_code) {
		case NACK:
			say_debug("|  NACK ballot:%"PRIu64, mp->ballot);
			assert(mp->ballot > ballot);
			min_ballot = mp->ballot;
			break;
		case PROMISE:
			say_debug("|  PROMISE ballot:%"PRIu64" value_len:%i", mp->ballot, mp->value_len);
			votes++;
			if (max == NULL || mp->ballot > max->ballot)
				max = mp;
			break;
		case DECIDE:
			say_debug("|  DECIDE ballot:%"PRIu64" value_len:%i", mp->ballot, mp->value_len);
			/* some other leader already decided on value */
			update_proposal_ballot(p, ULLONG_MAX);
			update_proposal_value(p, mp->value_len, mp->value, mp->tag);
			p->flags |= P_DECIDED;
			req_release(rsp);
			goto decide;
		default:
			assert(false);
		}
	}

	if (votes < quorum) {
		req_release(rsp);
		goto retry;
	}
	assert(max != NULL);

	if (max->value_len > 0) {
		say_debug("has value for SCN:%"PRIi64" value_len:%i value:%s",
			  p->scn, max->value_len,
			  tbuf_to_hex(&TBUF(max->value, max->value_len, fiber->pool)));
		update_proposal_value(p, max->value_len, max->value, max->tag);
	}

	req_release(rsp);

	/* phase 2 */

	if (p->ballot != ballot) /* oops, proposal changed behind our back */
		goto retry;

	if (p->value_len == 0) {
		p->value_len = value_len;
		p->value = salloc(value_len);
		memcpy(p->value, value, value_len);
		p->tag = tag;
	} else {
		evalue = 1;
	}
	rsp = propose(r, p);
	if (rsp == NULL)
		goto retry;

	votes = 0;
	FOREACH_REPLY(rsp, reply)
		if (reply->msg_code == ACCEPTED)
			votes++;
	req_release(rsp);

	if (votes < quorum)
		goto retry;

	if (p->ballot != ballot) /* oops, proposal changed behind our back */
		goto retry;

	struct tbuf *x = tbuf_alloc(fiber->pool);
	tbuf_append(x, &ballot, sizeof(ballot));
	tbuf_append(x, &p->tag, sizeof(p->tag));
	tbuf_append(x, &p->value_len, sizeof(p->value_len));
	tbuf_append(x, p->value, p->value_len);
	if ([r wal_row_submit:x->ptr len:tbuf_len(x) scn:p->scn tag:paxos_accept] != 1)
		panic("give up");

	/* notify others */
	if (p->ballot != ballot) /* oops, proposal changed behind our back */
		goto retry;
decide:
	say_debug("deciding");
	decide(r, p);

	maybe_wake_dumper(r, p);

	if (!evalue) {
		struct proposal *prev = prev_proposal(r, p);
		if ((prev->flags & P_APPLIED) == 0 && [r scn] + 1 != p->scn && 0) {
			prev->waiter = fiber;
			yield();
			prev->waiter = NULL;
		}

		if (p->waiter)
			fiber_wake(p->waiter, NULL);

		p->flags |= P_APPLIED;
		return 1;
	} else {
		learn(r, p);
		return 0;
	}
}


#if 0
static void
query(struct PaxosRecovery *r, struct iproto_peer *p, struct iproto_msg *msg)
{
	struct netmsg *m = peer_netmsg_tail(peer);
	struct msg_paxos *req = (struct msg_paxos *)msg;
	struct msg_paxos reply = PREPLY(ACCEPTED, req);
	struct proposal *p = find_proposal(req->scn);
	if (!paxos_leader()) {
		say_warn("%s: not paxos leader, ignoring query SCN:%"PRIi64, __func__, req->scn);
		return;
	}
	if (p == NULL || (p->flags & P_DECIDED) == 0) {
		say_warn("%s: not decided, ignoring query SCN:%"PRIi64, __func__, req->scn);

	}
	net_add_iov_dup(&m, &reply, sizeof(reply));
	say_debug("%s: sync:%i SCN:%"PRIi64" value_len:%i %s", __func__,
		  req->header.sync, req->scn, req->value_len,
		  tbuf_to_hex(&TBUF(req->value, req->value_len, fiber->pool)));
}
#endif

static void
recv_msg(struct conn *c, struct iproto *msg, void *arg)
{
	PaxosRecovery *pr = arg;

#ifdef RANDOM_DROP
	double drop = rand() / (double)RAND_MAX;
	static double drop_p;

	if (!drop_p) {
		char *drop_pstr = getenv("RANDOM_DROP");
		drop_p = drop_pstr ? atof(drop_pstr) : 0.01;
	}

	if (drop < drop_p) {
		say_debug("%s: op:0x%02x/%s sync:%i DROP", __func__,
			  msg->msg_code, paxos_msg_code_strs[msg->msg_code], msg->sync);
		return;
	}
#endif

	say_debug("%s: op:0x%02x/%s sync:%i", __func__,
		  msg->msg_code, paxos_msg_code_strs[msg->msg_code], msg->sync);

	switch (msg->msg_code) {
	case LEADER_PROPOSE: {
		struct msg_leader *pmsg = (struct msg_leader *)msg;
		if (ev_now() > leadership_expire || leader_id == pmsg->leader_id) {
			say_debug("|   LEADER_PROPOSE accept, expired:%.2f leader/proposed:%i/%i",
				  leadership_expire - ev_now(), leader_id, pmsg->leader_id);
			msg->msg_code = LEADER_ACK;
			leader_id = pmsg->leader_id;
			leadership_expire = pmsg->expire;
			notify_leadership_change(pr);
		} else {
			say_debug("|   LEADER_PROPOSE nack, expired:%.2f leader/propos:%i/%i",
				  leadership_expire - ev_now(), leader_id, pmsg->leader_id);
			msg->msg_code = LEADER_NACK;
			pmsg->leader_id = leader_id;
			pmsg->expire = leadership_expire;
		}
		if (c->state == CLOSED)
			return;

		struct netmsg *m = netmsg_tail(&c->out_messages);
		net_add_iov_dup(&m, pmsg, sizeof(*pmsg));
		ev_io_start(&c->out);
		break;
	}
	case PREPARE:
	case ACCEPT:
		if (leader_id == self_id) {
			say_warn("prepare/accept recieved by leader, ignoring");
			return;
		}
		acceptor(pr, c, msg);
		break;
	case DECIDE:
		learner(pr, msg);
		break;
#if 0
	case QUERY:
		query(pr, p, msg);
		break;
#endif
	default:
		say_warn("unable to reply unknown op:0x%02x peer:%s", msg->msg_code, conn_peer_name(c));
	}
}


static i64 follow_scn;
void
follow_from(PaxosRecovery *r, i64 scn)
{
	assert(follow_scn == 0);
	follow_scn = scn;
	fiber_wake(r->follower, NULL);
}

static void
followerf(va_list ap)
{
	PaxosRecovery *r = va_arg(ap, PaxosRecovery *);
	XLogPuller *puller = [[XLogPuller alloc] init];
	struct paxos_peer *leader;
loop:
	for (;;) {
		@try {
			follow_scn = 0;
			yield();

			while (!(leader = paxos_peer(r, leader_id)))
				fiber_sleep(1);

			say_debug("FOLLOW SCN:%"PRIi64 " feeder:%s", follow_scn, sintoa(&leader->feeder_addr));

			i64 initial_scn = follow_scn <= 1024 ? 1 : follow_scn - 1024;
			while ([puller handshake:&leader->feeder_addr scn:initial_scn] <= 0) {
				fiber_sleep(0.1);
			}

			for (;;) {
				const struct row_v12 *row;
				while ((row = [puller fetch_row])) {
					say_debug("%s: row SCN:%"PRIi64 " tag:%s", __func__,
						  row->scn, xlog_tag_to_a(row->tag));

					if (row->tag == wal_final_tag) {
						[puller close];
						say_debug("FOLLOW done");
						goto loop;
					}
					if (row->scn < follow_scn)
						continue;

					if (row->tag == wal_tag || row->tag == run_crc || row->tag == nop) {
						struct proposal *p = proposal(r, row->scn);
						update_proposal_ballot(p, ULLONG_MAX);
						update_proposal_value(p, row->len, (char *)row->data, row->tag);
						p->flags |= P_DECIDED;
						learn(r, p);
					}
				}
			}
		}
		@catch (Error *e) {
			say_error("replication failure: %s", e->reason);
			[puller close];
			fiber_sleep(1);
			fiber_gc();
		}
	}
	[puller free];
}

static void
wal_dump(va_list ap)
{
	PaxosRecovery *r = va_arg(ap, PaxosRecovery *);
	struct proposal *p = NULL;

loop:
	r->wal_dumper_busy = false;
	yield();
	p = RB_MIN(ptree, &r->proposals);

	while (p && p->flags & P_CLOSED)
		p = RB_NEXT(ptree, &r->proposals, p);

	while (p && p->flags & P_DECIDED) {
		if ([r wal_row_submit:p->value len:p->value_len scn:p->scn tag:p->tag] != 1)
			panic("give up");

		p->flags |= P_CLOSED;
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	goto loop;
}


static void
leader_catchup(PaxosRecovery *r)
{
	for (i64 i = [r scn]; i < r->max_scn; i++)
		run_protocol(r, i, "\0\0", 2, nop);
}

@implementation PaxosRecovery

- (id)
init_snap_dir:(const char *)snap_dirname
      wal_dir:(const char *)wal_dirname
 rows_per_wal:(int)wal_rows_per_file
  feeder_addr:(const char *)feeder_addr_
  fsync_delay:(double)wal_fsync_delay
run_crc_delay:(double)run_crc_delay
 nop_hb_delay:(double)nop_hb_delay
	flags:(int)flags
snap_io_rate_limit:(int)snap_io_rate_limit_
{
	struct octopus_cfg_paxos_peer *c;
	struct paxos_peer *ppeer;

	[super init_snap_dir:snap_dirname
		     wal_dir:wal_dirname
		rows_per_wal:wal_rows_per_file
		 feeder_addr:feeder_addr_
		 fsync_delay:wal_fsync_delay
	       run_crc_delay:run_crc_delay
		nop_hb_delay:nop_hb_delay
		       flags:flags
	  snap_io_rate_limit:snap_io_rate_limit_];

	SLIST_INIT(&group);
	RB_INIT(&proposals);

	if (flags & RECOVER_READONLY)
		return self;

	if (cfg.paxos_peer == NULL)
		panic("no paxos_peer given");

	self_id = cfg.paxos_self_id;
	say_info("configuring paxos peers");

	for (int i = 0; ; i++)
	{
		if ((c = cfg.paxos_peer[i]) == NULL)
			break;

		if (c->id >= MAX_IPROTO_PEERS)
			panic("too large peer id");

		if (paxos_peer(self, c->id) != NULL)
			panic("paxos peer %s already exists", c->name);

		ppeer = make_paxos_peer(c->id, c->name, c->addr, c->primary_port, c->feeder_port);
		if (!ppeer)
			panic("bad addr %s", c->addr);
		SLIST_INSERT_HEAD(&group, ppeer, link);
		say_info("  %s -> %s", c->name, c->addr);
	}

	if (!paxos_peer(self, self_id))
		panic("unable to find myself among paxos peers");

	SLIST_FOREACH(ppeer, &group, link) {
		if (ppeer->id == self_id)
			continue;
		SLIST_INSERT_HEAD(&remotes, &ppeer->iproto, link);
	}

	quorum = 2; /* FIXME: hardcoded */

	pool = palloc_create_pool("paxos");
	output_flusher = fiber_create("paxos/output_flusher", service_output_flusher);
	reply_reader = fiber_create("paxos/reply_reader", iproto_reply_reader, req_collect_reply);

	short accept_port;
	accept_port = ntohs(paxos_peer(self, self_id)->iproto.addr.sin_port);
	input_service = tcp_service(accept_port, NULL);
	fiber_create("paxos/worker", iproto_interact, input_service, recv_msg, self);
	fiber_create("paxos/rendevouz", iproto_rendevouz, NULL, &remotes, pool, reply_reader, output_flusher);
	fiber_create("paxos/elect", propose_leadership, self);
	follower = fiber_create("paxos/follower", followerf, self);
	wal_dumper = fiber_create("paxos/wal_dump", wal_dump, self);
	return self;
}

- (void)
enable_local_writes
{
	say_debug("%s", __func__);
	[self recover_finalize];
	local_writes = true;
	max_scn = scn;

	if (scn == 0) {
		for (;;) {
			struct paxos_peer *p;
			SLIST_FOREACH(p, &group, link) {
				if (p->id == self_id)
					continue;

				say_debug("feeding from %s", p->name);
				[self recover_follow_remote:&p->feeder_addr exit_on_eof:true];
				if (scn > 0)
					goto out;
			}
			fiber_sleep(1);
		}
	} else {
		[self configure_wal_writer];
	}
out:
	say_info("Loaded");
	strcpy(status, "active");
}

- (bool)
is_replica
{
	return leader_id < 0 || leader_id != self_id;
}

- (void)
leader_redirect_raise
{
	if (leader_id >= 0) {
		if (leader_id == self_id)
			return;

		iproto_raise_fmt(ERR_CODE_REDIRECT,
				 "%s", sintoa(&paxos_peer(self, leader_id)->primary_addr));
	} else {
		iproto_raise(ERR_CODE_LEADER_UNKNOW, "leader unknown");
	}
}

- (int)
submit:(void *)data len:(u32)len tag:(u16)tag
{
	if (!paxos_leader())
		[self leader_redirect_raise];

	@try {
		int r = run_protocol(self, [self next_scn], data, len, tag);
		expire_proposal(self);
		return r;
	}
	@catch (Error *e) {
		say_debug("aboring txn, [%s reason:\"%s\"] at %s:%d",
			  [[e class] name], e->reason, e->file, e->line);
		if (e->backtrace)
			say_debug("backtrace:\n%s", e->backtrace);
		return 0;
	}
}

- (void)
recover_row:(const struct row_v12 *)r
{
	say_debug("%s: lsn:%"PRIi64" SCN:%"PRIi64" tag:%s", __func__,
		  r->lsn, r->scn, xlog_tag_to_a(r->tag));

	switch (r->tag) {
	case paxos_prepare:
	case paxos_promise:
	case paxos_nop:
		lsn = r->lsn;
		struct tbuf buf = TBUF(r->data, r->len, NULL);
		u64 ballot = read_u64(&buf);
		struct proposal *p = proposal(self, r->scn);
		update_proposal_ballot(p, ballot);
		break;

	case paxos_accept:
	case paxos_propose:
		lsn = r->lsn;
		struct tbuf buf2 = TBUF(r->data, r->len, NULL);
		ballot = read_u64(&buf2);
		u16 tag = read_u16(&buf2);
		u32 value_len = read_u32(&buf2);
		void *value = read_bytes(&buf2, value_len);
		p = proposal(self, r->scn);
		update_proposal_value(p, value_len, value, tag);
		break;

	default:
		[super recover_row:r];
		break;
	}
}

- (i64) next_scn { return ++max_scn; }

@end

void
paxos_print(struct tbuf *out,
	    void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row),
	    const struct row_v12 *row)
{
	u16 tag = row->tag, inner_tag;
	struct tbuf b = TBUF(row->data, row->len, fiber->pool);
	u64 ballot, value_len;

	switch (tag) {
	case paxos_prepare:
	case paxos_promise:
	case paxos_nop:
		ballot = read_u64(&b);
		tbuf_printf(out, "ballot:%"PRIi64, ballot);
		break;
	case paxos_propose:
	case paxos_accept:
		ballot = read_u64(&b);
		inner_tag = read_u16(&b);
		value_len = read_u32(&b);
		(void)value_len;
		tbuf_printf(out, "ballot:%"PRIi64" it:%s ", ballot, xlog_tag_to_a(inner_tag));
		handler(out, inner_tag, &b);
		break;
	}
}

register_source();

void __attribute__((constructor))
paxos_cons()
{
	slab_cache_init(&proposal_cache, sizeof(struct proposal), SLAB_GROW, "paxos/proposal");
}
