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
#import <mbox.h>

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

static u16 paxos_default_version;

struct msg_leader {
	struct iproto msg;
	u16 peer_id;
	u16 version;
	i16 leader_id;
	ev_tstamp expire;
} __attribute__((packed));


struct msg_paxos {
	struct iproto header;
	u16 peer_id;
	u16 version;
	i64 scn;
	u64 ballot;
	u16 tag;
	u32 value_len;
	char value[];
} __attribute__((packed));

struct proposal {
	const i64 scn;
	u64 ballot, pre_ballot;
	u32 flags;
	u32 value_len; /* must be same type with msg_paxos->value_len */
	char *value;
	u16 tag;
	struct fiber *waiter;
	ev_tstamp delay, tstamp;
	RB_ENTRY(proposal) link;
	struct fiber *locker;
};

#define P_DECIDED	0x01
#define P_APPLIED	0x02
#define P_CLOSED	0x04
#define P_LOCK		0x08
#define P_PRE_LOCK	0x10

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

static int leader_id = -1, self_id;
static ev_tstamp leadership_expire;
static const ev_tstamp leader_lease_interval = 10;
static const ev_tstamp paxos_default_timeout = 0.2;

struct service *mesh_service;

static struct service *input_service;

extern void title(const char *fmt, ...); /* FIXME: hack */

static int catchup_done;

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
				 .peer_id = self_id,
				 .version = paxos_default_version,
				 .tag = tag,
				 .value_len = value_len };

	struct iproto_req *req = req_make(paxos_msg_code_strs[code], r->quorum, timeout,
					  &msg.header, value, value_len);

	say_debug("%s: > %s sync:%u SCN:%"PRIi64" ballot:%"PRIu64" timeout:%.2f",
		  __func__, paxos_msg_code_strs[code], req->header->sync, scn, ballot, timeout);
	say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(tag), value_len,
		   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

	broadcast(&r->remotes, req);
}

static void
paxos_reply(struct paxos_peer *peer, struct conn *c, const struct msg_paxos *req,
	    enum paxos_msg_code code, u64 ballot, const struct proposal *p)
{
	/* FIXME: is it possible to extrace "struct conn *c" from peer ? */

	if (c->state == CLOSED)
		return;

	struct msg_paxos *msg = p0alloc(c->pool, sizeof(*msg));
	msg->header = (struct iproto){ code,
				       sizeof(*msg) - sizeof(struct iproto) + (p ? p->value_len : 0),
				       req->header.sync };
	msg->scn = req->scn;
	msg->ballot = ballot ?: req->ballot;
	msg->peer_id = self_id;
	msg->version = paxos_default_version;

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

	say_debug("%s: > peer:%i/%s %s sync:%i SCN:%"PRIi64" ballot:%"PRIu64,
		  __func__, peer->id, peer->name, paxos_msg_code_strs[code], msg->header.sync,
		  msg->scn, msg->ballot);
	if (p)
		say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(p->tag), p->value_len,
			   tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));

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
			say_info("leader unknown, %i -> %i", prev_leader, leader_id);
		title("paxos_slave");
	} else if (!paxos_leader()) {
		if (prev_leader != leader_id)
			say_info("leader is %s, %i -> %i", paxos_peer(r, leader_id)->name,
				prev_leader, leader_id);
		title("paxos_slave");
	} else if (paxos_leader()) {
		if (prev_leader != leader_id) {
			say_info("I am leader, %i -> %i", prev_leader, leader_id);
			catchup_done = 0;
			leader_catchup(r);
		}
		title("paxos_leader");
	}
	prev_leader = leader_id;
}

static void
giveup_leadership()
{
	leader_id = -2;
}

static void
propose_leadership(va_list ap)
{
	PaxosRecovery *r = va_arg(ap, PaxosRecovery *);

	struct msg_leader leader_propose = { .msg = { .data_len = sizeof(leader_propose) - sizeof(struct iproto),
						      .sync = 0,
						      .msg_code = LEADER_PROPOSE },
					     .peer_id = self_id,
					     .version = paxos_default_version,
					     .leader_id = self_id };
	fiber_sleep(0.3); /* wait connections to be up */
	for (;;) {
		if (ev_now() > leadership_expire)
			leader_id = -1;

		if (leader_id < 0) {
			ev_tstamp delay = drand(leader_lease_interval * 0.1);
			if (leader_id == -2)
				delay += leader_lease_interval * 2;
			fiber_sleep(delay);
		} else {
			if (!paxos_leader())
				fiber_sleep(leadership_expire + leader_lease_interval * .01 - ev_now());
			else
				fiber_sleep(leadership_expire - leader_lease_interval * .1 - ev_now());
		}

		if (leader_id >= 0 && !paxos_leader())
			continue;

		leader_propose.expire = ev_now() + leader_lease_interval;
		broadcast(&r->remotes, req_make("leader_propose", 2, 1.0,
						&leader_propose.msg, NULL, 0));
		struct iproto_req *req = yield();

		int votes = 0;
		struct msg_leader *nack_msg = NULL;
		FOREACH_REPLY(req, reply) {
			if (reply->msg_code == LEADER_ACK) {
				votes++;
			} else {
				assert(reply->msg_code == LEADER_NACK);
				nack_msg = (struct msg_leader *)reply;
			}
		}
		if (votes >= req->quorum) {
			say_debug("%s: quorum reached v/q:%i/%i", __func__, votes, req->quorum);
			leadership_expire = leader_propose.expire;
			leader_id = self_id;
		} else {
			if (nack_msg && leader_propose.expire - nack_msg->expire > leader_lease_interval * .05 ) {
				struct paxos_peer *peer = paxos_peer(r, nack_msg->peer_id);
				say_debug("%s: nack from peer:%i/%s leader_id:%i", __func__,
					  peer->id, peer->name, nack_msg->leader_id);
				leadership_expire = nack_msg->expire;
				leader_id = nack_msg->leader_id;
			} else {
				say_debug("%s: no quorum v/q:%i/%i", __func__, votes, req->quorum);
			}
		}
		req_release(req);
		notify_leadership_change(r);
	}
}

static void
leader_reply(struct PaxosRecovery *pr, struct paxos_peer *peer, struct conn *c, struct iproto *msg)
{
	struct msg_leader *pmsg = (struct msg_leader *)msg;
	const char *ret = "accept";
	const ev_tstamp to_expire = leadership_expire - ev_now();
	(void)peer;

	say_debug("|   LEADER_PROPOSE to_expire:%.2f leader/propos:%i/%i",
		  to_expire, leader_id, pmsg->leader_id);

	if (leader_id == pmsg->leader_id) {
		say_debug("    -> same");
		msg->msg_code = LEADER_ACK;
		if (pmsg->leader_id != self_id)
			leadership_expire = pmsg->expire;
	} else if (to_expire < 0) {
		say_debug("    -> expired");
		msg->msg_code = LEADER_ACK;
		if (pmsg->leader_id != self_id) {
			leader_id = pmsg->leader_id;
			leadership_expire = pmsg->expire;
			notify_leadership_change(pr);
		}
	} else {
		ret = "nack";
		msg->msg_code = LEADER_NACK;
		pmsg->leader_id = leader_id;
		pmsg->expire = leadership_expire;
	}
	if (c->state == CLOSED)
		return;

	say_debug("|   -> reply with %s", ret);

	struct netmsg *m = netmsg_tail(&c->out_messages);
	net_add_iov_dup(&m, pmsg, sizeof(*pmsg));
	ev_io_start(&c->out);
}

void
plock(struct proposal *p)
{
	say_debug3("%s: SCN:%"PRIi64, __func__, p->scn);
	assert((p->flags & P_LOCK) == 0);
	p->flags |= P_LOCK;
	p->locker = fiber;
}

void
punlock(struct proposal *p)
{
	say_debug3("%s: SCN:%"PRIi64, __func__, p->scn);
	assert(p->flags & P_LOCK);
	p->flags &= ~P_LOCK;
	p->locker = NULL;
	if (p->waiter) {
		fiber_wake(p->waiter, p);
		p->waiter = NULL;
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
	assert(p->flags & P_LOCK);
	say_debug2("%s: SCN:%"PRIi64" ballot:%"PRIu64, __func__, p->scn, ballot);
	assert(p->ballot <= ballot);
	p->ballot = ballot;
}

static void
update_proposal_pre_ballot(struct proposal *p, u64 ballot)
{
	assert(p->flags & P_PRE_LOCK);
	say_debug2("%s: SCN:%"PRIi64" ballot:%"PRIu64, __func__, p->scn, ballot);
	assert(p->pre_ballot <= ballot);
	p->pre_ballot = ballot;
}

static void
update_proposal_value(struct proposal *p, u32 value_len, const char *value, u16 tag)
{
	say_debug2("%s: SCN:%"PRIi64" tag:%s value_len:%i value:%s", __func__,
		   p->scn, xlog_tag_to_a(tag), value_len,
		   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

	if (p->value_len != value_len) {
		assert(value_len > 0); /* value never goes empty */
		assert((p->flags & P_DECIDED) == 0); /* P_DECIDED is immutable */
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
	assert(scn > 1);
	assert(scn >= r->app_scn);
	struct proposal *p = slab_cache_alloc(&proposal_cache);
	struct proposal ini = { .scn = scn, .delay = paxos_default_timeout, .tstamp = ev_now() };
	memcpy(p, &ini, sizeof(*p));
	RB_INSERT(ptree, &r->proposals, p);
	if (r->max_scn < scn)
		r->max_scn = scn;
	return p;
}

static struct proposal *
proposal(PaxosRecovery *r, i64 scn)
{
	assert(scn > 0);
	return find_proposal(r, scn) ?: create_proposal(r, scn);
}

#if 0
static struct proposal *
prev_proposal(PaxosRecovery *r, struct proposal *p)
{
	if (p->scn == 1)
		return NULL;
	return RB_PREV(ptree, &r->proposals, p) ?: create_proposal(r, p->scn - 1);
}
#endif

static void
delete_proposal(PaxosRecovery *r, struct proposal *p)
{
	RB_REMOVE(ptree, &r->proposals, p);
	if (p->value)
		sfree(p->value);
	slab_cache_free(&proposal_cache, p);
}

static void
expire_proposal(PaxosRecovery *r)
{
	struct proposal *p, *tmp;
	i64 scn = [r scn];

	RB_FOREACH_SAFE(p, ptree, &r->proposals, tmp) {
		if (p->flags & (P_LOCK|P_PRE_LOCK))
			break;

		if ((p->flags & P_CLOSED) == 0)
			break;

		if (p->scn > scn - 64)
			break;

		delete_proposal(r, p);
	}
}

static void
promise(PaxosRecovery *r, struct paxos_peer *peer, struct conn *c, struct proposal *p, struct msg_paxos *req)
{
	assert(p->flags & P_LOCK);
	if ([r wal_row_submit:&req->ballot len:sizeof(req->ballot) scn:req->scn tag:paxos_promise] != 1)
		return;

	u64 old_ballot = p->ballot;
	update_proposal_ballot(p, req->ballot);
	paxos_reply(peer, c, req, PROMISE, old_ballot, p);
}

static void
accepted(PaxosRecovery *r, struct paxos_peer *peer, struct conn *c, struct proposal *p, struct msg_paxos *req)
{
	assert(req->scn == p->scn);
	assert(p->ballot <= req->ballot);
	assert(req->value_len > 0);
	assert(p->flags & P_LOCK);

	struct tbuf *x = tbuf_alloc(fiber->pool);
	tbuf_append(x, &req->ballot, sizeof(req->ballot));
	tbuf_append(x, &req->tag, sizeof(req->tag));
	tbuf_append(x, &req->value_len, sizeof(req->value_len));
	tbuf_append(x, req->value, req->value_len);

	if ([r wal_row_submit:x->ptr len:tbuf_len(x) scn:req->scn tag:paxos_accept] != 1)
		return;

	update_proposal_ballot(p, req->ballot);
	update_proposal_value(p, req->value_len, req->value, req->tag);
	paxos_reply(peer, c, req, ACCEPTED, 0, NULL);

}

static struct iproto_req *
prepare(PaxosRecovery *r, struct proposal *p, u64 ballot)
{
	if ([r wal_row_submit:&ballot len:sizeof(ballot) scn:p->scn tag:paxos_prepare] != 1)
		panic("give up");
	update_proposal_pre_ballot(p, ballot);
	paxos_broadcast(r, PREPARE, p->delay, p->scn, ballot, NULL, 0, 0);
	return yield();
}

static struct iproto_req *
propose(PaxosRecovery *r, u64 ballot, i64 scn, const char *value, u32 value_len, u16 tag)
{
	struct tbuf *m = tbuf_alloc(fiber->pool);
	tbuf_append(m, &ballot, sizeof(ballot));
	tbuf_append(m, &tag, sizeof(tag));
	tbuf_append(m, &value_len, sizeof(value_len));
	tbuf_append(m, value, value_len);
	if ([r wal_row_submit:m->ptr len:tbuf_len(m) scn:scn tag:paxos_propose] != 1)
		panic("give up");
	paxos_broadcast(r, ACCEPT, paxos_default_timeout, scn, ballot, value, value_len, tag);
	return yield();
}

static void
nack(struct paxos_peer *peer, struct conn *c, struct msg_paxos *req, u64 ballot)
{
	paxos_reply(peer, c, req, NACK, ballot, NULL);
}

static void
decided(struct paxos_peer *peer, struct conn *c, struct msg_paxos *req, struct proposal *p)
{
	paxos_reply(peer, c, req, DECIDE, p->ballot, p);
}

static void maybe_wake_dumper(PaxosRecovery *r, struct proposal *p);

static void
mark_applied(PaxosRecovery *r, struct proposal *p)
{
	assert(p->flags & P_DECIDED);
	p->flags |= P_APPLIED;

	while (p && p->scn - r->app_scn == 1 && p->flags & P_APPLIED) {
		r->app_scn = p->scn;
		p = RB_NEXT(ptree, &r->proposals, p);
	}
}

static void
learn(PaxosRecovery *r, struct proposal *p)
{
loop:
	if (!p)
		return;

	assert([r scn] <= r->app_scn);
	assert(r->app_scn <= r->max_scn);

	if (p->scn != r->app_scn + 1)
		return;

	if (p->flags & P_APPLIED)
		return;

	if ((p->flags & P_DECIDED) == 0)
		return;

	say_debug("%s: SCN:%"PRIi64" ballot:%"PRIu64, __func__, p->scn, p->ballot);
	say_debug2("|  value_len:%i value:%s",
		   p->value_len, tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));


	[r apply:&TBUF(p->value, p->value_len, NULL) tag:p->tag]; /* FIXME: what to do if this fails ? */
	mark_applied(r, p);

	p = RB_NEXT(ptree, &r->proposals, p);
	goto loop;
}


static void
learner(PaxosRecovery *r, struct paxos_peer *peer, struct iproto *msg)
{
	struct msg_paxos *mp = (struct msg_paxos *)msg;
	if (mp->peer_id == self_id)
		return;
	if (mp->scn <= r->app_scn)
		return;

	struct proposal *p = proposal(r, mp->scn);

	say_debug("%s: < peer:%i/%s sync:%i type:DECIDE SCN:%"PRIi64" ballot:%"PRIu64" tag:%s",
		  __func__, peer->id, peer->name, msg->sync, mp->scn, mp->ballot, xlog_tag_to_a(mp->tag));
	say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(mp->tag), mp->value_len,
		   tbuf_to_hex(&TBUF(mp->value, mp->value_len, fiber->pool)));

	if (p->flags & P_LOCK) {
		say_warn("%s: SCN:%"PRIi64" ignoring concurent update", __func__, p->scn);
		return;
	}

	if (p->flags & P_DECIDED) {
		assert(memcmp(mp->value, p->value, MIN(mp->value_len, p->value_len)) == 0);
		assert(p->tag == mp->tag);
		return;
	}

	if (p->ballot > mp->ballot) {
		say_warn("%s: SCN:%"PRIi64" ignoring stale DECIDE", __func__, p->scn);
		return;
	}

	plock(p);
	update_proposal_ballot(p, mp->ballot);
	update_proposal_value(p, mp->value_len, mp->value, mp->tag);
	p->flags |= P_DECIDED;
	punlock(p);

	learn(r, p);
	maybe_wake_dumper(r, p);
}

static void
acceptor(PaxosRecovery *r, struct paxos_peer *peer, struct conn *c, struct iproto *msg)
{
	struct msg_paxos *mp = (struct msg_paxos *)msg;
	struct proposal *p;

	say_debug("%s: SCN:%"PRIi64, __func__, mp->scn);

	if (mp->scn <= r->app_scn) {
		paxos_reply(peer, c, mp, STALE, 0, NULL);
		return;
	}

	p = proposal(r, mp->scn);
	if (p->flags & P_LOCK) {
		say_warn("%s: SCN:%"PRIi64" ignoring concurent update", __func__, p->scn);
		return;
	}

	if (p->flags & P_DECIDED) {
		/* if we already knew the value, notify current leader immediately */
		if (!paxos_leader())
			decided(peer, c, mp, p);
		return;
	}

	if (p->ballot > mp->ballot) {
		nack(peer, c, mp, p->ballot);
		return;
	}

	assert(p->ballot <= mp->ballot);
	say_debug("%s: < peer:%i/%s type:%s sync:%i SCN:%"PRIi64" ballot:%"PRIu64,
		  __func__, peer->id, peer->name, paxos_msg_code_strs[msg->msg_code],
		  msg->sync, mp->scn, mp->ballot);
	say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(mp->tag), mp->value_len,
		   tbuf_to_hex(&TBUF(mp->value, mp->value_len, fiber->pool)));

	plock(p);
	switch (msg->msg_code) {
	case PREPARE:
		assert(p->ballot <= mp->ballot);
		promise(r, peer, c, p, mp);
		break;
	case ACCEPT:
		accepted(r, peer, c, p, mp);
		break;
	default:
		say_error("%s: < unexpected msg type: %s", __func__, paxos_msg_code_strs[msg->msg_code]);
		break;
	}
	punlock(p);
}



static u64
run_protocol(PaxosRecovery *r, i64 scn, u64 ballot, char *value, u32 value_len, u16 tag)
{
	struct iproto_req *rsp;
	bool has_old_value = false;
	int votes;
	const int quorum = 2; /* FIXME: hardcoded */
	u64 min_ballot = 1;

#ifndef NDEBUG
	struct proposal *p = proposal(r, scn);
	assert((p->flags & P_DECIDED) == 0);
#endif

	say_debug("%s: SCN:%"PRIi64, __func__, scn);
	say_debug2("|  tag:%s value_len:%u value:%s", xlog_tag_to_a(tag), value_len,
		   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

retry:
	if (!paxos_leader()) { /* FIXME: leadership is required only for leading SCN */
		say_debug("not a leader, givin up");
		return 0;
	}

	if (ballot < min_ballot)
		ballot = ((min_ballot & ~0xff) + 0x100) | (self_id & 0xff);

	assert(ballot > 0);
	rsp = prepare(r, proposal(r, scn), ballot);
	if (rsp == NULL) {
		fiber_sleep(0.01);
		goto retry;
	}

	say_debug("PREPARE reply SCN:%"PRIi64, scn);
	struct msg_paxos *max = NULL;
	votes = 0;
	FOREACH_REPLY(rsp, reply) {
		struct msg_paxos *mp = (struct msg_paxos *)reply;
		switch(reply->msg_code) {
		case NACK:
			say_debug("|  NACK SCN:%"PRIi64" ballot:%"PRIu64, mp->scn, mp->ballot);
			assert(mp->ballot > ballot);
			min_ballot = mp->ballot;
			break;
		case PROMISE:
			say_debug("|  PROMISE SCN:%"PRIi64" ballot:%"PRIu64" value_len:%i",
				  mp->scn, mp->ballot, mp->value_len);
			votes++;
			if (mp->value_len > 0 && (max == NULL || mp->ballot > max->ballot))
				max = mp;

			break;
		case DECIDE:
			say_debug("|  DECIDE SCN:%"PRIi64" ballot:%"PRIu64" value_len:%i",
				  mp->scn, mp->ballot, mp->value_len);
			/* some other leader already decided on value */

			struct proposal *p = proposal(r, scn);
			if (p->flags & P_LOCK)
				return 0;
			plock(p);
			update_proposal_ballot(p, ULLONG_MAX);
			update_proposal_value(p, mp->value_len, mp->value, mp->tag);
			p->flags |= P_DECIDED;
			punlock(p);
			req_release(rsp);
			return 0;
		case STALE:
			giveup_leadership();
			return 0;

		default:
			assert(false);
		}
	}

	if (votes < quorum) {
		req_release(rsp);
		goto retry;
	}

	if (max && (max->value_len != value_len || memcmp(max->value, value, value_len) != 0))
	{
		has_old_value = 1;
		say_debug("has REMOTE value for SCN:%"PRIi64" value_len:%i value:%s",
			  scn, max->value_len,
			  tbuf_to_hex(&TBUF(max->value, max->value_len, fiber->pool)));

		value = salloc(max->value_len);
		memcpy(value, max->value, max->value_len);
		value_len = max->value_len;
		tag = max->tag;
	}
	req_release(rsp);


	rsp = propose(r, ballot, scn, value, value_len, tag);
	if (rsp == NULL)
		goto retry;

	votes = 0;
	FOREACH_REPLY(rsp, reply) {
		if (reply->msg_code == ACCEPTED) {
			say_debug("|  ACCEPTED SCN:%"PRIi64, scn);
			votes++;
		} else {
			say_debug("|  XXXXX%i SCN:%"PRIi64, reply->msg_code, scn);
		}
	}
	req_release(rsp);

	if (votes < quorum)
		goto retry;

	paxos_broadcast(r, DECIDE, 0, scn, ballot, value, value_len, tag);
	return has_old_value ? 0 : ballot;
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
	struct msg_paxos *h = (struct msg_paxos *)msg;
	struct paxos_peer *peer = paxos_peer(pr, h->peer_id);


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

	say_debug("%s: peer:%i/%s op:0x%02x/%s sync:%i", __func__,
		  peer->id, peer->name, msg->msg_code, paxos_msg_code_strs[msg->msg_code], msg->sync);

	if (h->version != paxos_default_version) {
		say_warn("%s: bad version %i, closing connect from peer %i",
			 __func__, h->version, h->peer_id);
		conn_close(c);
	}

	if (!peer) {
		say_warn("%s: closing connect from unknown peer %i", __func__, h->peer_id);
		conn_close(c);
		return;
	}

	switch (msg->msg_code) {
	case LEADER_PROPOSE:
		leader_reply(pr, peer, c, msg);
		break;
	case PREPARE:
	case ACCEPT:
		acceptor(pr, peer, c, msg);
		break;
	case DECIDE:
		learner(pr, peer, msg);
		break;
#if 0
	case QUERY:
		query(p, pr, msg);
		break;
#endif
	default:
		say_warn("unable to reply unknown op:0x%02x peer:%s", msg->msg_code, conn_peer_name(c));
	}
}


static i64 follow_scn;
static int
follow_from(PaxosRecovery *r, i64 scn)
{
	if (follow_scn > 0)
		return 0;
	follow_scn = scn;
	fiber_wake(r->follower, NULL);
	return 1;
}

static void
follow_leader_fib(va_list ap)
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
				fiber_sleep(0.1);

			say_info("follow_leader: SCN:%"PRIi64 " feeder:%s", follow_scn, sintoa(&leader->feeder_addr));

			i64 initial_scn = follow_scn <= 1024 ? 1 : follow_scn - 1024,
				max_scn = r->max_scn;
			while ([puller handshake:&leader->feeder_addr scn:initial_scn] <= 0) {
				fiber_sleep(0.1);
			}

			for (;;) {
				const struct row_v12 *row;
				while ((row = [puller fetch_row])) {
					if (row->tag != wal_tag && row->tag != run_crc && row->tag != nop)
						continue;

					if (row->scn == max_scn) {
						say_warn("exit on max SCN:%"PRIi64
							 " rSCN:%"PRIi64
							 " rappSCN:%"PRIi64
							 " rmaxSCN:%"PRIi64,
							 max_scn, [r scn], r->app_scn, r->max_scn);
						[puller close];
						fiber_sleep(3);
						goto loop;
					}

					if (row->scn <= r->app_scn)
						continue;

					struct proposal *p = proposal(r, row->scn);
					if (p->flags & (P_LOCK|P_PRE_LOCK)) {
						say_warn("%s: skipping locked SCN:%"PRIi64, __func__, p->scn);
						continue;
					}
					if (p->flags & P_DECIDED)
						assert(p->value_len == row->len &&
						       p->tag == row->tag &&
						       !memcmp(p->value, row->data, row->len));
					plock(p);
					update_proposal_ballot(p, ULLONG_MAX);
					update_proposal_value(p, row->len, (char *)row->data, row->tag);
					p->flags |= P_DECIDED;
					punlock(p);
					learn(r, p);
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
close_with_nop(PaxosRecovery *r, struct proposal *p)
{
	say_debug("%s: SCN:%"PRIi64, __func__, p->scn);
	assert((p->flags & P_PRE_LOCK) == 0);
	p->flags |= P_PRE_LOCK;
	u64 ballot = MAX(p->ballot, p->pre_ballot);
	run_protocol(r, p->scn, ballot, "\0\0", 2, nop);
	p->flags &= ~P_PRE_LOCK;
}

static struct mbox wal_dumper_mbox;

static void
maybe_wake_dumper(PaxosRecovery *r, struct proposal *p)
{
	if (p->scn - [r scn] < 8)
		return;

	struct mbox_msg *m = palloc(r->wal_dumper->pool, sizeof(*m));
	m->msg = (void *)1;
	mbox_put(&wal_dumper_mbox, m);
}

static void
wal_dumper_fib(va_list ap)
{
	PaxosRecovery *r = va_arg(ap, PaxosRecovery *);
	struct proposal *p = NULL;

loop:
	mbox_timedwait(&wal_dumper_mbox, 1);
	while (mbox_get(&wal_dumper_mbox)); /* flush mbox */
	fiber_gc(); /* NB: put comment */

	p = RB_MIN(ptree, &r->proposals);
	while (p && p->flags & P_CLOSED) {
		say_debug2("wal_dump:  % 8"PRIi64" CLOSED", p->scn);
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	while (p && p->scn <= r->app_scn) {
		assert(p->flags & P_DECIDED);
		say_debug2("wal_dump:  % 8"PRIi64" APPLIED", p->scn);
		assert([r scn] + 1 == p->scn);
		if ([r wal_row_submit:p->value len:p->value_len scn:p->scn tag:p->tag] != 1)
			panic("give up");

		p->flags |= P_CLOSED;
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	if (!paxos_leader() && p) {
		if ((p->scn > r->app_scn && ev_now() - p->tstamp > 1) ||
		    r->max_scn - [r scn] > cfg.wal_writer_inbox_size * 1.1)
		{
			if (follow_scn == 0) {
				say_info("wal_dump: requesting follow, SCN delay:%.2f gap:%"PRIu64,
					 ev_now() - p->tstamp, r->max_scn - [r scn]);
				follow_from(r, [r scn]);
			}
		}
	}

	while (p) {
		say_debug2("wal_dump:  % 8"PRIi64" %s", p->scn,
			  p->flags & P_DECIDED ? "DECIDED" : "");
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	expire_proposal(r);
	goto loop;
}


static void
leader_catchup(PaxosRecovery *r)
{
	say_debug("%s: SCN:%"PRIi64 " max_scn:%"PRIi64, __func__, [r scn], r->max_scn);

	for (i64 i = r->app_scn + 1; i <= r->max_scn; i++) {
		struct proposal *p = proposal(r, i);

		while (p->flags & P_PRE_LOCK) {
			if (!paxos_leader())
				return;

			fiber_sleep(0.01);
		}

		while ((p->flags & P_DECIDED) == 0) {
			if (!paxos_leader())
				return;

			close_with_nop(r, p);
		}
		learn(r, p);
	}
	assert(r->app_scn == r->max_scn);
	catchup_done = 1;
}

void
paxos_stat(va_list ap)
{
	struct PaxosRecovery *r = va_arg(ap, typeof(r));
loop:
	say_info("SCN:%"PRIi64" "
		 "AppSCN:%"PRIi64" "
		 "MaxSCN:%"PRIi64" "
		 "leader:%i %s",
		 [r scn], r->app_scn, r->max_scn,
		 leader_id, paxos_leader() ? "leader" : "");

	fiber_sleep(1);
	goto loop;
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

		say_info("  %s -> %s", c->name, c->addr);

		struct paxos_peer *p = make_paxos_peer(c->id, c->name,
						       c->addr, c->primary_port, c->feeder_port);

		if (!p)
			panic("bad addr %s", c->addr);
		SLIST_INSERT_HEAD(&group, p, link);
		SLIST_INSERT_HEAD(&remotes, &p->iproto, link);
	}

	if (!paxos_peer(self, self_id))
		panic("unable to find myself among paxos peers");

	quorum = 2; /* FIXME: hardcoded */

	pool = palloc_create_pool("paxos");
	output_flusher = fiber_create("paxos/output_flusher", service_output_flusher);
	reply_reader = fiber_create("paxos/reply_reader", iproto_reply_reader);

	short accept_port;
	accept_port = ntohs(paxos_peer(self, self_id)->iproto.addr.sin_port);
	input_service = tcp_service(accept_port, NULL);
	fiber_create("paxos/worker", iproto_interact, input_service, recv_msg, self);

	fiber_create("paxos/rendevouz", iproto_rendevouz,
		     NULL, &remotes, pool, reply_reader, output_flusher);
	fiber_create("paxos/elect", propose_leadership, self);
	follower = fiber_create("paxos/follower", follow_leader_fib, self);
	mbox_init(&wal_dumper_mbox);
	wal_dumper = fiber_create("paxos/wal_dump", wal_dumper_fib, self);

	fiber_create("paxos/stat", paxos_stat, self);
	return self;
}

- (void)
enable_local_writes
{
	say_debug("%s", __func__);
	[self recover_finalize];
	local_writes = true;
	app_scn = max_scn = scn;

	if ([self scn] == 0) {
		for (;;) {
			struct paxos_peer *p;
			SLIST_FOREACH(p, &group, link) {
				if (p->id == self_id)
					continue;

				say_debug("feeding from %s", p->name);
				[self recover_follow_remote:&p->feeder_addr exit_on_eof:true];
				if ([self scn] > 0)
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

	if (!catchup_done)
		iproto_raise(ERR_CODE_LEADER_UNKNOW, "leader not ready");

	@try {
		i64 cur_scn = [self next_scn];
		struct proposal *p = proposal(self, cur_scn);
		assert((p->flags & P_PRE_LOCK) == 0);
		p->flags |= P_PRE_LOCK;
		u64 ballot = run_protocol(self, cur_scn, MAX(p->ballot, p->pre_ballot),
					  data, len, tag);

		p->flags &= ~P_PRE_LOCK;

		if (ballot) {
			while (p->flags & P_LOCK) {
				say_debug("%s: locked", __func__);
				assert(p->waiter == NULL);
				p->waiter = fiber;
				yield();
			}

			assert(ballot >= p->ballot); /* FIXME: check for value eq */

			plock(p);
			update_proposal_ballot(p, ballot);
			update_proposal_value(p, len, data, tag);
			p->flags |= P_DECIDED;
			punlock(p);

			mark_applied(self, p);
			maybe_wake_dumper(self, p);
		}

		return ballot > 0;
	}
	@catch (Error *e) {
		say_debug("aboring txn, [%s reason:\"%s\"] at %s:%d",
			  [[e class] name], e->reason, e->file, e->line);
		if (e->backtrace)
			say_debug("backtrace:\n%s", e->backtrace);
		return 0;
	}
}

- (int)
snapshot_write_header_rows:(XLog *)snap
{
	struct tbuf *buf = tbuf_alloc(fiber->pool);
	struct proposal *p;

	RB_FOREACH(p, ptree, &proposals) {
		if (p->scn <= scn)
			continue;
		if (p->flags & P_APPLIED)
			tbuf_append(buf, &p->scn, sizeof(p->scn));
	}

	if (tbuf_len(buf) == 0)
		return 0;

	if ([snap append_row:buf->ptr len:tbuf_len(buf) scn:scn tag:snap_skip_scn] < 0) {
		say_error("unable write snap_applied_scn");
		return -1;
	}

	return 0;
}

- (void)
recover_row:(const struct row_v12 *)r
{
	say_debug("%s: lsn:%"PRIi64" SCN:%"PRIi64" tag:%s", __func__,
		  r->lsn, r->scn, xlog_tag_to_a(r->tag));

	struct tbuf buf;
	struct proposal *p = NULL, *tmp;
	u64 ballot;

	switch (r->tag) {
	case paxos_prepare:
		lsn = r->lsn;
		buf = TBUF(r->data, r->len, NULL);
		ballot = read_u64(&buf);
		p = proposal(self, r->scn);
		p->flags |= P_PRE_LOCK;
		update_proposal_pre_ballot(p, ballot);
		p->flags &= ~P_PRE_LOCK;
		break;

	case paxos_promise:
	case paxos_nop:
		lsn = r->lsn;
		buf = TBUF(r->data, r->len, NULL);
		ballot = read_u64(&buf);
		p = proposal(self, r->scn);
		plock(p);
		update_proposal_ballot(p, ballot);
		punlock(p);
		break;

	case paxos_accept:
	case paxos_propose:
		lsn = r->lsn;
		buf = TBUF(r->data, r->len, NULL);
		ballot = read_u64(&buf);
		u16 tag = read_u16(&buf);
		u32 value_len = read_u32(&buf);
		void *value = read_bytes(&buf, value_len);
		p = proposal(self, r->scn);
		plock(p);
		update_proposal_value(p, value_len, value, tag);
		punlock(p);
		break;

	default:
		[super recover_row:r];
		break;
	}

	if (p && p->scn == next_skip_scn) {
		p->flags |= P_APPLIED;

		next_skip_scn = tbuf_len(&skip_scn) > 0 ?
				read_u64(&skip_scn) : 0;
	}

	RB_FOREACH_SAFE(p, ptree, &proposals, tmp) {
		if (r->scn - p->scn > 8)
			break;
		delete_proposal(self, p);
	}
}

- (i64) next_scn { return ++max_scn; }

- (int)
submit_run_crc
{
	/* history overflow */
	if (max_scn - scn > nelem(crc_hist))
		return -1;

	return [super submit_run_crc];
}

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
		assert(value_len == tbuf_len(&b));
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
