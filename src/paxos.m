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
	_(STALE, 0xfa)

enum paxos_msg_code ENUM_INITIALIZER(PAXOS_CODE);
const char *paxos_msg_code[] = ENUM_STR_INITIALIZER(PAXOS_CODE);
const int proposal_history_size = 16 * 1024;
const int quorum = 2; /* FIXME: hardcoded */

struct paxos_peer {
	struct iproto_peer paxos, primary;
	int id;
	const char *name, *primary_addr;
	struct feeder_param feeder;
	SLIST_ENTRY(paxos_peer) link;
};

struct paxos_peer *
make_paxos_peer(int id, const char *name, const char *addr,
		short primary_port, short feeder_port)
{
	struct sockaddr_in sin;
	struct paxos_peer *p;

	if (atosin(addr, &sin) == -1)
		return NULL;

	p = xcalloc(1, sizeof(*p));
	p->id = id;
	p->name = name;
	p->paxos = (struct iproto_peer){ .id = id,
					 .name = name,
					 .addr = sin,
					 .c = { .fd = -1 } };
	p->primary = p->paxos;
	p->primary.addr.sin_port = htons(primary_port);
	p->primary_addr = strdup(sintoa(&p->primary.addr));

	memset(&p->feeder, 0, sizeof(p->feeder));
	p->feeder.ver = 1;
	p->feeder.addr = sin;
	p->feeder.addr.sin_port = htons(feeder_port);
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
	struct iproto header;
	u16 peer_id;
	u16 version; /* FIXME: move version before peer_id */
	i16 leader_id;
	ev_tstamp expire;
} __attribute__((packed));


struct msg_paxos {
	struct iproto header;
	u16 peer_id;
	u16 version; /* FIXME: move version before peer_id */
	i64 scn;
	u64 ballot;
	u16 tag;
	u32 value_len;
	u8 value[];
} __attribute__((packed));

struct proposal {
	const i64 scn;
	u64 prepare_ballot, ballot;
	/* since we using same rb_tree for both proposer & acceptor
	   we have to store their ballot separately, so proposers ballot
	   updates will not affect acceptors ballot */

	u32 flags;
	u32 value_len; /* must be same type with msg_paxos->value_len */
	u8 *value;
	u16 tag;
	struct fiber *waiter;
	ev_tstamp delay, tstamp;
	RB_ENTRY(proposal) link;
	struct fiber *locker;
};

struct paxos_request {
	struct paxos_peer *peer;
	struct conn *c; /* incoming connection,
			   don't confuse with outgoing connection peer->c */
	const struct msg_paxos *msg;
	struct proposal *p;
};

#define P_APPLIED	0x01
#define P_CLOSED	0x02
/*
  1. P_CLOSED: Continious from initial SCN up to [Recovery lsn]. There are no gaps. Proposal may be P_CLOSED
             only after being decided.
  2. P_APPLIED: Proposal my be P_APPLIED only after being decided, that is ballot == ULLONG_MAX
              Leader can apply proposal out of order because it may assume theirs independece (this
              follows from the following observation: caller of [Recovery submit] already took all
	      neccecary locks.)
              Accpetors(non leader) _must_ apply proposals in order because they cannot prove
	      proposal's independece.
 */


struct slab_cache proposal_cache;
static int
proposal_cmp(const struct proposal *a, const struct proposal *b)
{
	return (a->scn < b->scn) ? -1 : (a->scn > b->scn);
}
#ifndef __unused
#define __unused    _unused_
#endif
RB_GENERATE_STATIC(ptree, proposal, link, proposal_cmp)

static int leader_id = -1, self_id;
static ev_tstamp leadership_expire;
static const ev_tstamp leader_lease_interval = 10;
static const ev_tstamp paxos_default_timeout = 0.2;

struct service *mesh_service;

static int catchup_done;
static void catchup(PaxosRecovery *r, i64 upto_scn);

static bool
paxos_leader()
{
	return leader_id >= 0 && leader_id == self_id;
}

static u32
paxos_broadcast(PaxosRecovery *r, struct iproto_mbox *mbox,
		enum paxos_msg_code code, /* ev_tstamp timeout, */
		i64 scn, u64 ballot, const char *value, u32 value_len, u16 tag)
{
	struct msg_paxos msg = { .header = { .data_len = sizeof(msg) - sizeof(struct iproto),
					     .msg_code = code },
				 .scn = scn,
				 .ballot = ballot,
				 .peer_id = self_id,
				 .version = paxos_default_version,
				 .tag = tag,
				 .value_len = value_len };

	struct iovec iov[1] = { { .iov_base = (char *)value,
				  .iov_len = value_len } };
	iproto_broadcast(mbox, &r->paxos_remotes, &msg.header, iov, 1);

	say_debug("%s: > %s sync:%u SCN:%"PRIi64" ballot:%"PRIu64,
		  __func__, paxos_msg_code[code], msg.header.sync, scn, ballot);
	if (code != PREPARE)
		say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(tag), value_len,
			   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));
	return msg.header.sync;
}


static void
paxos_reply(struct paxos_request *req, enum paxos_msg_code code, u64 ballot)
{
	const struct msg_paxos *req_msg = req->msg;
	const struct proposal *p = req->p;
	struct conn *c = req->c;

	if (c->state < CONNECTED) {
		say_debug("not connected: ignoring fd:%i state:%i", c->fd, c->state);
		return;
	}

	struct msg_paxos *msg = p0alloc(c->pool, sizeof(*msg));
	msg->header = (struct iproto){ code,
				       sizeof(*msg) - sizeof(struct iproto) + (p ? p->value_len : 0),
				       req_msg->header.sync };
	msg->scn = req_msg->scn;
	msg->ballot = ballot ?: req_msg->ballot;
	msg->peer_id = self_id;
	msg->version = paxos_default_version;

	struct netmsg_head *h = &c->out_messages;

	if (p) {
		msg->value_len = p->value_len;
		msg->tag = p->tag;

		net_add_iov(h, msg, sizeof(*msg));
		if (p->value_len)
			net_add_iov_dup(h, p->value, p->value_len);
	} else {
		net_add_iov(h, msg, sizeof(*msg));
	}

	say_debug("%s: > peer:%i/%s %s sync:%i SCN:%"PRIi64" ballot:%"PRIu64,
		  __func__, req->peer->id, req->peer->name, paxos_msg_code[code], msg->header.sync,
		  msg->scn, msg->ballot);
	if (p)
		say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(p->tag), p->value_len,
			   tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));
	ev_io_start(&c->out);
}

static void
notify_leadership_change(PaxosRecovery *r)
{
	static int prev_leader = -1;
	if (leader_id < 0) {
		if (prev_leader != leader_id)
			say_info("leader unknown, %i -> %i", prev_leader, leader_id);
		[r status_update:STANDBY fmt:"paxos/slave"];
	} else if (!paxos_leader()) {
		if (prev_leader != leader_id)
			say_info("leader is %s, %i -> %i", paxos_peer(r, leader_id)->name,
				prev_leader, leader_id);
		[r status_update:STANDBY fmt:"paxos/slave"];
	} else if (paxos_leader()) {
		if (prev_leader != leader_id) {
			say_info("I am leader, %i -> %i", prev_leader, leader_id);
			catchup_done = 0;

			catchup(r, r->max_scn);
			catchup_done = 1;
			if (r->app_scn < r->max_scn) {
				say_warn("leader catchup failed AppSCN:%"PRIi64" MaxSCN:%"PRIi64,
					 r->app_scn, r->max_scn);
				leader_id = -2;
				title("paxos_catchup_fail");
				return;
			}
		}
		[r status_update:PRIMARY fmt:"paxos/leader"];
	}
	prev_leader = leader_id;
}

void
giveup_leadership()
{
	if (paxos_leader())
		leader_id = -2;
}

static void
propose_leadership(va_list ap)
{
	PaxosRecovery *r = va_arg(ap, PaxosRecovery *);

	struct msg_leader leader_propose = { .header = { .msg_code = LEADER_PROPOSE,
							 .data_len = sizeof(leader_propose) - sizeof(struct iproto) },
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

		struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
		iproto_broadcast(&mbox, &r->paxos_remotes,
				 &leader_propose.header, NULL, 0);
		assert(leader_propose.header.data_len == 14);
		mbox_timedwait(&mbox, quorum, 1);
		iproto_mbox_release(&mbox);

		int votes = 0;
		struct msg_leader *nack_msg = NULL;

		struct iproto *reply;
		int reply_count = 0;
		while ((reply = iproto_mbox_get(&mbox))) {
			reply_count++;
			if (reply->msg_code == LEADER_ACK) {
				votes++;
			} else {
				assert(reply->msg_code == LEADER_NACK);
				nack_msg = (struct msg_leader *)reply;
			}
		}
		if (votes >= quorum) {
			say_debug("%s: quorum reached v/q:%i/%i", __func__, votes, quorum);
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
				say_debug("%s: no quorum v/q:%i/%i", __func__, votes, quorum);
			}
		}
		notify_leadership_change(r);
	}
}

#ifdef RANDOM_DROP
#define PAXOS_MSG_DROP(h)						\
	double drop = rand() / (double)RAND_MAX;			\
	static double drop_p;						\
	if (!drop_p) {							\
		char *drop_pstr = getenv("RANDOM_DROP");		\
		drop_p = drop_pstr ? atof(drop_pstr) : RANDOM_DROP;	\
	}								\
	if (drop < drop_p) {						\
		say_debug("%s: op:0x%02x/%s sync:%i DROP", __func__,	\
			  (h)->msg_code, paxos_msg_code[(h)->msg_code], (h)->sync); \
		return;							\
	}
#else
#define PAXOS_MSG_DROP(h) (void)h
#endif

#define PAXOS_MSG_CHECK(msg, c, peer)	({				\
	if ((msg)->version != paxos_default_version) {			\
		say_warn("%s: bad version %i, closing connect from peer %i", \
			 __func__, (msg)->version, (msg)->peer_id);	\
		conn_close(c);						\
		return;							\
	}								\
	if (!peer) {					\
		say_warn("%s: closing connect from unknown peer %i", __func__, (msg)->peer_id); \
		conn_close(c);						\
		return;							\
	}								\
	PAXOS_MSG_DROP(&(msg)->header);					\
})


static void
leader(struct iproto *msg, struct conn *c)
{
	struct PaxosRecovery *r = (void *)c->service - offsetof(PaxosRecovery, service);
	struct msg_leader *pmsg = (struct msg_leader *)msg;
	struct paxos_peer *peer = paxos_peer(r, pmsg->peer_id);
	const char *ret = "accept";
	const ev_tstamp to_expire = leadership_expire - ev_now();

	PAXOS_MSG_CHECK(pmsg, c, peer);

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
			notify_leadership_change(r);
		}
	} else {
		ret = "nack";
		msg->msg_code = LEADER_NACK;
		pmsg->leader_id = leader_id;
		pmsg->expire = leadership_expire;
	}
	if (c->state < CONNECTED)
		return;

	say_debug("|   -> reply with %s", ret);

	net_add_iov_dup(&c->out_messages, pmsg, sizeof(*pmsg));
	ev_io_start(&c->out);
}


static struct proposal *
find_proposal(PaxosRecovery *r, i64 scn)
{
	return RB_FIND(ptree, &r->proposals, &(struct proposal){ .scn = scn });
}

static void
update_proposal_ballot(struct proposal *p, u64 ballot)
{
	say_debug2("%s: SCN:%"PRIi64" ballot:%"PRIu64, __func__, p->scn, ballot);
	assert(p->ballot <= ballot);
	if (p->ballot == ULLONG_MAX)
		assert(ballot == ULLONG_MAX); /* decided proposal is immutable */
	p->ballot = ballot;
}


static void
update_proposal_value(struct proposal *p, u32 value_len, const u8 *value, u16 tag)
{
	say_debug2("%s: SCN:%"PRIi64" tag:%s value_len:%i value:%s", __func__,
		   p->scn, xlog_tag_to_a(tag), value_len,
		   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

	if (p->ballot == ULLONG_MAX) { /* decided proposal is immutable */
		assert(value_len == p->value_len);
		assert(memcmp(value, p->value, value_len) == 0);
		assert(p->tag == tag);
		return;
	}

	if (p->value_len != value_len) {
		assert(value_len > 0); /* value never goes empty */
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

static void
delete_proposal(PaxosRecovery *r, struct proposal *p)
{
	RB_REMOVE(ptree, &r->proposals, p);
	if (p->value)
		sfree(p->value);
	slab_cache_free(&proposal_cache, p);
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

	for (struct proposal *m = RB_MIN(ptree, &r->proposals);
	     r->max_scn - m->scn > proposal_history_size && m->flags & P_CLOSED;
	     m = RB_MIN(ptree, &r->proposals))
	{
		delete_proposal(r, m);
	}

	return p;
}

static struct proposal *
proposal(PaxosRecovery *r, i64 scn)
{
	assert(scn > 0);
	return find_proposal(r, scn) ?: create_proposal(r, scn);
}


#define nack(req, nack_ballot, msg_ballot) ({					\
	assert(nack_ballot != ULLONG_MAX);						\
	say_info("NACK(%i%s) sync:%i SCN:%"PRIi64" ballot:%"PRIu64 " nack_ballot:%"PRIi64, \
		 __LINE__, (msg_ballot & 0xff) == self_id ? "self" : "", \
		 (req)->msg->header.sync, (req)->p->scn, (req)->p->ballot, (nack_ballot)); \
	paxos_reply((req), NACK, (nack_ballot));				\
})

static int
submit(XLogWriter *r, const void *data, u32 data_len, i64 scn, u16 tag)
{
	struct row_v12 row = { .scn = scn,
			       .tag = tag };

	struct wal_pack pack;
	if (!wal_pack_prepare(r, &pack))
		return 0;
	wal_pack_append_row(&pack, &row);
	wal_pack_append_data(&pack, &row, data, data_len);
	return [r wal_pack_submit];
}

static void
decided(struct paxos_request *req)
{
	if (req->peer->id == self_id)
		return;

	paxos_reply(req, DECIDE, req->p->ballot);
}

static void
promise(PaxosRecovery *r, struct paxos_request *req)
{
	struct proposal *p = req->p;
	const struct msg_paxos *msg = req->msg;
	u64 ballot = p->ballot;

	if (msg->ballot <= p->ballot) {
		nack(req, p->ballot, msg->ballot);
		return;
	}

	if (submit(r, &msg->ballot, sizeof(msg->ballot), msg->scn, paxos_promise | TAG_SYS) != 1)
		return;

	if (p->ballot == ULLONG_MAX) {
		decided(req);
		return;
	}

	if (msg->ballot <= p->ballot) {
		nack(req, p->ballot, msg->ballot);
		return;
	}

	assert(p->ballot < msg->ballot);
	ballot = p->ballot; /* concurent update is possible */
	update_proposal_ballot(p, msg->ballot);
	paxos_reply(req, PROMISE, ballot);
}

static void
accepted(PaxosRecovery *r, struct paxos_request *req)
{
	const struct msg_paxos *msg = req->msg;
	struct proposal *p = req->p;
	assert(msg->scn == p->scn);
	assert(msg->value_len > 0);

	if (msg->ballot < p->ballot) {
		nack(req, p->ballot, msg->ballot);
		return;
	}

	struct tbuf *buf = tbuf_alloc(fiber->pool);
	tbuf_append(buf, &msg->ballot, sizeof(msg->ballot));
	tbuf_append(buf, &msg->tag, sizeof(msg->tag));
	tbuf_append(buf, &msg->value_len, sizeof(msg->value_len));
	tbuf_append(buf, msg->value, msg->value_len);

	if (submit(r, buf->ptr, tbuf_len(buf), msg->scn, paxos_accept | TAG_SYS) != 1)
		return;

	if (p->ballot == ULLONG_MAX) {
		decided(req);
		return;
	}

	if (msg->ballot < p->ballot) {
		nack(req, p->ballot, msg->ballot);
		return;
	}

	assert(msg->ballot >= p->ballot);
	update_proposal_ballot(p, msg->ballot);
	update_proposal_value(p, msg->value_len, msg->value, msg->tag);
	paxos_reply(req, ACCEPTED, 0);
}

static u32
prepare(PaxosRecovery *r, struct iproto_mbox *mbox, struct proposal *p, u64 ballot)
{
	if (submit(r, &ballot, sizeof(ballot), p->scn, paxos_prepare | TAG_SYS) != 1)
		return 0;
	p->prepare_ballot = ballot;

	iproto_mbox_init(mbox, fiber->pool, IPROTO_MBOX_STATIC_SYNC_SIZE);
	u32 sync = paxos_broadcast(r, mbox, PREPARE, p->scn, ballot, NULL, 0, 0);
	mbox_timedwait(mbox, quorum, p->delay);
	return sync;
}

static u32
propose(PaxosRecovery *r, struct iproto_mbox *mbox,
	u64 ballot, i64 scn, const char *value, u32 value_len, u16 tag)
{
	assert((tag & ~TAG_MASK) != 0);
	assert((tag & TAG_MASK) < paxos_prepare || (tag & TAG_MASK) > paxos_nop);
	assert(value_len > 0);

	iproto_mbox_init(mbox, fiber->pool, IPROTO_MBOX_STATIC_SYNC_SIZE);
	u32 sync = paxos_broadcast(r, mbox, ACCEPT, scn, ballot, value, value_len, tag);
	mbox_timedwait(mbox, quorum, paxos_default_timeout);
	return sync;
}


static void maybe_wake_dumper(PaxosRecovery *r, struct proposal *p);

static void
mark_applied(PaxosRecovery *r, struct proposal *p)
{
	assert(p->ballot == ULLONG_MAX);
	p->flags |= P_APPLIED;

	while (p && p->scn - r->app_scn == 1 && p->flags & P_APPLIED) {
		r->app_scn = p->scn;
		p = RB_NEXT(ptree, &r->proposals, p);
	}
}

static void
learn(PaxosRecovery *r, struct proposal *p)
{
	if (p == NULL && r->app_scn < r->max_scn)
		p = proposal(r, r->app_scn + 1);

	say_debug("%s: from SCN:%"PRIi64, __func__, p ? p->scn : -1);

	for (; p != NULL; p = RB_NEXT(ptree, &r->proposals, p)) {
		assert([r scn] <= r->app_scn);
		assert(r->app_scn <= r->max_scn);

		if (p->scn != r->app_scn + 1)
			return;

		if (p->ballot != ULLONG_MAX)
			return;

		if (p->flags & P_APPLIED)
			return;

		say_debug("%s: SCN:%"PRIi64" ballot:%"PRIu64, __func__, p->scn, p->ballot);
		say_debug2("|  value_len:%i value:%s",
			   p->value_len, tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));


		@try {
			[r apply:&TBUF(p->value, p->value_len, fiber->pool) tag:p->tag];
			mark_applied(r, p);
		}
		@catch (Error *e) {
			say_warn("aborting txn, [%s reason:\"%s\"] at %s:%d",
				 [[e class] name], e->reason, e->file, e->line);
			break;
		}
	}
}

static void
msg_dump(const char *prefix, const struct paxos_peer *peer, const struct iproto *msg)
{
	const struct msg_paxos *req = (struct msg_paxos *)msg;
	const char *code = paxos_msg_code[msg->msg_code];
	say_debug("%s peer:%i/%s sync:%i type:%s SCN:%"PRIi64" ballot:%"PRIu64" tag:%s",
		  prefix, peer->id, peer->name, msg->sync, code, req->scn, req->ballot, xlog_tag_to_a(req->tag));
	say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(req->tag), req->value_len,
		   tbuf_to_hex(&TBUF(req->value, req->value_len, fiber->pool)));
}

static void
learner(struct iproto *msg, struct conn *c)
{
	struct PaxosRecovery *r = (void *)c->service - offsetof(PaxosRecovery, service);
	struct msg_paxos *req = (struct msg_paxos *)msg;
	struct paxos_peer *peer = paxos_peer(r, req->peer_id);

	PAXOS_MSG_CHECK(req, c, peer);

	msg_dump("learner: <", peer, msg);

	if (req->peer_id == self_id)
		return;

	if (req->scn <= r->app_scn)
		return;

	struct proposal *p = proposal(r, req->scn);

	if (p->ballot == ULLONG_MAX) {
		assert(memcmp(req->value, p->value, MIN(req->value_len, p->value_len)) == 0);
		assert(p->tag == req->tag);
		return;
	}

	update_proposal_value(p, req->value_len, req->value, req->tag);
	update_proposal_ballot(p, ULLONG_MAX);

	learn(r, p);
	maybe_wake_dumper(r, p);
}

static void
acceptor(struct iproto *imsg, struct conn *c)
{
	struct PaxosRecovery *r = (void *)c->service - offsetof(PaxosRecovery, service);
	struct msg_paxos *msg = (struct msg_paxos *)imsg;
	struct paxos_request req = { .msg = msg,
				     .peer = paxos_peer(r, msg->peer_id),
				     .c = c };

	PAXOS_MSG_CHECK(req.msg, req.c, req.peer);

	msg_dump("acceptor: <", req.peer, imsg);

	if (msg->scn <= r->app_scn) {
		/* the proposal in question was decided too long ago,
		   no further progress is possible */

		struct proposal *min = RB_MIN(ptree, &r->proposals);
		if (!min || msg->scn < min->scn) {
			say_error("STALE SCN:%"PRIi64 " minSCN:%"PRIi64, msg->scn, min ? min->scn : -1);
			paxos_reply(&req, STALE, 0);
			return;
		}

		req.p = proposal(r, msg->scn);
		assert(req.p->ballot == ULLONG_MAX);

		decided(&req);
		return;
	}

	req.p = proposal(r, msg->scn);
	if (req.p->ballot == ULLONG_MAX) {
		decided(&req);
		return;
	}

	switch (imsg->msg_code) {
	case PREPARE:
		promise(r, &req);
		break;
	case ACCEPT:
		accepted(r, &req);
		break;
	default:
		assert(false);
	}
}


static u64 next_ballot(u64 min)
{
	/* lower 8 bits is our id (unique in cluster)
	   high bits - counter */

	assert(min != ULLONG_MAX);
	u64 ballot = (min & ~0xff) | (self_id & 0xff);
	do
		ballot += 0x100;
	while (ballot < min);
	return ballot;
}

static u64
run_protocol(PaxosRecovery *r, struct proposal *p, char *value, u32 value_len, u16 tag)
{
	assert((tag & ~TAG_MASK) != 0);
	assert((tag & TAG_MASK) < paxos_prepare || (tag & TAG_MASK) > paxos_nop);
	assert(value_len > 0);

	bool has_old_value = false;
	u64 ballot = p->prepare_ballot, nack_ballot = 0;
	int votes;
	struct iproto_mbox mbox;

	say_debug("%s: SCN:%"PRIi64, __func__, p->scn);
	say_debug2("|  tag:%s value_len:%u value:%s", xlog_tag_to_a(tag), value_len,
		   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

retry:
	if (p->ballot == ULLONG_MAX)
		goto decide;

	ballot = next_ballot(MAX(ballot, p->ballot));

	u32 sync = prepare(r, &mbox, p, ballot);
	say_debug("PREPARE reply sync:%i SCN:%"PRIi64, sync, p->scn);
	struct msg_paxos *req, *max = NULL;
	votes = 0;

	if (mbox.msg_count == 0)
		say_debug("|  SCN:%"PRIi64" EMPTY", p->scn);

	if (mbox.msg_count == 0 ||
	    (mbox.msg_count == 1 && ((struct msg_paxos *)iproto_mbox_peek(&mbox))->peer_id == self_id))
		p->delay *= 1.25;

	while ((req = (struct msg_paxos *)iproto_mbox_get(&mbox))) {
		say_debug("|  %s SCN:%"PRIi64" ballot:%"PRIu64" value_len:%i",
			  paxos_msg_code[req->header.msg_code], req->scn, req->ballot, req->value_len);

		switch(req->header.msg_code) {
		case NACK:
			nack_ballot = req->ballot;
			break;
		case PROMISE:
			votes++;
			if (req->value_len > 0 && (max == NULL || req->ballot > max->ballot))
				max = req;
			break;
		case DECIDE:
			update_proposal_value(p, req->value_len, req->value, req->tag);
			update_proposal_ballot(p, ULLONG_MAX);
			iproto_mbox_release(&mbox);
			goto decide;
		case STALE: {
			struct paxos_peer *p;
			XLogPuller *puller = [[XLogPuller alloc] init];

			SLIST_FOREACH(p, &r->group, link) {
				if (p->id == self_id)
					continue;

				say_debug("feeding from %s", p->name);
				[puller feeder_param:&p->feeder];
				if ([puller handshake:[r scn]] <= 0)
					continue;
				while ([r pull_wal:puller] != 1);
				break;
			}
			[puller free];
			return 0;
		}
		default:
			abort();
		}
	}

	if (votes < quorum) {
		if (nack_ballot > ballot) { /* we have a hint about ballot */
			assert(nack_ballot != ULLONG_MAX);
			ballot = nack_ballot;
		}

		iproto_mbox_release(&mbox);
		fiber_sleep(0.001 * rand() / RAND_MAX);
		goto retry;
	}

	if (max && (max->value_len != value_len || memcmp(max->value, value, value_len) != 0))
	{
		has_old_value = 1;
		say_debug("has REMOTE value for SCN:%"PRIi64" value_len:%i value:%s",
			  p->scn, max->value_len,
			  tbuf_to_hex(&TBUF(max->value, max->value_len, fiber->pool)));

		value = salloc(max->value_len);
		memcpy(value, max->value, max->value_len);
		value_len = max->value_len;
		tag = max->tag;
	}
	iproto_mbox_release(&mbox);

	sync = propose(r, &mbox, ballot, p->scn, value, value_len, tag);
	say_debug("PROPOSE reply sync:%i SCN:%"PRIi64, sync, p->scn);

	if (mbox.msg_count == 0)
		say_debug("|  SCN:%"PRIi64" EMPTY", p->scn);

	votes = 0;
	while ((req = (struct msg_paxos *)iproto_mbox_get(&mbox))) {
		say_debug("|  %s SCN:%"PRIi64" ballot:%"PRIu64" value_len:%i",
			  paxos_msg_code[req->header.msg_code], req->scn, req->ballot, req->value_len);

		switch (req->header.msg_code) {
		case ACCEPTED:
			votes++;
			break;
		case DECIDE:
			update_proposal_value(p, req->value_len, req->value, req->tag);
			update_proposal_ballot(p, ULLONG_MAX);
			iproto_mbox_release(&mbox);
			goto decide;
			break;
		case NACK:
			nack_ballot = req->ballot;
			break;
		}
	}

	iproto_mbox_release(&mbox);

	if (votes < quorum) {
		if (nack_ballot > ballot) { /* we have a hint about ballot */
			assert(nack_ballot != ULLONG_MAX);
			ballot = nack_ballot;
		}

		fiber_sleep(0.001 * rand() / RAND_MAX);
		goto retry;
	}

	paxos_broadcast(r, NULL, DECIDE, p->scn, ballot, value, value_len, tag);
	if (has_old_value)
		sfree(value);
	return has_old_value ? 0 : ballot;

decide:
	if (p->tag == tag &&
	    p->value_len == value_len &&
	    memcmp(p->value, value, value_len) == 0 &&
	    p->flags != P_APPLIED)
		return p->ballot; /* somebody just decided on our value for us */
	else
		return 0;
}


struct wal_msg { STAILQ_ENTRY(wal_msg) link; };
static MBOX(, wal_msg) wal_dumper_mbox = MBOX_INITIALIZER(wal_dumper_mbox);

static void
maybe_wake_dumper(PaxosRecovery *r, struct proposal *p)
{
	if (p->scn - [r scn] < 8)
		return;

	struct wal_msg *m = palloc(r->wal_dumper->pool, sizeof(*m));
	mbox_put(&wal_dumper_mbox, m, link);
}

static void
wal_dumper_fib(va_list ap)
{
	PaxosRecovery *r = va_arg(ap, PaxosRecovery *);
	struct proposal *p = NULL;

loop:
	mbox_timedwait(&wal_dumper_mbox, 1, 1);
	while (mbox_get(&wal_dumper_mbox, link)); /* flush mbox */
	fiber_gc(); /* NB: put comment */

	p = RB_MIN(ptree, &r->proposals);
	say_debug("wal_dump SCN:%"PRIi64 " appSCN:%"PRIi64 " maxSCN:%"PRIi64,
		  [r scn], r->app_scn, r->max_scn);

	while (p && p->flags & P_CLOSED) {
		say_debug2("|  % 8"PRIi64" CLOSED", p->scn);
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	while (p && p->scn <= r->app_scn) {
		assert(p->ballot == ULLONG_MAX);
		assert(p->flags & P_APPLIED);
		say_debug2("|  % 8"PRIi64" APPLIED", p->scn);
		assert([r scn] + 1 == p->scn);
		/* TODO: batch write */
		if (submit(r, p->value, p->value_len, p->scn, p->tag) != 1) {
			say_error("unable to writer wal row");
			goto loop;
		}
		if (p->tag == (run_crc | TAG_WAL))
			[r verify_run_crc:&TBUF(p->value, p->value_len, NULL)];


		p->flags |= P_CLOSED;
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	if (!paxos_leader()) {
		bool delay_too_big = p && ev_now() - p->tstamp > 1;
		bool too_many_not_applied = r->max_scn - [r scn] > cfg.wal_writer_inbox_size * 1.1;
		if (delay_too_big || too_many_not_applied) {
			/* if we run protocol on recent proposals we
			   will interfere with current leader */
			for (;;) {
				struct proposal *next = RB_NEXT(ptree, &r->proposals, p);
				if (!next || ev_now() - next->tstamp > 0.2)
					break;
				p = next;
			}
			catchup(r, p->scn);
		}
	}

	goto loop;
}


static u64
close_with_nop(PaxosRecovery *r, struct proposal *p)
{
	say_debug("%s: SCN:%"PRIi64, __func__, p->scn);
	assert(p->ballot != ULLONG_MAX);
	return run_protocol(r, p, "\0\0", 2, TAG_WAL | nop);
}


static void
catchup(PaxosRecovery *r, i64 upto_scn)
{
	say_debug("%s: SCN:%"PRIi64 " upto_scn:%"PRIi64, __func__, [r scn], upto_scn);

	for (i64 i = r->app_scn + 1; i <= upto_scn; i++) {
		struct proposal *p = proposal(r, i);
		say_debug("|	SCN:%"PRIi64" ballot:%"PRIi64, p->scn, p->ballot);
		if (p->ballot == ULLONG_MAX)
			continue;

		if (close_with_nop(r, p) != ULLONG_MAX) {
			say_warn("can't close SCN:%"PRIi64, p->scn);
			/* undecided proposal between app_scn and max_scn:
			   learning upto max_scn is impossible */
			return;
		}
	}
	learn(r, NULL);
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
 feeder_param:(struct feeder_param*)param
	flags:(int)flags
{
	struct octopus_cfg_paxos_peer *c;

	[super init_snap_dir:snap_dirname
		     wal_dir:wal_dirname
		rows_per_wal:wal_rows_per_file
		feeder_param:param
		       flags:flags];

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

		if (paxos_peer(self, c->id) != NULL)
			panic("paxos peer %s already exists", c->name);

		say_info("  %s -> %s", c->name, c->addr);

		const char *name = c->id == self_id ? "self" : c->name;
		struct paxos_peer *p = make_paxos_peer(c->id, name,
						       c->addr, c->primary_port, c->feeder_port);

		if (!p)
			panic("bad addr %s", c->addr);

		SLIST_INSERT_HEAD(&group, p, link);
		SLIST_INSERT_HEAD(&paxos_remotes, &p->paxos, link);
		SLIST_INSERT_HEAD(&primary_group, &p->primary, link);
	}

	if (!paxos_peer(self, self_id))
		panic("unable to find myself among paxos peers");

	fiber_create("paxos/stat", paxos_stat, self);
	return self;
}

- (void)
enable_local_writes
{
	say_debug("%s", __func__);
	[self recover_finalize];
	local_writes = true;

	if (scn != 0)
		[self configure_wal_writer];

	XLogPuller *puller = [[XLogPuller alloc] init];
	for (;;) {
		struct paxos_peer *p;
		SLIST_FOREACH(p, &group, link) {
			if (p->id == self_id)
				continue;

			[puller feeder_param:&p->feeder];
			if ([puller handshake:scn] <= 0)
				continue;

			say_debug("loading from %s", p->name);
			@try {
				[self pull_wal:puller];
			}
			@finally {
				[puller close];
			}

			if ([self scn] > 0)
				goto exit;
		}
		fiber_sleep(1);
	}
exit:
	[puller free];
	if (!configured)
		[self configure_wal_writer];

	app_scn = scn;

	/* boot from initial state or from non paxos xlogs
	   there is no TAG_SYS records => we don't create any proposals => max_scn == 0 */
	if (max_scn == 0)
		max_scn = scn;

	struct proposal *min = RB_MIN(ptree, &self->proposals);
	say_info("SCN:%"PRIi64" minSCN:%"PRIi64" appSCN:%"PRIi64" maxSCN:%"PRIi64,
		 scn, min ? min->scn : -1, app_scn, max_scn);

	const char *addr = sintoa(&paxos_peer(self, self_id)->paxos.addr);
	tcp_iproto_service(&service, addr, NULL, iproto_wakeup_workers);

	service_register_iproto_block(&service, LEADER_PROPOSE, leader, 0);
	service_register_iproto_block(&service, PREPARE, acceptor, 0);
	service_register_iproto_block(&service, ACCEPT, acceptor, 0);
	service_register_iproto_block(&service, DECIDE, learner, 0);
	for (int i = 0; i < 3; i++)
		fiber_create("paxos/worker", iproto_worker, &service);

	reply_reader = fiber_create("paxos/reply_reader", iproto_reply_reader, iproto_collect_reply);
	output_flusher = fiber_create("paxos/output_flusher", conn_flusher);
	fiber_create("paxos/rendevouz", iproto_rendevouz, NULL, &paxos_remotes, reply_reader, output_flusher);
	fiber_create("paxos_p/rendevouz", iproto_rendevouz, NULL, &primary_group, reply_reader, output_flusher);
	fiber_create("paxos/elect", propose_leadership, self);

	wal_dumper = fiber_create("paxos/wal_dump", wal_dumper_fib, self);
}

- (void)
leader_redirect_raise
{
	if (leader_id >= 0) {
		if (leader_id == self_id)
			return;

		const char *addr = paxos_peer(self, leader_id)->primary_addr;
		iproto_raise(ERR_CODE_REDIRECT, addr);
	} else {
		iproto_raise(ERR_CODE_LEADER_UNKNOW, "leader unknown");
	}
}

- (struct iproto_peer *)
leader_primary
{
	assert(!paxos_leader());
	return &paxos_peer(self, leader_id)->primary;
}

- (bool)
is_replica
{
	return leader_id < 0 || leader_id != self_id;
}

- (void)
check_replica
{
	if (!paxos_leader())
		[self leader_redirect_raise];

	if (!catchup_done)
		iproto_raise(ERR_CODE_LEADER_UNKNOW, "leader not ready");
}

- (int)
submit:(void *)data len:(u32)len tag:(u16)tag
{
	if (!configured)
		return 0;

	@try {
		i64 cur_scn = [self next_scn];
		struct proposal *p = proposal(self, cur_scn);
		u64 ballot = run_protocol(self, p, data, len, tag);

		if (ballot) {
			update_proposal_value(p, len, data, tag);
			update_proposal_ballot(p, ULLONG_MAX);

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
	return 0; /* make apple's gcc happy */
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

	if ([snap append_row:buf->ptr len:tbuf_len(buf) scn:scn tag:(snap_skip_scn | TAG_SNAP)] == NULL) {
		say_error("unable write snap_applied_scn");
		return -1;
	}

	return 0;
}

- (void)
recover_row:(struct row_v12 *)r
{
	say_debug("%s: lsn:%"PRIi64" SCN:%"PRIi64" tag:%s", __func__,
		  r->lsn, r->scn, xlog_tag_to_a(r->tag));

	struct tbuf buf;
	struct proposal *p = NULL;
	u64 ballot;

	int tag = r->tag & TAG_MASK;
	int tag_type = r->tag & ~TAG_MASK;

	if (tag_type == TAG_SYS) {
		switch (tag) {
		case paxos_prepare:
			lsn = r->lsn;
			buf = TBUF(r->data, r->len, NULL);
			ballot = read_u64(&buf);
			p = proposal(self, r->scn);
			p->prepare_ballot = ballot;
			return;

		case paxos_promise:
		case paxos_nop:
			lsn = r->lsn;
			buf = TBUF(r->data, r->len, NULL);
			ballot = read_u64(&buf);
			p = proposal(self, r->scn);
			if (p->ballot < ballot)
				update_proposal_ballot(p, ballot);
			return;

		case paxos_accept:
		case paxos_propose:
			lsn = r->lsn;
			buf = TBUF(r->data, r->len, NULL);
			ballot = read_u64(&buf);
			u16 tag = read_u16(&buf);
			u32 value_len = read_u32(&buf);
			const u8 *value = read_bytes(&buf, value_len);
			p = proposal(self, r->scn);
			if (p->ballot <= ballot) {
				update_proposal_ballot(p, ballot);
				update_proposal_value(p, value_len, value, tag);
			}
			return;

		case snap_initial:
		case snap_final:
			[super recover_row:r];
			return;
		}
	}

	switch (tag_type) {
	case TAG_SNAP:
		[super recover_row:r];
		return;

	default:
		p = proposal(self, r->scn);
		update_proposal_value(p, r->len, r->data, r->tag);
		update_proposal_ballot(p, ULLONG_MAX);
		p->flags |= P_CLOSED;

		if (r->scn == next_skip_scn) {
			p->flags |= P_APPLIED;
			next_skip_scn = tbuf_len(&skip_scn) > 0 ?
					read_u64(&skip_scn) : 0;
			return;
		}

		[super recover_row:r];
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
	u16 tag = row->tag & TAG_MASK, inner_tag;
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

		switch(inner_tag & TAG_MASK) {
		case run_crc: {
			i64 scn = read_u64(&b);
			u32 log = read_u32(&b);
			(void)read_u32(&b); /* ignore run_crc_mod */
			tbuf_printf(out, "SCN:%"PRIi64 " log:0x%08x", scn, log);
			break;
		}
		case nop:
			break;
		default:
			handler(out, inner_tag, &b);
		}
		break;
	}
}

register_source();

void __attribute__((constructor))
paxos_cons()
{
	slab_cache_init(&proposal_cache, sizeof(struct proposal), SLAB_GROW, "paxos/proposal");
}
