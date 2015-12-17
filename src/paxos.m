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
#import <shard.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

@interface Paxos (Internal)
- (int) write_scn:(i64)scn_ data:(const void *)data len:(u32)len tag:(u16)tag;
@end


#define PAXOS_CODE(_)					\
	_(NACK,	0xfff0)					\
	_(LEADER_PROPOSE, 0xfff1)			\
	_(LEADER_ACK, 0xfff2)				\
	_(LEADER_NACK, 0xfff3)				\
	_(PREPARE, 0xfff4)				\
	_(PROMISE, 0xfff5)				\
	_(ACCEPT, 0xfff6)				\
	_(ACCEPTED, 0xfff7)				\
	_(DECIDE, 0xfff8)				\
	_(STALE, 0xfffa)

enum paxos_msg_code ENUM_INITIALIZER(PAXOS_CODE);
const char *paxos_msg_code[] = ENUM_STR_INITIALIZER(PAXOS_CODE);
const int proposal_history_size = 16 * 1024;
const int quorum = 2; /* FIXME: hardcoded */

static struct palloc_pool *paxos_pool;

struct paxos_peer {
	int id;
	const char *name;
	struct sockaddr_in addr;
	struct iproto_egress *egress;
	struct feeder_param feeder;
	SLIST_ENTRY(paxos_peer) link;
};

struct paxos_peer *
make_paxos_peer(int id, const char *name)
{
	const struct sockaddr_in *sin;
	struct paxos_peer *p;

	sin = shard_addr(name, PORT_PRIMARY);
	if (sin == NULL)
		return NULL;

	p = xcalloc(1, sizeof(*p));
	p->id = id;
	p->name = strdup(name);

	p->addr = *sin;
	p->feeder.ver = 1;
	p->feeder.addr = *shard_addr(name, PORT_REPLICATION);
	p->feeder.filter = (struct feeder_filter){.type = FILTER_TYPE_LUA,
						  .name = "tag_wal"};

	p->egress = iproto_remote_add_peer(NULL, &p->addr, paxos_pool);
	p->egress->ts.name = p->name;

	return p;
}

struct paxos_peer *
paxos_peer(Paxos *paxos, int id)
{
	struct paxos_peer *p;
	SLIST_FOREACH(p, &paxos->group, link)
		if (p->id == id)
			return p;
	return NULL;
}

static const char *
paxos_peer_name(struct Paxos *paxos, int id)
{
	static char buf[32];
	struct paxos_peer *peer = paxos_peer(paxos, id);
	assert(peer || id == paxos->self_id);
	snprintf(buf, sizeof(buf), "%i/%s", id, peer ? peer->name : "self");
	return buf;
}

static u16 paxos_default_version;

struct msg_leader {
	struct iproto header;
	u16 version;
	u16 peer_id;
	i16 leader_id;
	ev_tstamp expire;
} __attribute__((packed));


struct msg_paxos {
	struct iproto header;
	u16 version;
	u16 peer_id;
	u32 msg_id;
	i64 scn;
	u64 ballot;
	u16 tag;
	u32 value_len;
	char value[];
} __attribute__((packed));


struct paxos_request {
	const struct msg_paxos *msg;
	const char *value;
	struct proposal *p;
	enum { PAXOS_REQ_INTERNAL, PAXOS_REQ_REMOTE } type;
	union {
		struct netmsg_head *wbuf; /* wbuf of incoming connection */
		struct iproto_mbox *mbox;
	};

};


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

static ev_tstamp leadership_expire;
static const ev_tstamp leader_lease_interval = 10;
static const ev_tstamp paxos_default_timeout = 0.2;

static void catchup(Paxos *paxos, i64 upto_scn);

static const char *
scn_info(Paxos *paxos)
{
	static char buf[64];
	const struct proposal *p = RB_MIN(ptree, &paxos->proposals);
	snprintf(buf, sizeof(buf),
		 "minSCN:%"PRIi64" SCN:%"PRIi64" maxSCN:%"PRIi64,
		 p ? p->scn : -1, paxos->scn, paxos->max_scn);
	return buf;
}

static bool
paxos_leader(Paxos *paxos)
{
	return paxos->leader_id >= 0 && paxos->leader_id == paxos->self_id;
}

static void acceptor(Paxos *paxos, struct paxos_request *req);

static u32
paxos_broadcast(Paxos *paxos, struct iproto_mbox *mbox,
		enum paxos_msg_code code, /* ev_tstamp timeout, */
		i64 scn, u64 ballot, const char *value, u32 value_len, u16 tag)
{
	static u32 msg_id;
	struct msg_paxos msg = { .header = { .data_len = sizeof(msg) - sizeof(struct iproto),
					     .msg_code = code,
					     .shard_id = paxos->id },
				 .scn = scn,
				 .ballot = ballot,
				 .peer_id = paxos->self_id,
				 .msg_id = msg_id++,
				 .version = paxos_default_version,
				 .tag = tag,
				 .value_len = value_len };

	struct iovec iov[1] = { { .iov_base = (char *)value,
				  .iov_len = value_len } };

	say_debug("%s: [%i]%s> %s SCN:%"PRIi64" ballot:%"PRIx64,
		  __func__, msg.msg_id, mbox ? "" : " OWT", paxos_msg_code[code], scn, ballot);
	assert(tag == 0 || value_len > 0);
	if (tag != 0)
		say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(tag), value_len,
			   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

	iproto_mbox_broadcast(mbox, &paxos->paxos_remotes, &msg.header, iov, 1);
	if (code == PREPARE || code == ACCEPT) {
		struct paxos_request req = { .msg = &msg, .value = value, .type = PAXOS_REQ_INTERNAL, {.mbox = mbox} };
		acceptor(paxos, &req);
	}
	return msg.msg_id;
}


static void
paxos_respond(Paxos *paxos, struct paxos_request *req, enum paxos_msg_code code, u64 ballot)
{
	struct msg_paxos *msg = NULL;
	const struct proposal *p = req->p;
	int value_len = p ? p->value_len : 0,
	      msg_len = sizeof(*msg) + value_len;

	switch (req->type) {
	case PAXOS_REQ_REMOTE:
		msg = palloc(req->wbuf->pool, msg_len);
		net_add_iov(req->wbuf, msg, msg_len);
		break;
	case PAXOS_REQ_INTERNAL:
		msg = palloc(req->mbox->pool, msg_len);
		iproto_mbox_put(req->mbox, &msg->header);
		break;
	}

	*msg = (struct msg_paxos){ .header = { .msg_code = code,
					       .shard_id = req->msg->header.shard_id,
					       .data_len = msg_len - sizeof(struct iproto),
					       .sync = req->msg->header.sync },
				   .scn = req->msg->scn,
				   .ballot = ballot,
				   .peer_id = paxos->self_id,
				   .msg_id = req->msg->msg_id,
				   .version = paxos_default_version };

	say_debug("%s: [%i]> %s sync:%i SCN:%"PRIi64" ballot:%"PRIx64,
		  __func__, msg->msg_id, paxos_msg_code[code], msg->header.sync, msg->scn, msg->ballot);
	if (p) {
		msg->tag = p->tag;
		msg->value_len = value_len;
		memcpy(msg->value, p->value, value_len);

		if (p->tag != 0) /* нет тега -> нет значения */
			say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(p->tag), p->value_len,
				   tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));
	}
}

static ev_tstamp
propose_leadership(Paxos *paxos, struct iproto_mbox *mbox, int leader_id)
{
	say_debug("PROPOSE_LEADERSHIP leader:%i", leader_id);
	ev_tstamp expire = leader_id < 0 ? 0 : ev_now() + leader_lease_interval;
	struct msg_leader leader_propose = { .header = { .msg_code = LEADER_PROPOSE,
							 .shard_id = paxos->id,
							 .data_len = sizeof(leader_propose) - sizeof(struct iproto) },
					     .peer_id = paxos->self_id,
					     .version = paxos_default_version,
					     .leader_id = leader_id,
					     .expire = expire };
	iproto_mbox_broadcast(mbox, &paxos->paxos_remotes,
			      &leader_propose.header, NULL, 0);
	if (mbox) {
		mbox_timedwait(mbox, quorum, 1);
		say_debug2("PROPOSE_LEADERSHIP got %i replies", mbox->msg_count);
	}
	return leader_propose.expire;
}

void
giveup_leadership(Paxos *paxos)
{
	assert(paxos_leader(paxos));
	paxos->leader_id = -1;
	leadership_expire = -1;
	propose_leadership(paxos, NULL, -1);
}


static void
paxos_elect(va_list ap)
{
	Paxos *paxos = va_arg(ap, Paxos *);
	fiber->ushard = paxos->id;

	fiber_sleep(0.3); /* wait connections to be up */
	for (;;) {
		if (ev_now() > leadership_expire)
			paxos->leader_id = -1;

		if (paxos->leader_id < 0) {
			ev_tstamp delay = drand(leader_lease_interval * 0.1);
			if (paxos->leader_id == -2)
				delay += leader_lease_interval * 2;
			fiber_sleep(delay);
		} else {
			if (!paxos_leader(paxos))
				fiber_sleep(leadership_expire + leader_lease_interval * .01 - ev_now());
			else
				fiber_sleep(leadership_expire - leader_lease_interval * .1 - ev_now());
		}

		if (paxos->leader_id >= 0 && !paxos_leader(paxos))
			continue;

		struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
		ev_tstamp proposed_expire = propose_leadership(paxos, &mbox, paxos->self_id);

		if (paxos->leader_id >= 0 && !paxos_leader(paxos)) {
			/* while we were waiting for quorum, the new leader has been elected */
			iproto_mbox_release(&mbox);
			continue;
		}

		int votes = 0;
		struct msg_leader *nack_msg = NULL;

		struct iproto *reply;
		int reply_count = 0;
		while ((reply = iproto_mbox_get(&mbox))) {
			struct msg_leader *leader_reply = (struct msg_leader *)reply;
			assert(paxos->self_id != leader_reply->peer_id);
			reply_count++;
			if (reply->msg_code == LEADER_ACK) {
				votes++;
			} else {
				assert(reply->msg_code == LEADER_NACK);
				nack_msg = (struct msg_leader *)reply;
			}
		}
		if (votes >= quorum - 1) { // -1 because we don't message ourselfs
			say_debug("%s: quorum reached v/q:%i/%i", __func__, votes, quorum);
			leadership_expire = proposed_expire;
			paxos->leader_id = paxos->self_id;
		} else {
			if (nack_msg && proposed_expire - nack_msg->expire > leader_lease_interval * .05 ) {
				struct paxos_peer *peer = paxos_peer(paxos, nack_msg->peer_id);
				say_debug("%s: nack from peer:%i/%s leader_id:%i", __func__,
					  peer->id, peer->name, nack_msg->leader_id);
				leadership_expire = nack_msg->expire;
				paxos->leader_id = nack_msg->leader_id;
			} else {
				say_debug("%s: no quorum v/q:%i/%i", __func__, votes, quorum);
			}
		}
		[paxos adjust_route];
		iproto_mbox_release(&mbox);
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

#define PAXOS_MSG_CHECK(wbuf, msg, peer)	({			\
	struct netmsg_io *io = container_of(wbuf, struct netmsg_io, wbuf); \
	if ((msg)->version != paxos_default_version) {			\
		say_warn("%s: bad version %i, closing connect from peer %i", \
			 __func__, (msg)->version, (msg)->peer_id);	\
		if (io->fd != -1) [io close];				\
		return;							\
	}								\
	if (!peer) {					\
		say_warn("%s: closing connect from unknown peer %i",	\
			 __func__, (msg)->peer_id);			\
		if (io->fd != -1) [io close];				\
		return;							\
	}								\
	PAXOS_MSG_DROP(&(msg)->header);					\
})

#define RT_SHARD(arg) ({ if (!arg) iproto_raise(ERR_CODE_BAD_CONNECTION, "unkown shard"); \
			 (Paxos *)((struct shard_route *)arg)->shard; })
static void
leader(struct netmsg_head *wbuf, struct iproto *msg, void *arg)
{
	Paxos *paxos = RT_SHARD(arg);
	struct msg_leader *pmsg = (struct msg_leader *)msg;
	struct paxos_peer *peer = paxos_peer(paxos, pmsg->peer_id);
	const char *ret = "accept";
	const ev_tstamp to_expire = leadership_expire - ev_now();

	say_debug("%s: msg:%x", __func__, msg->msg_code);

	PAXOS_MSG_CHECK(wbuf, pmsg, peer);

	say_debug("<LEADER_PROPOSE from %i/%s to_expire:%.2f leader:%i/propos:%i",
		  peer->id, peer->name, to_expire, paxos->leader_id, pmsg->leader_id);

	if (paxos->leader_id == pmsg->leader_id) {
		say_debug("|     proposal matches current leader");
		msg->msg_code = LEADER_ACK;
		if (pmsg->leader_id != paxos->self_id)
			leadership_expire = pmsg->expire;
		if (paxos->leader_id < 0)
			leadership_expire = 0;
	} else if (to_expire < 0) {
		say_debug("|     current leader expired");
		msg->msg_code = LEADER_ACK;
		if (pmsg->leader_id != paxos->self_id) {
			paxos->leader_id = pmsg->leader_id;
			leadership_expire = pmsg->expire;
			[paxos adjust_route];
		}
	} else if (paxos->leader_id == pmsg->peer_id && pmsg->leader_id < 0) {
		ret = "ack giveup";
		msg->msg_code = LEADER_ACK;
		paxos->leader_id = pmsg->leader_id;
		leadership_expire = 0;
	} else {
		ret = "nack";
		msg->msg_code = LEADER_NACK;
		pmsg->leader_id = paxos->leader_id;
		pmsg->expire = leadership_expire;
	}

	pmsg->peer_id = paxos->self_id;
	say_debug("|   -> reply with %s leader:%i expire:%.2f", ret, pmsg->leader_id, pmsg->expire - ev_now());
	net_add_iov_dup(wbuf, pmsg, sizeof(*pmsg));
}


static struct proposal *
find_proposal(Paxos *paxos, i64 scn)
{
	return RB_FIND(ptree, &paxos->proposals, &(struct proposal){ .scn = scn });
}

void
proposal_update_ballot(struct proposal *p, u64 ballot)
{
	say_debug2("%s: SCN:%"PRIi64" ballot:%"PRIx64, __func__, p->scn, ballot);
	assert(p->ballot <= ballot);
	if (p->ballot == ULLONG_MAX)
		assert(ballot == ULLONG_MAX); /* decided proposal is immutable */
	p->ballot = ballot;
}


void
proposal_update_value(struct proposal *p, u32 value_len, const char *value, u16 tag)
{
	say_debug2("%s: SCN:%"PRIi64" tag:%s value_len:%i", __func__,
		   p->scn, xlog_tag_to_a(tag), value_len);
	say_debug3("\tvalue:%s", tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

	assert(tag == 0 || value_len > 0);

	if (p->ballot == ULLONG_MAX) { /* decided proposal is immutable */
		assert(p->tag == tag);
		assert(value_len == p->value_len);
		assert(memcmp(value, p->value, value_len) == 0);
		return;
	}

	if (p->value_len != value_len) {
		assert(value_len > 0); /* value never goes empty */
		if (value_len > p->value_len) {
			if (p->value)
			 	free(p->value);
			p->value = malloc(value_len);
		}
		p->value_len = value_len;
		p->tag = tag;
	}
	/*
	  p->value may be same as valye:
	  catchup() takes inital value as p->value and broadcasts it.
	  broadcast() uses PAXOS_REQ_INTERNAL (which doesn't copy value) to send message to own acceptor
	  acceptor() uses this value in proposal_update_value()

	  what is better: check for equality or use memmove?
	*/
	memmove(p->value, value, value_len);
}

static void
delete_proposal(Paxos *paxos, struct proposal *p)
{
	RB_REMOVE(ptree, &paxos->proposals, p);
	if (p->value)
		free(p->value);
	slab_cache_free(&proposal_cache, p);
}

static void
purge_walled_proposals(struct Paxos *paxos)
{
	struct proposal *p;
	while ((p = RB_MIN(ptree, &paxos->proposals))) {
		if (paxos->max_scn - p->scn < proposal_history_size)
			break;
		if ((p->flags & P_WALED) == 0)
			break;

		delete_proposal(paxos, p);
	}

}
static struct proposal *
create_proposal(Paxos *paxos, i64 scn)
{
	assert(scn > paxos->scn);
	struct proposal *p = slab_cache_alloc(&proposal_cache);
	struct proposal ini = { .scn = scn, .delay = paxos_default_timeout, .tstamp = ev_now() };
	memcpy(p, &ini, sizeof(*p));

	RB_INSERT(ptree, &paxos->proposals, p);
	if (paxos->max_scn < scn)
		paxos->max_scn = scn;

	purge_walled_proposals(paxos);
	return p;
}

struct proposal *
proposal(Paxos *paxos, i64 scn)
{
	assert(scn > 0);
	struct proposal *p = find_proposal(paxos, scn);
	if (p == NULL)
		p = create_proposal(paxos, scn);
	return p;
}


#define nack(paxos, req, msg_ballot) ({			\
	i64 nack_ballot = (req)->p->ballot;		\
	assert(nack_ballot != ULLONG_MAX);						\
	say_info("NACK(%i%s) sync:%i SCN:%"PRIi64" ballot:%"PRIx64" nack_ballot:%"PRIx64, \
		 __LINE__, (msg_ballot & 0xff) == (paxos)->self_id ? "self" : "", \
		 (req)->msg->header.sync, (req)->p->scn, (req)->p->ballot, (nack_ballot)); \
	paxos_respond((paxos), (req), NACK, (nack_ballot));		\
})

static int
submit(Paxos *paxos, const void *data, u32 data_len, i64 scn, u16 tag)
{
	return [paxos write_scn:scn data:data len:data_len tag:tag];
}

static void
decided(Paxos *paxos, struct paxos_request *req)
{
	paxos_respond(paxos, req, DECIDE, req->p->ballot);
}

static void
promise(Paxos *paxos, struct paxos_request *req)
{
	struct proposal *p = req->p;
	const struct msg_paxos *msg = req->msg;
	u64 ballot = p->ballot;

	if (msg->ballot <= p->ballot) {
		nack(paxos, req, msg->ballot);
		return;
	}

	int wal_count = submit(paxos, &msg->ballot, sizeof(msg->ballot), msg->scn, paxos_promise | TAG_SYS);

	/* code below makes no difference between WAL write error and stale PREPARE:
	   in both cases promise we can't send promise */
	if (p->ballot == ULLONG_MAX) {
		decided(paxos, req);
	} else if (msg->ballot <= p->ballot || wal_count != 1) {
		nack(paxos, req, msg->ballot);
	} else {
		assert(p->ballot < msg->ballot);
		ballot = p->ballot; /* concurent update is possible */
		proposal_update_ballot(p, msg->ballot);
		paxos_respond(paxos, req, PROMISE, ballot);
	}
}

static void
accepted(Paxos *paxos, struct paxos_request *req)
{
	const struct msg_paxos *msg = req->msg;
	struct proposal *p = req->p;
	assert(msg->scn == p->scn);
	assert(msg->value_len > 0);

	if (msg->ballot < p->ballot) {
		nack(paxos, req, msg->ballot);
		return;
	}

	struct tbuf *buf = tbuf_alloc(fiber->pool);
	tbuf_append(buf, &msg->ballot, sizeof(msg->ballot));
	tbuf_append(buf, &msg->tag, sizeof(msg->tag));
	tbuf_append(buf, &msg->value_len, sizeof(msg->value_len));
	tbuf_append(buf, req->value, msg->value_len);
	int wal_count = submit(paxos, buf->ptr, tbuf_len(buf), msg->scn, paxos_accept | TAG_SYS);

	if (p->ballot == ULLONG_MAX) {
		decided(paxos, req);
	} else if (msg->ballot < p->ballot || wal_count != 1) {
		nack(paxos, req, msg->ballot);
	} else {
		proposal_update_value(p, msg->value_len, req->value, msg->tag);
		proposal_update_ballot(p, msg->ballot);
		paxos_respond(paxos, req, ACCEPTED, msg->ballot);
	}
}

static u32
prepare(Paxos *paxos, struct iproto_mbox *mbox, struct proposal *p, u64 ballot)
{
	iproto_mbox_init(mbox, fiber->pool);
	u32 msg_id = paxos_broadcast(paxos, mbox, PREPARE, p->scn, ballot, NULL, 0, 0);
	mbox_timedwait(mbox, quorum, p->delay);
	return msg_id;
}

static u32
propose(Paxos *paxos, struct iproto_mbox *mbox,
	u64 ballot, i64 scn, const char *value, u32 value_len, u16 tag)
{
	assert((tag & ~TAG_MASK) != 0);
	assert((tag & TAG_MASK) < paxos_promise || (tag & TAG_MASK) > paxos_nop);
	assert(value_len > 0);

	iproto_mbox_init(mbox, fiber->pool);
	u32 msg_id = paxos_broadcast(paxos, mbox, ACCEPT, scn, ballot, value, value_len, tag);
	mbox_timedwait(mbox, quorum, paxos_default_timeout);
	return msg_id;
}


static void maybe_wake_dumper(Paxos *paxos, struct proposal *p);

void
proposal_mark_applied(Paxos *paxos, struct proposal *p)
{
	assert(p->ballot == ULLONG_MAX);
	assert((p->flags & P_APPLIED) == 0);
	assert(paxos->scn + 1 == p->scn);

	p->flags |= P_APPLIED;
	paxos->scn = p->scn;
	say_debug("%s: new SCN:%"PRIi64, __func__, paxos->scn);
	if (p->waiter)
		fiber_wake(p->waiter, NULL);
}

static void
learn(Paxos *paxos, struct proposal *p)
{
	if (p == NULL && paxos->scn < paxos->max_scn)
		p = proposal(paxos, paxos->scn + 1);

	say_debug("%s: SCN:%"PRIi64 " MaxSCN:%"PRIi64 " from SCN:%"PRIi64, __func__,
		  paxos->scn, paxos->max_scn,
		  p ? p->scn : -1);

	for (; p != NULL; p = RB_NEXT(ptree, &r->proposals, p)) {
		assert(paxos->scn <= paxos->max_scn);

		say_debug2("   proposal flags:%u SCN:%"PRIi64" ballot:%"PRIx64, p->flags, p->scn, p->ballot);
		if (p->scn != paxos->scn + 1)
			return;

		if (p->ballot != ULLONG_MAX)
			return;

		if (p->flags & P_APPLIED)
			return;

		say_debug("%s: SCN:%"PRIi64" ballot:%"PRIx64, __func__, p->scn, p->ballot);
		say_debug2("|  value_len:%i value:%s",
			   p->value_len, tbuf_to_hex(&TBUF(p->value, p->value_len, fiber->pool)));


		@try {
			[[paxos executor] apply:&TBUF(p->value, p->value_len, fiber->pool) tag:p->tag];
			proposal_mark_applied(paxos, p);
		}
		@catch (Error *e) {
			say_warn("aborting txn, [%s reason:\"%s\"] at %s:%d",
				 [[e class] name], e->reason, e->file, e->line);
			[e release];
			break;
		}
	}
}

static void
msg_dump(const char *prefix, const struct paxos_peer *peer, const struct msg_paxos *req)
{
	const char *code = paxos_msg_code[req->header.msg_code];
	switch (req->header.msg_code) {
	case PREPARE:
		say_debug("%s peer:%i/%s sync:%i type:%s SCN:%"PRIi64" ballot:%"PRIx64,
			  prefix, peer->id, peer->name, req->header.sync, code, req->scn, req->ballot);
		break;
	default:
		say_debug("%s peer:%i/%s sync:%i type:%s SCN:%"PRIi64" ballot:%"PRIx64" tag:%s",
			  prefix, peer->id, peer->name, req->header.sync, code, req->scn, req->ballot, xlog_tag_to_a(req->tag));
		say_debug2("|  tag:%s value_len:%i value:%s", xlog_tag_to_a(req->tag), req->value_len,
			   tbuf_to_hex(&TBUF(req->value, req->value_len, fiber->pool)));
	}
}

static void
learner(struct netmsg_head *wbuf, struct iproto *imsg, void *arg)
{
	Paxos *paxos = RT_SHARD(arg);
	struct msg_paxos *msg = (struct msg_paxos *)imsg;
	struct paxos_peer *peer = paxos_peer(paxos, msg->peer_id);
	struct proposal *p;

	PAXOS_MSG_CHECK(wbuf, msg, peer);

	msg_dump("learner: <", peer, msg);

	if (msg->peer_id == paxos->self_id)
		return;

	if (msg->scn <= paxos->scn) {
		/* если у нас такой proposal есть, то сверим его содержимое для надежности */
		p = find_proposal(paxos, msg->scn);
		if (!p)
			return;
	} else {
		p = proposal(paxos, msg->scn);
	}

	assert(msg->scn > paxos->scn || p->ballot == ULLONG_MAX);
	if (p->ballot == ULLONG_MAX) {
		assert(p->tag == msg->tag);
		assert(msg->value_len == p->value_len);
		assert(memcmp(msg->value, p->value, p->value_len) == 0);
		return;
	}

	proposal_update_value(p, msg->value_len, msg->value, msg->tag);
	proposal_update_ballot(p, ULLONG_MAX);

	learn(paxos, p);
	maybe_wake_dumper(paxos, p);
}

static void
acceptor(Paxos *paxos, struct paxos_request *req)
{
	const struct msg_paxos *msg = req->msg;

	if (msg->scn <= paxos->scn) {
		/* the proposal in question was decided too long ago,
		   no further progress is possible */

		struct proposal *min = RB_MIN(ptree, &paxos->proposals);
		if (!min || msg->scn < min->scn) {
			say_error("STALE SCN:%"PRIi64 " minSCN:%"PRIi64, msg->scn, min ? min->scn : -1);
			paxos_respond(paxos, req, STALE, 0);
			return;
		}

		req->p = proposal(paxos, msg->scn);
		assert(req->p->ballot == ULLONG_MAX);

		decided(paxos, req);
		return;
	}

	req->p = proposal(paxos, msg->scn);
	if (req->p->ballot == ULLONG_MAX) {
		decided(paxos, req);
		return;
	}

	switch (msg->header.msg_code) {
	case PREPARE:
		promise(paxos, req);
		break;
	case ACCEPT:
		accepted(paxos, req);
		break;
	default:
		assert(false);
	}
}

static void
iproto_acceptor(struct netmsg_head *wbuf, struct iproto *imsg, void *arg __attribute__((unused)))
{
	struct Paxos *paxos = RT_SHARD(arg);
	struct msg_paxos *msg = (struct msg_paxos *)imsg;
	struct paxos_request req = { .msg = msg, .value = msg->value, .type = PAXOS_REQ_REMOTE, {.wbuf = wbuf} };
	struct paxos_peer *peer = paxos_peer(paxos, msg->peer_id);
	PAXOS_MSG_CHECK(wbuf, msg, peer);

	msg_dump("acceptor: <", peer, msg);
	acceptor(paxos, &req);
}

static u64 next_ballot(Paxos *paxos, u64 min)
{
	/* lower 8 bits is our id (unique in cluster)
	   high bits - counter */

	assert(min != ULLONG_MAX);
	u64 ballot = (min & ~0xff) | (paxos->self_id & 0xff);
	do
		ballot += 0x100;
	while (ballot < min);
	return ballot;
}

static int
run_protocol(Paxos *paxos, struct proposal *p, char *value, u32 value_len, u16 tag)
{
	assert((tag & ~TAG_MASK) != 0);
	assert((tag & TAG_MASK) < paxos_promise || (tag & TAG_MASK) > paxos_nop);
	assert(value_len > 0);

	char *orig_value = NULL;
	u32 orig_value_len = 0;
	u16 orig_tag = 0;

	u64 ballot = 0, nack_ballot = 0;
	int votes;
	struct iproto_mbox mbox = {.sent = 0};

	say_debug("%s: SCN:%"PRIi64, __func__, p->scn);
	say_debug2("|  tag:%s value_len:%u value:%s", xlog_tag_to_a(tag), value_len,
		   tbuf_to_hex(&TBUF(value, value_len, fiber->pool)));

retry:
	if (p->ballot == ULLONG_MAX)
		goto decide;

	ballot = next_ballot(paxos, MAX(ballot, p->ballot));

	u32 msg_id = prepare(paxos, &mbox, p, ballot);
	say_debug("[%i] PREPARE SCN:%"PRIi64" %i replies", msg_id, p->scn, mbox.msg_count);
	struct msg_paxos *req, *max = NULL;
	votes = 0;

	if (mbox.msg_count == 0 ||
	    (mbox.msg_count == 1 && ((struct msg_paxos *)iproto_mbox_peek(&mbox))->peer_id == paxos->self_id))
		p->delay *= 1.25;

	while ((req = (struct msg_paxos *)iproto_mbox_get(&mbox))) {
		assert(req->msg_id == msg_id);
		say_debug("|  %s peer:%s SCN:%"PRIi64" ballot:%"PRIx64" tag:%s value_len:%i",
			  paxos_msg_code[req->header.msg_code], paxos_peer_name(paxos, req->peer_id), req->scn, req->ballot,
			  xlog_tag_to_a(req->tag), req->value_len);

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
			proposal_update_value(p, req->value_len, req->value, req->tag);
			proposal_update_ballot(p, ULLONG_MAX);
			iproto_mbox_release(&mbox);
			goto decide;
		case STALE:
			giveup_leadership(paxos);
			return 0;
		default:
			abort();
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

	if (max && (max->tag != tag || max->value_len != value_len || memcmp(max->value, value, value_len) != 0))
	{
		say_debug("has REMOTE value for SCN:%"PRIi64" tag:%s value_len:%i value:%s",
			  p->scn, xlog_tag_to_a(max->tag), max->value_len,
			  tbuf_to_hex(&TBUF(max->value, max->value_len, fiber->pool)));

		if (orig_value == NULL) {
			orig_value = value;
			orig_value_len = orig_value_len;
			orig_tag = tag;
			value_len = 0; // force copy creation
		}
		if (value_len < max->value_len)
			value = palloc(fiber->pool, max->value_len);
		memcpy(value, max->value, max->value_len);
		value_len = max->value_len;
		tag = max->tag;
	}

	msg_id = propose(paxos, &mbox, ballot, p->scn, value, value_len, tag);
	say_debug("[%i] PROPOSE SCN:%"PRIi64" %i replies", msg_id, p->scn, mbox.msg_count);

	votes = 0;
	while ((req = (struct msg_paxos *)iproto_mbox_get(&mbox))) {
		assert(req->msg_id == msg_id);
		say_debug("|  %s peer:%s SCN:%"PRIi64" ballot:%"PRIx64" value_len:%i",
			  paxos_msg_code[req->header.msg_code], paxos_peer_name(paxos, req->peer_id),
			  req->scn, req->ballot, req->value_len);

		switch (req->header.msg_code) {
		case ACCEPTED:
			votes++;
			break;
		case DECIDE:
			proposal_update_value(p, req->value_len, req->value, req->tag);
			proposal_update_ballot(p, ULLONG_MAX);
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

	paxos_broadcast(paxos, NULL, DECIDE, p->scn, /*ballot*/ ULLONG_MAX, value, value_len, tag);

	/* must update proposal because our own acceptor may fail to do so
	   (for example if WAL write failed) */
	proposal_update_value(p, value_len, value, tag);
	proposal_update_ballot(p, ULLONG_MAX);

	maybe_wake_dumper(paxos, p);

decide:
	if (orig_value) {
		value = orig_value;
		value_len = orig_value_len;
		tag = orig_tag;
	}

	if (p->tag != tag ||
	    p->value_len != value_len ||
	    memcmp(p->value, value, value_len) != 0 ||
	    p->flags & P_APPLIED)
		return 0;

	if (paxos->scn + 1 != p->scn) {
		struct proposal *pp = proposal(paxos, paxos->scn);
		assert((pp->flags & P_APPLIED) == 0);
		pp->waiter = fiber;
		yield();
	}

	return 1;
}


struct wal_msg { TAILQ_ENTRY(wal_msg) link; };
static MBOX(, wal_msg) wal_dumper_mbox = MBOX_INITIALIZER(wal_dumper_mbox);

static void
maybe_wake_dumper(Paxos *paxos, struct proposal *p)
{
	if (p->scn - paxos->scn < 8)
		return;

	if (!paxos->wal_dumper)
		return;

	struct wal_msg *m = palloc(paxos->wal_dumper->pool, sizeof(*m));
	mbox_put(&wal_dumper_mbox, m, link);
}

static void
wal_dumper_fib(va_list ap)
{
	Paxos *paxos = va_arg(ap, Paxos *);
	struct proposal *p = NULL;
	fiber->ushard = paxos->id;
loop:
	mbox_timedwait(&wal_dumper_mbox, 1, 1);
	while (mbox_get(&wal_dumper_mbox, link)); /* flush mbox */
	fiber_gc(); /* NB: put comment */

	p = RB_MIN(ptree, &paxos->proposals);
	// say_debug("wal_dump %s", scn_info(paxos));

	while (p && p->flags & P_WALED) {
		say_debug2("|  % 8"PRIi64" CLOSED", p->scn);
		p = RB_NEXT(ptree, &r->proposals, p);
	}

	while (p && p->scn <= paxos->scn) {
		assert(p->ballot == ULLONG_MAX);
		assert(p->flags & P_APPLIED);
		say_debug2("|  % 8"PRIi64" APPLIED", p->scn);

		/* TODO: batch write */
		if (submit(paxos, p->value, p->value_len, p->scn, p->tag) != 1) {
			say_error("unable to writer wal row");
			goto loop;
		}

		p->flags |= P_WALED;
		p = RB_NEXT(ptree, &r->proposals, p);
	}
	if (!paxos_leader(paxos)) {
		bool delay_too_big = p && ev_now() - p->tstamp > 1;
		bool too_many_not_applied = paxos->max_scn - paxos->scn > cfg.wal_writer_inbox_size * 1.1;
		if (delay_too_big || too_many_not_applied) {
			/* if we run protocol on recent proposals we
			   will interfere with current leader */
			for (;;) {
				struct proposal *next = RB_NEXT(ptree, &r->proposals, p);
				if (!next || ev_now() - next->tstamp > 0.2)
					break;
				p = next;
			}
			catchup(paxos, p->scn);
		}
	}

	goto loop;
}


static void
catchup(Paxos *paxos, i64 upto_scn)
{
	say_debug("%s: SCN:%"PRIi64 " upto_scn:%"PRIi64, __func__, paxos->scn, upto_scn);

	for (i64 i = paxos->scn + 1; i <= upto_scn; i++) {
		struct proposal *p = proposal(paxos, i);
		say_debug("|	SCN:%"PRIi64" ballot:%"PRIx64, p->scn, p->ballot);
		if (p->ballot == ULLONG_MAX)
			continue;

		say_debug("%s: SCN:%"PRIi64, __func__, p->scn);
		assert(p->ballot != ULLONG_MAX);

		if (p->value_len || p->tag)
			run_protocol(paxos, p, (void *)p->value, p->value_len, TAG_WAL | p->tag);
		else
			run_protocol(paxos, p, "\0\0", 2, TAG_WAL | nop);
		learn(paxos, p);
	}
	learn(paxos, NULL);
	assert(paxos->scn >= upto_scn);
}

void
paxos_stat(va_list ap)
{
	Paxos *paxos = va_arg(ap, Paxos *);
loop:
	say_info("shard:%i %s leader:%i %s",
		 paxos->id,
		 scn_info(paxos), paxos->leader_id,
		 paxos_leader(paxos) ? "leader" : "");

	fiber_sleep(50);
	goto loop;
}

static void
learn_wal(Paxos *paxos, id<XLogPullerAsync> puller)
{
	struct row_v12 *row;
	struct proposal *p = NULL;

	[puller recv_row];
	while ((row = [puller fetch_row])) {
		if (row->scn <= paxos->scn)
			continue;
		if ((row->tag & ~TAG_MASK) != TAG_WAL)
			continue;

		p = proposal(paxos, row->scn);
		if (p->flags & P_APPLIED) {
			assert(p->tag == row->tag);
			assert(row->len == p->value_len);
			assert(memcmp(row->data, p->value, row->len) == 0);
			p = NULL;
		} else {
			proposal_update_value(p, row->len, (const char *)row->data, row->tag);
			proposal_update_ballot(p, ULLONG_MAX);
		}
	}
	if (p)
		learn(paxos, NULL);
}

static void
learner_puller(va_list ap)
{
	Paxos *paxos = va_arg(ap, Paxos *);
	int i = va_arg(ap, int);

	if (paxos->self_id == i)
		return;

	XLogPuller *puller = [[XLogPuller alloc] init];
again:
	fiber_gc();
	fiber_sleep(0.5);

	@try {
		struct paxos_peer *peer = paxos_peer(paxos, i);
		assert(peer != NULL);
		[puller feeder_param:&peer->feeder];
		if ([puller handshake:paxos->scn] < 0)
			goto again;

		for (;;)
			learn_wal(paxos, puller);
	}
	@catch (Error *e) {
		say_warn("puller failed, [%s reason:\"%s\"] at %s:%d",
			 [[e class] name], e->reason, e->file, e->line);
		[e release];
	}
	@finally {
		[puller close];
	}
	goto again;
}


@implementation Paxos

- (id)
init_id:(int)shard_id scn:(i64)scn_ sop:(const struct shard_op *)sop
{
	[super init_id:shard_id scn:scn_ sop:sop];
	if (cfg.local_hot_standby)
		panic("wal_hot_standby is incompatible with paxos");

	SLIST_INIT(&group);
	RB_INIT(&proposals);

	self_id = leader_id = -1;
	say_info("configuring paxos peers");


	for (int i = 0; ; i++)
	{
		if (peer[i] == NULL)
			break;

		for (int j = 0; ; j++) {
			struct octopus_cfg_peer *c;
			if ((c = cfg.peer[j]) == NULL)
				break;

			if (strcmp(peer[i], c->name) != 0)
				continue;

			if (paxos_peer(self, c->id) != NULL)
				panic("paxos peer %s already exists", c->name);

			say_info("  %s -> %s", c->name, c->addr);

			if (strcmp(cfg.hostname, c->name) == 0) {
				self_id = c->id;
				continue;
			}

			struct paxos_peer *p = make_paxos_peer(c->id, c->name);
			if (!p)
				panic("bad addr %s", c->addr);

			SLIST_INSERT_HEAD(&group, p, link);
			SLIST_INSERT_HEAD(&paxos_remotes, p->egress, link);
		}
	}
	assert(self_id >= 0);
	fiber_create("paxos/stat", paxos_stat, self);
	return self;
}

- (void)
load_from_remote
{
	XLogRemoteReader *remote_reader = [[XLogRemoteReader alloc] init_shard:self];
	struct paxos_peer *p;
	SLIST_FOREACH(p, &group, link)
		[remote_reader load_from_remote:&p->feeder];
	[remote_reader free];
}

- (void)
start
{
	/* peer id starts from 1 */
	for (int i = 1; i <= 3; i++)
		fiber_create("paxos/puller", learner_puller, self, i);

	fiber_create("paxos/elect", paxos_elect, self);
	wal_dumper = fiber_create("paxos/wal_dump", wal_dumper_fib, self);
	[executor wal_final_row]; // FIXME: может быть надо как нибудь по другому сигнализировать executor-у что пора начинать?
}

- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	static unsigned count;
	static struct msg_void_ptr msg;
	if (++count % 32 == 0 && msg.link.tqe_prev == NULL)
	 	mbox_put(&recovery->run_crc_mbox, &msg, link);

	assert(recovery->writer != nil);
	struct proposal *p = proposal(self, ++max_scn);
	if (run_protocol(self, p, (char*)data, len, tag)) {
		proposal_mark_applied(self, p);
		return 1;
	}
	return 0;

}

- (int)
write_scn:(i64)scn_ data:(const void *)data len:(u32)len tag:(u16)tag
{
	struct row_v12 row = { .scn = scn_,
			       .tag = tag };
	row.shard_id = self->id;
	struct wal_pack pack;
	wal_pack_prepare(recovery->writer, &pack);
	wal_pack_append_row(&pack, &row);
	wal_pack_append_data(&pack, &row, data, len);
	struct wal_reply *reply = [recovery->writer wal_pack_submit];

	if (reply->row_count) {
		run_crc_scn = reply->scn;
		for (int i = 0; i < reply->crc_count; i++) {
			run_crc_log = reply->row_crc[i].value;
			run_crc_record(&run_crc_state, reply->row_crc[i]);
		}
	}
	return reply->row_count;
}

- (void)
recover_row_sys:(const struct row_v12 *)r
{
	say_debug2("%s: lsn:%"PRIi64" SCN:%"PRIi64" tag:%s", __func__,
		   r->lsn, r->scn, xlog_tag_to_a(r->tag));

	int tag = r->tag & TAG_MASK;
	int tag_type = r->tag & ~TAG_MASK;
	struct tbuf row_data = TBUF(r->data, r->len, NULL);

	u64 ballot;

	if (tag_type != TAG_SYS) {
		struct proposal *p = proposal(self, r->scn);
		proposal_update_value(p, r->len, (const char *)r->data, r->tag);
		proposal_update_ballot(p, ULLONG_MAX);
		p->flags |= P_WALED;
		proposal_mark_applied(self, p);
		return;
	}

	switch (tag) {
	case snap_initial:
		assert(snap_lsn == 0 || r->lsn == snap_lsn);
		if (r->len == sizeof(u32) * 3) { /* not a dummy row */
			(void)read_u32(&row_data);
			run_crc_log = read_u32(&row_data);
			(void)read_u32(&row_data); /* ignore run_crc_mod */
		}
		scn = max_scn = r->scn;
		// say_debug("%s: run_crc_log: 0x%x", __func__, run_crc_log);
		break;
	case snap_final:
		break;
	case run_crc:
		if (cfg.ignore_run_crc)
			break;

		if (r->len != sizeof(i64) + sizeof(u32) * 2)
			break;


		struct proposal *p = proposal(self, r->scn);
		proposal_update_value(p, r->len, (const char *)r->data, r->tag);
		proposal_update_ballot(p, ULLONG_MAX);
		p->flags |= P_WALED;
		proposal_mark_applied(self, p);

		run_crc_verify(&run_crc_state, &row_data);
		break;

	case paxos_promise:
	case paxos_nop:
		ballot = read_u64(&row_data);
		p = proposal(self, r->scn);
		/* there is no locking on proposalsm wherefore following possible:
		   two PREPARE with ballots 3 and 2 comes in a row.
		   becase proposals ballot updated _after_ WAL write
		   second PREPARE with ballot=2 will be exectued and WALed too
		 */

		if (p->ballot < ballot)
			proposal_update_ballot(p, ballot);
		return;

	case paxos_accept:
		ballot = read_u64(&row_data);
		u16 tag = read_u16(&row_data);
		u32 value_len = read_u32(&row_data);
		const char *value = read_bytes(&row_data, value_len);
		p = proposal(self, r->scn);
		if (p->ballot <= ballot) {
			proposal_update_value(p, value_len, value, tag);
			proposal_update_ballot(p, ballot);
		}
		return;
	}
}

- (void)
recover_row:(struct row_v12 *)r
{
	// calculate run_crc _before_ calling executor: executor may change row
	if (scn_changer(r->tag))
		run_crc_calc(&run_crc_log, r->tag, r->data, r->len);

	if ((r->tag & ~TAG_MASK) != TAG_SYS)
		[executor apply:&TBUF(r->data, r->len, fiber->pool) tag:r->tag];
	else
		[self recover_row_sys:r];

	if (scn_changer(r->tag)) {
		run_crc_record(&run_crc_state, (struct run_crc_hist){ .scn = r->scn, .value = run_crc_log });
		scn = r->scn;
	}
}

- (int)
submit_run_crc
{
	/* history overflow */
	if (max_scn - scn > nelem(run_crc_state.hist))
		return -1;

	if (run_crc_scn == 0)
		return -1;

	struct tbuf *b = tbuf_alloc(fiber->pool);
	tbuf_append(b, &run_crc_scn, sizeof(scn));
	tbuf_append(b, &run_crc_log, sizeof(run_crc_log));
	typeof(run_crc_log) run_crc_mod = 0;
	tbuf_append(b, &run_crc_mod, sizeof(run_crc_mod));

	return [self submit:b->ptr len:tbuf_len(b) tag:run_crc|TAG_SYS];
}


- (bool)
is_replica
{
	return recovery->writer == nil || leader_id != self_id;
}

- (void)
adjust_route
{
	static int prev_leader = -255;
	say_info("%s leader:%i %s",
		 scn_info(self), leader_id,
		 paxos_leader(self) ? "leader" : "");

	if (prev_leader == leader_id)
		return;

	if (leader_id < 0) {
		say_info("leader unknown, %i -> %i", prev_leader, leader_id);
		update_rt(self->id, SHARD_MODE_LOADING, self, NULL);
		[self status_update:"paxos/slave"];
	} else if (!paxos_leader(self)) {
		struct paxos_peer *leader = paxos_peer(self, leader_id);
		update_rt(self->id, SHARD_MODE_PARTIAL_PROXY, self, leader->name);
		[self status_update:"paxos/slave"];
		say_info("leader is %s, %i -> %i", leader->name, prev_leader, leader->id);
	} else if (paxos_leader(self)) {
		say_info("I am leader, %i -> %i", prev_leader, leader_id);
		catchup(self, max_scn);
		if (scn < max_scn) {
			say_warn("leader catchup FAILED SCN:%"PRIi64" MaxSCN:%"PRIi64,
				 scn, max_scn);
			giveup_leadership(self);
			title("paxos_catchup_fail");
			return;
		}
		update_rt(self->id, SHARD_MODE_LOCAL, self, NULL);
		[self status_update:"paxos/leader"];
	}
	prev_leader = leader_id;
}

+ (void)
service:(struct iproto_service *)s
{
	service_register_iproto(s, LEADER_PROPOSE, leader, 0);
	service_register_iproto(s, PREPARE, iproto_acceptor, 0);
	service_register_iproto(s, ACCEPT, iproto_acceptor, 0);
	service_register_iproto(s, DECIDE, learner, 0);
}


@end

register_source();

void __attribute__((constructor))
paxos_ctor()
{
	paxos_pool = palloc_create_pool((struct palloc_config){.name = "paxos"});
	slab_cache_init(&proposal_cache, sizeof(struct proposal), SLAB_GROW, "paxos/proposal");
}
