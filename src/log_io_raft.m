/*
 * Copyright (C) 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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
#import <raft.h>
#import <iproto.h>
#import <mbox.h>
#import <shard.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

@interface Raft (xxx)
- (int)wal_voted_for:(uint8_t)peer_id;
- (int)wal_le:(const struct log_entry *)le;
- (void)apply_data:(const void *)data len:(int)len tag:(u16)tag;
@end

#ifdef RANDOM_DROP
#define MSG_DROP(h)						\
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
#define MSG_DROP(h) (void)h
#endif

#define MSG_CHECK(self, wbuf, type, msg)	({				\
	type *__msg = container_of((msg), type, iproto); \
	struct netmsg_io *io = container_of((wbuf), struct netmsg_io, wbuf); \
	if (__msg->version != proto_version) {				\
		say_warn("%s: bad version %i, closing connect from peer %i", \
			 __func__, __msg->version, __msg->peer_id);	\
		[io close];						\
		return;							\
	}								\
	if (__msg->peer_id >= 5 || *(self)->peer[__msg->peer_id] == 0)	\
	{								\
		say_warn("%s: closing connect from unknown peer %i",	\
			 __func__, __msg->peer_id);			\
		[io close];						\
		return;							\
	}								\
	MSG_DROP(msg);							\
	__msg;								\
})

const char *raft_msg_code[] = ENUM_STR_INITIALIZER(RAFT_CODE);
const int quorum = 2; /* FIXME: hardcoded */

static u16 proto_version;


struct msg_request_vote {
	struct iproto iproto;
	u16 version;
	u16 peer_id;
	i64 term;
	i64 last_log_index;
	i64 last_log_term;
}  __attribute__((packed));

struct msg_append_entries {
	struct iproto iproto;
	u16 version;
	u16 peer_id;
	i64 term;
	i64 prev_log_index;
	i64 prev_log_term;
	i64 leader_commit;
	i64 entry_term;
	u16 tag;
	u32 len;
	char val[];
}  __attribute__((packed));

struct msg_reply {
	struct iproto_retcode iproto;
	u16 version;
	u16 peer_id;
	i64 term;
	u8 result;
}  __attribute__((packed));

union msg {
	struct iproto iproto;
	struct msg_request_vote request_vote;
	struct msg_append_entries append_entries;
	struct msg_reply reply;
};


enum log_entry_state { WAL_APPEND = 1 << 0, /* writen raft_append */
		       WAL_COMMIT = 1 << 1, /* writen raft_commit */
		       DELETED    = 1 << 2,
		       REPLICATED = 1 << 3 };

struct log_entry {
	TAILQ_ENTRY(log_entry) link;
	i64 scn;
	i64 term;
	i32 len;
	u16 tag;
	int state;
	struct Fiber *worker;
	char data[];
};


static const char *
scn_info(Raft *self)
{
	static char buf[64];
	const struct log_entry *last = TAILQ_LAST(&self->log, log_tailq);
	snprintf(buf, sizeof(buf), "SCN:%"PRIi64" lastSCN:%"PRIi64,
		 self->scn, last->scn);
	return buf;
}


const ev_tstamp election_timeout = 1.5;

static void
update_deadline(Raft *self)
{
	static unsigned int seed;
	if (seed == 0)
		seed = getpid() ^ time(NULL);
	double timeout = election_timeout + election_timeout * rand_r(&seed) / RAND_MAX;
	self->election_deadline = ev_now() + timeout;
}

static int
check_term(Raft *self, i64 term)
{
	if (term < self->term)
		return -1;
	if (term == self->term)
		return 0;
	self->term = term;
	self->role = FOLLOWER;
	self->voted_for = -1;
	self->leader_id = -1;

	return 1;
}

static struct msg_reply *
msg_reply(struct iproto *_msg)
{
	struct iproto_retcode *msg = (struct iproto_retcode *)_msg;
	if (msg->ret_code != 0) {
		say_warn("%s: error from peer: sync:%x 0x%x %.*s", __func__, msg->sync,
			 msg->ret_code, msg->data_len - 4, msg->data);
		return NULL;
	}
	return container_of(msg, struct msg_reply, iproto);
}

static struct msg_reply *
raft_mbox_get(Raft *self, struct iproto_mbox *mbox)
{
	struct iproto *msg;
	while ((msg = iproto_mbox_get(mbox))) {
		struct msg_reply *reply = msg_reply(msg);
		if (reply) {
			assert(reply->peer_id != self->peer_id);
			return reply;
		}
	}
	return NULL;
}

static void
reply(Raft *self, struct netmsg_head *h, const struct iproto *msg, u8 result)
{
	struct msg_reply *reply = palloc(h->ctx->pool, sizeof(*reply));
	net_add_iov(h, reply, sizeof(*reply));
	*reply = (struct msg_reply){
		.iproto = { .shard_id = msg->shard_id,
			    .msg_code = msg->msg_code,
			    .data_len = sizeof(*reply) - sizeof(struct iproto),
			    .sync = msg->sync },
		.version = proto_version,
		.peer_id = self->peer_id,
		.result = result,
		.term = self->term
	};
}

#define APPEND_ENTRIES_INITIALIZER(msg, self, le) {			\
	.iproto = { .msg_code = RAFT_APPEND_ENTRIES,			\
		    .shard_id = (self)->id,				\
		    .data_len = sizeof(msg) - sizeof(struct iproto) },	\
	.version = proto_version,					\
	.peer_id = (self)->peer_id,					\
	.term = (self)->term,						\
	.prev_log_index = TAILQ_PREV((le), log_tailq, link)->scn,	\
	.prev_log_term = TAILQ_PREV((le), log_tailq, link)->term,	\
	.leader_commit = (self)->commited->scn,				\
	.entry_term = (le)->term,					\
	.tag = (le)->tag,						\
	.len = (le)->len						\
}

static int
send_log_entry(Raft *self, int peer_id, struct log_entry *le)
{
	struct log_entry *prev = TAILQ_PREV(le, log_tailq, link);
	if (self->role != LEADER) /* only leader can send entries */
		return -3;
	if (!prev)
		return -2;
	say_debug("%s: >> term:%"PRIi64" SCN:%"PRIi64" prev_log_scn:%"PRIi64" prev_log_term:%"PRIi64,
		   __func__, self->term, self->scn, prev->scn, prev->term);

	struct msg_append_entries msg = APPEND_ENTRIES_INITIALIZER(msg, self, le);
	struct iovec iov = { .iov_base = le->data, .iov_len = le->len };
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	iproto_mbox_send(&mbox, self->egress[peer_id], &msg.iproto, &iov, 1);
	mbox_timedwait(&mbox, 1, (election_timeout * 3));
	struct msg_reply *reply = raft_mbox_get(self, &mbox);
	int result = reply ? reply->result : -1;
	iproto_mbox_release(&mbox);
	return result;
}

static void
request_pull_entries(Raft *self, int peer_id)
{
	struct iproto msg = { .msg_code = RAFT_PULL_ENTRIES,
			      .shard_id = self->id,
			      .data_len = 0 };
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	iproto_mbox_send(&mbox, self->egress[peer_id], &msg, NULL, 0);
	mbox_wait(&mbox);
	iproto_mbox_release(&mbox);
}

static void
catchup(va_list ap)
{
	Raft *self = va_arg(ap, Raft *);
	int peer_id = va_arg(ap, int);

	self->catchup[peer_id] = fiber;
	struct log_entry *le;

again:
	le = TAILQ_LAST(&self->log, log_tailq);
	do {
		int ret = send_log_entry(self, peer_id, le);
		if (ret == -1) /* timeout */
			continue;
		else if (ret == 1)
			break;
		else if (ret == -2) {
			request_pull_entries(self, peer_id);
			goto again;
		} else if (ret == -3)
			goto exit;
	} while ((le = TAILQ_PREV(le, log_tailq, link)));

	le = TAILQ_NEXT(le, link);
	while (le) {
		int ret = send_log_entry(self, peer_id, le);
		if (ret == -1)
			continue;
		if (ret != 1)
			break;
		le = TAILQ_NEXT(le, link);
	}

exit:
	self->catchup[peer_id] = nil;
}

static int
wait_for_replies(Raft *self, struct iproto_mbox *mbox, ev_tstamp delay)
{
	struct mbox_consumer consumer = { .fiber = fiber,
					  .msg_count = 1 }; /* every put wakes */
	ev_timer w;
	if (delay) {
		w = (ev_timer){ .coro = 1 };
		ev_timer_init(&w, (void *)fiber, delay, 0);
		ev_timer_start(&w);
	}

	struct iproto *msg;
	struct msg_reply *reply;
	int vote_count = 0, reply_count = 0, quorum = (1 + self->remote_count) / 2;

	LIST_INSERT_HEAD(&(mbox)->consumer_list, &consumer, conslink);
	do {
		if (!TAILQ_FIRST(&mbox->msg_list)) {
			void *y = yield();
			if (y == &w || y == NULL) { /* interal or external timeout */
				if (y == NULL)
					vote_count = -1;
				fiber_cancel_wake(fiber);
				break;
			}
			assert(TAILQ_FIRST(&mbox->msg_list));
		}

		reply_count++; /* including errors */
		msg = iproto_mbox_get(mbox);
		reply = msg ? msg_reply(msg) : NULL;
		if (reply == NULL) /* error */
			continue;

		assert(reply->peer_id != self->peer_id);
		if (check_term(self, reply->term) > 0) { /* new leader exists */
			vote_count = -1;
			break;
		}

		vote_count += !!reply->result;

		if (msg->msg_code == RAFT_APPEND_ENTRIES &&
		    !reply->result &&
		    !self->catchup[reply->peer_id])
			fiber_create("raft/catchup", catchup, self, (int)reply->peer_id);

	} while (TAILQ_FIRST(&mbox->msg_list) ||
		 (vote_count < quorum && reply_count < self->remote_count));
	say_debug2("|\t%i replies, %i votes", reply_count, vote_count);

	LIST_REMOVE(&consumer, conslink);
	if (delay)
		ev_timer_stop(&w);
	return vote_count;
}


static int
request_vote(Raft *self)
{
	struct log_entry *last = TAILQ_LAST(&self->log, log_tailq);
	assert(last->scn > 0);
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	struct msg_request_vote msg = {
		.iproto = { .msg_code = RAFT_REQUEST_VOTE,
			    .shard_id = self->id,
			    .data_len = sizeof(msg) - sizeof(struct iproto) },
		.version = proto_version,
		.peer_id = self->peer_id,
		.term = self->term,
		.last_log_index = last->scn,
		.last_log_term = last->term
	};
	say_debug2("%s: >> term:%"PRIi64" last_scn:%"PRIi64" last_term:%"PRIi64,
		   __func__, self->term, last->scn, last->term);
	iproto_mbox_broadcast(&mbox, &self->remotes, &msg.iproto, NULL, 0);
	mbox_timedwait(&mbox, quorum - 1, self->election_deadline - ev_now());
	int votes = wait_for_replies(self, &mbox, self->election_deadline - ev_now());
	iproto_mbox_release(&mbox);
	return votes;
}

static void
send_keepalive(Raft *self)
{
	struct log_entry *last = TAILQ_LAST(&self->log, log_tailq);
	assert(last->scn > 0);

	struct msg_append_entries msg = {
		.iproto = { .msg_code = RAFT_APPEND_ENTRIES,
			    .shard_id = self->id,
			    .data_len = sizeof(msg) - sizeof(struct iproto) },
		.version = proto_version,
		.peer_id = self->peer_id,
		.term = self->term,
		.prev_log_index = last->scn,
		.prev_log_term = last->term,
		.leader_commit = self->scn, /* self->scn === self->commited->scn */
		.tag = 0, // no op
	};
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	iproto_mbox_broadcast(&mbox, &self->remotes, &msg.iproto, NULL, 0);
	fiber_sleep(election_timeout / 3);
	wait_for_replies(self, &mbox, election_timeout / 3); /* check term on replies */
	iproto_mbox_release(&mbox);
}

static void commit_nop(va_list ap);

static void
elect(va_list ap)
{
	Raft *self = va_arg(ap, Raft *);
	assert(self->elector == NULL);
	self->elector = fiber;
	fiber->ushard = self->id;

	for (;;) {
		update_deadline(self);
		switch (self->role) {
		case FOLLOWER:
			do fiber_sleep(self->election_deadline - ev_now());
			while (self->election_deadline > ev_now()); /* heartbeat from leader during sleep */
			self->role = CANDIDATE;
                        // fallthrough
		case CANDIDATE:
			update_deadline(self);
			break;
		case LEADER:
			send_keepalive(self);
			continue;
		}

		if ([self wal_voted_for:self->peer_id] != 1) {
			self->role = FOLLOWER; /* can't write to wal: step down */
			continue;
		}

		self->term++;
		int votes = request_vote(self);
		if (self->role != CANDIDATE) /* some other member won election */
			continue;
		if (votes >= quorum - 1) { // -1 because we don't message ourselfs
			say_debug2("%s: quorum reached v/q:%i/%i", __func__, votes, quorum);
			self->leader_id = self->peer_id;
			self->role = LEADER;
			fiber_create("raft/commit_nop", commit_nop, self);
			[self adjust_route];
		} else {
			say_debug2("%s: no quorum v/q:%i/%i", __func__, votes, quorum);
			fiber_sleep(self->election_deadline - ev_now()); /* wait till end of deadline */
		}
		prelease(fiber->pool);
	}
}


static Raft *
RT_SHARD(struct iproto *imsg)
{
	struct shard_route *route = shard_rt + imsg->shard_id;
	if (route->shard == nil) /* IPROTO_LOCAL callbacks must check for shard existance */
		iproto_raise(ERR_CODE_BAD_CONNECTION, "no such shard");
	if ([(id)route->shard class] != [Raft class])
		iproto_raise(ERR_CODE_BAD_CONNECTION, "not a Raft shard");
	return (Raft *)route->shard;
}

static void
request_vote_cb(struct netmsg_head *wbuf, struct iproto *msg)
{
	struct Raft *self = RT_SHARD(msg);
	struct msg_request_vote *request = MSG_CHECK(self, wbuf, struct msg_request_vote, msg);
	int result = 0;

	say_debug2("%s: << term:%"PRIi64" last_log_scn:%"PRIi64" last_log_term:%"PRIi64,
		   __func__, request->term, request->last_log_index, request->last_log_term);
	say_debug3("|\tpeer:%s op:0x%x sync:%u req_term:%"PRIi64,
		   net_fd_name(container_of(wbuf, struct netmsg_io, wbuf)->fd),
		   msg->msg_code, msg->sync, request->term);

	if (check_term(self, request->term) <= 0)	/* request from stale leader */
		goto reply;

	update_deadline(self);

	struct log_entry *last = TAILQ_LAST(&self->log, log_tailq);

	bool log_ok = request->last_log_term > last->term ||
		      (request->last_log_term == last->term &&
		       request->last_log_index >= last->scn);

	say_debug2("|\tlast_scn:%"PRIi64" last_term:%"PRIi64" log_ok:%i, voted_for:%i, req_peer_id:%i", last->scn, last->term, log_ok, self->voted_for, request->peer_id);

	if (log_ok &&
	    (self->voted_for < 0 || self->voted_for == request->peer_id) &&
	    [self wal_voted_for:request->peer_id] == 1) {
		result = 1;
		self->voted_for = request->peer_id;
	}
reply:
	say_debug2("|\treply with %s, reply_term:%"PRIi64, result ? "yes": "no", self->term);
	say_debug3("|\tpeer:%s op:0x%x sync:%u",
		   net_fd_name(container_of(wbuf, struct netmsg_io, wbuf)->fd),
		   msg->msg_code, msg->sync);
	reply(self, wbuf, msg, result);
}


static struct log_entry *
log_entry_alloc(Raft *self, i64 scn, i64 term, const void *data, int len, u16 tag)
{
	if (scn == INT64_MAX) {
		struct log_entry *last = TAILQ_LAST(&self->log, log_tailq);
		scn = last->scn + 1;
	}
	assert(scn > 0 && term >= 0);
	struct log_entry *le = salloc(sizeof(*le) + len);
	*le = (struct log_entry) {
		.scn = scn,
		.term = term,
		.tag = tag,
		.len = len
	};
	if (len)
		memcpy(le->data, data, len);
	say_debug2("%s: SCN:%"PRIi64" term:%"PRIi64, __func__, scn, term);
	TAILQ_INSERT_TAIL(&self->log, le, link);
	return le;
}

static void log_entry_discard(Raft *self, struct log_entry *le);

static void
log_truncate(Raft *self, struct log_entry *needle)
{
	struct log_entry *le, *tmp;
	TAILQ_FOREACH_REVERSE_SAFE(le, &self->log, log_tailq, link, tmp) {
		assert(le->scn > self->scn); /* транкейтить можно только незакоммиченный хвост */

		/* если мы были лидером, а затем стали репликой, то у нас
		   могут остаться "отставшие" фиберы которые либо ждали ответа от
		   WAL writer либо кворума на mbox.
		*/

		say_warn("%s: remove SCN:%"PRIi64, __func__, le->scn);
		le->state |= DELETED;
		if (le->worker) {
			assert(!self->loading); /* нет никаких worker-ов в момент начальной заргузки */
			say_debug("%s: TRUNCATE RESUME SCN:%"PRIi64, __func__, le->scn);
			resume(le->worker, NULL); /* synthetic timeout */
		} else {
			log_entry_discard(self, le);
		}
		if (le == needle)
			break;
	}
}

static struct log_entry *
log_entry_append(Raft *self, i64 scn, i64 term, const void *data, int len, u16 tag)
{
	say_debug2("%s: SCN:%"PRIi64" term:%"PRIi64" tag:%s len:%i", __func__,
		   scn, term, xlog_tag_to_a(tag), len);
	assert(TAILQ_FIRST(&self->log)->scn < scn);

	struct log_entry *le = TAILQ_LAST(&self->log, log_tailq);
	if (le->scn + 1 != scn) {
		while (le->scn > scn)
			le = TAILQ_PREV(le, log_tailq, link); /* мы всегда найдем le, т.к. log_match() проверяет наличие этой записи*/

		assert(le->scn == scn); // мы всегда должны ее найти
		if (le->term == term) {
			/* если терм совпадает, то это должна быть та же самая запись */
			assert(le->tag == tag &&
			       le->len == len &&
			       memcmp(le->data, data, len) == 0);
			/* т.к. запись таже самая хвост можно не транкейтить, он
			   может быть валидными */
			return le;
		}
		log_truncate(self, le);
		le = TAILQ_LAST(&self->log, log_tailq);
		/* FIXME: если это была единственная запись в self->log,
		   то le станет равен NULL, а assert() ниже упадет по SEGV */
		assert(le);
	}
	assert(le->scn + 1 == scn);
	return log_entry_alloc(self, scn, term, data, len, tag);
}

static void
log_entry_discard(Raft *self, struct log_entry *le)
{
	/* если на нашей записи случится io error , то и все
	   последующие WAL запросы в этой WAL эпохе будут неудачными.
	   Т.к. WAL writer откатывает запросы в обратном порядке,
	   то le все также должна остаться самой последней.
	   Аналогично в случае truncate: он это делает начиная
	   с последней le */
	assert(le == TAILQ_LAST(&self->log, log_tailq));

	/* это либо новосозданная запись, которая не попала в лог,
	   либо это запись, которую затранкейтили */
	assert(le->state == 0 || le->state & DELETED);

	TAILQ_REMOVE(&self->log, le, link);
	sfree(le);
}

static int
log_entry_persist(Raft *self, struct log_entry *le)
{
	assert (le->state == 0);
	assert(le == TAILQ_LAST(&self->log, log_tailq));

	assert(fiber->wake_link.tqe_prev == NULL);
	le->worker = fiber;
	int rc = [self wal_le:le];
	le->worker = NULL;
	if (rc == 1) {
		assert(le->scn < self->last_wal_append ||
		       le->scn == self->last_wal_append + 1);
		self->last_wal_append = le->scn;
		le->state |= WAL_APPEND;
		return 1;
	} else {
		assert(rc == 0 || rc == -1);
		/* 0: io error, -1: truncate (resume with NULL) */

		if (rc == 0) {
			say_warn("can't persist");
			self->role = FOLLOWER;
		}
		return 0;
	}
}


static void
log_entry_mark_commited(Raft *self, struct log_entry *le)
{
	self->commited = le; /* commited -> применен к хипу */
	self->scn = le->scn;

	if (self->scn % 32 == 0 && self->wal_dumper_idle)
		fiber_wake(self->wal_dumper, NULL);
}

static struct log_entry *
log_commit(Raft *self, i64 scn)
{
	if (self->scn >= scn)
		return NULL;

	self->leader_commit = scn;
	struct log_entry *le;

	le = TAILQ_NEXT(self->commited, link);
	while (le && le->state & (WAL_APPEND|REPLICATED) && le->scn <= scn) {
		assert(le->scn == self->commited->scn + 1);
		if (le->worker) {
			say_debug("%s: COMMIT RESUME SCN:%"PRIi64, __func__, le->scn);
			resume(le->worker, NULL); /* synthetic timeout */
		} else {
			[self apply_data:le->data len:le->len tag:le->tag];
			log_entry_mark_commited(self, le);
		}
		le = TAILQ_NEXT(self->commited, link);
	}

	return self->commited->scn == scn ? self->commited : NULL;
}

static int
log_match(struct Raft *self, const struct msg_append_entries *append)
{
	struct log_entry *prev = TAILQ_LAST(&self->log, log_tailq);
	while (prev && prev->scn > append->prev_log_index)
		prev = TAILQ_PREV(prev, log_tailq, link);

	if (prev && prev->scn == append->prev_log_index) {
		// FIXME: INDENT
		return prev->term == append->prev_log_term;
	}
	return -1;
}

static void
append_entries_cb(struct netmsg_head *wbuf, struct iproto *msg)
{
	struct Raft *self = RT_SHARD(msg);
	struct msg_append_entries *append = MSG_CHECK(self, wbuf, struct msg_append_entries, msg);
	int result = 0;
	int is_keepalive = append->tag == 0;

	if (!is_keepalive) {
		say_debug2("%s: << term:%"PRIi64" entry_term:%"PRIi64
			   " prev_log_scn:%"PRIi64" prev_log_term:%"PRIi64"%s",
			   __func__, append->term, append->entry_term,
			   append->prev_log_index, append->prev_log_term,
			   append->tag == 0 ? " keepalive" : "");
		say_debug3("|\tpeer:%s op:0x%x sync:%u req_term:%"PRIi64,
			   net_fd_name(container_of(wbuf, struct netmsg_io, wbuf)->fd),
			   msg->msg_code, msg->sync, append->term);
	}

	if (check_term(self, append->term) < 0)	/* request from stale leader */
		goto reply;

	if (self->leader_id != append->peer_id) {
		self->leader_id = append->peer_id;
		[self adjust_route];
	}

	update_deadline(self);

	int match = log_match(self, append);
	if (match == 1) {
		if (is_keepalive) {
			reply(self, wbuf, msg, 1); /* nothing to append because it's keepalive */
			return;
		}

		struct log_entry *le;
		le = log_entry_append(self, append->prev_log_index + 1, append->entry_term,
				      append->val, append->len, append->tag);
		if (le->state & WAL_APPEND) {
			result = 1; /* already written to WAL */
		} else {
			result = log_entry_persist(self, le);
			if (!result)
				log_entry_discard(self, le);
		}

		if (result)
			log_commit(self, MIN(le->scn, append->leader_commit));
	} else {
		say_debug2("|\tlog mismatch prev_log_scn:%"PRIi64 " prev_log_term:%"PRIi64 " tag:%s : %s",
			   append->prev_log_index, append->prev_log_term, xlog_tag_to_a(append->tag),
			   match == 0 ? "prev log entry term not equal" : "log entry not found");
	}

reply:
	say_debug2("|\treply with %s, reply_term:%"PRIi64, result ? "yes": "no", self->term);
	say_debug3("|\tpeer:%s op:0x%x sync:%u",
		   net_fd_name(container_of(wbuf, struct netmsg_io, wbuf)->fd),
		   msg->msg_code, msg->sync);
	reply(self, wbuf, msg, result);
}

static void wal_dump_commited(Raft *self);

static int
learn_wal(Raft *self, id<XLogPullerAsync> puller)
{
	struct row_v12 *row;
	struct log_entry *le = NULL;
	int result = 0;
	say_debug2("%s: SCN:%i", __func__, (int)self->scn);

	[puller recv_row];
	while ((row = [puller fetch_row])) {
		if (self->leader_id == self->peer_id ||
		    (row->tag & TAG_MASK) == wal_final ||
		    row->scn <= self->scn) {
			result = -1;
			break;
		}

		assert((row->tag & TAG_MASK) == raft_commit);
		assert (row->shard_id == self->id);
		assert(row->scn == self->scn + 1);

		struct tbuf row_data = TBUF(row->data, row->len, fiber->pool);
		u16 flags = read_u16(&row_data);
		i64 term = read_u64(&row_data);
		u16 tag = read_u16(&row_data);
		check_term(self, term);

		(void)flags;
		le = log_entry_append(self, row->scn, term, row_data.ptr, tbuf_len(&row_data), tag);
		le->state |= REPLICATED;

		assert(le->scn < self->last_wal_append ||
		       le->scn == self->last_wal_append + 1);

		self->last_wal_append = le->scn;
		log_commit(self, row->scn);
	}

	fiber_cancel_wake(self->wal_dumper);
	while (le && (le->state & WAL_COMMIT) == 0)
		wal_dump_commited(self);
	return result;
}


static void
pull_entries_cb(struct netmsg_head *wbuf, struct iproto *msg)
{
	struct Raft *self = RT_SHARD(msg);
	XLogPuller *puller = [[XLogPuller alloc] init];
	char feeder_param_arg[16];
	struct feeder_param feeder = { .filter = { .arg = feeder_param_arg } };

	@try {
		if (self->leader_id < 0)
			return;
		[self fill_feeder_param:&feeder peer:self->leader_id];
		[puller feeder_param:&feeder];
		if ([puller handshake:self->scn + 1] < 0)
			return;

		while (learn_wal(self, puller) != -1);
		static int count = 3;
		if (count-- == 0)
			abort();
	}
	@catch (Error *e) {
		say_warn("puller failed, [%s reason:\"%s\"] at %s:%d",
			 [[e class] name], e->reason, e->file, e->line);
		[e release];
	}
	@finally {
		[puller free];
		iproto_reply_small(wbuf, msg, 0);
	}
}

static void
purge_walled_log_entries(struct Raft *self)
{
	/* catchup fiber итерирует по всему списку,
	   поэтому пока он не завершится, нельзя удалять записи */
	for (int i = 0; i < nelem(self->catchup); i++)
		if (self->catchup[i])
			return;

	i64 stop = self->scn - cfg.wal_writer_inbox_size * 3;
	struct log_entry *le, *tmp;
	TAILQ_FOREACH_SAFE(le, &self->log, link, tmp) {
		if (le->scn > stop || (le->state & WAL_COMMIT) == 0)
			break;
		assert((le->state & (WAL_APPEND|WAL_COMMIT)) == (WAL_APPEND|WAL_COMMIT) ||
			le->state & REPLICATED);
		TAILQ_REMOVE(&self->log, le, link);
		sfree(le); /* поинтерами на закоммиченные записи не владеет ни один фибер. */
	}
}


static void
wal_dump_commited(Raft *self)
{
	struct log_entry *le, *pack_first = NULL;

 	le = self->commited;
	while (le && (le->state & WAL_COMMIT) == 0)  {
		pack_first = le;
		le = TAILQ_PREV(le, log_tailq, link);
	}

	struct wal_pack pack;
	int i = 1;
	assert(pack_first);
	le = pack_first;
	wal_pack_prepare(recovery->writer, &pack);
	do {
		assert((le->state & WAL_COMMIT) == 0);
		assert(le->scn == self->last_wal_commit + i);
		u16 flags = le->state & REPLICATED ? 1 << 8 : 0;
		struct row_v12 row = { .scn = le->scn,
				       .tag = raft_commit,
				       .shard_id = self->id };
		wal_pack_append_row(&pack, &row); // will copy &row
		wal_pack_append_data(&pack, &flags, sizeof(flags));
		wal_pack_append_data(&pack, &le->term, sizeof(le->term));
		wal_pack_append_data(&pack, &le->tag, sizeof(le->tag));
		wal_pack_append_data(&pack, le->data, le->len);

		if (pack.request->row_count == WAL_PACK_MAX)
			break;
		le = TAILQ_NEXT(le, link);
		i++;
	} while (le && le->scn < self->scn);

	struct wal_reply *reply = [recovery->writer wal_pack_submit];
	if (reply->row_count == 0)
		return;

	le = pack_first;
	while (reply->row_count) {
		assert(self->last_wal_commit + 1 == le->scn);
		self->last_wal_commit++;
		le->state |= WAL_COMMIT;
		le = TAILQ_NEXT(le, link);
		assert(le == NULL || (le->state & WAL_COMMIT) == 0);
		reply->row_count--;
	}
	purge_walled_log_entries(self);
}

static void
wal_dumper_fib(va_list ap)
{
	Raft *self = va_arg(ap, Raft *);
	fiber->ushard = self->id;

	while (1) {
		prelease(fiber->pool);
		self->wal_dumper_idle = true;
		yield();
		self->wal_dumper_idle = false;

		// if (!concurent_dump_running)
		wal_dump_commited(self);
	}
}

static int
commit_log_entry(Raft *self, struct log_entry *le)
{
	//palloc_register_cut_point(fiber->pool);
	struct msg_append_entries msg = APPEND_ENTRIES_INITIALIZER(msg, self, le);
	struct iovec iov = { .iov_base = le->data, .iov_len = le->len };
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	iproto_mbox_broadcast(&mbox, &self->remotes, &msg.iproto, &iov, 1);

	assert(self->role == LEADER); // FIXME: а может быть по другому?
	int result = 0;
	if (le->state == 0 && log_entry_persist(self, le) != 1) {
		result = -1;
		goto release;
	}

	ev_tstamp deadline = ev_now() + election_timeout;

	assert(le->state & WAL_APPEND);
	le->worker = fiber;
	int votes = wait_for_replies(self, &mbox, 0); /* FIXME: таймаут = ? */
	if (votes == 0) {
		ev_timer w = { .coro = 1 };
		ev_timer_init(&w, (void *)fiber, deadline - ev_now(), 0);
		ev_timer_start(&w);
		void *y = yield();
		ev_timer_stop(&w);
		assert(y == &w || y == NULL);
		if (y == NULL)
			votes = -1;
	}

	le->worker = NULL;

	if (votes >= 0) {
		result = votes >= quorum - 1;
	} else {
		/* wake up by log_truncate or by log_entry_commit */
		if (self->leader_commit >= le->scn)
			result = 1;
		else
			result = -1;
	}

release:
	iproto_mbox_release(&mbox);
	// palloc_cutoff(fiber->pool);

	switch (result) {
	case 1:
		log_commit(self, le->scn - 1); /* commit all previous entries */
		log_entry_mark_commited(self, le);
		break;
	case 0:
		break;
	case -1:
		log_entry_discard(self, le);
		break;
	}
	return result;
}



static void
commit_nop(va_list ap)
{
	char body[2] = {0};
	Raft *self = va_arg(ap, Raft *);

	self->nop_commited = 0;
	i64 term = self->term;

	struct log_entry *le = log_entry_alloc(self, INT64_MAX, term, body, nelem(body), nop);

	while (self->role == LEADER && term == self->term) {
		if (commit_log_entry(self, le) == 1) {
			self->nop_commited = 1;
			return;
		}
	}
}


static struct netmsg_pool_ctx raft_ctx;

@implementation Raft

- (id)
init_id:(int)shard_id scn:(i64)scn_ run_crc:(u32)run_crc_ sop:(const struct shard_op *)sop
{
	[super init_id:shard_id scn:scn_ run_crc:run_crc_ sop:sop];
	if (cfg.local_hot_standby)
		panic("local_hot_standby is incompatible with raft");

	TAILQ_INIT(&log);
	peer_id = leader_id = -1;
	if (sop->aux_len == 8)
		term = *(i64 *)(sop->aux);

	if (scn > 0) {
		struct log_entry *le = log_entry_alloc(self, scn_, term, NULL, 0, 0);
		le->state = WAL_APPEND|WAL_COMMIT;
		commited = le;
		self->last_wal_commit = scn_;
		self->last_wal_append = scn_;
	}
	return self;
}

- (void)
wal_final_row
{
	for (int i = 0; i < nelem(peer); i++) {
		if (*peer[i] == 0)
			continue;
		if (strcmp(cfg.hostname, peer[i]) == 0) {
			peer_id = i;
			continue;
		}

		const struct sockaddr_in *sin = peer_addr(peer[i], PORT_PRIMARY);
		self->egress[i] = iproto_remote_add_peer(NULL, sin, &raft_ctx);
		self->egress[i]->reply_with_retcode = true;
		SLIST_INSERT_HEAD(&remotes, self->egress[i], link);
		self->remote_count++;
	}
	assert(peer_id >= 0);

	[super wal_final_row];
}

- (void)
fill_feeder_param:(struct feeder_param *)param peer:(int)i
{

	[super fill_feeder_param:param peer:i];
	param->filter.name = "raft";
}

- (int)
submit:(const void *)data len:(u32)len tag:(u16)tag
{
	assert(recovery->writer != nil);

	if (role != LEADER || nop_commited == 0)
		return 0;
	/* FIXME: не должно быть предыдущих le, которые не закомичены
	   и не имеют ждущего фибера.  Если такое будет то все
	   сломается: текущая транзакция прежде чем вызвать submit
	   как-то нетривиальнно поменяла heap: взяла локи или создала
	   phi.  После того, как мы получим кворум на текущую
	   транзакцию мы автоматически закомитим все предыдущие. В
	   процессе этого мы будем либо будить фиберы либо вызывать
	   [apply]. [apply] может сломаться, если данные которые он
	   хочет поменять залочены или на них есть phi. А именно это и
	   будет: текущая транзакция держит локи.
	*/

	struct log_entry *tmp = self->commited;
	while ((tmp = TAILQ_NEXT(tmp, link)))
		assert(tmp->worker);

	struct log_entry *le = log_entry_alloc(self, INT64_MAX, self->term, data, len, tag);

	int result;
	assert(fiber->wake_link.tqe_prev == NULL);
	do result = commit_log_entry(self, le);
	while (result == 0);
	return result > 0;
}



- (void)
apply_data:(const void *)data len:(int)len tag:(u16)tag
{
	struct tbuf row_data = TBUF(data, len, fiber->pool);

	switch (tag & TAG_MASK) {
	case snap_initial:
	case snap_final:
		break;
	case shard_create:
		break;
	case shard_final:
		snap_loaded = true;
		break;
	case shard_alter:
	case nop:
		break;
	default:
		assert((tag & ~TAG_MASK) != TAG_SYS);
		[executor apply:&row_data tag:tag];
	}

}

static void
update_last_wal_append(Raft *self, const struct row_v12 *r, const struct log_entry *le)
{
	if (le->state == (WAL_APPEND|WAL_COMMIT))
		return;

	assert(le->scn < self->last_wal_append ||
	       le->scn == self->last_wal_append + 1);

	if (le->scn < self->last_wal_append) {
		say_warn("append backwards: lsn:%i %i -> %i", (int)r->lsn,
			 (int)self->last_wal_append, (int)le->scn);
	}

	self->last_wal_append = le->scn;
}

- (void)
recover_row:(struct row_v12 *)r
{
	struct tbuf row_data = TBUF(r->data, r->len, fiber->pool);
	struct log_entry *le;
	i64 row_term;
	u16 tag, flags;

	say_debug2("%s: LSN:%"PRIi64" tag:%s", __func__, r->lsn, xlog_tag_to_a(r->tag));
	switch (r->tag & TAG_MASK) {
	case shard_create:
		assert(r->shard_id == self->id);
		/* В случае создания пустого шарда на реплики приходит route_update
		   без указания SCN, run_crc. Поэтому создаю*/
		if (self->scn == -1)
			[self init_id:self->id scn:r->scn run_crc:r->run_crc sop:(void *)r->data];
		else
			assert(r->scn == self->scn);
		return;
	case raft_append:
		flags = read_u16(&row_data);
		row_term = read_u64(&row_data);
		if (term <= row_term)
			term = row_term;
		tag = read_u16(&row_data);
		le = log_entry_append(self, r->scn, row_term, row_data.ptr, tbuf_len(&row_data), tag);
		le->state = WAL_APPEND;
		update_last_wal_append(self, r, le);
		assert(self->last_wal_append == le->scn);
		break;
	case raft_commit:
		flags = read_u16(&row_data);
		row_term = read_u64(&row_data);
		if (term <= row_term)
			term = row_term;
		tag = read_u16(&row_data);
		le = log_entry_append(self, r->scn, row_term, row_data.ptr, tbuf_len(&row_data), tag);
		le->state |= WAL_COMMIT;
		if (flags & (1 << 8))
			le->state |= REPLICATED;
		update_last_wal_append(self, r, le);
		log_commit(self, r->scn);
		if ((r->scn % 128) == 0)
			purge_walled_log_entries(self);
		assert(last_wal_commit + 1 == le->scn);
		last_wal_commit++;
		break;
	case raft_vote:
		flags = read_u16(&row_data);
		row_term = read_u64(&row_data);
		assert(term <= row_term);
		term = row_term;
		voted_for = read_u8(&row_data);
		return;
	default:
		[self apply_data:r->data len:r->len tag:r->tag];
		break;
	}
}

- (bool)
is_replica
{
	return role != LEADER;
}


- (void)
adjust_route
{
	assert(!loading);
	static int prev_leader = -255;
	say_info("%s: %s leader:%i %s",
		 __func__, scn_info(self), leader_id,
		 self->role == LEADER ? "leader" : "");

	if (recovery->writer && wal_dumper == NULL) {
		fiber_create("raft/elect", elect, self);
		wal_dumper = fiber_create("raft/wal_dump", wal_dumper_fib, self);
		[executor wal_final_row];
	}
	if (prev_leader == leader_id)
		return;

	if (leader_id < 0) {
		say_info("leader unknown, %i -> %i", prev_leader, leader_id);
		update_rt(self->id, self, NULL, 0);
	} else if (self->role != LEADER) {
		update_rt(self->id, self, peer[leader_id], 0);
		say_info("leader is %s, %i -> %i", peer[leader_id], prev_leader, leader_id);
	} else {
		say_info("I am leader, %i -> %i", prev_leader, leader_id);
		update_rt(self->id, self, NULL, 0);
	}
	switch (role) {
	case FOLLOWER:  [self status_update:"raft/follower"]; break;
	case CANDIDATE: [self status_update:"raft/candidate"]; break;
	case LEADER:    [self status_update:"raft/leader"]; break;
	}

	prev_leader = leader_id;
}

- (int)
write_scn:(i64)scn_ header:(struct tbuf *)header data:(const void *)data len:(u32)len tag:(u16)tag
{
	struct row_v12 row = { .scn = scn_,
			       .tag = tag };
	row.shard_id = self->id;
	struct wal_pack pack;
	wal_pack_prepare(recovery->writer, &pack);
	wal_pack_append_row(&pack, &row);
	wal_pack_append_data(&pack, header->ptr, tbuf_len(header));
	wal_pack_append_data(&pack, data, len);
	assert(fiber->wake_link.tqe_prev == NULL);
	struct wal_reply *reply = [recovery->writer wal_pack_submit];
	return reply ? reply->row_count : -1;
}

- (int)
wal_voted_for:(u8)vote_peer_id
{
	struct tbuf buf = TBUF(NULL, 0, fiber->pool);
	u16 flags = 0;
	tbuf_append(&buf, &flags, sizeof(flags));
	tbuf_append(&buf, &term, sizeof(term));
	tbuf_append(&buf, &vote_peer_id, sizeof(vote_peer_id));
	return [self write_scn:(-1) header:&buf data:NULL len:0 tag:raft_vote];
}

- (int)
wal_le:(const struct log_entry *)le
{
	struct tbuf buf = TBUF(NULL, 0, fiber->pool);
	u16 flags = 0;
	tbuf_append(&buf, &flags, sizeof(flags));
	tbuf_append(&buf, &le->term, sizeof(le->term));
	tbuf_append(&buf, &le->tag, sizeof(le->tag));
	return [self write_scn:le->scn header:&buf data:le->data len:le->len tag:raft_append];
}

void
raft_service(struct iproto_service *s)
{
	netmsg_pool_ctx_init(&raft_ctx, "raft_pool", 64 * 1024);

        service_register_iproto(s, RAFT_REQUEST_VOTE, request_vote_cb, IPROTO_LOCAL|IPROTO_DROP_ERROR);
	service_register_iproto(s, RAFT_APPEND_ENTRIES, append_entries_cb, IPROTO_LOCAL|IPROTO_DROP_ERROR);
	service_register_iproto(s, RAFT_PULL_ENTRIES, pull_entries_cb, IPROTO_LOCAL|IPROTO_DROP_ERROR);
}

@end

register_source();
