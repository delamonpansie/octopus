/*
 * Copyright (C) 2010, 2011, 2012 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012 2014, 2016, 2017 Yuriy Vostrikov
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

#import <util.h>
#import <palloc.h>
#import <fiber.h>
#import <iproto.h>
#import <tbuf.h>
#import <say.h>
#import <assoc.h>
#import <salloc.h>
#import <index.h>
#import <objc.h>
#import <stat.h>

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

static struct slab_cache future_cache;
static struct mh_i32_t *sync2future;

static void __attribute__((constructor))
iproto_registry_init()
{
	sync2future = mh_i32_init(xrealloc);
}

u32
iproto_next_sync()
{
	static u32 iproto_sync;

	do iproto_sync++;
	while (unlikely(iproto_sync == 0 || mh_i32_exist(sync2future, iproto_sync)));

	return iproto_sync;
}

struct iproto_mbox *blackhole_mbox = (void *)(uintptr_t)1;
struct iproto_ingress *blackhole_ingress = (void *)(uintptr_t)1;

static void
mbox_future(struct iproto_egress *dst, struct iproto_mbox *mbox, u32 sync)
{
	struct iproto_future *future = slab_cache_alloc(&future_cache);
	mh_i32_put(sync2future, sync, future, NULL);
	TAILQ_INSERT_HEAD(&dst->future, future, link);
	future->dst = dst;
	future->sync = sync;
	if (mbox != blackhole_mbox) {
		future->type = IPROTO_FUTURE_MBOX;
		future->mbox = mbox;
		LIST_INSERT_HEAD(&mbox->waiting, future, waiting_link);
		mbox->sent++;
	} else {
		future->type = IPROTO_FUTURE_BLACKHOLE;
		future->mbox = NULL;
	}
}

static void
proxy_future(struct iproto_egress *dst, struct iproto_ingress *src, const struct iproto *msg, u32 sync)
{
	struct iproto_future *future = slab_cache_alloc(&future_cache);
	mh_i32_put(sync2future, sync, future, NULL);
	TAILQ_INSERT_HEAD(&dst->future, future, link);
	future->dst = dst;
	future->sync = sync;
	future->proxy_request = (struct iproto){ .shard_id = msg->shard_id,
						 .msg_code = msg->msg_code,
						 .sync = msg->sync };
	if (src != blackhole_ingress && src->fd != -1) {
		future->type = IPROTO_FUTURE_PROXY;
		future->ingress = src;
		LIST_INSERT_HEAD(&src->waiting, future, waiting_link);
	} else {
		future->type = IPROTO_FUTURE_BLACKHOLE;
		future->ingress = NULL;
	}
}

void
iproto_mbox_release(struct iproto_mbox *mbox)
{
	iproto_future_collect_orphans(&mbox->waiting);
	struct iproto_future *future, *tmp;
	TAILQ_FOREACH_SAFE(future, &mbox->msg_list, link, tmp)
		slab_cache_free(&future_cache, future);

	mbox->sent = 0;
	mbox_init(mbox);
}

static void iproto_egress_future_err(struct iproto_egress *c);

static void
prepare(struct iproto_egress *peer)
{
	if (peer->fd >= 0) {
		ev_io_start(&peer->out);
		return;
	}

	if (peer->unsent_limit) {
		int count = 0;
		struct iproto_future *f;
		TAILQ_FOREACH(f, &peer->future, link)
			count++;

		if (count < peer->unsent_limit)
			return;
	}
	iproto_egress_future_err(peer);
}

/* TODO: что делать, если на коннекте будет толпа подвисших future в состоянии orphan?
   Если в них хранить таймстемп отправки, то можно переодиически ходить таймером и удалять их,
   как только время их сиротства превысит нужно кол-во минут? */

static int
msg_send_wrap(struct iproto_egress *peer,
	 u32 wrap_code,
	 const struct iproto *orig_msg,
	 const struct iovec *iov, int iovcnt)
{
	struct netmsg_head *h = &peer->wbuf;
	prepare(peer);
	if (peer->wbuf.bytes > cfg.output_high_watermark)
		return 0;

	int msglen = sizeof(*orig_msg) + orig_msg->data_len;
	struct iproto *wrap = net_add_alloc(h, msglen + sizeof(*wrap));
	struct iproto *msg = wrap + 1;

	memcpy(msg, orig_msg, msglen);
	msg->sync = iproto_next_sync();
	for (int i = 0; i < iovcnt; i++)
		msg->data_len += iov[i].iov_len;

	wrap->msg_code = wrap_code;
	wrap->shard_id = msg->shard_id; // not used at reciever
	wrap->sync = msg->sync;
	wrap->data_len = msg->data_len + sizeof(*msg);

	for (int i = 0; i < iovcnt; i++)
		net_add_iov_dup(h, iov[i].iov_base, iov[i].iov_len);

	say_debug3("|    peer:%s\tPROXY op:0x%x sync:%u len:%zu data_len:%i", net_fd_name(peer->fd),
		   msg->msg_code, msg->sync, sizeof(*msg) + msg->data_len,
		   msg->data_len);
	return msg->sync;
}

static int
msg_send(struct iproto_egress *peer,
	 const struct iproto *orig_msg,
	 const struct iovec *iov, int iovcnt)
{
	struct netmsg_head *h = &peer->wbuf;
	prepare(peer);
	if (peer->wbuf.bytes > cfg.output_high_watermark)
		return 0;

	int msglen = sizeof(*orig_msg) + orig_msg->data_len;
	struct iproto *msg = net_add_alloc(h, msglen);
	memcpy(msg, orig_msg, msglen);
	msg->sync = iproto_next_sync();
	for (int i = 0; i < iovcnt; i++)
		msg->data_len += iov[i].iov_len;

	for (int i = 0; i < iovcnt; i++)
		net_add_iov_dup(h, iov[i].iov_base, iov[i].iov_len);

	say_debug3("|    peer:%s\top:0x%x sync:%u len:%zu data_len:%i", net_fd_name(peer->fd),
		   msg->msg_code, msg->sync, sizeof(*msg) + msg->data_len,
		   msg->data_len);
	return msg->sync;
}

u32
iproto_mbox_send(struct iproto_mbox *mbox, struct iproto_egress *peer,
		 const struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	u32 sync = msg_send(peer, msg, iov, iovcnt);
	if (sync)
		mbox_future(peer, mbox, sync);
	return sync;
}

int
iproto_mbox_broadcast(struct iproto_mbox *mbox, struct iproto_egress_list *list,
		      const struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	say_debug2("%s: op:0x%x", __func__, msg->msg_code);

	struct iproto_egress *peer;
	int ret = 0;
	SLIST_FOREACH(peer, list, link) {
		u32 sync = msg_send(peer, msg, iov, iovcnt);
		if (sync) {
			if (mbox)
				mbox_future(peer, mbox, sync);
			ret++;
		}
	}
	return ret;
}

struct iproto *
iproto_mbox_wait(struct iproto_mbox *mbox)
{
	struct iproto_future *future = mbox_wait(mbox);
	return future ? future->msg : NULL;
}

void
iproto_mbox_wait_all(struct iproto_mbox *mbox, ev_tstamp timeout)
{
	while (mbox->msg_count < mbox->sent)
		mbox_timedwait(mbox, mbox->sent, timeout);
}


struct iproto *
iproto_mbox_get(struct iproto_mbox *mbox)
{
	struct iproto_future *future = mbox_get(mbox, link);
	struct iproto *ret = NULL;
	if (future) {
		ret = future->msg;
		slab_cache_free(&future_cache, future);
	}
	return ret;
}

struct iproto *
iproto_mbox_peek(struct iproto_mbox *mbox)
{
	struct iproto_future *future = mbox_peek(mbox);
	return future ? future->msg : NULL;
}

void
iproto_mbox_put(struct iproto_mbox *mbox, struct iproto *msg)
{
	struct iproto_future *future = slab_cache_alloc(&future_cache);
	future->msg = msg;
	mbox_put(mbox, future, link);
}

struct iproto *
iproto_sync_send(struct iproto_egress *peer,
		 const struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	if (iproto_mbox_send(&mbox, peer, msg, iov, iovcnt) == 0)
		return NULL;
	struct iproto *future =  iproto_mbox_wait(&mbox);
	iproto_mbox_release(&mbox);
	return future;
}

u32
iproto_proxy_send(struct iproto_egress *to, struct iproto_ingress *from, u32 wrap_code,
		  const struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	say_debug2("%s: shard:%i op:0x%x %s", __func__, msg->shard_id, msg->msg_code, wrap_code ? "WRAP" : "");

	u32 sync = wrap_code ?
		   msg_send_wrap(to, wrap_code, msg, iov, iovcnt) :
		   msg_send(to, msg, iov, iovcnt);
	if (sync && from)
		proxy_future(to, from, msg, sync);
	return sync;
}

void
iproto_pinger(va_list ap)
{
	struct iproto_egress_list *list = va_arg(ap, struct iproto_egress_list *);

	for (;;) {
		fiber_gc();
		fiber_sleep(1);
		ev_tstamp sent = ev_now();
		int quorum = 0;
		struct iproto_egress *p;
		SLIST_FOREACH(p, list, link)
			quorum++;

		struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
		struct iproto ping = { .msg_code = MSG_PING };
		iproto_mbox_broadcast(&mbox, list, &ping, NULL, 0);
		mbox_timedwait(&mbox, quorum, 2.0);
		iproto_mbox_release(&mbox);

		say_info("ping q/c:%i:%i %.4f%s",
			 quorum, mbox.msg_count,
			 ev_now() - sent, mbox.msg_count ? "" : " [TIMEOUT]");
	}
}


void
iproto_future_collect_orphans(struct iproto_future_list *waiting)
{
	struct iproto_future *future, *tmp;
	LIST_FOREACH_SAFE(future, waiting, waiting_link, tmp)
		future->type = IPROTO_FUTURE_ORPHAN;
	LIST_INIT(waiting);
}

static void
iproto_future_err(struct iproto_future *future)
{
	struct iproto_mbox *mbox;
	struct netmsg_io *io;

	mh_i32_remove(sync2future, future->sync, NULL);
	switch (future->type) {
	case IPROTO_FUTURE_MBOX:
		LIST_REMOVE(future, waiting_link);
		mbox = future->mbox;
		future->msg = NULL;
		mbox_put(mbox, future, link);
		break;
	case IPROTO_FUTURE_PROXY:
		LIST_REMOVE(future, waiting_link);
		io = future->ingress;
		iproto_error(&io->wbuf, &future->proxy_request,
			     ERR_CODE_BAD_CONNECTION, "proxy connection failed");
		ev_io_start(&io->out);
		slab_cache_free(&future_cache, future);
		break;
	case IPROTO_FUTURE_BLACKHOLE:
	case IPROTO_FUTURE_ORPHAN:
		slab_cache_free(&future_cache, future);
		break;
	}
}

static void
iproto_egress_future_err(struct iproto_egress *c)
{
	struct iproto_future *future, *tmp;

	if (c->wbuf.bytes) {
		/* мы не знаем, остановилась ли запись ровно на пакете или нет.
		   поэтому для того, чтобы ибежать ошибок с частичной записью пакета
		   удалим все исходящие пакеты */
		netmsg_reset(&c->wbuf);
		c->wbuf.bytes = 0;
	}

	TAILQ_FOREACH_SAFE(future, &c->future, link, tmp)
		iproto_future_err(future);
	TAILQ_INIT(&c->future);
}


void
iproto_future_resolve(struct iproto_egress *peer, struct iproto *msg)
{
	struct iproto_mbox *mbox;
	struct iproto_future *future = NULL;

	say_debug2("%s: peer:%s op:0x%x sync:%u", __func__, net_fd_name(peer->fd), msg->msg_code, msg->sync);

	u32 k = mh_i32_get(sync2future, msg->sync);
	if (k != mh_end(sync2future)) {
		future = mh_i32_value(sync2future, k);
		mh_i32_del(sync2future, k);
		TAILQ_REMOVE(&peer->future, future, link);
	}

	if (unlikely(future == NULL || future->dst != peer)) {
		say_warn("martian reply from peer:%s op:0x%x sync:%u", net_fd_name(peer->fd), msg->msg_code, msg->sync);
		if (future)
			iproto_future_err(future);
		return;
	}

	if (peer->reply_with_retcode) {
		struct iproto_retcode *rmsg = (struct iproto_retcode *)msg;
		if (rmsg->ret_code)
			say_warn("error from peer:%s op:0x%x sync:%u 0x%x %.*s",
				 net_fd_name(peer->fd), rmsg->msg_code, rmsg->sync,
				 rmsg->ret_code, rmsg->data_len - 4, rmsg->data);
	}

	if (future->dst != peer) {
		say_warn("unexpected reply from peer:%s, sync:%u is expected to come from peer:%s",
			 net_fd_name(peer->fd), msg->sync, net_fd_name(future->dst->fd));
		iproto_future_err(future);
		return;
	}

	switch (future->type) {
	case IPROTO_FUTURE_MBOX:
		LIST_REMOVE(future, waiting_link);
		mbox = future->mbox;
		future->msg = palloc(mbox->pool, sizeof(*msg) + msg->data_len);
		memcpy(future->msg, msg, sizeof(*msg) + msg->data_len);
		mbox_put(mbox, future, link);
		break;
	case IPROTO_FUTURE_PROXY:
		LIST_REMOVE(future, waiting_link);
		if (future->ingress) {
			struct netmsg_io *io = future->ingress;
			msg->sync = future->proxy_request.sync;
			net_add_iov_dup(&io->wbuf, msg, sizeof(*msg) + msg->data_len);
			ev_io_start(&io->out);
		}
		slab_cache_free(&future_cache, future);
		break;
	case IPROTO_FUTURE_ORPHAN:
		say_debug3("orphan reply from peer:%s op:0x%x sync:%u", net_fd_name(peer->fd), msg->msg_code, msg->sync);
		// fall through
	case IPROTO_FUTURE_BLACKHOLE:
		slab_cache_free(&future_cache, future);
		break;
	}
}

@implementation iproto_egress
- (void)
data_ready
{
	struct iproto *req;
	while ((req = iproto_rbuf_req(self))) {
		int req_size = sizeof(*req) + req->data_len;
		rbuf_ltrim(self, req_size);
		iproto_future_resolve(self, req);
	}
}
- (void)
close
{
	iproto_egress_future_err(self);
	[super close];
}

- (id)
free
{
	iproto_egress_future_err(self); /* [free] may be called before fd gets connected (no [close] it this case) */
	return [super free];
}
@end

static struct tac_list iproto_tac_list;
struct iproto_egress *
iproto_remote_add_peer(struct iproto_egress *peer, const struct sockaddr_in *daddr, struct netmsg_pool_ctx *ctx)
{
	struct tac_state *ts;
	static struct Fiber *rendevouz_fiber;
	if (rendevouz_fiber == NULL)
		rendevouz_fiber = fiber_create("iproto_rendevouz", rendevouz, NULL, &iproto_tac_list);

	if (peer == nil) {
		SLIST_FOREACH(ts, &iproto_tac_list, link) {
			peer = container_of(ts, struct iproto_egress, ts);
			if (memcmp(&ts->daddr, daddr, sizeof(*daddr)) == 0 &&
			    peer->ctx == ctx)
				return peer;
		}

		peer = [iproto_egress alloc];
	}
	netmsg_io_init(peer, ctx, -1);

	ts = &peer->ts;
	ts->io = peer;
	ts->io->fd = ts->ev.fd = -1;
	memcpy(&ts->daddr, daddr, sizeof(*daddr));

	netmsg_io_retain(peer);
	ts->flags |= TAC_RECONNECT;
	SLIST_INSERT_HEAD(&iproto_tac_list, ts, link);
	fiber_wake(rendevouz_fiber, NULL);
	return peer;
}

void
iproto_remote_stop_reconnect(struct iproto_egress *peer)
{
	struct tac_state *ts = &peer->ts;
	if (ts->flags & TAC_RECONNECT) {
		abort_tcp_async_connect(ts);
		ts->flags &= ~TAC_RECONNECT;
		SLIST_REMOVE(&iproto_tac_list, ts, tac_state, link);
		netmsg_io_release(peer);
	}
}

static void __attribute__((constructor))
init_iproto_client(void)
{
	slab_cache_init(&future_cache, sizeof(struct iproto_future), SLAB_GROW, "net_io/iproto_future");
}

register_source();
