/*
 * Copyright (C) 2010, 2011, 2012 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012 2014 Yuriy Vostrikov
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

void __attribute__((constructor))
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

static void
mbox_future(struct iproto_egress *dst, struct iproto_mbox *mbox, u32 sync)
{
	struct iproto_future *future = slab_cache_alloc(&future_cache);
	mh_i32_put(sync2future, sync, future, NULL);
	TAILQ_INSERT_HEAD(&dst->future, future, link);
	future->dst = dst;
	future->sync = sync;
	future->type = IPROTO_FUTURE_MBOX;
	future->mbox = mbox;
	LIST_INSERT_HEAD(&mbox->waiting, future, waiting_link);
	mbox->sent++;
}

static void
proxy_future(struct iproto_egress *dst, struct iproto_ingress *src, const struct iproto *msg, u32 proxy_sync)
{
	struct iproto_future *future = slab_cache_alloc(&future_cache);
	mh_i32_put(sync2future, proxy_sync, future, NULL);
	TAILQ_INSERT_HEAD(&dst->future, future, link);
	future->dst = dst;
	future->sync = proxy_sync;
	future->proxy_request = (struct iproto){ .msg_code = msg->msg_code, .sync = msg->sync };
	if (src) {
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

static int
msg_send(struct iproto_egress *peer,
	 const struct iproto *orig_msg,
	 const struct iovec *iov, int iovcnt)
{
	if (peer->io.fd < 0)
		return 0;

	struct netmsg_head *h = &peer->io.wbuf;
	int msglen = sizeof(*orig_msg) + orig_msg->data_len;
	struct iproto *msg = palloc(h->pool, msglen);
	memcpy(msg, orig_msg, msglen);
	msg->sync = iproto_next_sync();
	for (int i = 0; i < iovcnt; i++)
		msg->data_len += iov[i].iov_len;
	net_add_iov(h, msg, msglen);

	for (int i = 0; i < iovcnt; i++)
		net_add_iov_dup(h, iov[i].iov_base, iov[i].iov_len);
	ev_io_start(&peer->io.out);

	say_debug3("|    peer:%s:\top:0x%x sync:%u len:%zu data_len:%i", net_peer_name(peer->io.fd),
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

void
iproto_proxy_send(struct iproto_egress *to, struct iproto_ingress *from,
		  const struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	u32 sync = msg_send(to, msg, iov, iovcnt);
	if (sync)
		proxy_future(to, from, msg, sync);
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
		struct iproto ping = { .msg_code = msg_ping };
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

void
iproto_future_resolve_err(struct iproto_egress *c)
{
	struct iproto_mbox *mbox;
	struct iproto_future *future, *tmp;
	struct netmsg_io *io;

	TAILQ_FOREACH_SAFE(future, &c->future, link, tmp) {
		switch (future->type) {
		case IPROTO_FUTURE_MBOX:
			mbox = future->mbox;
			future->msg = NULL;
			mbox_put(mbox, future, link);
			break;
		case IPROTO_FUTURE_PROXY:
			io = &future->ingress->io;
			iproto_error(&io->wbuf, &future->proxy_request,
				     ERR_CODE_BAD_CONNECTION, "proxy connection failed");
			ev_io_start(&io->out);
			LIST_REMOVE(future, waiting_link);
			slab_cache_free(&future_cache, future);
			break;
		case IPROTO_FUTURE_BLACKHOLE:
		case IPROTO_FUTURE_ORPHAN:
			slab_cache_free(&future_cache, future);
			break;
		}
	}
}


void
iproto_future_resolve(struct netmsg_io *io, struct iproto *msg)
{
	struct iproto_egress *peer = container_of(io, struct iproto_egress, io);
	struct iproto_mbox *mbox;
	struct iproto_future *future;

	say_debug2("%s: peer:%s op:0x%x sync:%u", __func__, net_peer_name(io->fd), msg->msg_code, msg->sync);

	u32 k = mh_i32_get(sync2future, msg->sync);
	if (k == mh_end(sync2future)) {
		say_debug("martian reply from peer:%s op:0x%x sync:%u", net_peer_name(io->fd), msg->msg_code, msg->sync);
		return;
	}

	future = mh_i32_value(sync2future, k);
	mh_i32_del(sync2future, k);
	assert(future->dst == peer);
	TAILQ_REMOVE(&peer->future, future, link);

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
			io = &future->ingress->io;
			msg->sync = future->proxy_request.sync;
			net_add_iov_dup(&io->wbuf, msg, sizeof(*msg) + msg->data_len);
			ev_io_start(&io->out);
		}
		slab_cache_free(&future_cache, future);
		break;
	case IPROTO_FUTURE_ORPHAN:
		say_debug3("orphan reply from peer:%s op:0x%x sync:%u", net_peer_name(io->fd), msg->msg_code, msg->sync);
	case IPROTO_FUTURE_BLACKHOLE:
		slab_cache_free(&future_cache, future);
		break;
	}
}

static int
has_full_req(const struct tbuf *buf)
{
	return tbuf_len(buf) >= sizeof(struct iproto) &&
	       tbuf_len(buf) >= sizeof(struct iproto) + iproto(buf)->data_len;
}


void
data_ready(struct netmsg_io *io, int __attribute__((unused)) r)
{
	struct tbuf *rbuf = &io->rbuf;

	while (has_full_req(rbuf)) {
		struct iproto *req = iproto(rbuf);
		assert((i32)req->data_len > 0 || req->msg_code == msg_ping);
		int req_size = sizeof(struct iproto) + req->data_len;
		tbuf_ltrim(rbuf, req_size);
		iproto_future_resolve(io, req);
	}
}

static void
egress_close(struct netmsg_io *io)
{
	struct iproto_egress *c = container_of(io, struct iproto_egress, io);
	iproto_future_resolve_err(c);
}

struct netmsg_io_vop egress_vop = { .data_ready = data_ready,
				    .close = egress_close };

static struct tac_list iproto_tac_list;
static struct Fiber *iproto_remote_rendevouz;
struct iproto_egress *
iproto_add_remote_peer(const struct sockaddr_in *daddr, struct palloc_pool *pool)
{
	struct iproto_egress *peer = xcalloc(1, sizeof(*peer));
	struct tac_state *ts = &peer->ts;
	memcpy(&ts->daddr, daddr, sizeof(*daddr));
	ts->io = &peer->io;
	ts->io->fd = ts->ev.fd = -1;
	netmsg_io_init(&peer->io, pool, &egress_vop, -1);
	SLIST_INSERT_HEAD(&iproto_tac_list, ts, link);
	if (iproto_remote_rendevouz == NULL)
		iproto_remote_rendevouz = fiber_create("iproto_rendevouz", rendevouz, NULL, &iproto_tac_list);
	else
		fiber_wake(iproto_remote_rendevouz, NULL);

	return peer;
}

void
iproto_close_remote_peer(struct iproto_egress *peer)
{
	SLIST_REMOVE(&iproto_tac_list, &peer->ts, tac_state, link);
	iproto_future_resolve_err(peer);
	netmsg_io_close(&peer->io);
	free(peer);
}

static void __attribute__((constructor))
init_iproto_client(void)
{
	slab_cache_init(&future_cache, sizeof(struct iproto_future), SLAB_GROW, "net_io/iproto_future");
}

register_source();
