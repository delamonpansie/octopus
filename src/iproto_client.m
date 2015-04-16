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

static struct mh_i32_t *req_registry;

void __attribute__((constructor))
iproto_registry_init()
{
	req_registry = mh_i32_init(xrealloc);
}

u32
iproto_next_sync()
{
	static u32 iproto_sync;
	iproto_sync++;
	if (unlikely(iproto_sync == 0))
		iproto_sync++;
	return iproto_sync;
}

void
iproto_mbox_release(struct iproto_mbox *mbox)
{
	for (int i = 0; i < mbox->sent; i++) {
		u32 k = mh_i32_get(req_registry, mbox->sync[i]);
		assert(k != mh_end(req_registry));
		mh_i32_del(req_registry, k);
	}
}

static void
msg_prepare(struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	for (int i = 0; i < iovcnt; i++)
		msg->data_len += iov[i].iov_len;
}

static void
msg_fixup_sync(struct iproto_mbox *mbox, struct iproto *msg)
{
	u32 sync = iproto_next_sync();
	msg->sync = sync;
	if (mbox) {
		assert(mbox->sent < mbox->sync_nelem);
		mbox->sync[mbox->sent++] = sync;
		mh_i32_put(req_registry, sync, mbox, NULL);
	}
}

static int
msg_send(struct iproto_mbox *mbox, struct iproto_peer *peer,
	 struct iproto *msg, int msglen,
	 const struct iovec *iov, int iovcnt)
{
	if (peer->c.state < CONNECTED)
		return 0;

	msg_fixup_sync(mbox, msg);

	struct netmsg_head *h = &peer->c.out_messages;
	net_add_iov_dup(h, msg, msglen);
	for (int i = 0; i < iovcnt; i++)
		net_add_iov_dup(h, iov[i].iov_base, iov[i].iov_len);
	ev_io_start(&peer->c.out);

	say_debug("|    peer:%i/%s\top:0x%x sync:%u len:%zu data_len:%i", peer->id, peer->name,
		  msg->msg_code, msg->sync, sizeof(*msg) + msg->data_len,
		  msg->data_len);
	return 1;
}

int
iproto_send(struct iproto_mbox *mbox, struct iproto_peer *peer,
	    struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	int msglen = sizeof(*msg) + msg->data_len;
	msg_prepare(msg, iov, iovcnt);
	return msg_send(mbox, peer, msg, msglen, iov, iovcnt);
}

int
iproto_broadcast(struct iproto_mbox *mbox, struct iproto_group *group,
		 struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	struct iproto_peer *peer;
	int ret = 0;
	int msglen = sizeof(*msg) + msg->data_len;
	msg_prepare(msg, iov, iovcnt);
	SLIST_FOREACH(peer, group, link)
		ret += msg_send(mbox, peer, msg, msglen, iov, iovcnt);
	return ret;
}

struct iproto *
iproto_wait(struct iproto_mbox *mbox)
{
	struct iproto_reply *reply = mbox_wait(mbox);
	return reply ? &reply->header : NULL;
}

struct iproto *
iproto_wait_sync(struct iproto_mbox *mbox, u32 sync)
{
	for (;;) {
		struct iproto_reply *reply = mbox_wait(mbox);
		if (!reply)
			return NULL;
		if (reply->header.sync == sync) {
			mbox_remove(mbox, reply);
			return &reply->header;
		}
	}
}

void
iproto_wait_all(struct iproto_mbox *mbox)
{
	while (mbox->msg_count < mbox->sent)
		mbox_wait(mbox);
}


struct iproto *
iproto_mbox_get(struct iproto_mbox *mbox)
{
	struct iproto_reply *reply = mbox_get(mbox, link);
	return reply ? &reply->header : NULL;
}

struct iproto *
iproto_mbox_peek(struct iproto_mbox *mbox)
{
	struct iproto_reply *reply = mbox_peek(mbox);
	return reply ? &reply->header : NULL;
}

struct iproto *
iproto_sync_send(struct iproto_peer *peer,
		 struct iproto *msg, const struct iovec *iov, int iovcnt)
{
	struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
	if (iproto_send(&mbox, peer, msg, iov, iovcnt) != 1)
		return NULL;
	struct iproto *reply =  iproto_wait(&mbox);
	iproto_mbox_release(&mbox);
	return reply;
}

void
iproto_pinger(va_list ap)
{
	struct iproto_group *group = va_arg(ap, struct iproto_group *);

	for (;;) {
		fiber_gc();
		fiber_sleep(1);
		ev_tstamp sent = ev_now();
		int quorum = 0;
		struct iproto_peer *p;
		SLIST_FOREACH(p, group, link)
			quorum++;

		struct iproto_mbox mbox = IPROTO_MBOX_INITIALIZER(mbox, fiber->pool);
		struct iproto ping = { .msg_code = msg_ping };
		iproto_broadcast(&mbox, group, &ping, NULL, 0);
		mbox_timedwait(&mbox, quorum, 2.0);
		iproto_mbox_release(&mbox);

		say_info("ping q/c:%i:%i %.4f%s",
			 quorum, mbox.msg_count,
			 ev_now() - sent, mbox.msg_count ? "" : " [TIMEOUT]");
	}
}

void
iproto_collect_reply(struct conn *c __attribute__((unused)), struct iproto *msg)
{
	struct iproto_mbox *mbox;
	struct iproto_reply *reply;

	u32 k = mh_i32_get(req_registry, msg->sync);
	if (k == mh_end(req_registry))
		return;
	mbox = mh_i32_value(req_registry, k);
	// FIXME: mh_i32_del(req_registry, k);
	reply = palloc(mbox->pool, sizeof(*reply) + msg->data_len);
	memcpy(&reply->header, msg, sizeof(*msg) + msg->data_len);
	mbox_put(mbox, reply, link);
}

void
iproto_reply_reader(va_list ap)
{
	void (*collect)(struct conn *c, struct iproto *msg) = va_arg(ap, typeof(collect));

	for (;;) {
		struct ev_watcher *w = yield();
		struct conn *c = w->data;
		struct iproto_peer *p = (void *)c - offsetof(struct iproto_peer, c);

		tbuf_ensure(c->rbuf, 16 * 1024);
		ssize_t r = tbuf_recv(c->rbuf, c->fd);
		if (r == 0) {
			say_info("peer %s closed connection", p->name);
			conn_close(c);
			continue;
		}
		if (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
			say_syswarn("peer %s recv() failed, closing connection", p->name);
			conn_close(c);
			continue;
		}

		while (tbuf_len(c->rbuf) >= sizeof(struct iproto) &&
		       tbuf_len(c->rbuf) >= sizeof(struct iproto) + iproto(c->rbuf)->data_len)
		{
			struct iproto *msg = c->rbuf->ptr;
			tbuf_ltrim(c->rbuf, sizeof(struct iproto) + msg->data_len);
			collect(c, msg);
		}

		conn_gc(NULL, c);
	}
}

void
iproto_rendevouz(va_list ap)
{
	struct sockaddr_in 	*self_addr = va_arg(ap, struct sockaddr_in *);
	struct iproto_group 	*group = va_arg(ap, struct iproto_group *);
	struct fiber 		*in = va_arg(ap, struct fiber *);
	struct fiber 		*out = va_arg(ap, struct fiber *);
	struct iproto_peer 	*p;
	ev_watcher		*w = NULL;
	ev_timer		timer = { .coro=1 };

	/* some warranty to be correctly initialized */
	SLIST_FOREACH(p, group, link) {
		p->last_connect_try = 0;
		p->c.state = CLOSED;
		p->c.fd = -1;
	}

	ev_timer_init(&timer, (void *)fiber, 1.0, 0.);

loop:
	SLIST_FOREACH(p, group, link) {
		enum tac_state	r;

		if (p->c.fd >= 0 && p->c.state != IN_CONNECT)
			continue;

		assert(p->c.state == IN_CONNECT || p->c.state == CLOSED);
		if (p->c.state == CLOSED) {
			assert(p->c.fd < 0);
			if (ev_now() - p->last_connect_try <= 1.0 /* no more then one reconnect in second */)
				continue;
			p->last_connect_try = ev_now();
		}

		r = tcp_async_connect(&p->c,
				      (p->c.state == IN_CONNECT) ? w : NULL, /* NULL means initial state for tcp_async_connect */
				      &p->addr, self_addr, 5);

		switch(r) {
			case tac_wait:
				p->c.state = IN_CONNECT;
				break; /* wait for event */
			case tac_error:
				p->c.state = CLOSED;
				p->c.fd = -1;
				if (!p->connect_err_said)
					say_syserror("connect to %s/%s failed", p->name, sintoa(&p->addr));
				p->connect_err_said = true;
				break;
			case tac_ok:
				conn_init(&p->c, NULL, p->c.fd, in, out, MO_STATIC | MO_MY_OWN_POOL);
				p->c.state = CONNECTED;
				ev_io_start(&p->c.in);
				say_info("connected to %s/%s", p->name, sintoa(&p->addr));
				p->connect_err_said = false;
				break;
			case tac_alien_event:
				break;
			default:
				abort();
		}
	}

	ev_timer_stop(&timer);
	ev_timer_init(&timer, (void *)fiber, 1.0, 0.);
	ev_timer_start(&timer);
	w = yield();
	ev_timer_stop(&timer);

	goto loop;
}
