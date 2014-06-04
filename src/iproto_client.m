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

static struct mhash_t *req_registry;
static struct slab_cache response_cache;

u32
iproto_next_sync()
{
	static u32 iproto_sync;
	iproto_sync++;
	if (unlikely(iproto_sync == 0))
		iproto_sync++;
	return iproto_sync;
}

int
init_iproto_peer(struct iproto_peer *p, int id, const char *name, const char *addr)
{
	if (req_registry == NULL)
		req_registry = mh_i32_init(xrealloc);

	*p = (struct iproto_peer){ .id = id,
				   .name = name,
				   .c = { .fd = -1 } };
	return atosin(addr, &p->addr);
}


static void
iproto_req_dump(struct iproto_req *r, const char *prefix)
{
	const char *status = "";
	if (r->closed)
		status = "[CLOSED]";
	if (r->count < r->quorum) {
		assert(r->closed);
		status = "[CLOSED,TIMEOUT]";
	}
	say_debug("%s: response:%s q/c:%i/%i %s", prefix, r->name, r->quorum, r->count, status);
	if (!r->waiter)
		return;

	int i;
	for (i = 0; i < nelem(r->reply) && r->reply[i]; i++)
		say_debug("|   reply[%i]: sync:%i op:0x%02x len:%i",
			  i, r->reply[i]->sync, r->reply[i]->msg_code, r->reply[i]->data_len);
}

static void
iproto_req_delete(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_req *r = (void *)w - offsetof(struct iproto_req, timer);
	ev_timer_stop(&r->timer);
	u32 k = mh_i32_get(req_registry, r->header.sync);
	assert(k != mh_end(req_registry));
	mh_i32_del(req_registry, k);
	slab_cache_free(&response_cache, r);
}

void
iproto_req_release(struct iproto_req *r)
{
	ev_timer_stop(&r->timer);
	ev_timer_init(&r->timer, iproto_req_delete, 15., 0.);
	ev_timer_start(&r->timer);
}

static void
iproto_req_timeout(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_req *r = (void *)w - offsetof(struct iproto_req, timer);
	r->closed = ev_now();
	if (r->waiter) {
		iproto_req_dump(r, __func__);
		fiber_wake(r->waiter, r);
	}
}

struct iproto_req *
iproto_req_make(u16 msg_code, ev_tstamp timeout, const char *name)
{
	struct iproto_req *r = slab_cache_alloc(&response_cache);
	*r = (struct iproto_req) { .name = name,
				   .count = 0,
				   .quorum = 1,
				   .timeout = timeout,
				   .sent = ev_now() };

	memset(&r->timer, 0, sizeof(r->timer));
	r->header = (struct iproto){ .msg_code = msg_code,
				     .sync = iproto_next_sync() };

	mh_i32_put(req_registry, r->header.sync, r, NULL);
	if (r->timeout > 0) {
		ev_timer_init(&r->timer, iproto_req_timeout, r->timeout, 0.);
		ev_timer_start(&r->timer);
		r->waiter = fiber;
	} else {
		iproto_req_release(r);
	}

	return r;
}

void
iproto_send(struct iproto_peer *peer, struct iproto_req *r,
	    const struct iovec *iov, int iovcnt)
{
	if (peer->c.state < CONNECTED)
		return;

	struct iproto *header = &r->header;
	for (int i = 0; i < iovcnt; i++)
		header->data_len += iov[i].iov_len;

	struct netmsg_head *h = &peer->c.out_messages;
	net_add_iov_dup(h, header, sizeof(*header));

	for (int i = 0; i < iovcnt; i++)
		net_add_iov_dup(h, iov[i].iov_base, iov[i].iov_len);

	ev_io_start(&peer->c.out);

	say_debug("|   peer:%i/%s op:0x%x len:%zu data_len:%i", peer->id, peer->name,
		  header->msg_code, sizeof(*header) + header->data_len,
		  header->data_len);

	if (r->waiter)
		r->reply = p0alloc(r->waiter->pool, sizeof(struct iproto *) * 2);
}

void
iproto_broadcast(struct iproto_group *group, int quorum, struct iproto_req *r,
		 const struct iovec *iov, int iovcnt)
{
	assert(r != NULL);
	assert(r->header.msg_code != 0);
	struct iproto_peer *peer;
	int peers_count = 0;
	struct iproto *header = &r->header;

	for (int i = 0; i < iovcnt; i++)
		header->data_len += iov[i].iov_len;

	r->quorum = quorum;
	SLIST_FOREACH(peer, group, link) {
		peers_count++;
		if (peer->c.state < CONNECTED)
			continue;

		struct netmsg_head *h = &peer->c.out_messages;
		net_add_iov_dup(h, header, sizeof(*header));
		for (int i = 0; i < iovcnt; i++)
			net_add_iov_dup(h, iov[i].iov_base, iov[i].iov_len);
		ev_io_start(&peer->c.out);

		say_debug("|   peer:%i/%s op:0x%x len:%zu data_len:%i", peer->id, peer->name,
			  header->msg_code, sizeof(*header) + header->data_len,
			  header->data_len);
	}

	if (r->waiter)
		r->reply = p0alloc(r->waiter->pool, sizeof(struct iproto *) * (peers_count + 1));
}


void
iproto_pinger(va_list ap)
{
	struct iproto_group *group = va_arg(ap, struct iproto_group *);
	struct iproto_req *r;

	for (;;) {
		fiber_sleep(1);
		int quorum = 0;
		struct iproto_peer *p;
		SLIST_FOREACH(p, group, link)
			quorum++;
		ev_tstamp sent = ev_now();

		iproto_broadcast(group, quorum, iproto_req_make(msg_ping, 2.0, "ping"), NULL, 0);
		r = yield();

		say_info("ping r:%p q/c:%i:%i %.4f%s", r,
			 r->quorum, r->count,
			 ev_now() - sent, r->count == 0 ? " [TIMEOUT]" : "");

		iproto_req_release(r);
	}
}

void
iproto_collect_reply(struct conn *c, struct iproto *msg)
{
	struct iproto_peer *p = (void *)c - offsetof(struct iproto_peer, c);
	u32 k = mh_i32_get(req_registry, msg->sync);
	if (k == mh_end(req_registry)) {
		say_warn("peer:%s op:0x%x sync:%i [STALE]", p->name, msg->msg_code, msg->sync);
		return;
	}

	struct iproto_req *r = mh_i32_value(req_registry, k);

	if (r->closed) {
		if (ev_now() - r->closed > r->timeout * 1.01)
			say_warn("stale reply: p:%i/%s op:0x%x sync:%i q:%i/c:%i late_after_close:%.4f",
				 p->id, p->name, msg->msg_code, msg->sync, r->quorum, r->count,
				 ev_now() - r->closed);
		return;
	}
	if (r->waiter) {
		size_t msg_len = sizeof(struct iproto) + msg->data_len;
		r->reply[r->count] = palloc(r->waiter->pool, msg_len);
		memcpy(r->reply[r->count], msg, msg_len);
	}
	if (++r->count == r->quorum) {
		assert(!r->closed);
		ev_timer_stop(&r->timer);
		r->closed = ev_now();
		iproto_req_dump(r, __func__);
		if (r->waiter)
			fiber_wake(r->waiter, r);
	}
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

void __attribute__((constructor))
iproto_client_init(void)
{
	slab_cache_init(&response_cache, sizeof(struct iproto_req), SLAB_GROW, "iproto/req");
}
