/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

const uint32_t msg_ping = 0xff00;
const uint32_t msg_replica = 0xff01;

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

struct tbuf *
iproto_parse(struct tbuf *in)
{
	if (tbuf_len(in) < sizeof(struct iproto))
		return NULL;
	if (tbuf_len(in) < sizeof(struct iproto) + iproto(in)->data_len)
		return NULL;

	return tbuf_split(in, sizeof(struct iproto) + iproto(in)->data_len);
}

struct worker_arg {
	struct netmsg_head * (*cb)(struct conn *c, struct iproto *);
	struct iproto *r;
	struct conn *c;
};

void
iproto_worker(va_list ap)
{
	struct service *service = va_arg(ap, typeof(service));
	struct worker_arg a;

	for (;;) {
		SLIST_INSERT_HEAD(&service->workers, fiber, worker_link);
		memcpy(&a, yield(), sizeof(a));

		a.c->ref++;

		@try {
			struct netmsg_head *reply = a.cb(a.c, a.r);
			netmsg_concat(&a.c->out_messages, reply);
		}
		@catch (Error *e) {
			u32 rc = ERR_CODE_UNKNOWN_ERROR;
			if ([e respondsTo:@selector(code)])
				rc = [(id)e code];
			else if ([e isMemberOf:[IndexError class]])
				rc = ERR_CODE_ILLEGAL_PARAMS;

			struct netmsg *m = netmsg_tail(&a.c->out_messages);
			struct netmsg_mark header_mark;
			netmsg_getmark(m, &header_mark);
			iproto_reply(&m, a.r->msg_code, a.r->sync);
			iproto_error(&m, &header_mark, rc, e->reason);
		}

		a.c->ref--;

		if (a.c->state == CLOSED)
			/* connection is already closed by other fiber */
			conn_close(a.c);

		if (!TAILQ_EMPTY(&a.c->out_messages.q))
			ev_io_start(&a.c->out);
	}
}


static void
err(struct netmsg **m, struct iproto *r)
{
	iproto_reply(m, r->msg_code, r->sync);
	iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown iproto command %i", r->msg_code);
}

static void
iproto_ping(struct netmsg **m, struct iproto *r)
{
	if (r->msg_code != msg_ping)
		err(m, r);

	net_add_iov_dup(m, r, sizeof(struct iproto));
}

void
service_register_iproto_stream(struct service *s, u32 cmd,
			       void (*cb)(struct netmsg **, struct iproto *),
			       int flags)
{
	s->ih[cmd & 0xff].cb.stream = cb;
	s->ih[cmd & 0xff].flags = flags | IPROTO_NONBLOCK;
}

void
service_register_iproto_block(struct service *s, u32 cmd,
			      struct netmsg_head *(*cb)(struct conn *, struct iproto *),
			      int flags)
{
	s->ih[cmd & 0xff].cb.block = cb;
	s->ih[cmd & 0xff].flags = flags & ~IPROTO_NONBLOCK;
}

void
service_iproto(struct service *s)
{
	for (int i = 0; i < 256; i++)
		service_register_iproto_stream(s, i, err, 0);
	service_register_iproto_stream(s, msg_ping, iproto_ping, IPROTO_NONBLOCK);
}

struct netmsg_head * box_cb(struct conn *c, struct iproto *request);

void
iproto_wakeup_workers(ev_prepare *ev)
{
	struct service *service = (void *)ev - offsetof(struct service, wakeup);
	struct conn *c = TAILQ_FIRST(&service->processing),
		 *last = TAILQ_LAST(&service->processing, conn_tailq);

loop:
	if (c == NULL)
		return;

	struct netmsg *m = netmsg_tail(&c->out_messages);
next_req:
	if (tbuf_len(c->rbuf) >= sizeof(struct iproto) &&
	    tbuf_len(c->rbuf) >= sizeof(struct iproto) + iproto(c->rbuf)->data_len)
	{
		struct iproto *request = iproto(c->rbuf);
		struct iproto_handler *ih = &service->ih[request->msg_code & 0xff];

		if (ih->flags & IPROTO_NONBLOCK) {
			tbuf_ltrim(c->rbuf, sizeof(struct iproto) + request->data_len);
			struct netmsg_mark header_mark;
			netmsg_getmark(m, &header_mark);
			@try {
				ih->cb.stream(&m, request);
				if (request->msg_code != msg_ping)
					iproto_commit(&header_mark, ERR_CODE_OK);
			}
			@catch (Error *e) {
				u32 rc = ERR_CODE_UNKNOWN_ERROR;
				if ([e respondsTo:@selector(code)])
					rc = [(id)e code];
				else if ([e isMemberOf:[IndexError class]])
					rc = ERR_CODE_ILLEGAL_PARAMS;

				/* FIXME: SEGV if cb.stream() fails before iproto_reply() */
				iproto_error(&m, &header_mark, rc, e->reason);
			}
			goto next_req; /* unfair but fast */
		}
		struct fiber *w = SLIST_FIRST(&service->workers);
		if (w) {
			tbuf_ltrim(c->rbuf, sizeof(struct iproto) + request->data_len);
			SLIST_REMOVE_HEAD(&service->workers, worker_link);
			TAILQ_REMOVE(&service->processing, c, processing_link);
			c->ref++;
			resume(w, &(struct worker_arg){ih->cb.block, request, c});
			c->ref--;
			if (c->state == CLOSED) {
				struct conn *next = TAILQ_NEXT(c, processing_link);
				conn_close(c);
				c = next;
				goto loop;
			}
			TAILQ_INSERT_TAIL(&service->processing, c, processing_link);
		}
	} else {
		TAILQ_REMOVE(&service->processing, c, processing_link);
		c->state = READING;
	}

	/* Prevent output owerflow by start reading if
	   output size is below output_low_watermark.
	   Otherwise output flusher will start reading,
	   when size of output is small enougth  */
	if (c->out_messages.bytes < cfg.output_low_watermark)
		ev_io_start(&c->in);

	if (!TAILQ_EMPTY(&c->out_messages.q)) {
		ev_io_start(&c->out);
		if (c->out_messages.bytes >= cfg.output_high_watermark)
			ev_io_stop(&c->in);
	}

	if (palloc_allocated(service->pool) > 64 * 1024 * 1024) /* FIXME: do it after change of that size */
		palloc_gc(service->pool);

	if (c == last) {
		return;
	} else {
		c = TAILQ_NEXT(c, processing_link);
		goto loop;
	}
}


void
iproto_interact(va_list ap)
{
	struct service *service = va_arg(ap, struct service *);
	iproto_callback *callback = va_arg(ap, iproto_callback *);
	void *arg = va_arg(ap, void *);
	struct conn *c;

next:
	c = TAILQ_FIRST(&service->processing);
	if (unlikely(c == NULL)) {
		SLIST_INSERT_HEAD(&service->workers, fiber, worker_link);
		yield();
		goto next;
	}

	TAILQ_REMOVE(&service->processing, c, processing_link);
	bool has_req = tbuf_len(c->rbuf) >= sizeof(struct iproto) &&
		       tbuf_len(c->rbuf) >= sizeof(struct iproto) + iproto(c->rbuf)->data_len;

	if (!has_req) {
		c->state = READING;
		if (c->out_messages.bytes < cfg.output_low_watermark)
			ev_io_start(&c->in);
		goto next;
	} else {
		TAILQ_INSERT_TAIL(&service->processing, c, processing_link);
	}

	struct iproto *request = iproto(c->rbuf);
	tbuf_ltrim(c->rbuf, sizeof(struct iproto) + request->data_len);

	if (unlikely(request->msg_code == msg_ping)) {
		struct netmsg *m = netmsg_tail(&c->out_messages);
		net_add_iov_dup(&m, request, sizeof(struct iproto));
	} else {
		c->ref++;
		callback(c, request, arg);
		c->ref--;
		if (c->state == CLOSED) {
			/* connection is already closed by other fiber */
			conn_close(c);
			goto next;
		}
	}

	if (!TAILQ_EMPTY(&c->out_messages.q)) {
		ev_io_start(&c->out);
		if (c->out_messages.bytes > cfg.output_high_watermark)
			ev_io_stop(&c->in);
	}

	if (palloc_allocated(service->pool) > 64 * 1024 * 1024) /* FIXME: do it after change of that size */
		palloc_gc(service->pool);

	fiber_gc();
	goto next;
}


void
iproto_reply(struct netmsg **m, u32 msg_code, u32 sync)
{
	struct iproto_retcode *h = palloc((*m)->head->pool, sizeof(*h));
	net_add_iov(m, h, sizeof(*h));
	h->msg_code = msg_code;
	h->data_len = sizeof(h->ret_code);
	h->sync = sync;
}

void
iproto_commit(struct netmsg_mark *mark, u32 ret_code)
{
	struct netmsg *m = mark->m;
	struct iproto_retcode *h = m->iov[mark->offset].iov_base;
	int len = 0, offset = mark->offset + 1;
	do {
		for (int i = offset; i < m->count; i++)
			len += m->iov[i].iov_len;
		offset = 0; /* offset used only for first netmsg */
	} while ((m = TAILQ_NEXT(m, link)) != NULL);
	h->ret_code = ret_code;
	h->data_len += len;
	say_debug("%s: op:%x data_len:%i sync:%i ret:%i", __func__,
		  h->msg_code, h->data_len, h->sync, h->ret_code);
}

void
iproto_error(struct netmsg **m, struct netmsg_mark *header_mark, u32 ret_code, const char *err)
{
	struct netmsg *h = header_mark->m;
	netmsg_rewind(m, header_mark); /* TODO: set iov's length to zero instead? */
	h->iov[header_mark->offset].iov_len = sizeof(struct iproto_retcode);
	struct iproto_retcode *header = h->iov[header_mark->offset].iov_base;
	header->data_len = sizeof(u32);
	header->ret_code = ret_code;
	if (err && strlen(err) > 0) {
		header->data_len += strlen(err);
		net_add_iov_dup(m, err, strlen(err));
	}
	say_debug("%s: op:%02x data_len:%i sync:%i ret:%i", __func__,
		  header->msg_code, header->data_len, header->sync, header->ret_code);
}


int
init_iproto_peer(struct iproto_peer *p, int id, const char *name, const char *addr)
{
	if (req_registry == NULL)
		req_registry = mh_i32_init(NULL);

	memset(p, 0, sizeof(*p));

	p->id = id;
	p->name = name;
	if (atosin(addr, &p->addr) == -1)
		return -1;

	p->c.fd = -1;
	return 0;
}

struct iproto_peer *
make_iproto_peer(int id, const char *name, const char *addr)
{
	struct iproto_peer *p = malloc(sizeof(*p));
	if (init_iproto_peer(p, id, name, addr) == -1) {
		free(p);
		return NULL;
	}
	return p;
}

static void
req_dump(struct iproto_req *r, const char *prefix)
{
	say_debug("%s: response:%s q/c:%i/%i %s", prefix, r->name, r->quorum, r->count,
		  r->closed ? "[CLOSED]" : "");
	for (int i = 0; i < nelem(r->reply) && r->reply[i]; i++)
		say_debug("|   reply: sync:%i op:0x%02x len:%i",
			  r->reply[i]->sync, r->reply[i]->msg_code, r->reply[i]->data_len);
}

static void
req_delete(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_req *r = (void *)w - offsetof(struct iproto_req, timer);
	ev_timer_stop(&r->timer);
	u32 k = mh_i32_get(req_registry, r->sync);
	assert(k != mh_end(req_registry));
	mh_i32_del(req_registry, k);
	slab_cache_free(&response_cache, r);
}

void
req_release(struct iproto_req *r)
{
	ev_timer_stop(&r->timer);
	ev_timer_init(&r->timer, req_delete, 15., 0.);
	ev_timer_start(&r->timer);
}

static void
req_timeout(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_req *r = (void *)w - offsetof(struct iproto_req, timer);
	r->closed = ev_now();
	if (r->waiter) {
		req_dump(r, __func__);
		fiber_wake(r->waiter, r);
	}
}

struct iproto_req *
req_make(const char *name, int quorum, ev_tstamp timeout,
	 struct iproto *header, const void *data, size_t data_len)
{
	struct iproto_req *r = slab_cache_alloc(&response_cache);
	*r = (struct iproto_req) { .name = name,
				   .count = 0,
				   .quorum = quorum,
				   .timeout = timeout,
				   .sent = ev_now(),
				   .header = header,
				   .data = data,
				   .data_len = data_len };
	memset(&r->timer, 0, sizeof(r->timer));
	r->sync = r->header->sync = iproto_next_sync();
	mh_i32_put(req_registry, r->sync, r, NULL);

	if (r->timeout > 0) {
		ev_timer_init(&r->timer, req_timeout, r->timeout, 0.);
		ev_timer_start(&r->timer);
		r->waiter = fiber;
	} else {
		req_release(r);
	}

	return r;
}


void
req_collect_reply(struct conn *c, struct iproto *msg)
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
		req_dump(r, __func__);
		if (r->waiter)
			fiber_wake(r->waiter, r);
	}
}


void
broadcast(struct iproto_group *group, struct iproto_req *r)
{
	assert(r != NULL);
	assert(r->header->msg_code != 0);
	struct iproto_peer *p;
	int header_len = sizeof(*r->header) + r->header->data_len;
	int peers_count = 0;

	if (r->data)
		r->header->data_len += r->data_len;

	SLIST_FOREACH(p, group, link) {
		peers_count++;
		if (p->c.fd < 0)
			continue;

		struct netmsg *m = netmsg_tail(&p->c.out_messages);
		net_add_iov_dup(&m, r->header, header_len);
		if (r->data)
			net_add_iov_dup(&m, r->data, r->data_len);
		say_debug("|   peer:%i/%s op:0x%x len:%zu data_len:%i", p->id, p->name,
			  r->header->msg_code, sizeof(struct iproto) + r->header->data_len,
			  r->header->data_len);
		ev_io_start(&p->c.out);
	}

	if (r->waiter)
		r->reply = p0alloc(r->waiter->pool, sizeof(struct iproto *) * peers_count);
}


void
iproto_pinger(va_list ap)
{
	struct iproto_group *group = va_arg(ap, struct iproto_group *);
	struct iproto ping = { .data_len = 0, .msg_code = msg_ping };
	struct iproto_req *r;

	for (;;) {
		fiber_sleep(1);
		int q = 0;
		struct iproto_peer *p;
		SLIST_FOREACH(p, group, link)
			q++;
		ev_tstamp sent = ev_now();

		broadcast(group, req_make("ping", q, 2.0, &ping, NULL, 0));
		r = yield();

		say_info("ping r:%p q/c:%i:%i %.4f%s", r,
			 r->quorum, r->count,
			 ev_now() - sent, r->count == 0 ? " [TIMEOUT]" : "");

		req_release(r);
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
		if (r == 0 || (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
			if (r < 0)
				say_info("closing conn r:%i errno:%i", (int)r, errno);
			else
				say_info("peer %s disconnected, fd:%i", p->name, c->fd);
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
	}
}

void
iproto_rendevouz(va_list ap)
{
	struct sockaddr_in 	*self_addr = va_arg(ap, struct sockaddr_in *);
	struct iproto_group 	*group = va_arg(ap, struct iproto_group *);
	struct palloc_pool 	*pool = va_arg(ap, struct palloc_pool *);
	struct fiber 		*in = va_arg(ap, struct fiber *);
	struct fiber 		*out = va_arg(ap, struct fiber *);
	struct iproto_peer 	*p;
	ev_watcher		*w = NULL;
	ev_timer		timer = { .coro=1 };
	int			ev_own_counter = 1;

	/* some warranty to be correctly initialized */
	SLIST_FOREACH(p, group, link) {
		p->in_connect = false;
		p->last_connect_try = 0;
		p->c.fd = -1;
	}

	ev_timer_init(&timer, (void *)fiber, 1.0, 0.);

loop:
	SLIST_FOREACH(p, group, link) {
		enum tac_state	r;

		if (p->c.fd >= 0 && p->in_connect == false)
			continue;

		if (p->in_connect == false) {
			assert(p->c.fd < 0);
			if (ev_now() - p->last_connect_try <= 1.0 /* no more then one reconnect in second */)
				continue;
			p->last_connect_try = ev_now();
		}

		r = tcp_async_connect(&p->c, 
				      (p->in_connect) ? w : NULL, /* NULL means initial state for tcp_async_connect */
				      &p->addr, self_addr, 5);

		switch(r) {
			case tac_wait:
				ev_own_counter++;
				p->in_connect = true;
				break; /* wait for event */
			case tac_error:
				ev_own_counter++;
				p->in_connect = false;
				p->c.fd = -1;
				if (!p->connect_err_said)
					say_syserror("connect to %s/%s failed", p->name, sintoa(&p->addr));
				p->connect_err_said = true;
				break;
			case tac_ok:
				ev_own_counter++;
				p->in_connect = false;
				conn_init(&p->c, pool, p->c.fd, in, out, REF_STATIC);
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

	assert(ev_own_counter > 0);
	ev_timer_stop(&timer);
	ev_timer_init(&timer, (void *)fiber, 1.0, 0.);
	ev_timer_start(&timer);
	w = yield();
	ev_timer_stop(&timer);

	ev_own_counter = (w == (ev_watcher*)&timer) ? 1 : 0;

	goto loop;
}

@implementation IProtoError
- (IProtoError *)
init_code:(u32)code_
     line:(unsigned)line_
     file:(const char *)file_
backtrace:(const char *)backtrace_
   reason:(const char *)reason_
{
	[self init_line:line_ file:file_ backtrace:backtrace_ reason:reason_];
	code = code_;
	return self;
}

- (IProtoError *)
init_code:(u32)code_
     line:(unsigned)line_
     file:(const char *)file_
backtrace:(const char *)backtrace_
   format:(const char *)format, ...
{
	va_list ap;
	va_start(ap, format);
	vsnprintf(buf, sizeof(buf), format, ap);
	va_end(ap);

	return [self init_code:code_ line:line_ file:file_
		     backtrace:backtrace_ reason:buf];
}

- (u32)
code
{
	return code;
}
@end

void __attribute__((constructor))
iproto_init(void)
{
	slab_cache_init(&response_cache, sizeof(struct iproto_req), SLAB_GROW, "iproto/req");
}

register_source();
