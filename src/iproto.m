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

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

const uint32_t msg_ping = 0xff00;
const uint32_t msg_replica = 0xff01;
STRS(error_codes, ERROR_CODES);
DESC_STRS(error_codes, ERROR_CODES);

static struct mhash_t *response_registry;

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


void
iproto_interact(va_list ap)
{
	struct service *service = va_arg(ap, struct service *);
	void (*callback)(struct conn *c, struct tbuf *request, void *arg) =
		va_arg(ap, void (*)(struct conn *c, struct tbuf *request, void *arg));
	void *arg = va_arg(ap, void *);
	struct tbuf *request;
	struct conn *c;

next:
	c = TAILQ_FIRST(&service->processing);
	if (unlikely(c == NULL)) {
		SLIST_INSERT_HEAD(&service->workers, fiber, worker_link);
		yield();
		goto next;
	}

	TAILQ_REMOVE(&service->processing, c, processing_link);

	request = iproto_parse(c->rbuf);
	if (request == NULL) {
		c->state = READING;
		if (c->out_messages.bytes < cfg.output_low_watermark)
			ev_io_start(&c->in);
		goto next;
	} else {
		TAILQ_INSERT_TAIL(&service->processing, c, processing_link);
	}

	u32 msg_code = iproto(request)->msg_code;
	if (unlikely(msg_code == msg_ping)) {
		struct netmsg *m = netmsg_tail(&c->out_messages);
		iproto(request)->data_len = 0;
		net_add_iov_dup(&m, request->ptr, sizeof(struct iproto));
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


struct iproto_peer *
make_iproto_peer(int id, const char *name, const char *addr)
{
	struct iproto_peer *p;

	if (response_registry == NULL)
		response_registry = mh_i32_init();

	p = calloc(1, sizeof(*p));
	if (atosin(addr, &p->addr) == -1) {
		free(p);
		return NULL;
	}

	p->id = id;
	p->name = strdup(name);

	say_debug("%s: %p/%s", __func__, p, p->name);
	return p;
}

static void
response_dump(struct iproto_response *r, const char *prefix)
{
	say_debug("%s: response:%s q/c:%i/%i %s", prefix, r->name, r->quorum, r->count,
		  r->closed ? "[CLOSED]" : "");
	for (int i = 0; i < nelem(r->reply) && r->reply[i]; i++)
		say_debug("|   reply: sync:%i op:0x%02x len:%i",
			  r->reply[i]->sync, r->reply[i]->msg_code, r->reply[i]->data_len);
}

static void
response_delete(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_response *r = (void *)w - offsetof(struct iproto_response, timeout);
	ev_timer_stop(&r->timeout);
	u32 k = mh_i32_get(response_registry, r->sync);
	assert(k != mh_end(response_registry));
	mh_i32_del(response_registry, k);
	palloc_destroy_pool(r->pool);
	slab_cache_free(&response_cache, r);
}

void
response_release(struct iproto_response *r)
{
	ev_timer_stop(&r->timeout);
	ev_timer_init(&r->timeout, response_delete, 15., 0.);
	ev_timer_start(&r->timeout);
}

static void
response_timeout(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_response *r = (void *)w - offsetof(struct iproto_response, timeout);
	r->closed = ev_now();
	if (r->waiter) {
		response_dump(r, __func__);
		fiber_wake(r->waiter, r);
	}
}

struct iproto_response *
response_make(const char *name, int quorum, ev_tstamp timeout)
{
	struct palloc_pool *pool = palloc_create_pool(name);
	struct iproto_response *r = slab_cache_alloc(&response_cache);
	memset(r, 0, sizeof(*r));
	r->name = name;
	r->sent = ev_now();
	r->sync = iproto_next_sync();
	mh_i32_put(response_registry, r->sync, r, NULL);
	r->quorum = quorum;
	r->delay = timeout;
	r->pool = pool;
	if (r->delay > 0) {
		ev_timer_init(&r->timeout, response_timeout, timeout, 0.);
		ev_timer_start(&r->timeout);
		if (quorum > 0)
			r->waiter = fiber;
	} else {
		response_release(r);
	}

	return r;
}


static void
response_collect_reply(struct conn *c, u32 k, struct iproto *msg)
{
	size_t msg_len = sizeof(struct iproto) + msg->data_len;
	struct iproto_response *r = mh_i32_value(response_registry, k);
	struct iproto_peer *p = (void *)c - offsetof(struct iproto_peer, c);

	if (r->closed) {
		if (ev_now() - r->closed > r->delay * 1.01)
			say_warn("stale reply: p:%i/%s op:0x%x sync:%i q:%i/c:%i late_after_close:%.4f",
				 p->id, p->name, msg->msg_code, msg->sync, r->quorum, r->count,
				 ev_now() - r->closed);
		return;
	}
	if (r->pool) {
		r->reply[r->count] = palloc(r->pool, msg_len);
		memcpy(r->reply[r->count], msg, msg_len);
	}
	if (++r->count == r->quorum) {
		assert(!r->closed);
		ev_timer_stop(&r->timeout);
		r->closed = ev_now();
		response_dump(r, __func__);
		if (r->waiter)
			fiber_wake(r->waiter, r);
	}
}


void
broadcast(struct iproto_group *group, struct iproto_response *r,
	  const struct iproto *msg, const void *data, size_t len)
{
	assert(msg->msg_code != 0);
	assert(r != NULL);
	struct iproto_peer *p;
	SLIST_FOREACH(p, group, link) {
		if (p->c.fd < 0)
			continue;

		int msg_len = sizeof(*msg) + msg->data_len;
		struct iproto *clone = palloc(p->c.pool, msg_len);
		memcpy(clone, msg, msg_len);
		clone->sync = r->sync;
		struct netmsg *m = netmsg_tail(&p->c.out_messages);
		net_add_iov(&m, clone, msg_len);
		if (data) {
			clone->data_len += len;
			net_add_iov_dup(&m, data, len);
		}
		say_debug("|   peer:%i/%s op:0x%x sync:%i len:%i data_len:%i", p->id, p->name,
			  clone->msg_code, clone->sync,
			  (int)sizeof(struct iproto) + clone->data_len, clone->data_len);
		ev_io_start(&p->c.out);
	}
}


void
iproto_pinger(va_list ap)
{
	struct iproto_group *group = va_arg(ap, struct iproto_group *);
	struct iproto ping = { .data_len = 0, .msg_code = msg_ping };
	struct iproto_response *r;

	for (;;) {
		fiber_sleep(1);
		int q = 0;
		struct iproto_peer *p;
		SLIST_FOREACH(p, group, link)
			q++;
		ev_tstamp sent = ev_now();

		broadcast(group, response_make("ping", q, 2.0), &ping, NULL, 0);
		r = yield();

		say_info("ping r:%p q/c:%i:%i %.4f%s", r,
			 r->quorum, r->count,
			 ev_now() - sent, r->count == 0 ? " [TIMEOUT]" : "");

		response_release(r);
	}
}

void
iproto_reply_reader(va_list ap __attribute__((unused)))
{
	for (;;) {
		struct ev_watcher *w = yield();
		struct conn *c = w->data;
		struct iproto_peer *p = (void *)c - offsetof(struct iproto_peer, c);

		tbuf_ensure(c->rbuf, 16 * 1024);
		ssize_t r = tbuf_recv(c->rbuf, c->fd);
		if (r == 0 || (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
			if (r < 0)
				say_info("closing conn r:%i errno:%i", (int)r, errno);
			else {
				say_info("peer %s disconnected, fd:%i", p->name, c->fd);
			}
			conn_close(c);
			continue;
		}

		while (tbuf_len(c->rbuf) >= sizeof(struct iproto) &&
		       tbuf_len(c->rbuf) >= sizeof(struct iproto) + iproto(c->rbuf)->data_len)
		{
			struct iproto *msg = c->rbuf->ptr;
			size_t msg_len = sizeof(struct iproto) + msg->data_len;
			tbuf_ltrim(c->rbuf, msg_len);

			u32 k = mh_i32_get(response_registry, msg->sync);
			if (k != mh_end(response_registry)) {
				response_collect_reply(c, k, msg); // FIXME: drop msg_len
			} else {
				say_warn("peer:%s op:0x%x sync:%i STALE", p->name, msg->msg_code, msg->sync);
			}
		}
	}
}

void
iproto_rendevouz(va_list ap)
{
	struct sockaddr_in *self_addr = va_arg(ap, struct sockaddr_in *);
	struct iproto_group *group = va_arg(ap, struct iproto_group *);
	struct palloc_pool *pool = va_arg(ap, struct palloc_pool *);
	struct fiber *in = va_arg(ap, struct fiber *);
	struct fiber *out = va_arg(ap, struct fiber *);
	struct iproto_peer *p;

loop:
	SLIST_FOREACH(p, group, link) {
		if (p->c.fd > 0)
			continue;

		int fd = tcp_connect(&p->addr, self_addr, 5);
		assert(p->c.fd < 0);

		if (fd > 0) {
			conn_init(&p->c, pool, fd, in, out, REF_STATIC);
			ev_io_start(&p->c.in);
			say_info("connected to %s/%s", p->name, sintoa(&p->addr));
			p->connect_err_said = false;
		} else {
			if (!p->connect_err_said)
				say_syserror("connect to %s/%s failed", p->name, sintoa(&p->addr));
			p->connect_err_said = true;
		}
	}

	fiber_sleep(1); /* no more then one reconnect in second */
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
	slab_cache_init(&response_cache, sizeof(struct iproto_response), SLAB_GROW, "iproto/response");
}

register_source();
