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
		if (c->fd < 0) {
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
		for (int i = offset; i < m->count; i++) {
			len += m->iov[i].iov_len;
			assert(i < 1024);
		}
		offset = 0;
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
	netmsg_rewind(m, header_mark);
	h->iov[header_mark->offset].iov_len = sizeof(struct iproto_retcode);
	struct iproto_retcode *header = h->iov[header_mark->offset].iov_base;
	header->data_len = sizeof(u32);
	header->ret_code = ret_code;
	if (err && strlen(err) > 0) {
		header->data_len += strlen(err);
		net_add_iov_dup(m, err, strlen(err));
	}
	say_debug("%s: op:%x data_len:%i sync:%i ret:%i", __func__,
		  header->msg_code, header->data_len, header->sync, header->ret_code);
}


struct iproto_peer *
make_iproto_peer(int id, const char *name, const char *addr, short primary_port, struct iproto_peer *next)
{
	struct iproto_peer *p;

	if (response_registry == NULL)
		response_registry = mh_i32_init();

	p = calloc(1, sizeof(*p));
	if (atosin(addr, &p->addr) == -1) {
		free(p);
		return NULL;
	}
	p->primary_addr = p->addr;
	p->primary_addr.sin_port = htons(primary_port);
	p->id = id;
	p->name = strdup(name);
	p->next = next;

	say_debug("%s: %s/%p", __func__, p->name, p);
	return p;
}

static void
response_timeout(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_response *r = (void *)w - offsetof(struct iproto_response, timeout);
	for (int i = 0; i < nelem(r->sync) && r->sync[i]; i++)
		say_debug("%s: sync:%i", __func__, r->sync[i]);
	r->closed = ev_now();
	fiber_wake(r->waiter, r);
}


struct iproto_response *
make_response(int quorum, ev_tstamp timeout)
{
	struct iproto_response *r = calloc(1, sizeof(*r));
	r->sent = ev_now();
	//FIXME: r->sync = iproto_next_sync();
	r->quorum = quorum;
	ev_timer_init(&r->timeout, response_timeout, timeout, 0.);
	ev_timer_start(&r->timeout);
	r->waiter = fiber;
	return r;
}

static void
delete_response(ev_timer *w, int events __attribute__((unused)))
{
	struct iproto_response *r = (void *)w - offsetof(struct iproto_response, timeout);
	ev_timer_stop(&r->timeout);
	for (int i = 0; i < nelem(r->sync) && r->sync[i]; i++) {
		u32 k = mh_i32_get(response_registry, r->sync[i]);
		assert(k != mh_end(response_registry));
		mh_i32_del(response_registry, k);
	}
	free(r);
}

void
release_response(struct iproto_response *r)
{
	ev_timer_stop(&r->timeout);
	ev_timer_init(&r->timeout, delete_response, 15., 0.);
	ev_timer_start(&r->timeout);
}


void
broadcast(struct iproto_peer *peers, struct iproto_response *r, const void *data, size_t len)
{
	say_debug("%s", __func__);
	for (struct iproto_peer *p = peers; p; p = p->next) {
		if (p->c.fd < 0)
			continue;

		void *clone = palloc(p->c.pool, len);
		memcpy(clone, data, len);
		if (r) {
			struct iproto *h = clone;
			h->sync = iproto_next_sync();
			say_debug("  peer:%s/%p op:%x sync:%i", p->name, p, h->msg_code, h->sync);
			for (int i = 0; i < nelem(r->sync); i++) {
				if (r->sync[i] == 0) {
					r->sync[i] = h->sync;
					mh_i32_put(response_registry, h->sync, r, NULL);
					break;
				}
			}
		}
		struct netmsg *m = netmsg_tail(&p->c.out_messages);
		net_add_iov(&m, clone, len);
		ev_io_start(&p->c.out);
	}
}

void
iproto_pinger(va_list ap)
{
	struct iproto_peer *peers = va_arg(ap, struct iproto_peer *);
	struct iproto ping = { .data_len = 0, .msg_code = msg_ping };
	struct iproto_response *r;

	for (;;) {
		fiber_sleep(1);
		int q = 0;
		for (struct iproto_peer *p = peers; p; p = p->next)
			q++;
		ev_tstamp sent = ev_now();

		broadcast(peers, make_response(q, 2.0), &ping, sizeof(ping));
		r = yield();

		say_info("ping q:%i/c:%i %.4f%s",
			 r->quorum, r->count,
			 ev_now() - sent, r->count == 0 ? " TIMEOUT" : "");

		release_response(r);
	}
}

static void
collect_response(u32 k, struct iproto *msg)
{
	struct iproto_response *r = mh_i32_value(response_registry, k);
	if (r->closed > 0 && ev_now() - r->closed > 1) {
		say_warn("%s: stale reply: op:%x sync:%i q:%i/c:%i %.4f", __func__,
			 msg->msg_code, msg->sync, r->quorum, r->count,
			 ev_now() - r->closed);
		return;
	}

	r->reply[r->count++] = msg;
	if (r->count == r->quorum) {
		ev_timer_stop(&r->timeout);
		// say_debug("%s: quorum reached", __func__);
		r->closed = ev_now();
		fiber_wake(r->waiter, r);
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
				say_debug("closing conn r:%i errno:%i", (int)r, errno);
			else {
				say_debug("peer %s disconnected, fd:%i", p->name, c->fd);
			}
			conn_close(c);
			continue;
		}
		while (tbuf_len(c->rbuf) >= sizeof(struct iproto) &&
		       tbuf_len(c->rbuf) >= sizeof(struct iproto) + iproto(c->rbuf)->data_len)
		{
			struct iproto *msg = c->rbuf->ptr;
			tbuf_ltrim(c->rbuf, sizeof(struct iproto) + msg->data_len);

			u32 k = mh_i32_get(response_registry, msg->sync);
			if (k != mh_end(response_registry)) {
				say_debug("peer:%s op:%x sync:%i", p->name, msg->msg_code, msg->sync);
				collect_response(k, msg);
			} else {
				say_debug("peer:%s op:%x sync:%i STALE", p->name, msg->msg_code, msg->sync);
			}
		}
	}
}

void
iproto_rendevouz(va_list ap)
{
	struct sockaddr_in *self_addr = va_arg(ap, struct sockaddr_in *);
	struct iproto_peer *peers = va_arg(ap, struct iproto_peer *);

loop:
	for (struct iproto_peer *p = peers; p; p = p->next) {
		if (p->c.fd > 0)
			continue;

		int fd = tcp_connect(&p->addr, self_addr, 5);
		assert(p->c.fd < 0);

		if (fd > 0) {
			p->c.fd = fd;
			ev_io_set(&p->c.out, fd, EV_WRITE);
			ev_io_set(&p->c.in, fd, EV_READ);
			ev_io_start(&p->c.in);
			say_info("connect with %s %s", p->name, sintoa(&p->addr));
			p->connect_err_said = false;
		} else {
			if (!p->connect_err_said)
				say_syserror("connect!");
			p->connect_err_said = true;
		}
	}

	fiber_sleep(0.1); /* no more then one reconnect in second */
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

register_source();
