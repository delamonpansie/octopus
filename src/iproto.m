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
#import <net_io.h>

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>


const uint32_t msg_ping = 0xff00;
STRS(error_codes, ERROR_CODES);
DESC_STRS(error_codes, ERROR_CODES);

static struct tbuf *
iproto_parse(struct tbuf *in)
{
	if (tbuf_len(in) < sizeof(struct iproto_header))
		return NULL;
	if (tbuf_len(in) < sizeof(struct iproto_header) + iproto(in)->len)
		return NULL;

	return tbuf_split(in, sizeof(struct iproto_header) + iproto(in)->len);
}


void
iproto_interact(va_list ap)
{
	struct service *service = va_arg(ap, struct service *);
	void (*callback)(struct conn *c, struct tbuf *request) =
		va_arg(ap, void (*)(struct conn *c, struct tbuf *request));
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
		ev_io_start(&c->in);
		goto next;
	} else {
		TAILQ_INSERT_TAIL(&service->processing, c, processing_link);
	}

	u32 msg_code = iproto(request)->msg_code;
	if (unlikely(msg_code == msg_ping)) {
		struct netmsg *m = netmsg_tail(&c->out_messages);
		iproto(request)->len = 0;
		net_add_iov_dup(&m, request->data, sizeof(struct iproto_header));
	} else {
		c->ref++;
		callback(c, request);
		c->ref--;
		if (c->fd < 0) {
			conn_close(c);
			goto next;
		}
	}

	if (!TAILQ_EMPTY(&c->out_messages.q))
		ev_io_start(&c->out);

	if (palloc_allocated(service->pool) > 64 * 1024 * 1024) /* FIXME: do it after change of that size */
		palloc_gc(service->pool);

	fiber_gc();
	goto next;
}


void
iproto_reply(struct netmsg **m, u32 msg_code, u32 sync)
{
	struct iproto_header_retcode *h = palloc((*m)->head->pool, sizeof(*h));
	net_add_iov(m, h, sizeof(*h));
	h->msg_code = msg_code;
	h->len = sizeof(h->ret_code);
	h->sync = sync;
}

void
iproto_commit(struct netmsg_mark *mark)
{
	struct netmsg *m = mark->m;
	struct iproto_header_retcode *h = m->iov[mark->offset].iov_base;
	int len = 0, offset = mark->offset + 1;
	do {
		for (int i = offset; i < m->count; i++) {
			len += m->iov[i].iov_len;
			assert(i < 1024);
		}
		offset = 0;
	} while ((m = TAILQ_NEXT(m, link)) != NULL);
	h->ret_code = ERR_CODE_OK;
	h->len += len;
}

void
iproto_error(struct netmsg **m, struct netmsg_mark *header_mark, u32 ret_code, const char *err)
{
	struct netmsg *h = header_mark->m;
	netmsg_rewind(m, header_mark);
	h->iov[header_mark->offset].iov_len = sizeof(struct iproto_header_retcode);
	struct iproto_header_retcode *header = h->iov[header_mark->offset].iov_base;
	header->len = sizeof(u32);
	header->ret_code = ret_code;
	if (err && strlen(err) > 0) {
		header->len += strlen(err);
		net_add_iov(m, err, strlen(err));
	}
}


static void
input_reader(va_list ap)
{
	struct service *service = va_arg(ap, struct service *);
	struct conn *c;
	ev_watcher *w;
	ssize_t r;

	say_info("input reader for service %p started", service);
	yield();
loop:
	w = yield();
	c = w->data;

	tbuf_ensure(c->rbuf, 12 * 1024);
	r = read(c->fd, c->rbuf->data + tbuf_len(c->rbuf), c->rbuf->size - tbuf_len(c->rbuf));

	if (r > 0) {
		c->rbuf->len += r;
		if (tbuf_len(c->rbuf) >= sizeof(struct iproto_header)) {
			if (tbuf_len(c->rbuf) > 8 * 1024)
				ev_io_stop(&c->in);
			if (c->state != PROCESSING) {
				TAILQ_INSERT_HEAD(&c->service->processing, c, processing_link);
				c->state = PROCESSING;
			}
		}
	} else if (r == 0 || /* r < 0 && */ (errno != EAGAIN && errno != EWOULDBLOCK)) {
		say_debug("closing conn r:%i errno:%i", (int)r, errno);
		conn_close(c);
	}

	goto loop;
}

static void
wakeup_workers(ev_prepare *ev)
{
	struct service *service = (void *)ev - offsetof(struct service, wakeup);
	struct fiber *w;

	while (!TAILQ_EMPTY(&service->processing)) {
		w = SLIST_FIRST(&service->workers);
		if (w == NULL)
			return;
		SLIST_REMOVE_HEAD(&service->workers, worker_link);
		resume(w, NULL);
	}
}

static void
service_gc(struct palloc_pool *pool, void *ptr)
{
	struct service *s = ptr;
	struct conn *c;

	s->pool = pool;
	LIST_FOREACH(c, &s->conn, link)
		conn_gc(pool, c);
}

static void
accept_client(int fd, void *data)
{
	struct service *service = data;
	struct conn *clnt = conn_create(service->pool, fd);
	LIST_INSERT_HEAD(&service->conn, clnt, link);
	clnt->service = service;
	ev_io_init(&clnt->out, (void *)service->output_flusher, fd, EV_WRITE);
	ev_io_init(&clnt->in, (void *)service->input_reader, fd, EV_READ);
	ev_io_start(&clnt->in);
	clnt->state = READING;
}

struct service *
iproto_service(u16 port, void (*on_bind)(int fd))
{
	struct service *service = calloc(1, sizeof(*service));
	char *name = malloc(13);  /* strlen("iproto:xxxxx") */
	snprintf(name, 13, "iproto:%i", port);

	TAILQ_INIT(&service->processing);
	service->pool = palloc_create_pool(name);
	service->name = name;

	palloc_register_gc_root(service->pool, service, service_gc);

	service->output_flusher = fiber_create("iproto/output_flusher", service_output_flusher);
	service->input_reader = fiber_create("iproto/input_reader", input_reader, service);
	service->acceptor = fiber_create("iproto/acceptor",
					 tcp_server, port, accept_client, on_bind, service);

	ev_prepare_init(&service->wakeup, (void *)wakeup_workers);
	ev_prepare_start(&service->wakeup);

	return service;
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
