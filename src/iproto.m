/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

const uint32_t msg_ping = 0xff00;

#define STAT(_) \
        _(IPROTO_WORKER_STARVATION, 1)			\
	_(IPROTO_STREAM_OP, 2)				\
	_(IPROTO_BLOCK_OP, 3)

enum iproto_stat ENUM_INITIALIZER(STAT);
static char * const stat_ops[] = ENUM_STR_INITIALIZER(STAT);
static int stat_base;

struct worker_arg {
	void (*cb)(struct netmsg_head *wbuf, struct iproto *);
	struct iproto *r;
	struct netmsg_io *io;
};

void
iproto_worker(va_list ap)
{
	struct iproto_service *service = va_arg(ap, typeof(service));
	struct worker_arg a;

	for (;;) {
		SLIST_INSERT_HEAD(&service->workers, fiber, worker_link);
		memcpy(&a, yield(), sizeof(a));

		@try {
			netmsg_io_retain(a.io);
			a.cb(&a.io->wbuf, a.r);
		}
		@catch (IProtoClose *e) {
			netmsg_io_close(a.io);
		}
		@catch (Error *e) {
			/* FIXME: where is no way to rollback modifications of wbuf.
			   cb() must not throw any exceptions after it modified wbuf */
			u32 rc = ERR_CODE_UNKNOWN_ERROR;
			if ([e respondsTo:@selector(code)])
				rc = [(id)e code];
			else if ([e isMemberOf:[IndexError class]])
				rc = ERR_CODE_ILLEGAL_PARAMS;

			iproto_error(&a.io->wbuf, a.r, rc, e->reason);
		}
		@finally {
			if (a.io->wbuf.bytes > 0 && a.io->fd > 0)
				ev_io_start(&a.io->out);
			netmsg_io_release(a.io);
		}

		fiber_gc();
	}
}


static void
err(struct netmsg_head *h __attribute__((unused)), struct iproto *r)
{
	iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown iproto command %i", r->msg_code);
}

void
iproto_ping(struct netmsg_head *h, struct iproto *r)
{
	net_add_iov_dup(h, r, sizeof(struct iproto));
}

void
service_register_iproto(struct iproto_service *s, u32 cmd,
			void (*cb)(struct netmsg_head *, struct iproto *),
			int flags)
{
	service_set_handler(s, (struct iproto_handler){
			.code = cmd,
			.cb = cb,
			.flags = flags
		});
}

static inline void
service_alloc_handlers(struct iproto_service *s, int capa)
{
	int i;
	s->ih_size = 0;
	s->ih = xcalloc(capa, sizeof(struct iproto_handler));
	s->ih_mask = capa - 1;
	for(i = 0; i < capa; i++) {
		s->ih[i].code = -1;
	}
}

static void
iproto_data_ready(struct netmsg_io *io, int __attribute__((unused)) r)
{
	struct iproto_ingress *client = container_of(io, struct iproto_ingress, io);
	struct iproto_service *service = client->service;
	/* client->service->processing will be traversed by wakeup_workers() */
	if (client->processing_link.tqe_prev == NULL)
		TAILQ_INSERT_TAIL(&service->processing, client, processing_link);

}

static void
iproto_close(struct netmsg_io *io)
{
	struct iproto_ingress *client = container_of(io, struct iproto_ingress, io);
	if (client->processing_link.tqe_prev != NULL) {
		TAILQ_REMOVE(&client->service->processing, client, processing_link);
		client->processing_link.tqe_prev = NULL;
	}
	LIST_REMOVE(client, link);
	netmsg_io_release(io);
}

static void
iproto_dealloc(struct netmsg_io *io)
{
	struct iproto_ingress *client = container_of(io, struct iproto_ingress, io);
	free(client);
}

static struct netmsg_io_vop iproto_vop = { .data_ready = iproto_data_ready,
					   .close = iproto_close,
					   .dealloc = iproto_dealloc};

static void
accept_client(int fd, void *data)
{
	struct iproto_service *service = data;
	struct iproto_ingress *client = xcalloc(sizeof(*client), 1);

	say_debug2("%s: peer %s", __func__, net_peer_name(fd));
	client->service = service;
	netmsg_io_init(&client->io, service->pool, &iproto_vop, fd);
	ev_io_start(&client->io.in);
	LIST_INSERT_HEAD(&service->clients, client, link);
	netmsg_io_retain(&client->io);
}

static void
service_gc(struct palloc_pool *pool, void *ptr)
{
	struct iproto_service *s = ptr;
	struct iproto_ingress *c;

	s->pool = pool;
	LIST_FOREACH(c, &s->clients, link)
		netmsg_io_gc(pool, c);
}

static void iproto_wakeup_workers(ev_prepare *ev);
void
iproto_service(struct iproto_service *service, const char *addr, void (*on_bind)(int fd))
{
	memset(service, 0, sizeof(*service));
	char *name = xmalloc(strlen("iproto:") + strlen(addr) + 1);
	sprintf(name, "tcp:%s", addr);

	TAILQ_INIT(&service->processing);
	service->pool = palloc_create_pool(name);
	service->name = name;
	service->batch = 32;

	palloc_register_gc_root(service->pool, service, service_gc);

	service->acceptor = fiber_create("tcp/acceptor", tcp_server, addr, accept_client, on_bind, service);
	if (service->acceptor == NULL)
		panic("unable to start tcp_service `%s'", addr);

	ev_prepare_init(&service->wakeup, (void *)iproto_wakeup_workers);
	ev_prepare_start(&service->wakeup);

	service_alloc_handlers(service, SERVICE_DEFAULT_CAPA);

	service_register_iproto(service, -1, err, IPROTO_NONBLOCK);
	service_register_iproto(service, msg_ping, iproto_ping, IPROTO_NONBLOCK);
}

void
service_set_handler(struct iproto_service *s, struct iproto_handler h)
{
	if (h.code == -1) {
		free(s->ih);
		service_alloc_handlers(s, SERVICE_DEFAULT_CAPA);
		s->default_handler = h;
		return;
	}
	if (s->ih_size > s->ih_mask / 3) {
		struct iproto_handler *old_ih = s->ih;
		int i, old_n = s->ih_mask + 1;
		service_alloc_handlers(s, old_n * 2);
		for(i = 0; i < old_n; i++) {
			if (old_ih[i].code != -1) {
				service_set_handler(s, old_ih[i]);
			}
		}
		free(old_ih);
	}
	int pos = h.code & s->ih_mask;
	int dlt = (h.code % s->ih_mask) | 1;
	while(s->ih[pos].code != h.code && s->ih[pos].code != -1)
		pos = (pos + dlt) & s->ih_mask;
	if (s->ih[pos].code != h.code)
		s->ih_size++;
	s->ih[pos] = h;
}

static int
has_full_req(const struct tbuf *buf)
{
	return tbuf_len(buf) >= sizeof(struct iproto) &&
	       tbuf_len(buf) >= sizeof(struct iproto) + iproto(buf)->data_len;
}

static void
process_requests(struct iproto_service *service, struct iproto_ingress *c)
{
	struct netmsg_io *io = &c->io;
	int batch = service->batch;

	netmsg_io_retain(io);
	while (has_full_req(&io->rbuf))
	{
		struct iproto *request = iproto(&io->rbuf);
		struct iproto_handler *ih = service_find_code(service, request->msg_code);
		size_t req_size = sizeof(struct iproto) + request->data_len;

		if (ih->flags & IPROTO_NONBLOCK) {
			tbuf_ltrim(&io->rbuf, req_size);
			stat_collect(stat_base, IPROTO_STREAM_OP, 1);
			struct netmsg_mark header_mark;
			netmsg_getmark(&io->wbuf, &header_mark);
			@try {
				ih->cb(&io->wbuf, request);
			}
			@catch (IProtoClose *e) {
				netmsg_io_close(io);
				goto out;
			}
			@catch (Error *e) {
				u32 rc = ERR_CODE_UNKNOWN_ERROR;
				if ([e respondsTo:@selector(code)])
					rc = [(id)e code];
				else if ([e isMemberOf:[IndexError class]])
					rc = ERR_CODE_ILLEGAL_PARAMS;

				netmsg_rewind(&io->wbuf, &header_mark);
				iproto_error(&io->wbuf, request, rc, e->reason);
			}
		} else {
			struct fiber *w = SLIST_FIRST(&service->workers);
			if (w) {
				tbuf_ltrim(&io->rbuf, req_size);
				stat_collect(stat_base, IPROTO_BLOCK_OP, 1);
				void *request_copy = memcpy(palloc(w->pool, req_size), request, req_size);

				SLIST_REMOVE_HEAD(&service->workers, worker_link);
				resume(w, &(struct worker_arg){ih->cb, request_copy, io});
			} else {
				stat_collect(stat_base, IPROTO_WORKER_STARVATION, 1);
				break; // FIXME: need state for this
			}

			if (--batch == 0)
				break;
		}
	}

	if (unlikely(io->fd == -1)) /* handler may close connection */
		goto out;

	if (tbuf_len(&io->rbuf) >= cfg.input_low_watermark && has_full_req(&io->rbuf))
		ev_io_stop(&io->in);

	if (!has_full_req(&io->rbuf))
	{
		TAILQ_REMOVE(&service->processing, c, processing_link);
		c->processing_link.tqe_prev = NULL;

		/* input buffer is empty or has partially read oversize request */
		ev_io_start(&io->in);
	} else if (batch < service->batch) {
		/* avoid unfair scheduling in case of absense of stream requests
		   and all workers being busy */
		TAILQ_REMOVE(&service->processing, c, processing_link);
		TAILQ_INSERT_TAIL(&service->processing, c, processing_link);
	}

#ifndef IPROTO_PESSIMISTIC_WRITES
	if (io->wbuf.bytes > 0) {
		ssize_t r = netmsg_writev(io->fd, &io->wbuf);
		if (r < 0) {
			say_syswarn("writev() to %s failed, closing connection",
				    net_peer_name(io->fd));
			netmsg_io_close(io);
			goto out;
		}
	}
#endif

	if (io->wbuf.bytes > 0) {
		ev_io_start(&io->out);

		/* Prevent output owerflow by start reading if
		   output size is below output_low_watermark.
		   Otherwise output flusher will start reading,
		   when size of output is small enought  */
		if (io->wbuf.bytes >= cfg.output_high_watermark)
			ev_io_stop(&io->in);
	}
out:
	netmsg_io_release(io);
}

static void
iproto_wakeup_workers(ev_prepare *ev)
{
	struct iproto_service *service = (void *)ev - offsetof(struct iproto_service, wakeup);
	struct iproto_ingress *c, *tmp, *last;
	struct palloc_pool *saved_pool = fiber->pool;
	assert(saved_pool == sched.pool);

	fiber->pool = service->pool;

	last = TAILQ_LAST(&service->processing, ingress_tailq);
	TAILQ_FOREACH_SAFE(c, &service->processing, processing_link, tmp) {
		process_requests(service, c);
		/* process_requests() may move *c to the end of tailq */
		if (c == last) break;
	}

	fiber->pool = saved_pool;

	size_t diff = palloc_allocated(service->pool) - service->pool_allocated;
	if (diff > 4 * 1024 * 1024 && diff > service->pool_allocated) {
		palloc_gc(service->pool);
		service->pool_allocated = palloc_allocated(service->pool);
	}
}


struct iproto_retcode *
iproto_reply(struct netmsg_head *h, const struct iproto *request, u32 ret_code)
{
	struct iproto_retcode *header = palloc(h->pool, sizeof(*header));
	net_add_iov(h, header, sizeof(*header));
	*header = (struct iproto_retcode){ .msg_code = request->msg_code,
					   .data_len = h->bytes,
					   .sync = request->sync,
					   .ret_code = ret_code };
	return header;
}

struct iproto_retcode *
iproto_reply_small(struct netmsg_head *h, const struct iproto *request, u32 ret_code)
{
	struct iproto_retcode *header = palloc(h->pool, sizeof(*header));
	net_add_iov(h, header, sizeof(*header));
	*header = (struct iproto_retcode){ .msg_code = request->msg_code,
					   .data_len = sizeof(ret_code),
					   .sync = request->sync,
					   .ret_code = ret_code };
	return header;
}

void
iproto_reply_fixup(struct netmsg_head *h, struct iproto_retcode *reply)
{
	reply->data_len = h->bytes - reply->data_len + sizeof(reply->ret_code);
}


void
iproto_error(struct netmsg_head *h, const struct iproto *request, u32 ret_code, const char *err)
{
	struct iproto_retcode *header = iproto_reply(h, request, ret_code);
	if (err && strlen(err) > 0)
		net_add_iov_dup(h, err, strlen(err));
	iproto_reply_fixup(h, header);
	say_debug("%s: op:0x%02x data_len:%i sync:%i ret:%i", __func__,
		  header->msg_code, header->data_len, header->sync, header->ret_code);
}

void
iproto_service_info(struct tbuf *out, struct iproto_service *service)
{
	struct iproto_ingress *c;
	struct netmsg *m;

	tbuf_printf(out, "%s:" CRLF, service->name);
	LIST_FOREACH(c, &service->clients, link) {
		struct netmsg_io *io = &c->io;
		tbuf_printf(out, "    - peer: %s" CRLF, net_peer_name(io->fd));
		tbuf_printf(out, "      fd: %i" CRLF, io->fd);
		tbuf_printf(out, "      state: %s%s" CRLF,
			    ev_is_active(&io->in) ? "in" : "",
			    ev_is_active(&io->out) ? "out" : "");
		tbuf_printf(out, "      rbuf: %i" CRLF, tbuf_len(&io->rbuf));
		tbuf_printf(out, "      pending_bytes: %zi" CRLF, io->wbuf.bytes);
		if (!TAILQ_EMPTY(&io->wbuf.q))
			tbuf_printf(out, "      out_messages:" CRLF);
		TAILQ_FOREACH(m, &io->wbuf.q, link)
			tbuf_printf(out, "      - { count: %i }" CRLF, m->count);
	}
}



@implementation IProtoClose
@end

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

static int
iproto_fixup_addr(struct octopus_cfg *cfg)
{
	extern void out_warning(int v, char *format, ...);

	if (cfg->primary_addr == NULL) {
		out_warning(0, "Option 'primary_addr' can't be NULL");
		return -1;
	}

	if (strchr(cfg->primary_addr, ':') == NULL && !cfg->primary_port) {
		out_warning(0, "Option 'primary_port' is not set");
		return -1;
	}

	if (net_fixup_addr(&cfg->primary_addr, cfg->primary_port) < 0)
		out_warning(0, "Option 'primary_addr' is overridden by 'primary_port'");

	if (net_fixup_addr(&cfg->secondary_addr, cfg->secondary_port) < 0)
		out_warning(0, "Option 'secondary_addr' is overridden by 'secondary_port'");

	return 0;
}

static struct tnt_module iproto_mod = {
	.check_config = iproto_fixup_addr
};

register_module(iproto_mod);

void __attribute__((constructor))
iproto_init(void)
{
	stat_base = stat_register(stat_ops, nelem(stat_ops));
}

register_source();
