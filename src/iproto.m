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
#import <shard.h>

#import <cfg/defs.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#define STAT(_) \
        _(IPROTO_WORKER_STARVATION, 1)			\
	_(IPROTO_STREAM_OP, 2)				\
	_(IPROTO_BLOCK_OP, 3)

enum iproto_stat ENUM_INITIALIZER(STAT);
static char * const stat_ops[] = ENUM_STR_INITIALIZER(STAT);
static int stat_base;

struct worker_arg {
	iproto_cb cb;
	struct iproto *r;
	struct netmsg_io *io;
	void *arg;
};

static int
exc_rc(Error *e)
{
	if ([e respondsTo:@selector(code)])
		return [(id)e code];
	if ([e isMemberOf:[IndexError class]])
		return ERR_CODE_ILLEGAL_PARAMS;
	return ERR_CODE_UNKNOWN_ERROR;
}

void
iproto_worker(va_list ap)
{
	struct iproto_service *service = va_arg(ap, typeof(service));
	struct worker_arg a;

	for (;;) {
		SLIST_INSERT_HEAD(&service->workers, fiber, worker_link);

		memcpy(&a, yield(), sizeof(a));
		size_t req_size = sizeof(struct iproto) + a.r->data_len;
		a.r = memcpy(palloc(fiber->pool, req_size), a.r, req_size);

		@try {
			netmsg_io_retain(a.io);
			fiber->ushard = a.r->shard_id;
			a.cb(&a.io->wbuf, a.r, a.arg);
		}
		@catch (Error *e) {
			/* FIXME: where is no way to rollback modifications of wbuf.
			   cb() must not throw any exceptions after it modified wbuf */

			iproto_error(&a.io->wbuf, a.r, exc_rc(e), e->reason);
			[e release];
		}
		@finally {
			fiber->ushard = -1;
#ifndef IPROTO_PESSIMISTIC_WRITES
			if (a.io->wbuf.bytes > 0 && a.io->fd > 0) {
				ssize_t r = netmsg_writev(a.io->fd, &a.io->wbuf);
				if (r < 0) {
					say_syswarn("writev() to %s failed, closing connection",
						    net_peer_name(a.io->fd));
					[a.io close];
				}
			}
#endif
			if (a.io->wbuf.bytes > 0 && a.io->fd > 0)
				ev_io_start(&a.io->out);
			netmsg_io_release(a.io);
		}

		fiber_gc();
	}
}


static void
err(struct netmsg_head *h __attribute__((unused)), struct iproto *r, void *arg __attribute__((unused)))
{
	iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown iproto command %i", r->msg_code);
}

void
iproto_ping(struct netmsg_head *h, struct iproto *r, void *arg __attribute__((unused)))
{
	net_add_iov_dup(h, r, sizeof(struct iproto));
}

@implementation iproto_ingress
- (void)
init:(int)fd_ pool:(struct palloc_pool *)pool_
{
	say_debug2("%s: peer %s", __func__, net_peer_name(fd_));
	netmsg_io_init(self, pool_, fd_);
	ev_io_start(&in);
}

- (void)
packet_ready:(struct iproto *)msg
{
	(void)msg;
}

- (void)
data_ready
{
	while (1) {
		struct iproto *msg = rbuf.ptr;
		int len = tbuf_len(&rbuf);
		if (len < sizeof(struct iproto) || len < sizeof(struct iproto) + msg->data_len)
			return;

		tbuf_ltrim(&rbuf, sizeof(struct iproto) + msg->data_len);
		[self packet_ready:msg];
	}
}

- (void)
close
{
	iproto_future_collect_orphans(&waiting);
	[super close];
}
@end

@implementation iproto_ingress_svc
- (void)
data_ready
{
	/* client->service->processing will be traversed by wakeup_workers() */
	if (processing_link.tqe_prev == NULL)
		TAILQ_INSERT_TAIL(&service->processing, self, processing_link);
}

- (void)
close
{
	if (processing_link.tqe_prev != NULL) {
		TAILQ_REMOVE(&service->processing, self, processing_link);
		processing_link.tqe_prev = NULL;
	}
	LIST_REMOVE(self, link);
	[super close];
	netmsg_io_release(self);
}

- (void)
init:(int)fd_ service:(struct iproto_service *)service_
{
	say_debug2("%s: service:%s peer:%s", __func__, service_->name, net_peer_name(fd_));
	service = service_;
	netmsg_io_init(self, service->pool, fd_);
	self->flags |= NETMSG_IO_SHARED_POOL;
	LIST_INSERT_HEAD(&service->clients, self, link);
	ev_io_start(&in);
}
@end

static void
service_gc(struct palloc_pool *pool, void *ptr)
{
	struct iproto_service *s = ptr;
	struct iproto_ingress_svc *c;

	s->pool = pool;
	LIST_FOREACH(c, &s->clients, link)
		netmsg_io_gc(pool, c);
}

static void
iproto_accept_client(int fd, void *data)
{
	struct iproto_service *service = data;
	[[service->ingress_class alloc] init:fd service:service];
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

static void iproto_wakeup_workers(ev_prepare *ev);
void
iproto_service(struct iproto_service *service, const char *addr)
{
	char *name = xmalloc(strlen("iproto:") + strlen(addr) + 1);
	sprintf(name, "tcp:%s", addr);

	TAILQ_INIT(&service->processing);
	service->pool = palloc_create_pool((struct palloc_config){.name = name});
	service->name = name;
	service->batch = 32;
	service->addr = strdup(addr);

	palloc_register_gc_root(service->pool, service, service_gc);

	if (service->ingress_class == Nil)
		service->ingress_class = [iproto_ingress_svc class];
	service->acceptor = fiber_create("tcp/acceptor", tcp_server, addr,
					 iproto_accept_client, service->on_bind, service);
	if (service->acceptor == NULL)
		panic("unable to start tcp_service `%s'", addr);

	ev_prepare_init(&service->wakeup, (void *)iproto_wakeup_workers);
	ev_prepare_start(&service->wakeup);

	service_alloc_handlers(service, SERVICE_DEFAULT_CAPA);

	service_register_iproto(service, -1, err, IPROTO_NONBLOCK|IPROTO_LOCAL);
	service_register_iproto(service, MSG_PING, iproto_ping, IPROTO_NONBLOCK|IPROTO_LOCAL);
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

void
service_register_iproto(struct iproto_service *s, u32 cmd, iproto_cb cb, int flags)
{
	service_set_handler(s, (struct iproto_handler){
			.code = cmd,
			.cb = cb,
			.flags = flags
		});
}

static int
has_full_req(const struct tbuf *buf)
{
	return tbuf_len(buf) >= sizeof(struct iproto) &&
	       tbuf_len(buf) >= sizeof(struct iproto) + iproto(buf)->data_len;
}


static int
local(struct iproto *msg, struct shard_route *route, struct iproto_ingress_svc *io, struct iproto_handler *ih)
{
	say_debug3("%s: peer:%s op:0x%x sync:%u%s%s", __func__,
		   net_peer_name(io->fd), msg->msg_code, msg->sync,
		   ih->flags & IPROTO_NONBLOCK ? " NONBLOCK" : "",
		   ih->flags & IPROTO_LOCAL ? " LOCAL" : "");
	if (ih->flags & IPROTO_NONBLOCK) {
		stat_collect(stat_base, IPROTO_STREAM_OP, 1);
		struct netmsg_mark header_mark;
		netmsg_getmark(&io->wbuf, &header_mark);
		@try {
			fiber->ushard = msg->shard_id;
			ih->cb(&io->wbuf, msg, route);
		}
		@catch (Error *e) {
			netmsg_rewind(&io->wbuf, &header_mark);
			iproto_error(&io->wbuf, msg, exc_rc(e), e->reason);
			[e release];
		}
		@finally {
			fiber->ushard = -1;
		}
	} else {
		struct iproto_service *service = io->service;
		struct Fiber *w = SLIST_FIRST(&service->workers);
		if (!w) {
			stat_collect(stat_base, IPROTO_WORKER_STARVATION, 1);
			// FIXME: need state for this
			return 0;
		}

		stat_collect(stat_base, IPROTO_BLOCK_OP, 1);
		SLIST_REMOVE_HEAD(&service->workers, worker_link);
		resume(w, (&(struct worker_arg){ih->cb, msg, io, route}));
		io->batch--;
	}
	return 1;
}

static int
error(struct iproto *msg, struct netmsg_io *io, const char *err)
{
	iproto_error(&io->wbuf, msg, ERR_CODE_NONMASTER, err);
	return 1;
}

static int
classify(struct iproto *msg, struct iproto_ingress_svc *io)
{
	struct shard_route *route;
	struct iproto_handler *ih;
	struct iproto *orig_msg = msg;
	struct iproto_egress *proxy;
	Shard<Shard> *shard;
	@try {
		if (msg->msg_code == MSG_IPROXY)
			msg++; // unwrap
		fiber->ushard = msg->shard_id;
		say_debug2("%s: %s peer:%s op:0x%x sync:%u  ", __func__, msg == orig_msg ? "" : "PROXY",
			   net_peer_name(io->fd), msg->msg_code, msg->sync);
		if (unlikely(msg->shard_id > nelem(shard_rt)))
			return error(msg, io, "no such shard");
		route = shard_rt + msg->shard_id;
		proxy = route->proxy;
		shard = route->shard;
		if (unlikely(shard && shard->loading))
			shard = nil;

		ih = service_find_code(io->service, msg->msg_code);
		if (ih->flags & IPROTO_LOCAL)
			goto local;
		if (orig_msg == msg) { /* not via proxy */
			if (proxy && (shard == nil || ih->flags & IPROTO_ON_MASTER)) {
				if (proxy == (void *)0x1)
					return error(msg, io, "replica is readonly");
				return !!iproto_proxy_send(proxy, io, MSG_IPROXY, msg, NULL, 0);
			}
			if (shard == nil)
				return error(msg, io, "no such shard");
		} else {
			if (shard == nil || (proxy && ih->flags & IPROTO_ON_MASTER))
				return error(msg, io, "route loop");
		}
	local:
		return local(msg, route, io, ih);
	}
	@finally {
		fiber->ushard = -1;
	}

}

static void
process_requests(struct iproto_service *service, struct iproto_ingress_svc *io)
{
	netmsg_io_retain(io);
	io->batch = service->batch;
	while (has_full_req(&io->rbuf))
	{
		struct iproto *msg = iproto(&io->rbuf);
		size_t msg_size = sizeof(struct iproto) + msg->data_len;

		if (classify(msg, io))
			tbuf_ltrim(&io->rbuf, msg_size);
		else
			break;

		if (io->batch <= 0)
			break;
	}

	if (unlikely(io->fd == -1)) /* handler may close connection */
		goto out;

	if (tbuf_len(&io->rbuf) >= cfg.input_low_watermark && has_full_req(&io->rbuf))
		ev_io_stop(&io->in);

	if (!has_full_req(&io->rbuf))
	{
		TAILQ_REMOVE(&service->processing, io, processing_link);
		io->processing_link.tqe_prev = NULL;

		/* input buffer is empty or has partially read oversize request */
		ev_io_start(&io->in);
	} else if (io->batch < service->batch) {
		/* avoid unfair scheduling in case of absense of stream requests
		   and all workers being busy */
		TAILQ_REMOVE(&service->processing, io, processing_link);
		TAILQ_INSERT_TAIL(&service->processing, io, processing_link);
	}

#ifndef IPROTO_PESSIMISTIC_WRITES
	if (io->wbuf.bytes > 0) {
		ssize_t r = netmsg_writev(io->fd, &io->wbuf);
		if (r < 0) {
			say_syswarn("writev() to %s failed, closing connection",
				    net_peer_name(io->fd));
			[io close];
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
	struct iproto_ingress_svc *c, *tmp, *last;
	struct palloc_pool *saved_pool = fiber->pool;
	assert(saved_pool == sched->pool);

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
	*header = (struct iproto_retcode){ .shard_id = request->shard_id,
					   .msg_code = request->msg_code,
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
	*header = (struct iproto_retcode){ .shard_id = request->shard_id,
					   .msg_code = request->msg_code,
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
	say_debug2("	%s", err);
}

void
iproto_error_fmt(struct netmsg_head *h, const struct iproto *request, u32 ret_code, const char *fmt, ...)
{
	static char buf[512];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	iproto_error(h, request, ret_code, buf);
}

void
iproto_service_info(struct tbuf *out, struct iproto_service *service)
{
	struct iproto_ingress_svc *c;
	struct netmsg *m;

	tbuf_printf(out, "%s:" CRLF, service->name);
	LIST_FOREACH(c, &service->clients, link) {
		struct netmsg_io *io = c;
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

@implementation IProtoError
- (IProtoError *)
init_code:(u32)code_
     line:(unsigned)line_
     file:(const char *)file_
{
	[self init_line:line_ file:file_];
	code = code_;
	return self;
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

#if CFG_primary_addr && CFG_primary_port && CFG_secondary_port
	if (strchr(cfg->primary_addr, ':') == NULL && !cfg->primary_port) {
		out_warning(0, "Option 'primary_port' is not set");
		return -1;
	}

	if (net_fixup_addr(&cfg->primary_addr, cfg->primary_port) < 0)
		out_warning(0, "Option 'primary_addr' is overridden by 'primary_port'");

	if (net_fixup_addr(&cfg->secondary_addr, cfg->secondary_port) < 0)
		out_warning(0, "Option 'secondary_addr' is overridden by 'secondary_port'");
#endif
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
