/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016, 2017 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016, 2017 Yuriy Vostrikov
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
#import <log_io.h>

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
	_(IPROTO_BLOCK_OP, 3)                           \
	_(IPROTO_CONNECTED, 4)                          \
	_(IPROTO_DISCONNECTED, 5)                       \
	_(IPROTO_WRITTEN, 6)                            \
	_(IPROTO_READ, 7)


enum iproto_stat ENUM_INITIALIZER(STAT);
static char const * const stat_ops[] = ENUM_STR_INITIALIZER(STAT);
static int stat_base;
static int stat_cb_base;

struct worker_arg {
	struct iproto_handler *ih;
	struct iproto *r;
	struct iproto_ingress_svc *io;
};

static int
exc_rc(Error *e)
{
	if ([e respondsTo:@selector(code)])
		return [(id)e code];
#if OCT_INDEX
	if ([e isMemberOf:[IndexError class]])
		return ERR_CODE_ILLEGAL_PARAMS;
#endif
	return ERR_CODE_UNKNOWN_ERROR;
}

static int
error(struct iproto_ingress_svc *io, struct iproto *msg, int rc, const char *err)
{
	struct iproto_handler *ih = service_find_code(io->service, msg->msg_code);
	if ((ih->flags & IPROTO_DROP_ERROR) == 0)
		iproto_error(&io->wbuf, msg, rc, err);
	return 1;
}

struct iproto *iproto_rbuf_req(struct netmsg_io *io)
{
	int len = rbuf_len(io);
	if (len < sizeof(struct iproto))
		return NULL;
	struct iproto *msg = io->rbuf.ptr;
	if (len < sizeof(*msg) + msg->data_len)
		return NULL;
	return msg;
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
		fiber->ushard = a.r->shard_id;
		netmsg_io_retain(a.io);

		struct rwlock *lock = &(shard_rt + a.r->shard_id)->lock;
		if ((a.ih->flags & IPROTO_WLOCK) == 0)
			rlock(lock);
		else
			wlock(lock);
#if CFG_warn_cb_time
		ev_tstamp start = ev_now();
#endif
		@try {
			a.ih->cb(&a.io->wbuf, a.r);
		}
		@catch (Error *e) {
			/* FIXME: where is no way to rollback modifications of wbuf.
			   cb() must not throw any exceptions after it modified wbuf */

			iproto_error(&a.io->wbuf, a.r, exc_rc(e), e->reason);
			[e release];
		}
#if CFG_warn_cb_time
		if (ev_now() - start > cfg.warn_cb_time)
			say_warn("too long IPROTO:%i %.3f sec", a.r->msg_code, ev_now() - start);
#endif

		if (a.io->fd >= 0 && a.io->prepare_link.le_prev == NULL) {
			LIST_INSERT_HEAD(&service->prepare, a.io, prepare_link);
			ev_io_start(&a.io->out);
		}

		if ((a.ih->flags & IPROTO_WLOCK) == 0)
			runlock(lock);
		else
			wunlock(lock);
		netmsg_io_release(a.io);
		fiber->ushard = -1;

		fiber_gc();

		if (unlikely(fiber->worker_link.sle_next == (void *)(uintptr_t)0xead))
			return;
	}
}


static void
err(struct netmsg_head *h __attribute__((unused)), struct iproto *r)
{
	iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown iproto command 0x%x", r->msg_code);
}

void
iproto_ping(struct netmsg_head *h, struct iproto *r)
{
	net_add_iov_dup(h, r, sizeof(struct iproto));
}

@implementation iproto_ingress
- (id)
init:(int)fd_ pool:(struct netmsg_pool_ctx *)ctx_
{
	say_trace("%s: peer %s", __func__, net_fd_name(fd_));
	netmsg_io_init(self, ctx_, fd_);
	ev_io_start(&in);
	return self;
}

- (void)
packet_ready:(struct iproto *)msg
{
	(void)msg;
}

- (void)
data_ready
{
	struct iproto *msg;
	while ((msg = iproto_rbuf_req(self))) {
		[self packet_ready:msg];
		rbuf_ltrim(self, sizeof(*msg) + msg->data_len);
	}
}

- (void)
close
{
	iproto_future_collect_orphans(&waiting);
	[super close];
}
@end

static int ingress_cnt = 0;
static void
report_ingress_cnt(int base _unused_)
{
	stat_report_gauge("IPROTO_CLIENTS", sizeof("IPROTO_CLIENTS"), ingress_cnt);
}

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
	ingress_cnt--;
	stat_sum_static(stat_base, IPROTO_DISCONNECTED, 1);
	if (processing_link.tqe_prev != NULL) {
		TAILQ_REMOVE(&service->processing, self, processing_link);
		processing_link.tqe_prev = NULL;
	}
	if (prepare_link.le_prev != NULL) {
		LIST_REMOVE(self, prepare_link);
		prepare_link.le_prev = NULL;
	}
	LIST_REMOVE(self, link);
	[super close];
	netmsg_io_release(self);
}

static void
iproto_service_svc_read_cb(ev_io *ev, int events)
{
	ssize_t r = netmsg_io_read_for_cb(ev, events);
	if (r > 0)
		stat_sum_static(stat_base, IPROTO_READ, r);
}

static void
iproto_service_svc_write_cb(ev_io *ev, int events)
{
	struct netmsg_io *io = container_of(ev, struct netmsg_io, out);

	netmsg_io_retain(io);
	ssize_t r = netmsg_io_write_for_cb(ev, events);
	if (r > 0)
		stat_sum_static(stat_base, IPROTO_WRITTEN, r);

	if (io->fd >= 0 &&
            ((rbuf_len(io) < cfg.input_low_watermark) || !iproto_rbuf_req(io) ) &&
	    io->wbuf.bytes < cfg.output_low_watermark)
		ev_io_start(&io->in);
	netmsg_io_release(io);
}

- (id)
init:(int)fd_ pool:(struct palloc_pool *)pool_
{
	(void)fd_;
	(void)pool_;
	assert(false);
}

- (void)
init:(int)fd_ service:(struct iproto_service *)service_
{
	say_trace("%s: service:%s peer:%s", __func__, service_->name, net_fd_name(fd_));
	ingress_cnt++;
	stat_sum_static(stat_base, IPROTO_CONNECTED, 1);
	service = service_;
	netmsg_io_init(self, &service->ctx, fd_);
	ev_init(&self->in, iproto_service_svc_read_cb);
	ev_init(&self->out, iproto_service_svc_write_cb);
	self->flags |= NETMSG_IO_SHARED_POOL;
	LIST_INSERT_HEAD(&service->clients, self, link);
	ev_io_start(&in);
}
@end

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
static void iproto_write_data(ev_prepare *ev);
void
iproto_service(struct iproto_service *service, const char *addr)
{
	char *name = xmalloc(strlen("iproto:") + strlen(addr) + 1);
	sprintf(name, "iproto:%s", addr);

	TAILQ_INIT(&service->processing);
	netmsg_pool_ctx_init(&service->ctx, name, 2 * 1024 * 1024);
	service->name = name;
	service->batch = 32;
	service->addr = strdup(addr);

	if (service->ingress_class == Nil)
		service->ingress_class = [iproto_ingress_svc class];
	service->acceptor = fiber_create("iproto/acceptor", tcp_server, addr,
					 iproto_accept_client, service->on_bind, service);
	if (service->acceptor == NULL)
		panic("unable to start iproto_service `%s'", addr);

	ev_prepare_init(&service->wakeup, (void *)iproto_wakeup_workers);
	ev_prepare_start(&service->wakeup);
	ev_prepare_init(&service->writeall, (void *)iproto_write_data);
	ev_set_priority(&service->writeall, -2);
	ev_prepare_start(&service->writeall);

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
local(struct iproto_ingress_svc *io, struct iproto *msg, struct iproto_handler *ih)
{
	say_trace("%s: peer:%s op:0x%x sync:%u%s%s", __func__,
		   net_fd_name(io->fd), msg->msg_code, msg->sync,
		   ih->flags & IPROTO_NONBLOCK ? " NONBLOCK" : "",
		   ih->flags & IPROTO_LOCAL ? " LOCAL" : "");
	if (ih->flags & IPROTO_NONBLOCK) {
		stat_collect(stat_base, IPROTO_STREAM_OP, 1);
		struct netmsg_mark header_mark;
		netmsg_getmark(&io->wbuf, &header_mark);
		@try {
			ih->cb(&io->wbuf, msg);
		}
		@catch (Error *e) {
			netmsg_rewind(&io->wbuf, &header_mark);
			iproto_error(&io->wbuf, msg, exc_rc(e), e->reason);
			[e release];
		}
	} else {
		struct iproto_service *service = io->service;
		struct Fiber *w = SLIST_FIRST(&service->workers);
		if (w) {
			SLIST_REMOVE_HEAD(&service->workers, worker_link);
		} else {
			if (ih->flags & IPROTO_SPAWN) {
				w = fiber_create("extra worker", iproto_worker, service);
				SLIST_REMOVE_HEAD(&service->workers, worker_link);
				w->worker_link.sle_next = (void *)(uintptr_t)0xead;
			} else {
				stat_collect(stat_base, IPROTO_WORKER_STARVATION, 1);
				// FIXME: need state for this
				return 0;
			}
		}
		stat_collect(stat_base, IPROTO_BLOCK_OP, 1);
		resume(w, (&(struct worker_arg){ih, msg, io}));
		io->batch--;
	}
	return 1;
}

static int
classify(struct iproto_ingress_svc *io, struct iproto *msg)
{
	struct shard_route *route;
	struct iproto_handler *ih;
	struct iproto *orig_msg = msg;
	struct iproto_egress *proxy;
	Shard<Shard> *shard;
	@try {
		if (msg->msg_code == MSG_IPROXY)
			msg++; // unwrap
		if (likely(msg->msg_code < MSG_PING))
			fiber->ushard = msg->shard_id;
		say_trace("%s: %s peer:%s op:0x%x sync:%u shard:%i ", __func__, msg == orig_msg ? "" : "PROXY",
			   net_fd_name(io->fd), msg->msg_code, msg->sync, msg->shard_id);
		if (unlikely(msg->shard_id > nelem(shard_rt)))
			return error(io, msg, ERR_CODE_NONMASTER, "no such shard");
		route = shard_rt + msg->shard_id;
		proxy = route->proxy;
		shard = route->shard;
		if (unlikely(shard && (shard->loading || !shard->executor)))
			shard = nil;

		ih = service_find_code(io->service, msg->msg_code);
		if (ih->flags & IPROTO_LOCAL)
			goto local;
		if (orig_msg == msg) { /* not via proxy */
			if (proxy && (shard == nil || ih->flags & IPROTO_ON_MASTER)) {
				if (proxy == (void *)0x1)
					return error(io, msg, ERR_CODE_NONMASTER, "replica is readonly");
				return !!iproto_proxy_send(proxy, io, MSG_IPROXY, msg, NULL, 0);
			}
			if (shard == nil)
				return error(io, msg, ERR_CODE_NONMASTER, "no such shard");
		} else {
			if (shard == nil || (proxy && ih->flags & IPROTO_ON_MASTER))
				return error(io, msg, ERR_CODE_NONMASTER, "route loop");
		}
		if (ih->flags & IPROTO_ON_MASTER && recovery->writer == nil)
			return error(io, msg, ERR_CODE_NONMASTER, "replica is readonly");
	local:
		return local(io, msg, ih);
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
	struct iproto *msg;
	while ((msg = iproto_rbuf_req(io)) && io->batch > 0) {
		size_t msg_size = sizeof(*msg) + msg->data_len;
		if (classify(io, msg) == 0)
			break;
		rbuf_ltrim(io, msg_size);
	}

	if (unlikely(io->fd == -1)) /* handler may close connection */
		goto out;

	if (!iproto_rbuf_req(io)) {
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
out:
	netmsg_io_release(io);
}

static void
service_prepare_io(struct iproto_ingress_svc *io)
{
	if (rbuf_len(io) >= cfg.input_low_watermark && iproto_rbuf_req(io)) {
		if (ev_now() - io->input_overflow_warn > 10) {
			say_warn("peer %s input buffer low watermark overflow (size %i)",
				 net_fd_name(io->fd), rbuf_len(io));
			io->input_overflow_warn = ev_now();
		}
		ev_io_stop(&io->in);
	}

	if (rbuf_len(io) < cfg.input_low_watermark && io->wbuf.bytes < cfg.output_low_watermark)
		ev_io_start(&io->in);

#ifndef IPROTO_PESSIMISTIC_WRITES
	if (io->wbuf.bytes > 0) {
		ssize_t r = netmsg_writev(io->fd, &io->wbuf);
		if (r < 0) {
			say_syswarn("writev() to %s failed, closing connection",
				    net_fd_name(io->fd));
			[io close];
			return;
		} else {
			stat_sum_static(stat_base, IPROTO_WRITTEN, r);
		}
	}
#endif

	if (io->wbuf.bytes > 0) {
		ev_io_start(&io->out);

		/* Prevent output owerflow by start reading if
		   output size is below output_low_watermark.
		   Otherwise output flusher will start reading,
		   when size of output is small enought  */
		if (io->wbuf.bytes >= cfg.output_high_watermark) {
			say_warn("peer %s output buffer high watermark (size %zi)",
				 net_fd_name(io->fd), io->wbuf.bytes);
			ev_io_stop(&io->in);
		}
	} else {
		ev_io_stop(&io->out);
	}
}

static void
iproto_wakeup_workers(ev_prepare *ev)
{
	struct iproto_service *service = container_of(ev, struct iproto_service, wakeup);
	struct iproto_ingress_svc *c, *tmp, *last;
	palloc_register_cut_point(fiber->pool);
	do {
		last = TAILQ_LAST(&service->processing, ingress_tailq);
		TAILQ_FOREACH_SAFE(c, &service->processing, processing_link, tmp) {
			if (c->prepare_link.le_prev == NULL)
				LIST_INSERT_HEAD(&service->prepare, c, prepare_link);

			process_requests(service, c);
			/* process_requests() may move *c to the end of tailq */
			if (c == last) break;
		}
	} while (!SLIST_EMPTY(&service->workers) && !TAILQ_EMPTY(&service->processing));
	netmsg_pool_ctx_gc(&service->ctx);
	palloc_cutoff(fiber->pool);
}

static void
iproto_write_data(ev_prepare *ev) {
	struct iproto_service *service = container_of(ev, struct iproto_service, writeall);
	struct iproto_ingress_svc *c, *tmp;
	size_t allocated = palloc_allocated(fiber->pool);
	LIST_FOREACH_SAFE(c, &service->prepare, prepare_link, tmp) {
		LIST_REMOVE(c, prepare_link);
		c->prepare_link.le_prev = NULL;
		service_prepare_io(c);
	}
	assert(palloc_allocated(fiber->pool) == allocated);
}


struct iproto_retcode *
iproto_reply(struct netmsg_head *h, const struct iproto *request, u32 ret_code)
{
	say_trace("%s: peer:%s op:0x%x sync:%u ret_code:%i", __func__,
		   net_fd_name(container_of(h, struct netmsg_io, wbuf)->fd),
		   request->msg_code, request->sync, ret_code);

	struct iproto_retcode *header = net_add_alloc(h, sizeof(*header));
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
	say_trace("%s: peer:%s op:0x%x sync:%u ret_code:%i", __func__,
		   net_fd_name(container_of(h, struct netmsg_io, wbuf)->fd),
		   request->msg_code, request->sync, ret_code);

	struct iproto_retcode *header = net_add_alloc(h, sizeof(*header));
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
	say_debug("%s: shard:%i op:0x%02x data_len:%i sync:%i ret:%i", __func__,
		  header->shard_id, header->msg_code, header->data_len, header->sync, header->ret_code);
	say_trace("	%s", err);
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
#if 0
	struct netmsg *m;
#endif
	tbuf_printf(out, "%s:" CRLF, service->name);
	LIST_FOREACH(c, &service->clients, link) {
		struct netmsg_io *io = c;
		tbuf_printf(out, "    - peer: %s" CRLF, net_fd_name(io->fd));
		tbuf_printf(out, "      fd: %i" CRLF, io->fd);
		tbuf_printf(out, "      state: %s%s" CRLF,
			    ev_is_active(&io->in) ? "in" : "",
			    ev_is_active(&io->out) ? "out" : "");
		tbuf_printf(out, "      rbuf: %i" CRLF, rbuf_len(io));
		tbuf_printf(out, "      pending_bytes: %zi" CRLF, io->wbuf.bytes);
#if 0
		if (!TAILQ_EMPTY(&io->wbuf.q))
			tbuf_printf(out, "      out_messages:" CRLF);
		TAILQ_FOREACH(m, &io->wbuf.q, link)
			tbuf_printf(out, "      - { count: %i }" CRLF, m->count);
#endif
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

#if CFG_primary_addr
static int
iproto_fixup_addr(struct octopus_cfg *cfg)
{
	extern void out_warning(int v, char *format, ...);
	if (cfg->primary_addr == NULL) {
		out_warning(0, "Option 'primary_addr' can't be NULL");
		return -1;
	}
#if CFG_primary_port && CFG_secondary_port
	if (strchr(cfg->primary_addr, ':') == NULL &&
	    strchr(cfg->primary_addr, '/') == NULL &&
	    !cfg->primary_port)
	{
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

void iproto_init(void);
static struct tnt_module iproto_mod = {
	.init = iproto_init,
	.check_config = iproto_fixup_addr
};

register_module(iproto_mod);
#endif

void
iproto_init(void)
{
	stat_base = stat_register(stat_ops, nelem(stat_ops));
	stat_cb_base = stat_register_callback("stat", report_ingress_cnt);
}

register_source();
