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
	void (*cb)(struct iproto *, struct conn *c);
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
			a.cb(a.r, a.c);
		}
		@catch (Error *e) {
			u32 rc = ERR_CODE_UNKNOWN_ERROR;
			if ([e respondsTo:@selector(code)])
				rc = [(id)e code];
			else if ([e isMemberOf:[IndexError class]])
				rc = ERR_CODE_ILLEGAL_PARAMS;

			iproto_error(&a.c->out_messages, a.r, rc, e->reason);
		}

		if (a.c->out_messages.bytes > 0 && a.c->state != CLOSED)
			ev_io_start(&a.c->out);

		conn_unref(a.c);
		fiber_gc();
	}
}


static void
err(struct netmsg_head *h __attribute__((unused)),
    struct iproto *r,
    struct conn *c __attribute__((unused)))
{
	iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown iproto command %i", r->msg_code);
}

void
iproto_ping(struct netmsg_head *h, struct iproto *r, struct conn *c __attribute__((unused)))
{
	net_add_iov_dup(h, r, sizeof(struct iproto));
}

void
service_register_iproto_stream(struct service *s, u32 cmd,
			       void (*cb)(struct netmsg_head *, struct iproto *, struct conn *),
			       int flags)
{
	service_set_handler(s, (struct iproto_handler){
			.code = cmd,
			.cb = {.stream = cb},
			.flags = flags | IPROTO_NONBLOCK
		});
}

void
service_register_iproto_block(struct service *s, u32 cmd,
			      void (*cb)(struct iproto *, struct conn *),
			      int flags)
{
	service_set_handler(s, (struct iproto_handler){
			.code = cmd,
			.cb = {.block = cb},
			.flags = flags & ~IPROTO_NONBLOCK
		});
}

void
tcp_iproto_service(struct service *service, const char *addr, void (*on_bind)(int fd), void (*wakeup_workers)(ev_prepare *))
{
	tcp_service(service, addr, on_bind, wakeup_workers ?: iproto_wakeup_workers);
	service_register_iproto_stream(service, -1, err, 0);
	service_register_iproto_stream(service, msg_ping, iproto_ping, IPROTO_NONBLOCK);
}

static int
process_requests(struct conn *c)
{
	struct service *service = c->service;
	int batch = service->batch;
	int r = 0;

	c->ref++;
	while (tbuf_len(c->rbuf) >= sizeof(struct iproto) &&
	       tbuf_len(c->rbuf) >= sizeof(struct iproto) + iproto(c->rbuf)->data_len)
	{
		struct iproto *request = iproto(c->rbuf);
		struct iproto_handler *ih = service_find_code(service, request->msg_code);

		if (ih->flags & IPROTO_NONBLOCK) {
			stat_collect(stat_base, IPROTO_STREAM_OP, 1);
			tbuf_ltrim(c->rbuf, sizeof(struct iproto) + request->data_len);
			struct netmsg_mark header_mark;
			netmsg_getmark(&c->out_messages, &header_mark);
			@try {
				ih->cb.stream(&c->out_messages, request, c);
			}
			@catch (Error *e) {
				u32 rc = ERR_CODE_UNKNOWN_ERROR;
				if ([e respondsTo:@selector(code)])
					rc = [(id)e code];
				else if ([e isMemberOf:[IndexError class]])
					rc = ERR_CODE_ILLEGAL_PARAMS;

				netmsg_rewind(&c->out_messages, &header_mark);
				iproto_error(&c->out_messages, request, rc, e->reason);
			}
		} else {
			struct fiber *w = SLIST_FIRST(&service->workers);
			if (w) {
				stat_collect(stat_base, IPROTO_BLOCK_OP, 1);
				size_t req_size = sizeof(struct iproto) + request->data_len;
				void *request_copy = palloc(w->pool, req_size);
				memcpy(request_copy, request, req_size);
				tbuf_ltrim(c->rbuf, req_size);
				SLIST_REMOVE_HEAD(&service->workers, worker_link);
				resume(w, &(struct worker_arg){ih->cb.block, request_copy, c});
				r++;
			} else {
				stat_collect(stat_base, IPROTO_WORKER_STARVATION, 1);
				break; // FIXME: need state for this
			}

			if (--batch == 0)
				break;
		}
	}

	if (unlikely(c->state == CLOSED)) /* handler may close connection */
		goto out;

	if (tbuf_len(c->rbuf) < sizeof(struct iproto) ||
	    tbuf_len(c->rbuf) < sizeof(struct iproto) + iproto(c->rbuf)->data_len)
	{
		TAILQ_REMOVE(&service->processing, c, processing_link);
		c->processing_link.tqe_prev = NULL;
	} else if (batch < service->batch) {
		/* avoid unfair scheduling in case of absense of stream requests
		   and all workers being busy */
		TAILQ_REMOVE(&service->processing, c, processing_link);
		TAILQ_INSERT_TAIL(&service->processing, c, processing_link);
	}

#ifndef IPROTO_PESSIMISTIC_WRITES
	if (c->out_messages.bytes > 0) {
		ssize_t r = conn_write_netmsg(c);
		if (r < 0) {
			say_syswarn("%s writev() failed, closing connection",
				    c->service->name);
			conn_close(c);
			goto out;
		}
	}
#endif

	if (c->out_messages.bytes > 0) {
		ev_io_start(&c->out);

		/* Prevent output owerflow by start reading if
		   output size is below output_low_watermark.
		   Otherwise output flusher will start reading,
		   when size of output is small enought  */
		if (c->out_messages.bytes >= cfg.output_high_watermark)
			ev_io_stop(&c->in);
	}
out:
	conn_unref(c);

	return r;
}


void
iproto_wakeup_workers(ev_prepare *ev)
{
	struct service *service = (void *)ev - offsetof(struct service, wakeup);
	struct conn *c, *tmp, *last;
	struct palloc_pool *saved_pool = fiber->pool;
	assert(saved_pool == sched.pool);

	fiber->pool = service->pool;

	last = TAILQ_LAST(&service->processing, conn_tailq);
	TAILQ_FOREACH_SAFE(c, &service->processing, processing_link, tmp) {
		process_requests(c);
		/* process_requests() may move *c to the end of tailq */
		if (c == last) break;
	}

	fiber->pool = saved_pool;

	if (palloc_allocated(service->pool) - service->pool_allocated > 64 * 1024 * 1024) {
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
	say_debug("%s: op:%02x data_len:%i sync:%i ret:%i", __func__,
		  header->msg_code, header->data_len, header->sync, header->ret_code);
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
