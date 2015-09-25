/*
 * Copyright (C) 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

#ifndef IPROTO_H
#define IPROTO_H

#include <util.h>
#include <objc.h>
#include <tbuf.h>
#include <net_io.h>
#include <mbox.h>
#include <objc.h>

#include <stdint.h>

#include <iproto_def.h>

#define MSG_PING  0xff00
#define MSG_REPLICA  0xff01
#define MSG_SHARD  0xff02


static inline struct iproto *iproto(const struct tbuf *t)
{
	return (struct iproto *)t->ptr;
}

static inline struct iproto_retcode *iproto_retcode(const struct tbuf *t)
{
	return (struct iproto_retcode *)t->ptr;
}

static inline struct iproto *iproto_parse(struct tbuf *t)
{
	if (tbuf_len(t) < sizeof(struct iproto) ||
	    tbuf_len(t) < sizeof(struct iproto) + iproto(t)->data_len)
		return NULL;

	struct iproto *ret = iproto(t);
	tbuf_ltrim(t, sizeof(struct iproto) + ret->data_len);
	return ret;
}


struct netmsg_head;
struct iproto_retcode * iproto_reply(struct netmsg_head *h, const struct iproto *request, u32 ret_code);
void iproto_reply_fixup(struct netmsg_head *h, struct iproto_retcode *reply);
struct iproto_retcode * iproto_reply_small(struct netmsg_head *h, const struct iproto *request, u32 ret_code);
void iproto_error(struct netmsg_head *h, const struct iproto *request, u32 ret_code, const char *err);
void iproto_error_fmt(struct netmsg_head *h, const struct iproto *request, u32 ret_code, const char *fmt, ...);


LIST_HEAD(iproto_future_list, iproto_future);
@interface iproto_ingress: netmsg_io {
@public
	LIST_ENTRY(iproto_ingress) link;
	TAILQ_ENTRY(iproto_ingress) processing_link;

	struct iproto_future_list waiting;
	struct iproto_service *service;
}
- (void)init:(int)fd_ service:(struct iproto_service *)service_;
@end

@interface iproto_egress: netmsg_io {
@public
	struct tac_state ts;

	SLIST_ENTRY(iproto_egress) link;
	TAILQ_HEAD(,iproto_future) future;
}
@end
SLIST_HEAD(iproto_egress_list, iproto_egress);


void iproto_ping(struct netmsg_head *h, struct iproto *r, void *arg __attribute__((unused)));

@class Shard;
@protocol Shard;

enum { IPROTO_NONBLOCK = 1, IPROTO_PROXY = 2, IPROTO_FORCE_LOCAL = 4 };
typedef void (*iproto_cb)(struct netmsg_head *, struct iproto *, void *);
struct iproto_handler {
	iproto_cb cb;
	int flags;
	int code;
};

struct iproto_service {
	struct palloc_pool *pool;
	size_t pool_allocated; /* used for differential calls to palloc_gc */
	const char *name;
	TAILQ_HEAD(ingress_tailq, iproto_ingress) processing;
	LIST_HEAD(, iproto_ingress) clients;
	struct Fiber *acceptor;
	SLIST_HEAD(, Fiber) workers; /* <- handlers */
	int batch;
	ev_prepare wakeup;

	enum { SERVICE_SHARDED = 1 } options;
	struct iproto_handler default_handler;
	int ih_size, ih_mask;
	struct iproto_handler *ih;
	Class ingress_class;
	void (*on_bind)(int fd);
	const char *addr;
};
void iproto_service(struct iproto_service *service, const char *addr);
void iproto_service_info(struct tbuf *out, struct iproto_service *service);
void iproto_worker(va_list ap);
#define SERVICE_DEFAULT_CAPA 0x100
void service_set_handler(struct iproto_service *s, struct iproto_handler h);
static inline struct iproto_handler *service_find_code(struct iproto_service *s, int code)
{
	int pos = code & s->ih_mask;
	if (s->ih[pos].code == -1) return &s->default_handler;
	if (s->ih[pos].code == code) return &s->ih[pos];
	int dlt = (code % s->ih_mask) | 1;
	do {
		pos = (pos + dlt) & s->ih_mask;
		if (s->ih[pos].code == code) return &s->ih[pos];
	} while(s->ih[pos].code != -1);
	return &s->default_handler;
}

void
service_register_iproto(struct iproto_service *s, u32 cmd, iproto_cb cb, int flags);

struct iproto_future {
	TAILQ_ENTRY(iproto_future) link; /* shared by connection->future and mbox */
	LIST_ENTRY(iproto_future) waiting_link; /* ingres, who waits for reply: either proxy or mbox */
	struct iproto_egress *dst; /* always exists and connected */
	union {
		struct {
			struct iproto_ingress *ingress; /* always exists and connected */
			struct iproto proxy_request;
		};
		struct {
			struct iproto_mbox *mbox;
			struct iproto *msg; // FTF
		};
	};
	enum iproto_future_type { IPROTO_FUTURE_MBOX, IPROTO_FUTURE_PROXY, IPROTO_FUTURE_ORPHAN, IPROTO_FUTURE_BLACKHOLE } type;
	u32 sync;
};


void iproto_future_collect_orphans(struct iproto_future_list *waiting);
void iproto_future_resolve_err(struct iproto_egress *peer);
void iproto_future_resolve(struct iproto_egress *peer, struct iproto *msg);

struct iproto_mbox {
	MBOX(, iproto_future);
	struct iproto_future_list waiting;
	struct palloc_pool *pool;
	int sent;
};

#define IPROTO_MBOX_INITIALIZER(mbox, pool) \
	{ MBOX_INITIALIZER(mbox), LIST_HEAD_INITIALIZER(&mbox->waiting), pool, 0}


#define iproto_mbox_init(mbox, xpool) ({ mbox_init((mbox)); LIST_INIT(&(mbox)->waiting); (mbox)->sent = 0; (mbox)->pool = (xpool); })
void iproto_mbox_release(struct iproto_mbox *mbox);
struct iproto *iproto_mbox_get(struct iproto_mbox *mbox);
struct iproto *iproto_mbox_peek(struct iproto_mbox *mbox);
void iproto_mbox_put(struct iproto_mbox *mbox, struct iproto *msg);

void iproto_pinger(va_list ap);


u32 iproto_mbox_send(struct iproto_mbox *mbox, struct iproto_egress *peer,
		     const struct iproto *msg, const struct iovec *iov, int iovcnt);
int iproto_mbox_broadcast(struct iproto_mbox *mbox, struct iproto_egress_list *group,
			  const struct iproto *msg, const struct iovec *iov, int iovcnt);
void iproto_mbox_wait_all(struct iproto_mbox *mbox, ev_tstamp timeout);

struct iproto *iproto_sync_send(struct iproto_egress *peer,
				const struct iproto *msg, const struct iovec *iov, int iovcnt);

void iproto_proxy_send(struct iproto_egress *to, struct iproto_ingress *from,
		       const struct iproto *msg, const struct iovec *iov, int iovcnt);

struct iproto_egress *iproto_remote_add_peer(struct iproto_egress *peer, const struct sockaddr_in *daddr, struct palloc_pool *pool);
void iproto_remote_stop_reconnect(struct iproto_egress *peer);


@interface IProtoClose : Error
@end

@interface IProtoError : Error {
@public
	u32 code;
}
- (IProtoError *)init_code:(u32)code_
		      line:(unsigned)line_
		      file:(const char *)file_;
- (u32)code;
@end

#define iproto_exc(err, msg)						\
	[[IProtoError with_reason: (msg)]				\
		init_code:(err) line:__LINE__ file:__FILE__]
#define iproto_fexc(err, fmt, ...)					\
	[[IProtoError with_format: (fmt), ##__VA_ARGS__]		\
		init_code:(err) line:__LINE__ file:__FILE__]

#define iproto_raise(...) @throw iproto_exc(__VA_ARGS__)
#define iproto_raise_fmt(...) @throw iproto_fexc(__VA_ARGS__)

enum error_codes ENUM_INITIALIZER(ERROR_CODES);

#endif
