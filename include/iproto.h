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

#include <stdint.h>

#include <iproto_def.h>

extern const uint32_t msg_ping;
extern const uint32_t msg_replica;

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

void iproto_worker(va_list ap);

struct iproto_peer {
	struct conn c;
	SLIST_ENTRY(iproto_peer) link;
	int id;
	const char *name;
	struct sockaddr_in addr;
	bool connect_err_said;

	ev_tstamp last_connect_try;
};
SLIST_HEAD(iproto_group, iproto_peer);

void iproto_ping(struct netmsg_head *h, struct iproto *r, struct conn *c);

void tcp_iproto_service(struct service *service, const char *addr, void (*on_bind)(int fd), void (*wakeup_workers)(ev_prepare *));
void iproto_wakeup_workers(ev_prepare *ev);

void
service_register_iproto_stream(struct service *s, u32 cmd,
			       void (*cb)(struct netmsg_head *, struct iproto *, struct conn *),
			       int flags);
void
service_register_iproto_block(struct service *s, u32 cmd,
			      void (*cb)(struct iproto *, struct conn *),
			      int flags);

struct iproto_reply {
	STAILQ_ENTRY(iproto_reply) link;
	struct iproto header;
};

#define IPROTO_MBOX_STATIC_SYNC_SIZE 3
struct iproto_mbox {
	MBOX(, iproto_reply);
	struct palloc_pool *pool;
	int sync_nelem, sent;
	u32 sync[IPROTO_MBOX_STATIC_SYNC_SIZE];
};

#define IPROTO_MBOX_INITIALIZER(mbox, pool) \
	{ MBOX_INITIALIZER(mbox), pool, IPROTO_MBOX_STATIC_SYNC_SIZE, 0, { 0 }}


#define iproto_mbox_init(mbox, xpool, nelem) ({ mbox_init((mbox)); (mbox)->sent = 0; (mbox)->pool = (xpool); (mbox)->sync_nelem = nelem; })
void iproto_mbox_release(struct iproto_mbox *mbox);
struct iproto *iproto_mbox_get(struct iproto_mbox *mbox);
struct iproto *iproto_mbox_peek(struct iproto_mbox *mbox);

u32 iproto_next_sync();
void iproto_reply_reader(va_list ap);
void iproto_collect_reply(struct conn *c, struct iproto *msg);
void iproto_rendevouz(va_list ap);
void iproto_pinger(va_list ap);


int iproto_send(struct iproto_mbox *mbox, struct iproto_peer *peer,
		struct iproto *msg, const struct iovec *iov, int iovcnt);
int iproto_broadcast(struct iproto_mbox *mbox, struct iproto_group *group,
		     struct iproto *msg, const struct iovec *iov, int iovcnt);
struct iproto *iproto_sync_send(struct iproto_peer *peer,
				struct iproto *msg, const struct iovec *iov, int iovcnt);
struct iproto *iproto_wait_sync(struct iproto_mbox *mbox, u32 sync);
void iproto_wait_all(struct iproto_mbox *mbox);

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
