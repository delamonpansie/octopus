/*
 * Copyright (C) 2011 Mail.RU
 * Copyright (C) 2011 Yuriy Vostrikov
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


#import <tbuf.h>
#import <tarantool.h>
#import <tarantool_ev.h>
#import <palloc.h>

#include <third_party/queue.h>

#include <stdbool.h>
#include <sys/uio.h>
#include <netinet/in.h>


struct fiber;
struct service;

struct netmsg;
TAILQ_HEAD(netmsg_tailq, netmsg);

struct netmsg {
	struct palloc_pool *pool;
	struct netmsg_tailq *tailq;

	int offset, count;

	TAILQ_ENTRY(netmsg) link;

	struct iovec iov[1024];
	struct tnt_object *ref[1024];
};

struct netmsg_mark {
	struct netmsg *m;
	int offset;
};

#define REF_MALLOC -1
#define REF_STATIC -2

struct conn {
	struct palloc_pool *pool;
	struct tbuf *rbuf;
	int fd, ref;
	struct netmsg_tailq out_messages;

	enum { READING, PROCESSING } state;
	LIST_ENTRY(conn) link;
	TAILQ_ENTRY(conn) processing_link;
	SLIST_ENTRY(conn) pool_link;
	ev_io in, out;
	struct service *service;
};

struct service {
	struct palloc_pool *pool;
	int listen_fd;
	const char *name;
	TAILQ_HEAD(, conn) processing;
	LIST_HEAD(, conn) conn;
	struct fiber *acceptor, *handler, *input_reader, *output_flusher;
	SLIST_HEAD(, fiber) workers;
	ev_prepare wakeup;
};

struct netmsg *netmsg_tail(struct netmsg_tailq *q, struct palloc_pool *pool);
void netmsg_concat(struct netmsg *dst, struct netmsg_tailq *src);
void netmsg_release(struct netmsg *m);
void netmsg_rewind(struct netmsg **m, struct netmsg_mark *mark);
void netmsg_getmark(struct netmsg *m, struct netmsg_mark *mark);

void net_add_iov(struct netmsg **m, const void *buf, size_t len);
struct iovec *net_reserve_iov(struct netmsg **m);
void net_add_iov_dup(struct netmsg **m, const void *buf, size_t len);
void net_add_ref_iov(struct netmsg **m, struct tnt_object *obj, const void *buf, size_t len);
void net_add_lua_iov(struct netmsg **m, lua_State *L, int str);

struct conn *conn_create(struct palloc_pool *pool, int fd);
void conn_init(struct conn *c, struct palloc_pool *pool, int fd, int ref);
void conn_close(struct conn *c);
void conn_gc(struct palloc_pool *pool, void *ptr);
int conn_readahead(struct conn *c, size_t min);
ssize_t conn_read(struct conn *c, void *buf, size_t count);
ssize_t conn_write(struct conn *c, const void *buf, size_t count);
void conn_write_netmsg(struct conn *c);
ssize_t conn_flush(struct conn *c);

int tcp_connect(struct sockaddr_in *dst, struct sockaddr_in *src, ev_tstamp timeout);
void tcp_server(va_list ap);
void udp_server(va_list ap);

int atosockaddr_in(const char *orig, struct sockaddr_in *addr);
