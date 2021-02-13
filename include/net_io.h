/*
 * Copyright (C) 2011-2017 Mail.RU
 * Copyright (C) 2011-2017, 2020 Yury Vostrikov
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

#ifndef NET_IO_H
#define NET_IO_H

#include <tbuf.h>
#include <octopus.h>
#include <octopus_ev.h>
#include <palloc.h>
#include <objc.h>

#include <third_party/queue.h>

#include <stdbool.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <sys/un.h>

struct service;

struct netmsg;
TAILQ_HEAD(netmsg_tailq, netmsg);

struct netmsg_pool_ctx {
	struct palloc_pool *pool;
	char _dummy[24 - sizeof(struct palloc_pool *)];
};

struct netmsg_head {
	size_t bytes;
	char _dummy[56 - sizeof(size_t)];
};

#define NETMSG_IO_SHARED_POOL	1
#define NETMSG_IO_LINGER_CLOSE	2
@interface netmsg_io : Object {
@public
	struct netmsg_pool_ctx *ctx;
	struct tbuf rbuf;
	struct netmsg_head wbuf;
	ev_io in, out;
	int fd, rc, flags;
}
- (void)release; /* do not override : IMP caching in process_requests()  */
- (id)retain; /* do not override : IMP caching in process_requests()  */
- (void)close;
- (void)linger_close;
- (void)data_ready;
- (void)tac_event:(int)fd; /* called on tcp_async_connect() result */
@end

struct netmsg_mark {
	char _dummy[40];
};

void netmsg_pool_ctx_init(struct netmsg_pool_ctx *ctx, const char *name, int limit);
void netmsg_pool_ctx_gc(struct netmsg_pool_ctx *ctx);

void netmsg_head_init(struct netmsg_head *h, struct netmsg_pool_ctx *ctx);
void netmsg_head_dealloc(struct netmsg_head *h);

void netmsg_rewind(struct netmsg_head *h, const struct netmsg_mark *mark);
void netmsg_getmark(struct netmsg_head *h, struct netmsg_mark *mark);
void netmsg_reset(struct netmsg_head *h);

void net_add_iov(struct netmsg_head *o, const void *buf, size_t len);
void *net_add_alloc(struct netmsg_head *o, size_t len);

void net_add_iov_dup(struct netmsg_head *o, const void *buf, size_t len);
#define net_add_dup(o, buf) net_add_iov_dup(o, (buf), sizeof(*(buf)))
void net_add_ref_iov(struct netmsg_head *o, uintptr_t ref, const void *buf, size_t len);
void net_add_obj_iov(struct netmsg_head *o, struct tnt_object *obj, const void *buf, size_t len);

ssize_t netmsg_writev(int fd, struct netmsg_head *head);

void netmsg_io_init(struct netmsg_io *io, struct netmsg_pool_ctx *ctx, int fd);

ssize_t netmsg_io_write_for_cb(ev_io *ev, int events);
ssize_t netmsg_io_read_for_cb(ev_io *ev, int events);
void netmsg_io_write_cb(ev_io *ev, int events);
void netmsg_io_read_cb(ev_io *ev, int events);

void netmsg_io_setfd(struct netmsg_io *io, int fd);

static inline void netmsg_io_retain(struct netmsg_io *io)
{
	io->rc++;
}
static inline void netmsg_io_release(struct netmsg_io *io)
{
	if (--io->rc == 0)
		[io free];
}

int rbuf_len(const struct netmsg_io *io);
void rbuf_ltrim(struct netmsg_io *io, int size);
ssize_t rbuf_recv(struct netmsg_io *io, int size);

enum tac_result {
	tac_error = -1,
	tac_wait = -2,
	tac_alien_event = -3
};
enum tac_flag { TAC_RECONNECT = 1 };
struct tac_state {
	struct netmsg_io *io;
	struct ev_io ev;
	struct ev_timer timer;
	ev_tstamp error_tstamp;
	bool error_printed;
	struct sockaddr_in daddr;
	unsigned flags;
	SLIST_ENTRY(tac_state) link;
};
SLIST_HEAD(tac_list, tac_state);

enum tac_result tcp_async_connect(struct tac_state *s, ev_watcher *w /* result of yield() */,
				  struct sockaddr_in      *src,
				  ev_tstamp               timeout);
void abort_tcp_async_connect(struct tac_state *s);
int tcp_connect(struct sockaddr_in *dst, struct sockaddr_in *src, ev_tstamp timeout);
void rendevouz(va_list ap);

struct tcp_server_state {
	const char *addr;
	void (*handler)(int fd, void *data, struct tcp_server_state *state);
	void (*on_bind)(int fd);
	void *data;
	struct sockaddr_storage saddr;
	ev_io io;
};
void tcp_server(va_list ap);
void tcp_server_stop(struct tcp_server_state *state);
void udp_server(va_list ap);
int server_socket(int type, struct sockaddr *saddr, int nonblock,
		  void (*on_bind)(int fd), void (*sleep)(ev_tstamp tm));

int atosin(const char *orig, struct sockaddr_in *addr);
int atosun(const char *orig, struct sockaddr_un *addr);
int atosaddr(const char *orig, struct sockaddr *addr);
const char *sintoa(const struct sockaddr_in *addr);
const char *saddrtoa(const struct sockaddr *addr);
const char *net_fd_name(int fd);
const char *net_sin_name(const struct sockaddr_in *addr);
int net_fixup_addr(char **addr, int port);
#endif
