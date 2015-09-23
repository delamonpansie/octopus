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

/* macro for generating laujit's cdefs*/
#ifndef LUA_DEF
# define LUA_DEF
#endif
@class Fiber;
struct service;

struct netmsg;
TAILQ_HEAD(netmsg_tailq, netmsg);

struct netmsg_head {
	struct netmsg_tailq q;
	struct palloc_pool *pool;
	ssize_t bytes;
	struct iovec *last_used_iov; /* cache for iovec joining */
};

#ifndef IOV_MAX
#  define IOV_MAX 1024
#endif


#define NETMSG_IOV_SIZE 64
struct netmsg {
	TAILQ_ENTRY(netmsg) link; /* first sizeof(void *) bytes are trashed by salloc() */
	int count;

	struct iovec iov[NETMSG_IOV_SIZE];
	uintptr_t ref[NETMSG_IOV_SIZE];
};

@interface netmsg_io : Object {
@public
	struct palloc_pool *pool;
	struct tbuf rbuf;
	struct netmsg_head wbuf;
	ev_io in, out;
	int fd, rc;
	const struct netmsg_io_vop *vop;
}
- (void)close;
- (void)data_ready:(int)r;
- (void)tac_event:(int)fd; /* called on tcp_async_connect() result */
@end

struct netmsg_mark {
	struct netmsg *m;
	struct iovec iov;
	int offset;
};

void netmsg_head_init(struct netmsg_head *h, struct palloc_pool *pool) LUA_DEF;
void netmsg_head_dealloc(struct netmsg_head *h) LUA_DEF;

struct netmsg *netmsg_concat(struct netmsg_head *dst, struct netmsg_head *src) LUA_DEF;
void netmsg_rewind(struct netmsg_head *h, const struct netmsg_mark *mark) LUA_DEF;
void netmsg_getmark(struct netmsg_head *h, struct netmsg_mark *mark) LUA_DEF;
void netmsg_reset(struct netmsg_head *h);

void net_add_iov(struct netmsg_head *o, const void *buf, size_t len) LUA_DEF;
void net_add_iov_dup(struct netmsg_head *o, const void *buf, size_t len) LUA_DEF;
#define net_add_dup(o, buf) net_add_iov_dup(o, (buf), sizeof(*(buf)))
void net_add_ref_iov(struct netmsg_head *o, uintptr_t ref, const void *buf, size_t len) LUA_DEF;
void net_add_obj_iov(struct netmsg_head *o, struct tnt_object *obj, const void *buf, size_t len) LUA_DEF;
void netmsg_verify_ownership(struct netmsg_head *h); /* debug method */

ssize_t netmsg_writev(int fd, struct netmsg_head *head);

struct iproto;

void netmsg_io_init(struct netmsg_io *io, struct palloc_pool *pool, int fd);
void netmsg_io_gc(struct palloc_pool *pool, void *ptr);

int netmsg_io_write_cb(ev_io *ev, int __attribute__((unused)) events);
int netmsg_io_read_cb(ev_io *ev, int __attribute__((unused)) events);

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
	const char *name;
	unsigned flags;
	SLIST_ENTRY(tac_state) link;
};
SLIST_HEAD(tac_list, tac_state);

enum tac_result tcp_async_connect(struct tac_state *s, ev_watcher *w /* result of yield() */,
				  struct sockaddr_in      *src,
				  ev_tstamp               timeout);
int tcp_connect(struct sockaddr_in *dst, struct sockaddr_in *src, ev_tstamp timeout);
void rendevouz(va_list ap);

struct tcp_server_state {
	const char *addr;
	void (*handler)(int fd, void *data, struct tcp_server_state *state);
	void (*on_bind)(int fd);
	void *data;
	struct sockaddr_in saddr;
	ev_io io;
};
void tcp_server(va_list ap);
void tcp_server_stop(struct tcp_server_state *state);
void udp_server(va_list ap);
int server_socket(int type, struct sockaddr_in *src, int nonblock,
		  void (*on_bind)(int fd), void (*sleep)(ev_tstamp tm));

int atosin(const char *orig, struct sockaddr_in *addr) LUA_DEF;
const char *sintoa(const struct sockaddr_in *addr);
int net_fixup_addr(char **addr, int port);
const char *net_peer_name(int fd);


void luaT_opennet(struct lua_State *L);
int luaT_pushnetmsg(struct lua_State *L);
#endif
