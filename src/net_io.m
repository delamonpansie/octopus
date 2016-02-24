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

#import <net_io.h>
#import <palloc.h>
#import <salloc.h>
#import <fiber.h>
#import <util.h>
#import <say.h>
#import <objc.h>

#include <third_party/queue.h>

#include <errno.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <sysexits.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#if HAVE_VALGRIND_VALGRIND_H && !defined(NVALGRIND)
# include <valgrind/valgrind.h>
# include <valgrind/memcheck.h>
#else
# define VALGRIND_MAKE_MEM_DEFINED(_qzz_addr, _qzz_len) (void)0
# define VALGRIND_MAKE_MEM_UNDEFINED(_qzz_addr, _qzz_len) (void)0
# define VALGRIND_MALLOCLIKE_BLOCK(addr, sizeB, rzB, is_zeroed) (void)0
# define VALGRIND_FREELIKE_BLOCK(addr, rzB) (void)0
#endif

static struct slab_cache netmsg_cache;

static struct netmsg *
netmsg_alloc(struct netmsg_head *h)
{
	struct netmsg *m = slab_cache_alloc(&netmsg_cache);
	TAILQ_INSERT_HEAD(&h->q, m, link);
	h->last_used_iov = m->iov;

	VALGRIND_MAKE_MEM_DEFINED(&m->count, sizeof(m->count));
	VALGRIND_MAKE_MEM_DEFINED(m->iov, sizeof(m->iov));
	VALGRIND_MAKE_MEM_DEFINED(m->ref, sizeof(m->ref));
	return m;
}

static void netmsg_releaser(struct netmsg *, int);
void
netmsg_free(struct netmsg *m)
{
	netmsg_releaser(m, 0);
	slab_cache_free(&netmsg_cache, m);
}

void
netmsg_head_init(struct netmsg_head *h, struct palloc_pool *pool)
{
	TAILQ_INIT(&h->q);
	h->pool = pool;
	h->bytes = 0;
	netmsg_alloc(h);
}

void
netmsg_head_dealloc(struct netmsg_head *h)
{
	struct netmsg *m, *tmp;
	TAILQ_FOREACH_SAFE(m, &h->q, link, tmp)
		netmsg_free(m);
}


static void
netmsg_release(struct netmsg *m, int from, int count)
{
	bool have_lua_refs = 0;
	for (int i = from; i < count; i++) {
		if (m->ref[i] == 0)
			continue;

		if (m->ref[i] & 1)
			have_lua_refs = 1;
		else {
#ifdef OCT_OBJECT
			object_decr_ref((struct tnt_object *)m->ref[i]);
#else
			abort();
#endif
		}
	}

	if (have_lua_refs) {
		lua_State *L = fiber->L;
		lua_getglobal(L, "__netmsg_unref");
		lua_pushlightuserdata(L, m);
		lua_pushinteger(L, from);
		lua_call(L, 2, 0);
	}
}

static void
netmsg_releaser(struct netmsg *m, int from)
{
	netmsg_release(m, from, m->count - from);
	memset(m->ref + from, 0, (m->count - from) * sizeof(m->ref[0]));
	memset(m->iov + from, 0, (m->count - from) * sizeof(m->iov[0]));
	m->count = from;
}

static void
netmsg_releasel(struct netmsg *m, int count)
{
	int live_count = m->count - count;
	netmsg_release(m, 0, count);
	memmove(m->ref, m->ref + count, live_count * sizeof(m->ref[0]));
	memmove(m->iov, m->iov + count, live_count * sizeof(m->iov[0]));
	memset(m->ref + live_count, 0, count * sizeof(m->ref[0]));
	memset(m->iov + live_count, 0, count * sizeof(m->iov[0]));
	m->count -= count;
}

/* WARNING: call only if tailq length > 2  */
static void
netmsg_dealloc(struct netmsg_tailq *q, struct netmsg *m)
{
	TAILQ_REMOVE(q, m, link);
	netmsg_free(m);
}

void
netmsg_reset(struct netmsg_head *h)
{
	struct netmsg *m, *tmp;
	m = TAILQ_FIRST(&h->q);
	netmsg_releaser(m, 0);
	h->last_used_iov = m->iov;
	for (m = TAILQ_NEXT(m, link); m; m = tmp) {
		tmp = TAILQ_NEXT(m, link);
		netmsg_dealloc(&h->q, m);
	}
}

static void
netmsg_gc(struct palloc_pool *pool, struct netmsg *m)
{
	for (unsigned i = 0; i < m->count; i++) {
		if (m->ref[i] != 0 || m->iov[i].iov_len == 0)
			continue;

		void *ptr = palloc(pool, m->iov[i].iov_len);
		memcpy(ptr, m->iov[i].iov_base, m->iov[i].iov_len);
		m->iov[i].iov_base = ptr;
	}
}

struct netmsg *
netmsg_concat(struct netmsg_head *dst, struct netmsg_head *src)
{
	struct netmsg *m, *tmp;

	if (src->bytes == 0)
		return TAILQ_FIRST(&dst->q);

	if (dst->bytes == 0) {
		m = TAILQ_FIRST(&dst->q);
		assert(m->count == 0);
		TAILQ_REMOVE(&dst->q, m, link);
		netmsg_free(m);
	}

	dst->bytes += src->bytes;
	src->bytes = 0;

	if (src->pool != dst->pool)
		TAILQ_FOREACH(m, &src->q, link)
			netmsg_gc(dst->pool, m);

	TAILQ_FOREACH_REVERSE_SAFE(m, &src->q, netmsg_tailq, link, tmp) {
		TAILQ_REMOVE(&src->q, m, link); // FIXME: TAILQ_INIT ?
		TAILQ_INSERT_HEAD(&dst->q, m, link);
	}
	return TAILQ_FIRST(&dst->q);
}

void
netmsg_rewind(struct netmsg_head *h, const struct netmsg_mark *mark)
{
	struct netmsg *m, *tvar;
	TAILQ_FOREACH_SAFE(m, &h->q, link, tvar) {
		if (m == mark->m)
			break;

		for (int i = 0; i < m->count; i++)
			h->bytes -= m->iov[i].iov_len;

		netmsg_dealloc(&h->q, m);
	}
	assert(m == mark->m);

	for (int i = mark->offset; i < mark->m->count; i++)
		h->bytes -= mark->m->iov[i].iov_len;
	netmsg_releaser(mark->m, mark->offset);
	*(m->iov + m->count) = mark->iov;
}

void
netmsg_getmark(struct netmsg_head *h, struct netmsg_mark *mark)
{
	struct netmsg *m = TAILQ_FIRST(&h->q);
	mark->m = m;
	mark->offset = m->count;
	mark->iov = *(m->iov + m->count);
}


void
net_add_iov(struct netmsg_head *h, const void *buf, size_t len)
{
	struct iovec *v = h->last_used_iov;
	h->bytes += len;

	if (v->iov_base + v->iov_len == buf) {
		v->iov_len += len;
	} else {
		struct netmsg *m = TAILQ_FIRST(&h->q);
		v = m->iov + m->count;
		v->iov_base = (char *)buf;
		v->iov_len = len;
		h->last_used_iov = v;

		/* *((*m)->ref + (*m)->count) is NULL here. see netmsg_unref() */
		if (unlikely(++(m->count) == nelem(m->iov)))
			netmsg_alloc(h);
	}
}

void
net_add_iov_dup(struct netmsg_head *h, const void *buf, size_t len)
{
	void *copy = palloc(h->pool, len);
	memcpy(copy, buf, len);
	net_add_iov(h, copy, len);
}

#ifdef OCT_OBJECT
static struct iovec dummy; /* dummy iovec not adjacent to anything else */

void
net_add_ref_iov(struct netmsg_head *h, uintptr_t obj, const void *buf, size_t len)
{
	struct netmsg *m = TAILQ_FIRST(&h->q);
	struct iovec *v = m->iov + m->count;
	v->iov_base = (char *)buf;
	v->iov_len = len;
	h->last_used_iov = &dummy;

	h->bytes += len;
	m->ref[m->count] = obj;

	if (unlikely(++(m->count) == nelem(m->iov)))
		netmsg_alloc(h);
}

void
net_add_obj_iov(struct netmsg_head *o, struct tnt_object *obj, const void *buf, size_t len)
{
	assert(((uintptr_t)obj & 1) == 0);
	object_incr_ref(obj);
	net_add_ref_iov(o, (uintptr_t)obj, buf, len);
}
#endif

void
netmsg_verify_ownership(struct netmsg_head *h)
{
	struct netmsg *m;

	TAILQ_FOREACH(m, &h->q, link)
		for (int i = 0; i < m->count; i++)
			if (m->ref[i] != 0)
				assert(!palloc_owner(h->pool, m->iov[i].iov_base));
			else
				assert(palloc_owner(h->pool, m->iov[i].iov_base));
}


static struct iovec *
netmsg2iovec(struct iovec *buf, struct netmsg *m)
{
	int free = IOV_MAX;
	do {
		memcpy(buf, m->iov, sizeof(*buf) * m->count);
		buf += m->count;
		free -= m->count;

		m = TAILQ_PREV(m, netmsg_tailq, link);
	} while (m != NULL && free >= m->count);
	return buf;
}

static struct iovec iovcache[IOV_MAX];
ssize_t
netmsg_writev(int fd, struct netmsg_head *head)
{
	struct iovec *iov = iovcache, *end;
	ssize_t result = 0;

	if (unlikely(head->bytes == 0))
		return result;

	end = netmsg2iovec(iov, TAILQ_LAST(&head->q, netmsg_tailq));

	int iov_count = end - iov;
	do {
		ssize_t r = writev(fd, iov, end - iov);
		if (unlikely(r < 0)) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;

			if (result == 0)
				result = r;
			break;
		};
		head->bytes -= r;
		result += r;

		if (head->bytes == 0) {
			netmsg_reset(head);
			return result;
		}

		do {
			if (iov->iov_len > r) {
				iov->iov_base += r;
				iov->iov_len -= r;
				break;
			} else {
				r -= iov->iov_len;
				iov++;
			}
		} while (r > 0);
	} while (end > iov);

	int iov_unsent = end - iov;
	iov_count -= iov_unsent;

	struct netmsg *m = TAILQ_LAST(&head->q, netmsg_tailq), *prev;
	while (iov_count >= m->count) {
		prev = TAILQ_PREV(m, netmsg_tailq, link);
		if (!prev)
			break;
		iov_count -= m->count;
		netmsg_dealloc(&head->q, m);
		m = prev;
	}

	if (iov_count)
		netmsg_releasel(m, iov_count);

	if (iov_unsent)
		*m->iov = *iov;

	return result;
}

void
netmsg_io_write_cb(ev_io *ev, int __attribute__((unused)) events)
{
	struct netmsg_io *io = container_of(ev, struct netmsg_io, out);

	ssize_t r = netmsg_writev(ev->fd, &io->wbuf);
	if (r < 0 && (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
		say_syswarn("writev(%i) to %s failed", ev->fd, net_peer_name(ev->fd));
		[io close];
		return;
	}

	if (io->wbuf.bytes == 0)
		ev_io_stop(ev);
}

void
netmsg_io_read_cb(ev_io *ev, int __attribute__((unused)) events)
{
	struct netmsg_io *io = container_of(ev, struct netmsg_io, in);

	if ((io->flags & NETMSG_IO_SHARED_POOL) == 0) {
		size_t diff = palloc_allocated(io->pool) - io->pool_allocated;
		if (diff > 256 * 1024  && diff > io->pool_allocated) {
			palloc_gc(io->pool);
			io->pool_allocated = palloc_allocated(io->pool);
		}
	}


	tbuf_ensure(&io->rbuf, 16 * 1024);
	ssize_t r = tbuf_recv(&io->rbuf, ev->fd);
	[io data_ready];

	if (r == 0) {
		say_debug("peer %s closed connection", net_peer_name(ev->fd));
		[io close];
		return;
	}

	if (r < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
			say_syswarn("recv(%i) from %s failed", ev->fd, net_peer_name(ev->fd));
			[io close];
		}
		return;
	}
}

void
netmsg_head_gc(struct palloc_pool *pool, void *ptr)
{
	struct netmsg_head *head = ptr;
	struct netmsg *m;
	TAILQ_FOREACH(m, &head->q, link)
		netmsg_gc(pool, m);

	head->pool = pool;
}

void
netmsg_io_gc(struct palloc_pool *pool, void *ptr)
{
	struct netmsg_io *io = ptr;
	struct netmsg *m;

	tbuf_gc(pool, &io->rbuf);
	TAILQ_FOREACH(m, &io->wbuf.q, link)
		netmsg_gc(pool, m);

	io->pool = io->wbuf.pool = pool;
}

void
netmsg_io_init(struct netmsg_io *io, struct palloc_pool *pool, int fd)
{
	assert(pool != NULL);
	netmsg_io_retain(io);
	netmsg_head_init(&io->wbuf, pool);
	io->rbuf = TBUF(NULL, 0, pool);
	io->pool = pool;
	palloc_register_gc_root(pool, io, netmsg_io_gc);
	ev_init(&io->in, netmsg_io_read_cb);
	ev_init(&io->out, netmsg_io_write_cb);

	if (fd >= 0)
		netmsg_io_setfd(io, fd);
	else
		io->fd = -1;
	say_debug2("%s: %p fd:%i", __func__, io, io->fd);
}

void
netmsg_io_setfd(struct netmsg_io *io, int fd)
{
	io->fd = fd;
	ev_io_set(&io->in, fd, EV_READ);
	ev_io_set(&io->out, fd, EV_WRITE);
}

@implementation netmsg_io
- (id)
retain
{
	netmsg_io_retain(self);
	return self;
}

- (void)
release
{
	netmsg_io_release(self);
}


/* never call [free] directly, use [release] */
- (void)
free
{
	if (fd != -1)
		[self close];
	tbuf_reset(&rbuf);
	palloc_unregister_gc_root(pool, self);
	netmsg_head_dealloc(&wbuf);
	[super free];
	say_debug2("%s: %p", __func__, self);
}

- (void)
data_ready
{
}

- (void)
tac_event:(int)event
{
	(void)event;
}

- (void)
close
{
	if (fd < 0)
		return;
	say_debug("closing connection to %s", net_peer_name(fd));
	if (close(fd) < 0)
		say_syswarn("close");
	ev_io_stop(&out);
	ev_io_stop(&in);
	fd = in.fd = out.fd = -1;
}

@end
const char *
net_peer_name(int fd)
{
	static char buf[22];  /* aaa.bbb.ccc.ddd:xxxxx */
	struct sockaddr_in peer;
	socklen_t peer_len = sizeof(peer);

	if (fd < 3)
		return NULL;

	memset(&peer, 0, peer_len);
	if (getpeername(fd, (struct sockaddr *)&peer, &peer_len) < 0)
		return NULL;

	uint32_t zero = 0;
	if (memcmp(&peer.sin_addr, &zero, sizeof(zero)) == 0)
		return NULL;

	snprintf(buf, sizeof(buf), "%s:%d", inet_ntoa(peer.sin_addr), ntohs(peer.sin_port));
	return buf;
}

enum tac_result
tcp_async_connect(struct tac_state *s, ev_watcher *w /* result of yield() */,
		  struct sockaddr_in      *src,
		  ev_tstamp               timeout)
{
	int fd = s->ev.fd;
	if (fd < 0) {
		if (ev_now() - s->error_tstamp <= 1.0) /* no more then one reconnect in second */
			goto error;

		/* init */
		int	optval = 1;

		fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0) {
			say_syserror("socket");
			goto error;
		}
		assert(fd > 0);

		if (ioctl(fd, FIONBIO, &optval) < 0) {
			say_syserror("ioctl");
			goto error;
		}

		if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1 ||
		    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) == -1) {
			say_syserror("setsockopt");
			goto error;
		}

		if (src) {
			if (bind(fd, (struct sockaddr *)src, sizeof(*src)) < 0) {
				say_syserror("bind(%s)", sintoa(src));
				goto error;
			}
		}

		if (connect(fd, (struct sockaddr *)&s->daddr, sizeof(s->daddr)) < 0) {
			if (errno != EINPROGRESS) {
				say_syserror("connect");
				goto error;
			}
		}

		ev_io_init(&s->ev, (void *)fiber, fd, EV_WRITE);
		ev_timer_init(&s->timer, (void *)fiber, timeout, 0.);
		s->ev.coro = s->timer.coro = 1;
		if (timeout > 0)
			ev_timer_start(&s->timer);
		ev_io_start(&s->ev);

		return tac_wait;
	}

	if (w != (ev_watcher *)&s->ev && w != (ev_watcher *)&s->timer)
		return tac_alien_event;

	ev_timer_stop(&s->timer);
	ev_io_stop(&s->ev);
	s->ev.fd = -1;

	if (w == (ev_watcher *)&s->timer) {
		errno = ETIMEDOUT;
		goto error;
	}

	int		optval = 1;
	socklen_t 	optlen = sizeof(optval);
	if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
		goto error;

	if (optval != 0) {
		errno = optval;
		goto error;
	}

	return fd;
error:
	s->error_tstamp = ev_now();
	if (fd > 0)
		close(fd);
	return tac_error;
}

void
rendevouz(va_list ap)
{
	struct sockaddr_in 	*self_addr = va_arg(ap, struct sockaddr_in *);
	struct tac_list 	*list = va_arg(ap, struct tac_list *);
	struct tac_state 	*ts, *tmp;
	ev_watcher		*w = NULL;
	ev_timer		timer = { .coro=1 };

	ev_timer_init(&timer, (void *)fiber, 0, 1.);
	ev_timer_start(&timer);
loop:
	w = yield();

	SLIST_FOREACH_SAFE(ts, list, link, tmp) {
		if (ts->io->fd >= 0)
			continue;

		int r = tcp_async_connect(ts, w, self_addr, 5);
		switch (r) {
		case tac_wait:
		case tac_alien_event:
			continue;
		case tac_error:
			if (!ts->error_printed)
				say_syserror("connect to %s/%s failed", ts->name, sintoa(&ts->daddr));
			ts->error_printed = true;
			break;
		default:
			assert(ts->io->fd < 0);
			say_info("connect(%i) to %s/%s", r, ts->name, sintoa(&ts->daddr));
			netmsg_io_setfd(ts->io, r);
			ev_io_start(&ts->io->in);
			if (ts->io->wbuf.bytes > 0)
				ev_io_start(&ts->io->out);
			ts->error_printed = false;
			break;
		}
		[ts->io tac_event:r];
	}

	goto loop;
}



int
tcp_connect(struct sockaddr_in *daddr, struct sockaddr_in *saddr, ev_tstamp timeout) {
	struct tac_state s = { .daddr = *daddr, .ev = {.fd = -1} };
	ev_watcher *w = NULL;

	for(;;) {
		int r = tcp_async_connect(&s, w, saddr, timeout);
		switch (r) {
		case tac_wait:
			w = yield();
			break;
		case tac_error:
			return -1;
		case tac_alien_event:
			abort();
		default:
			return r;
		}
	}
}

struct sockaddr_in *
sinany(struct sockaddr_in *sin, int port)
{
	memset(sin, 0, sizeof(struct sockaddr_in));
	sin->sin_family = AF_INET;
	sin->sin_port = htons(port);
	sin->sin_addr.s_addr = INADDR_ANY;
	return sin;
}

int
server_socket(int type, struct sockaddr_in *sin, int nonblock,
	      void (*on_bind)(int fd), void (*sleep)(ev_tstamp tm))
{
	int fd;
	bool warning_said = false;
	int one = 1;
	struct linger ling = { 0, 0 };
	nonblock = !!nonblock;

	if ((fd = socket(AF_INET, type, 0)) == -1) {
		say_syserror("socket");
		return -1;
	}

	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == -1 ||
	    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) == -1 ||
	    setsockopt(fd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling)) == -1)
	{
		say_syserror("setsockopt");
		return -1;
	}

	if (type == SOCK_STREAM)
		if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == -1) {
			say_syserror("setsockopt");
			return -1;
		}

	if (ioctl(fd, FIONBIO, &nonblock) < 0) {
		say_syserror("ioctl");
		return -1;
	}

#if OCT_CHILDREN
	int keepalive_count = 0;
#endif
retry_bind:
	if (bind(fd, (struct sockaddr *)sin, sizeof(*sin)) == -1) {
		if (on_bind != NULL)
			on_bind(-1);

		if (errno == EADDRINUSE && sleep != NULL) {
			if (!warning_said) {
				say_syserror("bind(%s)", sintoa(sin));
				say_info("will retry binding after 0.1 seconds.");
				warning_said = true;
			}
			sleep(0.1);

#if OCT_CHILDREN
			/* it is possible to main process die while we looping here
			   so, ping it at least one time a second and die with him*/
			if (master_pid != getpid() && keepalive_count++ > 10) {
				keepalive_count = 0;
				keepalive();
			}
#endif
			goto retry_bind;
		}
		say_syserror("bind(%s)", sintoa(sin));
		return -1;
	}

	if (on_bind != NULL)
		on_bind(fd);

	if (type == SOCK_STREAM)
		if (listen(fd, cfg.backlog) == -1) {
			say_syserror("listen");
			close(fd);
			return -1;
		}

	say_info("bound to %s/%s", type == SOCK_STREAM ? "TCP" : "UDP", sintoa(sin));
	return fd;
}


void
tcp_server(va_list ap)
{
	struct tcp_server_state state;
	state.addr = va_arg(ap, const char *);
	state.handler = va_arg(ap, void (*)(int, void *, struct tcp_server_state *));
	state.on_bind = va_arg(ap, void (*)(int fd));
	state.data = va_arg(ap, void *);

	int cfd, fd, one = 1;

	if (!state.addr)
		return; /* exit before yield() will prevent fiber creation */

	if (atosin(state.addr, &state.saddr) < 0)
		return;

	if ((fd = server_socket(SOCK_STREAM, &state.saddr, 1, state.on_bind, fiber_sleep)) < 0)
		return;

	state.io = (ev_io){ .coro = 1 };
	ev_io_init(&state.io, (void *)fiber, fd, EV_READ);

	ev_io_start(&state.io);
	while (ev_is_active(&state.io)) {
		yield();

		while ((cfd = accept(fd, NULL, NULL)) > 0) {
			if (ioctl(cfd, FIONBIO, &one) < 0) {
				say_syserror("ioctl");
				close(cfd);
				continue;
			}

			if (setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == -1) {
				say_syserror("setsockopt failed");
				/* Do nothing, not a fatal error.  */
			}

			state.handler(cfd, state.data, &state);
			if (!ev_is_active(&state.io)) {
				return;
			}
		}

		if (errno == EINVAL || errno == EBADF || errno == ENOTSOCK) {
			say_debug("tcp_socket acceptor were closed on : %s", state.addr);
			ev_io_stop(&state.io);
			break;
		}

		if (errno == EMFILE) {
			say_error("can't accept, too many open files, throttling");
			ev_io_stop(&state.io);
			fiber_sleep(0.5);
			ev_io_start(&state.io);
			continue;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			continue;

		say_syserror("accept");
		fiber_sleep(1);
	}
}

void
tcp_server_stop(struct tcp_server_state *state)
{
	ev_io_stop(&state->io);
	close(state->io.fd);
}

void
udp_server(va_list ap)
{
	const char *addr = va_arg(ap, const char *);
	void (*handler)(const char *buf, ssize_t len, void *data) =
		va_arg(ap, void (*)(const char *, ssize_t, void *));
	void (*on_bind)(int fd) = va_arg(ap, void (*)(int fd));
	void *data = va_arg(ap, void *);
	int fd;
	struct sockaddr_in saddr;

	if (!addr)
		return; /* exit before yield() will prevent fiber creation */

	if (atosin(addr, &saddr) < 0)
		return;

	if ((fd = server_socket(SOCK_DGRAM, &saddr, 1, on_bind, NULL)) < 0)
		return;

	ssize_t sz;
	const unsigned MAXUDPPACKETLEN = 65527 + 1; /* +1 for \0 */

#if HAVE_RECVMMSG
#ifndef RECVMMSG_VEC
#define RECVMMSG_VEC 128
#endif
	struct mmsghdr *msgvec;
	struct iovec *iovec;

	msgvec = xcalloc(RECVMMSG_VEC, sizeof(*msgvec));
	iovec = xcalloc(RECVMMSG_VEC, sizeof(*iovec));

	char *buf = xmalloc(MAXUDPPACKETLEN * RECVMMSG_VEC);

	for (int i = 0; i < RECVMMSG_VEC; i++) {
		iovec[i].iov_base = buf + MAXUDPPACKETLEN * i;
		iovec[i].iov_len = MAXUDPPACKETLEN;
		msgvec[i].msg_hdr.msg_iov = iovec + i;
		msgvec[i].msg_hdr.msg_iovlen = 1;
	}
#else
	char *buf = xmalloc(MAXUDPPACKETLEN);
#endif
	ev_io io = { .coro = 1};
	ev_io_init(&io, (void *)fiber, fd, EV_READ);

	ev_io_start(&io);
	while (1) {
		yield();

#if HAVE_RECVMMSG
		while ((sz = recvmmsg(fd, msgvec, RECVMMSG_VEC, MSG_DONTWAIT, NULL)) > 0)
			for (int i = 0; i < sz; i++)
				handler(msgvec[i].msg_hdr.msg_iov->iov_base,
					msgvec[i].msg_len,
					data);
#else
		while ((sz = recv(fd, buf, MAXUDPPACKETLEN, MSG_DONTWAIT)) > 0)
			handler(buf, sz, data);
#endif
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			continue;

		say_syserror("recvfrom");
		fiber_sleep(1);
	}
}

int
atosin(const char *orig, struct sockaddr_in *addr)
{
	int port;
	addr->sin_family = AF_UNSPEC;

	if (orig == NULL || *orig == 0) {
		addr->sin_addr.s_addr = INADDR_ANY;
		return -1;
	}

	char str[25];
	strncpy(str, orig, 25);
	str[24] = 0;

	char *colon = strchr(str, ':');

	if (colon != NULL) {
		*colon = 0;
		port = atoi(colon + 1); /* port is next after ':' */
	} else {
		port = atoi(str);
	}

	if (port <= 0 || port >= 0xffff) {
		say_error("bad port in addr: %s", orig);
		return -1;
	}

	memset(addr, 0, sizeof(*addr));

	if (colon == NULL || colon == str) { /* "33013" ":33013" */
		addr->sin_addr.s_addr = INADDR_ANY;
	} else {
		if (inet_aton(str, &addr->sin_addr) == 0) {
			say_syserror("inet_aton");
			return -1;
		}
	}

	addr->sin_family = AF_INET;
	addr->sin_port = htons(port);
	return 0;
}

const char *
sintoa(const struct sockaddr_in *addr)
{
	static char buf[22]; /* strlen(xxx.xxx.xxx.xxx:yyyyy) + 1 */
	snprintf(buf, sizeof(buf), "%s:%i",
		 inet_ntoa(addr->sin_addr), ntohs(addr->sin_port));
	return buf;
}

int
net_fixup_addr(char **addr, int port)
{
	if (*addr == NULL && port == 0)
		return 0;

	if (*addr == NULL && port > 0) /* port override disabled addr */
		*addr = "";

	assert(*addr);

	if (port) {
		if (strlen(*addr) == 0) { /* special case for INADDR_ANY, compat with prev. versions */
			char *tmp = malloc(6);
			sprintf(tmp, "%i", port);
			*addr = tmp;
			return 1;
		}

		int ret = 1;
		char *c = strchr(*addr, ':');
		if (c)  {
			if (atoi(c + 1) == port) /* already fixed */
				return 0;
			*c = 0;
			ret = -1;
		}

		char *end;
		int aport = strtol(*addr, &end, 10);
		if (*end == 0) {
			if (aport == port) /* already fixed, INADDR_ANY special case */
				return 0;
			*addr = "";
			ret = -1;
		}

		/* len = addr + ':' + 5 digit max + \0 */
		char *tmp = malloc(strlen(*addr) + 7);
		sprintf(tmp, "%s:%i", *addr, port);
		*addr = tmp;
		return ret;
	}

	if (port == 0 && strlen(*addr) == 0) {
		*addr = NULL;
		return 1;
	}

	return 0;
}

static void netmsg_ctor(void *ptr)
{
	struct netmsg *n = ptr;
	memset(n->ref, 0, NETMSG_IOV_SIZE * sizeof(n->ref[0]));
	memset(n->iov, 0, NETMSG_IOV_SIZE * sizeof(n->iov[0]));
	n->count = 0;
}

static void __attribute__((constructor))
init_slab_cache(void)
{
	slab_cache_init(&netmsg_cache, sizeof(struct netmsg), SLAB_GROW, "net_io/netmsg");
	netmsg_cache.ctor = netmsg_ctor;
}


register_source();
