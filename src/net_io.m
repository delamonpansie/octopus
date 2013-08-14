/*
 * Copyright (C) 2011, 2012 Mail.RU
 * Copyright (C) 2011, 2012 Yuriy Vostrikov
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

struct slab_cache conn_cache, netmsg_cache;

static struct netmsg *
netmsg_alloc(struct netmsg_head *h)
{
	struct netmsg *n = slab_cache_alloc(&netmsg_cache);

	memset(n->ref, 0, NETMSG_IOV_SIZE * sizeof(n->ref[0]));
	memset(n->iov, 0, NETMSG_IOV_SIZE * sizeof(n->iov[0]));
	n->count = 0;
	n->barrier = 0;

	TAILQ_INSERT_HEAD(&h->q, n, link);
	return n;
}

void
netmsg_head_init(struct netmsg_head *h, struct palloc_pool *pool)
{
	TAILQ_INIT(&h->q);
	h->pool = pool;
	h->bytes = 0;
	netmsg_alloc(h);
}

static void
netmsg_unref(struct netmsg *m, int from)
{
	bool have_lua_refs = 0;
	for (int i = from; i < m->count; i++) {
		if (m->ref[i] == 0)
			continue;

		if (m->ref[i] & 1)
			have_lua_refs = 1;
		else
			object_decr_ref((struct tnt_object *)m->ref[i]);
	}

	if (have_lua_refs) {
		lua_State *L = fiber->L;
		lua_getglobal(L, "__netmsg_unref");
		lua_pushlightuserdata(L, m);
		lua_pushinteger(L, from);
		lua_call(L, 2, 0);
	}
	memset(m->ref + from, 0, (m->count - from) * sizeof(m->ref[0]));
	memset(m->iov + from, 0, (m->count - from) * sizeof(m->iov[0]));
}

void
netmsg_release(struct netmsg_head *h, struct netmsg *m)
{
	netmsg_unref(m, 0);
	m->count = 0;
	m->barrier = 0;
	if (TAILQ_FIRST(&h->q) == m)
		return;

	TAILQ_REMOVE(&h->q, m, link);
	slab_cache_free(&netmsg_cache, m);
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
netmsg_rewind(struct netmsg_head *h, struct netmsg_mark *mark)
{
	struct netmsg *m, *tvar;
	TAILQ_FOREACH_SAFE(m, &h->q, link, tvar) {
		if (m == mark->m)
			break;

		for (int i = 0; i < m->count; i++)
			h->bytes -= m->iov[i].iov_len;
		netmsg_release(h, m);
	}
	assert(m == mark->m);

	for (int i = mark->offset; i < mark->m->count; i++)
		h->bytes -= mark->m->iov[i].iov_len;
	netmsg_unref(mark->m, mark->offset);

	m->count = mark->offset;
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
	struct netmsg *m = TAILQ_FIRST(&h->q);
	struct iovec *v = m->iov + m->count;

	h->bytes += len;
	v->iov_base = (char *)buf;
	v->iov_len = len;

	/* *((*m)->ref + (*m)->count) is NULL here. see netmsg_unref() */

	if (unlikely(++(m->count) == nelem(m->iov)))
		netmsg_alloc(h);
}


struct iovec *
net_reserve_iov(struct netmsg_head *h)
{
	struct netmsg *m = TAILQ_FIRST(&h->q);
	struct iovec *v = m->iov + m->count;
	net_add_iov(h, NULL, 0);
	return v;
}

void
net_add_iov_dup(struct netmsg_head *h, const void *buf, size_t len)
{
	void *copy = palloc(h->pool, len);
	memcpy(copy, buf, len);
	net_add_iov(h, copy, len);
}

void
net_add_ref_iov(struct netmsg_head *h, uintptr_t obj, const void *buf, size_t len)
{
	struct netmsg *m = TAILQ_FIRST(&h->q);
	struct iovec *v = m->iov + m->count;
	v->iov_base = (char *)buf;
	v->iov_len = len;

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
	void *prev = NULL;
	do {
		struct iovec *src = m->iov,
			     *end = src + m->count;

		while (src < end) {
			if (prev == src->iov_base) {
				(buf - 1)->iov_len += src->iov_len;
				prev += src->iov_len;
			} else {
				*buf++ = *src;
				free--;
				prev = src->iov_base + src->iov_len;
			}
			src++;
		}
		m->barrier = buf;
		m = TAILQ_PREV(m, netmsg_tailq, link);
	} while (m != NULL && free >= m->count);
	return buf;
}

ssize_t
conn_write_netmsg(struct conn *c)
{
	struct netmsg_head *head = &c->out_messages;
	struct iovec *iov = c->iov, *end;
	ssize_t result = 0;

	if (unlikely(c->iov_offset & 1)) {
		iov = c->iov + (c->iov_offset >> 1);
		end = c->iov_end;
	} else {
		if (unlikely(head->bytes == 0))
			return result;

		iov = c->iov;
		end = netmsg2iovec(iov, TAILQ_LAST(&head->q, netmsg_tailq));
	}

	while (end > iov) {
		ssize_t r = writev(c->fd, iov, end - iov);
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
	};


	if (end != iov) {
		/* lower bit is used as flag */
		c->iov_offset = (iov - c->iov) << 1 | 1;
		c->iov_end = end;
		if (TAILQ_FIRST(&head->q)->barrier != NULL)
			netmsg_alloc(head);
	} else {
		c->iov_offset = 0;
	}


	struct netmsg *m, *tmp;
	TAILQ_FOREACH_REVERSE_SAFE(m, &head->q, netmsg_tailq, link, tmp) {
		if (m->barrier == NULL || m->barrier > iov)
			break;
		netmsg_release(head, m);
	}

	return result;
}

ssize_t
conn_flush(struct conn *c)
{
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, c->fd, EV_WRITE);
	ev_io_start(&io);
	do {
		yield();
	} while (conn_write_netmsg(c) > 0);
	ev_io_stop(&io);

	return c->out_messages.bytes == 0 ? 0 : -1;
}

void
conn_set(struct conn *c, int fd)
{
	assert(c->out.cb != NULL && c->in.cb != NULL);
	assert(fd >= 0);

	c->fd = fd;
	ev_io_set(&c->in, c->fd, EV_READ);
	ev_io_set(&c->out, c->fd, EV_WRITE);
}

struct conn *
conn_init(struct conn *c, struct palloc_pool *pool, int fd, struct fiber *in, struct fiber *out,
	  enum conn_memory_ownership memory_ownership)
{
	assert(in != NULL && out != NULL);

	say_debug("%s: c:%p fd:%i", __func__, c, fd);
	assert(memory_ownership & MO_CONN_OWNERSHIP_MASK);
	if (!c) {
		assert(memory_ownership & MO_SLAB);
		c = slab_cache_alloc(&conn_cache);
	}

	c->memory_ownership = memory_ownership;
	if (pool == NULL || memory_ownership & MO_MY_OWN_POOL) {
		c->memory_ownership |= MO_MY_OWN_POOL;

		c->pool = palloc_create_pool("connection owned pool");
	} else {
		c->pool = pool;
	}

	netmsg_head_init(&c->out_messages, c->pool);

	c->ref = 0;
	c->fd = fd;
	c->state = fd >= 0 ? CONNECTED : CLOSED;
	c->peer_name[0] = 0;
	c->processing_link.tqe_prev = NULL;
	c->iov_offset = 0;

	ev_init(&c->in, (void *)in);
	ev_init(&c->out, (void *)out);
	c->out.coro = c->in.coro = 1;
	c->out.data = c->in.data = c;


	c->rbuf = tbuf_alloc(c->pool);

	if (fd >= 0)
		conn_set(c, fd);
	return c;
}


void
conn_gc(struct palloc_pool *pool, void *ptr)
{
	struct conn *c = ptr;
	struct netmsg *m;

	if (c->memory_ownership & MO_MY_OWN_POOL) {
		assert(pool == NULL);

		if (palloc_allocated(c->pool) < 128 * 1024)
			return;

		pool = palloc_create_pool("connection owned new pool");
	}

	c->rbuf = tbuf_clone(pool, c->rbuf);
	TAILQ_FOREACH(m, &c->out_messages.q, link)
		netmsg_gc(pool, m);

	if (c->memory_ownership & MO_MY_OWN_POOL)
		palloc_destroy_pool(c->pool);

	c->pool = c->out_messages.pool = pool;
}

ssize_t
conn_recv(struct conn *c)
{
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, c->fd, EV_READ);
	ev_io_start(&io);
	yield();
	ev_io_stop(&io);
	tbuf_ensure(c->rbuf, 16 * 1024);
	return tbuf_recv(c->rbuf, c->fd);
}

void
conn_reset(struct conn *c)
{
	struct netmsg *m, *tmp;
	TAILQ_FOREACH_SAFE(m, &c->out_messages.q, link, tmp)
		netmsg_release(&c->out_messages, m);

	c->iov_offset = 0;
	c->out_messages.bytes = 0;
}

static void
conn_free(struct conn *c)
{
	/*  as long as struct conn *C is alive, c->out_messages may be populated
	    by callbacks even if c->fd == -1, so drop all this data */
	conn_reset(c);
	slab_cache_free(&netmsg_cache, TAILQ_FIRST(&c->out_messages.q));

	if (c->service)
		LIST_REMOVE(c, link);
	c->service = NULL;

	if (c->memory_ownership & MO_MY_OWN_POOL)
		palloc_destroy_pool(c->pool);

	c->pool = NULL;
	c->out_messages.pool = NULL;

	switch (c->memory_ownership & MO_CONN_OWNERSHIP_MASK) {
		case MO_STATIC:
			break;
		case MO_MALLOC:
			free(c);
			break;
		case MO_SLAB:
			slab_cache_free(&conn_cache, c);
			break;
		default:
			abort();
	}
}

void
conn_unref(struct conn *c)
{
	assert(c->ref > 0);
	if (--c->ref == 0) {
		assert(c->state == CLOSED);
		conn_free(c);
	}
}

int
conn_close(struct conn *c)
{
	int r = 0;
	assert(c->fd > 0);

	tbuf_reset(c->rbuf);
	ev_io_stop(&c->out);
	ev_io_stop(&c->in);
	c->in.fd = c->out.fd = -1;

	/* the conn will either free'd or put back to pool,
	   so next time it get used it will be configure by conn_init */
	if ((c->memory_ownership & MO_CONN_OWNERSHIP_MASK) != MO_STATIC)
		c->in.cb = c->out.cb = NULL;

	r = close(c->fd);
	c->fd = -1;
	c->state = CLOSED;
	c->peer_name[0] = 0;

	if (c->service && c->processing_link.tqe_prev != NULL) {
		TAILQ_REMOVE(&c->service->processing, c, processing_link);
		c->processing_link.tqe_prev = NULL;
	}

	if (c->service)
		c->ref--; /* call to conn_unref() will cause recurion */

	/* either no refcounting used or c->service was the last owner.
	   release memory, since conn_unref() woudn't be called in both cases. */
	if (c->ref == 0)
		conn_free(c);

	return r;
}

ssize_t
conn_read(struct conn *c, void *buf, size_t count)
{
	ssize_t r, done = 0;
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, c->fd, EV_READ);
	ev_io_start(&io);

	while (count > done) {
		yield();
		r = read(c->fd, buf + done, count - done);

		if (unlikely(r <= 0)) {
			if (r < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					continue;
				say_syserror("%s: read", __func__);
				break;
			}
			if (r == 0) {
				say_debug("%s: c:%p fd:%i eof", __func__, c, c->fd);
				break;
			}
		}
		done += r;
	}
	ev_io_stop(&io);

	return done;
}

ssize_t
conn_write(struct conn *c, const void *buf, size_t count)
{
	int r;
	unsigned int done = 0;
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, c->fd, EV_WRITE);
	ev_io_start(&io);

	do {
		yield();
		if ((r = write(c->fd, buf + done, count - done)) < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				continue;
			say_syserror("%s: write", __func__);
			break;
		}
		done += r;
	} while (count != done);
	ev_io_stop(&io);

	return done;
}

char *
conn_peer_name(struct conn *c)
{
	struct sockaddr_in peer;
	socklen_t peer_len = sizeof(peer);

	if (c->fd < 3)
		return NULL;

	if (c->peer_name[0] != 0)
		return c->peer_name;

	memset(&peer, 0, peer_len);
	if (getpeername(c->fd, (struct sockaddr *)&peer, &peer_len) < 0)
		return NULL;

	uint32_t zero = 0;
	if (memcmp(&peer.sin_addr, &zero, sizeof(zero)) == 0)
		return NULL;

	snprintf(c->peer_name, sizeof(c->peer_name),
		 "%s:%d", inet_ntoa(peer.sin_addr), ntohs(peer.sin_port));

	return c->peer_name;
}

void
conn_flusher(va_list ap __attribute__((unused)))
{
	for (;;) {
		struct conn *c = ((struct ev_watcher *)yield())->data;
		ssize_t r = conn_write_netmsg(c);

		if (r < 0) {
			say_syswarn("%s%swritev() failed, closing connection",
				    c->service ? c->service->name : "",
				    c->service ? " " : "");
			conn_close(c);
			continue;
		}

		if (c->out_messages.bytes == 0)
			ev_io_stop(&c->out);

		/* c->processing_link.tqe_prev == NULL implies
		   that we'r reading (possibly) an oversize request */
		if ((tbuf_len(c->rbuf) < cfg.input_low_watermark || c->processing_link.tqe_prev == NULL) &&
		    c->out_messages.bytes < cfg.output_low_watermark &&
		    c->state != CLOSED)
			ev_io_start(&c->in);
	}
}

enum tac_state
tcp_async_connect(struct conn *c, ev_watcher *w /* result of yield() */,
		  struct sockaddr_in      *dst,
		  struct sockaddr_in      *src,
		  ev_tstamp               timeout)
{
	if (w == NULL) {
		/* init */
		int	optval = 1;

		c->fd = -1;
		c->out.coro = 1;
		c->timer.coro = 1;

		c->fd = socket(AF_INET, SOCK_STREAM, 0);
		if (c->fd < 0) {
			say_syserror("socket");
			goto error;
		}

		if (ioctl(c->fd, FIONBIO, &optval) < 0) {
			say_syserror("ioctl");
			goto error;
		}

		if (setsockopt(c->fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1 ||
		    setsockopt(c->fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) == -1) {
			say_syserror("setsockopt");
			goto error;
		}

		if (src) {
			if (bind(c->fd, (struct sockaddr *)src, sizeof(*src)) < 0) {
				say_syserror("bind(%s)", sintoa(src));
				goto error;
			}
		}

		if (setsockopt(c->fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) < 0)
			say_syserror("setsockopt(SO_KEEPALIVE)");

		if (connect(c->fd, (struct sockaddr *)dst, sizeof(*dst)) < 0) {
			if (errno != EINPROGRESS)
				goto error;
		}

		ev_io_init(&c->out, (void *)fiber, c->fd, EV_WRITE);
		ev_timer_init(&c->timer, (void *)fiber, timeout, 0.);
		if (timeout > 0)
			ev_timer_start(&c->timer);
		ev_io_start(&c->out);

		return tac_wait;
	}

	int		optval = 1;
	socklen_t 	optlen = sizeof(optval);

	if (w == (ev_watcher *)&c->timer) {
		ev_timer_stop(&c->timer);
		ev_io_stop(&c->out);
		goto error;
	}

	if (w != (ev_watcher *)&c->out)
		return tac_alien_event;

	ev_timer_stop(&c->timer);
	ev_io_stop(&c->out);

	if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
		goto error;

	if (optval != 0) {
		errno = optval;
		goto error;
	}
	return tac_ok;

error:
	close(c->fd);
	c->fd = -1;

	return tac_error;
}

int
tcp_connect(struct sockaddr_in *dst, struct sockaddr_in *src, ev_tstamp timeout) {
	struct 		conn			c;
	ev_watcher 				*w = NULL;
	int					fd;

	for(;;) {
		switch(tcp_async_connect(&c, w, dst, src, timeout)) {
			case tac_ok:
				return c.fd;
			case tac_wait:
				w = yield();
				break;
			case tac_error:
				return -1;
			case tac_alien_event:
			default:
				abort();
		}
	}

	return fd;
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


	int keepalive_count = 0;
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

			/* it is possible to main process die while we looping here
			   so, ping it at least one time a second and die with him*/
			if (keepalive_count++ > 10) {
				keepalive_count = 0;
				keepalive();
			}
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
	const char *addr = va_arg(ap, const char *);
	void (*handler)(int fd, void *data) = va_arg(ap, void (*)(int, void *));
	void (*on_bind)(int fd) = va_arg(ap, void (*)(int fd));
	void *data = va_arg(ap, void *);

	struct sockaddr_in saddr;
	int cfd, fd, one = 1;

	if (!addr)
		return; /* exit before yield() will prevent fiber creation */

	if (atosin(addr, &saddr) < 0)
		return;

	if ((fd = server_socket(SOCK_STREAM, &saddr, 1, on_bind, fiber_sleep)) < 0)
		return;

	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_READ);

	ev_io_start(&io);
	while (1) {
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

			handler(cfd, data);
		}

		if (errno == EMFILE) {
			say_error("can't accept, too many open files, throttling");
			ev_io_stop(&io);
			fiber_sleep(0.5);
			ev_io_start(&io);
			continue;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			continue;

		say_syserror("accept");
		fiber_sleep(1);
	}
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

	const unsigned MAXUDPPACKETLEN = 65527 + 1; /* +1 for \0 */
	char *buf = xmalloc(MAXUDPPACKETLEN);
	ssize_t sz;
	ev_io io = { .coro = 1};
	ev_io_init(&io, (void *)fiber, fd, EV_READ);

	ev_io_start(&io);
	while (1) {
		yield();

		while ((sz = recv(fd, buf, MAXUDPPACKETLEN, MSG_DONTWAIT)) > 0)
			handler(buf, sz, data);

		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			continue;

		say_syserror("recvfrom");
		fiber_sleep(1);
	}
}

static void
input_reader(va_list ap __attribute__((unused)))
{
	struct conn *c;
	ev_watcher *w;
	ssize_t r;

loop:
	w = yield();
	c = w->data;

	tbuf_ensure(c->rbuf, cfg.input_buffer_size);
	r = tbuf_recv(c->rbuf, c->fd);

	if (likely(r > 0)) {
		/* trigger processing of data.
		   c->service->processing will be traversed by wakeup_workers() */
		if (c->processing_link.tqe_prev == NULL)
			TAILQ_INSERT_HEAD(&c->service->processing, c, processing_link);
	} else if (r == 0) {
		say_debug("%s client closed connection", c->service->name);
		conn_close(c);
	} else if (r < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			goto loop;
		say_syswarn("%s recv() failed, closing connection", c->service->name);
		conn_close(c);
	}

	goto loop;
}

void
wakeup_workers(ev_prepare *ev)
{
	struct service *service = (void *)ev - offsetof(struct service, wakeup);
	struct fiber *w;

	while (!TAILQ_EMPTY(&service->processing)) {
		w = SLIST_FIRST(&service->workers);
		if (w == NULL)
			return;
		SLIST_REMOVE_HEAD(&service->workers, worker_link);
		resume(w, NULL);
	}
}

static void
service_gc(struct palloc_pool *pool, void *ptr)
{
	struct service *s = ptr;
	struct conn *c;

	s->pool = pool;
	LIST_FOREACH(c, &s->conn, link)
		conn_gc(pool, c);
}

static void
accept_client(int fd, void *data)
{
	struct service *service = data;
	struct conn *clnt = conn_init(NULL, service->pool, fd,
				      service->input_reader, service->output_flusher, MO_SLAB);
	clnt->ref++; /* there is special handling of this in conn_close() */
	LIST_INSERT_HEAD(&service->conn, clnt, link);
	clnt->service = service;
	ev_io_start(&clnt->in);
	clnt->state = CONNECTED;
}

void
tcp_service(struct service *service, const char *addr, void (*on_bind)(int fd), void (*wakeup_workers)(ev_prepare *))
{
	memset(service, 0, sizeof(*service));
	char *name = xmalloc(strlen("iproto:") + strlen(addr) + 1);
	sprintf(name, "tcp:%s", addr);

	TAILQ_INIT(&service->processing);
	service->pool = palloc_create_pool(name);
	service->name = name;
	service->batch = 64;

	palloc_register_gc_root(service->pool, service, service_gc);

	service->output_flusher = fiber_create("tcp/output_flusher", conn_flusher);
	service->input_reader = fiber_create("tcp/input_reader", input_reader);
	service->acceptor = fiber_create("tcp/acceptor", tcp_server, addr, accept_client, on_bind, service);
	if (service->acceptor == NULL)
		panic("unable to start tcp_service `%s'", addr);

	ev_prepare_init(&service->wakeup, (void *)wakeup_workers);
	ev_prepare_start(&service->wakeup);
}

void
service_info(struct tbuf *out, struct service *service)
{
	struct conn *c;
	struct netmsg *m;

	tbuf_printf(out, "%s:" CRLF, service->name);
	LIST_FOREACH(c, &service->conn, link) {
		tbuf_printf(out, "    - peer: %s" CRLF, conn_peer_name(c));
		tbuf_printf(out, "      fd: %i" CRLF, c->fd);
		tbuf_printf(out, "      state: %i,%s%s" CRLF, c->state,
			    ev_is_active(&c->in) ? "in" : "",
			    ev_is_active(&c->out) ? "out" : "");
		tbuf_printf(out, "      rbuf: %i" CRLF, tbuf_len(c->rbuf));
		tbuf_printf(out, "      pending_bytes: %zi" CRLF, c->out_messages.bytes);
		if (!TAILQ_EMPTY(&c->out_messages.q))
			tbuf_printf(out, "      out_messages:" CRLF);
		TAILQ_FOREACH(m, &c->out_messages.q, link)
			tbuf_printf(out, "      - { count: %i }" CRLF, m->count);
	}
}

int
atosin(const char *orig, struct sockaddr_in *addr)
{
	int port;
	char *str = strdupa(orig);
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
	addr->sin_family = AF_INET;

	if (colon == NULL || colon == str) { /* "33013" ":33013" */
		addr->sin_addr.s_addr = INADDR_ANY;
	} else {
		if (inet_aton(str, &addr->sin_addr) == 0) {
			say_syserror("inet_aton");
			return -1;
		}
	}

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

static void __attribute__((constructor))
init_slab_cache(void)
{
	slab_cache_init(&conn_cache, sizeof(struct conn), SLAB_GROW, "net_io/conn");
	slab_cache_init(&netmsg_cache, sizeof(struct netmsg), SLAB_GROW, "net_io/netmsg");
}


register_source();
