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
	n->count = n->offset = 0;
	n->head = h;

	TAILQ_INSERT_TAIL(&h->q, n, link);
	return n;
}

struct netmsg *
netmsg_tail(struct netmsg_head *h)
{
	if (!TAILQ_EMPTY(&h->q))
		return TAILQ_LAST(&h->q, netmsg_tailq);
	else
		return netmsg_alloc(h);
}


static void
netmsg_unref(struct netmsg *m, int from)
{
	struct tnt_object **obj = m->ref;

	for (int i = from; i < m->count; i++) {
		if (obj[i] == 0)
			continue;

		if ((uintptr_t)obj[i] & 1)
			luaL_unref(root_L, LUA_REGISTRYINDEX, (uintptr_t)obj[i] >> 1);
		else
			object_decr_ref(obj[i]);
		obj[i] = 0;
	}
}

void
netmsg_release(struct netmsg *m)
{
	netmsg_unref(m, 0);
	TAILQ_REMOVE(&m->head->q, m, link);
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
	struct netmsg *m, *tmp, *tail;

	tail = TAILQ_EMPTY(&dst->q) ? NULL : TAILQ_LAST(&dst->q, netmsg_tailq);

	dst->bytes += src->bytes;
	src->bytes = 0;
	TAILQ_FOREACH_SAFE(m, &src->q, link, tmp) {
		TAILQ_REMOVE(&src->q, m, link); /* FIXME: TAILQ_INIT ? */
		if (src->pool != dst->pool)
			netmsg_gc(dst->pool, m);

		if (tail && nelem(tail->iov) - tail->count > m->count) {
			memcpy(tail->iov + tail->count, m->iov, sizeof(m->iov[0]) * m->count);
			memcpy(tail->ref + tail->count, m->ref, sizeof(m->ref[0]) * m->count);
			tail->count += m->count;

			slab_cache_free(&netmsg_cache, m);
		} else {
			m->head = dst;
			TAILQ_INSERT_TAIL(&dst->q, m, link);
			tail = m;
		}
	}
	return tail;
}

void
netmsg_rewind(struct netmsg **m, struct netmsg_mark *mark)
{
	struct netmsg *tail, *tvar;
	struct netmsg_head *h = (*m)->head;

	TAILQ_FOREACH_REVERSE_SAFE(tail, &h->q, netmsg_tailq, link, tvar) {
		if (tail == mark->m)
			break;
		netmsg_release(tail);
	}

	netmsg_unref(mark->m, mark->offset + 1);
	*m = mark->m;
	(*m)->count = mark->offset + 1;
}

void
netmsg_getmark(struct netmsg *m, struct netmsg_mark *mark)
{
	mark->m = m;
	mark->offset = m->count;
}

static void __attribute__((noinline))
enlarge(struct netmsg **m)
{
	*m = netmsg_alloc((*m)->head);
}


void
net_add_iov(struct netmsg **m, const void *buf, size_t len)
{
	struct iovec *v = (*m)->iov + (*m)->count;
	v->iov_base = (char *)buf;
	v->iov_len = len;

	(*m)->head->bytes += len;
	/* *((*m)->ref + (*m)->count) is NULL here. see netmsg_unref() */

#ifdef NET_IO_TIMESTAMPS
	(*m)->tstamp[(*m)->count] = ev_now();
#endif
	if (unlikely(++(*m)->count == nelem((*m)->iov)))
		enlarge(m);
}

struct iovec *
net_reserve_iov(struct netmsg **m)
{
	struct iovec *v = (*m)->iov + (*m)->count;
	net_add_iov(m, NULL, 0);
	return v;
}

void
net_add_iov_dup(struct netmsg **m, const void *buf, size_t len)
{
	void *copy = palloc((*m)->head->pool, len);
	memcpy(copy, buf, len);
	return net_add_iov(m, copy, len);
}

void
net_add_ref_iov(struct netmsg **m, struct tnt_object *obj, const void *buf, size_t len)
{
	struct tnt_object **ref = (*m)->ref + (*m)->count;
	struct iovec *v = (*m)->iov + (*m)->count;
	v->iov_base = (char *)buf;
	v->iov_len = len;

	(*m)->head->bytes += len;
	*ref = obj;
	object_incr_ref(obj);

#ifdef NET_IO_TIMESTAMPS
	(*m)->tstamp[(*m)->count] = ev_now();
#endif

	if (unlikely(++(*m)->count == nelem((*m)->iov)))
		enlarge(m);
}

extern const char *netmsglib_name;
void
net_add_lua_iov(struct netmsg **m, lua_State *L, int str)
{
	struct tnt_object **ref = (*m)->ref + (*m)->count;
	struct iovec *v = (*m)->iov + (*m)->count;

	lua_pushvalue(L, str);
	v->iov_base = (char *)lua_tolstring(L, -1, &v->iov_len);
	(*m)->head->bytes += v->iov_len;
	uintptr_t obj = luaL_ref(L, LUA_REGISTRYINDEX);
	*ref = (void *)(obj * 2 + 1);

#ifdef NET_IO_TIMESTAMPS
	(*m)->tstamp[(*m)->count] = ev_now();
#endif

	if (unlikely(++(*m)->count == nelem((*m)->iov)))
		enlarge(m);
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


ssize_t
conn_write_netmsg(struct conn *c)
{
	struct netmsg_head *head = &c->out_messages;
	struct netmsg *m;
	ssize_t ret = 0;
restart:
	m = TAILQ_FIRST(&head->q);
	if (m == NULL)
		return ret;

	struct iovec *iov = m->iov + m->offset;
	unsigned iov_cnt = m->count - m->offset;
	ssize_t r = 0;

	while (iov_cnt > 0) {
		r = writev(c->fd, iov, iov_cnt);
		if (unlikely(r < 0)) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;

			say_syserror("%s: writev", __func__);
			if (ret == 0)
				ret = r;
			break;
		};
		head->bytes -= r;
		ret += r;

		do {
			if (iov->iov_len > r) {
				iov->iov_base += r;
				iov->iov_len -= r;
				break;
			} else {
				r -= iov->iov_len;
				iov++;
				iov_cnt--;
			}
		} while (iov_cnt > 0);
	};

#ifdef NET_IO_TIMESTAMPS
	for (unsigned i = m->offset; i < m->count - iov_cnt; i++)
		if (ev_now() - m->tstamp[i] > NET_IO_TIMESTAMPS)
			say_warn("net_io c:%p out:%i delay: %.5f",
				 c, ev_is_active(&c->out),
				 ev_now() - m->tstamp[i]);
#endif

	if (iov_cnt > 0) {
		m->offset = m->count - iov_cnt;
		return ret;
	} else {
		netmsg_release(m);
		goto restart;
	}
}

ssize_t
conn_flush(struct conn *c)
{
	assert(c->out.cb == (void *)fiber);
	ev_io_start(&c->out);
	do {
		yield();
	} while (conn_write_netmsg(c) > 0);
	ev_io_stop(&c->out);

	return TAILQ_EMPTY(&c->out_messages.q) ? 0 : -1;
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
conn_init(struct conn *c, struct palloc_pool *pool, int fd, struct fiber *in, struct fiber *out, int ref)
{
	assert(ref >= -2 && ref <= 0);
	assert(in != NULL && out != NULL);

	say_debug("%s: c:%p fd:%i", __func__, c, fd);
	if (!c) {
		assert(ref == 0);
		c = slab_cache_alloc(&conn_cache);
	}

	TAILQ_INIT(&c->out_messages.q);
	c->out_messages.pool = pool;
	c->out_messages.bytes = 0;
	c->ref = ref;
	c->fd = fd;
	c->pool = pool;
	c->state = -1;
	c->peer_name[0] = 0;
	c->rbuf = tbuf_alloc(c->pool);

	ev_init(&c->in, (void *)in);
	ev_init(&c->out, (void *)out);
	c->out.coro = c->in.coro = 1;
	c->out.data = c->in.data = c;

	if (fd >= 0)
		conn_set(c, fd);
	return c;
}


void
conn_gc(struct palloc_pool *pool, void *ptr)
{
	struct conn *c = ptr;
	struct netmsg *m;

	c->pool = pool;
	c->rbuf = tbuf_clone(pool, c->rbuf);
	TAILQ_FOREACH(m, &c->out_messages.q, link)
		netmsg_gc(pool, m);
}

ssize_t
conn_recv(struct conn *c)
{
	ssize_t r;
	assert(c->in.cb == (void *)fiber);
	tbuf_ensure(c->rbuf, 16 * 1024);

	ev_io_start(&c->in);
again:
	yield();
	r = tbuf_recv(c->rbuf, c->fd);
	if (unlikely(r < 0)) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			goto again;
		say_syserror("%s", __func__);
	}
	ev_io_stop(&c->in);
	return r;
}


int
conn_close(struct conn *c)
{
	int r = 0;

	if (c->fd > 0) {
		tbuf_reset(c->rbuf);
		ev_io_stop(&c->out);
		ev_io_stop(&c->in);
		c->in.fd = c->out.fd = -1;

		/* the conn will either free'd or put back to pool,
		   so next time it get used it will be configure by conn_init */
		if (c->ref != REF_STATIC)
			c->in.cb = c->out.cb = NULL;

		r = close(c->fd);
		c->fd = -1;
		c->peer_name[0] = 0;

		if (!TAILQ_EMPTY(&c->out_messages.q)) {
			say_error("client unexpectedly gone, some data unwritten");
			struct netmsg *m, *tmp;
			TAILQ_FOREACH_SAFE(m, &c->out_messages.q, link, tmp)
				netmsg_release(m);
		}

		if (c->service && c->state == PROCESSING) {
			TAILQ_REMOVE(&c->service->processing, c, processing_link);
			c->state = -1;
		}
		c->state = CLOSED;
	}

	switch (c->ref) {
	case REF_STATIC:
		return r;
	case REF_MALLOC:
		free(c);
		return r;
	case 0:
		if (c->service)
			LIST_REMOVE(c, link);

		c->service = NULL;
		c->pool = NULL;
		slab_cache_free(&conn_cache, c);
		return r;
	default:
		/* c->ref > 0 => some fiber still holding ref to connection */
		return 0;
	}
}

ssize_t
conn_read(struct conn *c, void *buf, size_t count)
{
	ssize_t r, done = 0;
	assert(c->in.cb == (void *)fiber);

	ev_io_start(&c->in);
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

	ev_io_stop(&c->out);
	return done;
}

ssize_t
conn_write(struct conn *c, const void *buf, size_t count)
{
	int r;
	unsigned int done = 0;
	assert(c->out.cb == (void *)fiber);
	ev_io_start(&c->out);

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
	ev_io_stop(&c->out);

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
service_output_flusher(va_list ap __attribute__((unused)))
{
	for (;;) {
		struct conn *c = ((struct ev_watcher *)yield())->data;
		ssize_t r = conn_write_netmsg(c);
		if (r < 0)
			conn_close(c);

		if (TAILQ_EMPTY(&c->out_messages.q))
			ev_io_stop(&c->out);

		if ((tbuf_len(c->rbuf) < cfg.input_low_watermark || c->state == READING) &&
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
	} else {
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
			return -1;
		}

	say_info("bound to %s/%s", type == SOCK_STREAM ? "TCP" : "UDP", sintoa(sin));
	return fd;
}


void
tcp_server(va_list ap)
{
	int port = va_arg(ap, int);
	void (*handler)(int fd, void *data) = va_arg(ap, void (*)(int, void *));
	void (*on_bind)(int fd) = va_arg(ap, void (*)(int fd));
	void *data = va_arg(ap, void *);

	int cfd, fd, one = 1;
	struct sockaddr_in addr;

	sinany(&addr, port);
	if ((fd = server_socket(SOCK_STREAM, &addr, 1, on_bind, fiber_sleep)) < 0)
		exit(EX_OSERR); /* TODO: better error handling */

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
	int port = va_arg(ap, int);
	void (*handler)(const char *buf, ssize_t len, void *data) =
		va_arg(ap, void (*)(const char *, ssize_t, void *));
	void (*on_bind)(int fd) = va_arg(ap, void (*)(int fd));
	void *data = va_arg(ap, void *);
	int fd;
	struct sockaddr_in addr;

	sinany(&addr, port);
	if ((fd = server_socket(SOCK_DGRAM, &addr, 1, on_bind, NULL)) < 0)
		exit(EX_OSERR); /* TODO: better error handling */

	const unsigned MAXUDPPACKETLEN = 128;
	char buf[MAXUDPPACKETLEN];
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
input_reader(va_list ap)
{
	struct service *service = va_arg(ap, struct service *);
	struct conn *c;
	ev_watcher *w;
	ssize_t r;

	say_info("input reader for service %s started", service->name);
loop:
	w = yield();
	c = w->data;

	tbuf_ensure(c->rbuf, cfg.input_buffer_size);
	r = tbuf_recv(c->rbuf, c->fd);

	if (likely(r > 0)) {
		if (tbuf_len(c->rbuf) > cfg.input_high_watermark)
			ev_io_stop(&c->in);
		if (c->state != PROCESSING) {
			TAILQ_INSERT_HEAD(&c->service->processing, c, processing_link);
			c->state = PROCESSING;
		}
	} else if (r == 0) {
		say_debug("closing conn c:%p fd:%i EOF", c, c->fd);
		conn_close(c);
	} else if (r < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			goto loop;
		say_debug("closing conn c:%p fd:%i r:%i errno:%i", c, c->fd, (int)r, errno);
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
				      service->input_reader, service->output_flusher, 0);
	LIST_INSERT_HEAD(&service->conn, clnt, link);
	clnt->service = service;
	ev_io_start(&clnt->in);
	clnt->state = READING;
}

struct service *
tcp_service(u16 port, void (*on_bind)(int fd), void (*wakeup_workers)(ev_prepare *))
{
	struct service *service = calloc(1, sizeof(*service));
	char *name = malloc(13);  /* strlen("iproto:xxxxx") */
	snprintf(name, 13, "tcp:%i", port);

	TAILQ_INIT(&service->processing);
	service->pool = palloc_create_pool(name);
	service->name = name;

	palloc_register_gc_root(service->pool, service, service_gc);

	service->output_flusher = fiber_create("tcp/output_flusher", service_output_flusher);
	service->input_reader = fiber_create("tcp/input_reader", input_reader, service);
	service->acceptor = fiber_create("tcp/acceptor", tcp_server, port, accept_client, on_bind, service);

	ev_prepare_init(&service->wakeup, (void *)wakeup_workers);
	ev_prepare_start(&service->wakeup);

	return service;
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
			tbuf_printf(out, "      - { offt: %i, count: %i }" CRLF, m->offset, m->count);
	}
}

int
atosin(const char *orig, struct sockaddr_in *addr)
{
	int port;
	char *str = strdupa(orig);
	char *colon = strchr(str, ':');

	if (colon == NULL)
		return -1;

	*colon = 0;

	memset(addr, 0, sizeof(*addr));
	addr->sin_family = AF_INET;

	if (strcmp(str, "ANY") != 0) {
		if (inet_aton(str, &addr->sin_addr) == 0) {
			say_syserror("inet_aton");
			return -1;
		}
	} else {
		addr->sin_addr.s_addr = INADDR_ANY;
	}

	port = atoi(colon + 1); /* port is next after ':' */
	if (port <= 0 || port >= 0xffff) {
		say_error("bad port: %s", colon + 1);
		return -1;
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

void __attribute__((constructor))
init_slab_cache(void)
{
	slab_cache_init(&conn_cache, sizeof(struct conn), SLAB_GROW, "net_io/conn");
	slab_cache_init(&netmsg_cache, sizeof(struct netmsg), SLAB_GROW, "net_io/netmsg");
}

register_source();
