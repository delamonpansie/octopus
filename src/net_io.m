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
#import <fiber.h>
#import <util.h>

#include <third_party/queue.h>

#include <stdlib.h>
#include <sys/uio.h>
#include <sysexits.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

SLIST_HEAD(, conn) conn_pool;
struct netmsg_tailq netmsg_pool;

static struct netmsg *
netmsg_alloc(struct netmsg_head *h)
{
	struct netmsg *n = TAILQ_FIRST(&netmsg_pool);
	if (!n)
		n = calloc(1, sizeof(*n));
	else
		TAILQ_REMOVE(&netmsg_pool, n, link);

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
		return  netmsg_alloc(h);
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
	}
}

void
netmsg_release(struct netmsg *m)
{
	netmsg_unref(m, 0);
	TAILQ_REMOVE(&m->head->q, m, link);
	TAILQ_INSERT_HEAD(&netmsg_pool, m, link);
}

static void
netmsg_gc(struct palloc_pool *pool, struct netmsg *m)
{
	for (int i = 0; i < m->count; i++) {
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

			TAILQ_INSERT_HEAD(&netmsg_pool, m, link);
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
	struct tnt_object **ref = (*m)->ref + (*m)->count;
	struct iovec *v = (*m)->iov + (*m)->count;
	v->iov_base = (char *)buf;
	v->iov_len = len;

	(*m)->head->bytes += len;
	*ref = NULL;

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

#ifdef NET_IO_TIMESTAMPS
	(*m)->tstamp[(*m)->count] = ev_now();
#endif

	if (unlikely(++(*m)->count == nelem((*m)->iov)))
		enlarge(m);

	object_incr_ref(obj);
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


struct netmsg *
conn_write_netmsg(struct conn *c)
{
	struct netmsg *m;
restart:
	m = TAILQ_FIRST(&c->out_messages.q);
	if (m == NULL)
		return NULL;

	struct iovec *iov = m->iov + m->offset;
	int iov_cnt = m->count - m->offset;
	ssize_t r = 0;
	while (iov_cnt > 0) {
		r = writev(c->fd, iov, MIN(iov_cnt, IOV_MAX));
		if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
				break;
		if (r < 0) {
			conn_close(c);
			break;
		};
		m->head->bytes -= r;

		while (iov_cnt > 0) {
			if (iov->iov_len > r) {
				iov->iov_base += r;
				iov->iov_len -= r;
				break;
			} else {
				r -= iov->iov_len;
				iov++;
				iov_cnt--;
			}
		}
	};

#ifdef NET_IO_TIMESTAMPS
	for (int i = m->offset; i < m->count - iov_cnt; i++)
		if (ev_now() - m->tstamp[i] > NET_IO_TIMESTAMPS)
			say_warn("net_io c:%p out:%i delay: %.5f",
				 c, ev_is_active(&c->out),
				 ev_now() - m->tstamp[i]);
#endif

	if (iov_cnt > 0) {
		m->offset = m->count - iov_cnt;
		return m;
	} else {
		netmsg_release(m);
		goto restart;
	}
}

ssize_t
conn_flush(struct conn *c)
{
	ev_io io = { .coro = 1, .cb = NULL };

	ev_io_init(&io, (void *)fiber, c->fd, EV_WRITE);
	ev_io_start(&io);
	do {
		yield();
	} while (conn_write_netmsg(c) && c->fd > 0);
	ev_io_stop(&io);

	return TAILQ_EMPTY(&(c->out_messages.q))  ? 0 : -1;
}

struct conn *
conn_create(struct palloc_pool *pool, int fd)
{
	struct conn *c = SLIST_FIRST(&conn_pool);
	if (c)
		SLIST_REMOVE_HEAD(&conn_pool, pool_link);
	else
		c = calloc(1, sizeof(*c));

	conn_init(c, pool, fd, 0);
	return c;
}

void
conn_init(struct conn *c, struct palloc_pool *pool, int fd, int ref)
{
	c->out.coro = c->in.coro = 1;
	c->out.data = c->in.data = c;

	TAILQ_INIT(&c->out_messages.q);
	c->out_messages.pool = pool;
	c->out_messages.bytes = 0;
	c->ref = ref;
	c->fd = fd;
	c->pool = pool;
	c->state = -1;
	c->peer_name[0] = 0;
	c->rbuf = tbuf_alloc(c->pool);
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
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, c->fd, EV_READ);
	tbuf_ensure(c->rbuf, 16 * 1024);

	ev_io_start(&io);
again:
	yield();
	r = tbuf_recv(c->rbuf, c->fd);
	if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		goto again;
	if (r < 0)
		say_syserror("%s:", __func__);
	ev_io_stop(&io);
	return r;
}


void
conn_close(struct conn *c)
{
	tbuf_reset(c->rbuf);
	if (c->fd > 0) {
		ev_io_stop(&c->out);
		ev_io_stop(&c->in);

		close(c->fd);
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
	}

	switch (c->ref) {
	case REF_STATIC:
		return;
	case REF_MALLOC:
		free(c);
		return;
	case 0:
		if (c->service)
			LIST_REMOVE(c, link);

		SLIST_INSERT_HEAD(&conn_pool, c, pool_link);
		c->service = NULL;
		c->pool = NULL;
		return;
	}
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

		if ((r = read(c->fd, buf + done, count - done)) <= 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				continue;
			else
				break;
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
	ev_io io = { .coro = 1, .cb = NULL };

	if ((r = write(c->fd, buf + done, count - done)) == -1) {
		if (errno != EAGAIN && errno != EWOULDBLOCK)
			return r;
	}
	done += r;

	if (count == done)
		return done;

	do {
		if ((r = write(c->fd, buf + done, count - done)) == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				if (!io.cb) {
					ev_io_init(&io, (void *)fiber, c->fd, EV_WRITE);
					ev_io_start(&io);
				}
				yield();
				continue;
			}
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
service_output_flusher(va_list ap __attribute__((unused)))
{
	for (;;) {
		struct conn *c = ((struct ev_watcher *)yield())->data;
		if (conn_write_netmsg(c) == NULL)
			ev_io_stop(&c->out);

		if ((tbuf_len(c->rbuf) < cfg.input_low_watermark || c->state == READING) &&
		    c->out_messages.bytes < cfg.output_low_watermark)
			ev_io_start(&c->in);
	}
}

int
tcp_connect(struct sockaddr_in *dst, struct sockaddr_in *src, ev_tstamp timeout)
{
	int fd, optval = 1, flags;
	socklen_t optlen = sizeof(optval);
	ev_io io = { .coro = 1 };
	ev_timer timer = { .coro = 1 };
	ev_watcher *w;

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		say_syserror("socket");
		goto error;
	}

	if ((flags = fcntl(fd, F_GETFL, 0)) < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		say_syserror("fcntl");
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

	if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) < 0)
		say_syserror("setsockopt(SO_KEEPALIVE)");

	if (connect(fd, (struct sockaddr *)dst, sizeof(*dst)) < 0) {
		if (errno != EINPROGRESS)
			goto error;
	}

	ev_io_init(&io, (void *)fiber, fd, EV_WRITE);
	ev_timer_init(&timer, (void *)fiber, timeout, 0.);
	if (timeout > 0)
		ev_timer_start(&timer);

	ev_io_start(&io);
	w = yield();
	ev_timer_stop(&timer);
	ev_io_stop(&io);

	if (w == (ev_watcher *)&timer)
		goto error;

	if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
		goto error;

	if (optval != 0) {
		errno = optval;
		goto error;
	}

	return fd;
      error:
	if (fd)
		close(fd);
	return -1;
}

static int
server_socket(int type, struct in_addr *src, int port, void (*on_bind)(int fd))
{
	int fd;
	bool warning_said = false;
	int flags, one = 1;
	struct sockaddr_in sin;
	struct linger ling = { 0, 0 };

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

	if ((flags = fcntl(fd, F_GETFL, 0)) < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		say_syserror("fcntl");
		return -1;
	}

	memset(&sin, 0, sizeof(struct sockaddr_in));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);

	if (src == NULL)
		sin.sin_addr.s_addr = INADDR_ANY;
	else
		memcpy(&sin.sin_addr.s_addr, src, sizeof(*src));


	for (;;) {
		if (bind(fd, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
			if (on_bind != NULL)
				on_bind(-1);

			if (errno == EADDRINUSE)
				goto sleep_and_retry;
			say_syserror("bind(%s)", sintoa(&sin));
			return -1;
		}

		if (on_bind != NULL)
			on_bind(fd);

		if (type == SOCK_STREAM)
			if (listen(fd, cfg.backlog) == -1) {
				say_syserror("listen");
				return -1;
			}

		say_info("bound to %s port %i", type == SOCK_STREAM ? "TCP" : "UDP", port);
		break;

	      sleep_and_retry:
		if (!warning_said) {
			say_warn("port %i is already in use, "
				 "will retry binding after 0.1 seconds.", port);
			warning_said = true;
		}
		fiber_sleep(0.1);
	}
	return fd;
}


void
tcp_server(va_list ap)
{
	int port = va_arg(ap, int);
	void (*handler)(int fd, void *data) = va_arg(ap, void (*)(int, void *));
	void (*on_bind)(int fd) = va_arg(ap, void (*)(int fd));
	void *data = va_arg(ap, void *);

	int cfd, fd, one = 1, flags;

	if ((fd = server_socket(SOCK_STREAM, NULL, port, on_bind)) < 0)
		exit(EX_OSERR); /* TODO: better error handling */

	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_READ);

	ev_io_start(&io);
	while (1) {
		yield();

		while ((cfd = accept(fd, NULL, NULL)) > 0) {
			if ((flags = fcntl(cfd, F_GETFL, 0)) < 0 ||
			    fcntl(cfd, F_SETFL, flags | O_NONBLOCK) < 0)
			{
				say_syserror("fcntl");
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
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			say_syserror("accept");
			continue;
		}
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

	if ((fd = server_socket(SOCK_DGRAM, NULL, port, on_bind)) < 0)
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

		if (!(errno == EAGAIN || errno == EWOULDBLOCK))
			say_syserror("recvfrom");
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
