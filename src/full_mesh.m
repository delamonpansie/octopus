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

#import <util.h>
#import <assoc.h>
#import <net_io.h>
#import <palloc.h>
#import <say.h>
#import <fiber.h>
#import <full_mesh.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static struct palloc_pool *pool;
static struct fiber *output_flusher, *input_reader;

static struct mhash_t *response_registry;
static i64 seq;

static int self_id, mesh_peers;
static struct mesh_peer *peers;


#define foreach_peer(p) for (struct mesh_peer *p = peers; p ; p = p->next)

struct netmsg *
peer_netmsg_tail(struct mesh_peer *p)
{
	return netmsg_tail(&p->c.out_messages, pool);
}

static i64
next_seq(void)
{
	return (++seq << 8) | (self_id & 0xff);
}

int
hostid(u64 seq)
{
	return seq & 0xff;
}

static void
peer_connect(struct mesh_peer *p, int fd)
{
	p->c.fd = fd;
	ev_io_set(&p->c.out, fd, EV_WRITE);
	ev_io_set(&p->c.in, fd, EV_READ);
	ev_io_start(&p->c.in);

	say_info("connect with %s %s", p->name, sintoa(&p->addr));
}

static void
rendevouz(va_list ap)
{
	struct sockaddr_in *self_addr = va_arg(ap, struct sockaddr_in *);

loop:
	foreach_peer (p) {
		/* This delay randomizes order in which mesh peers connects to each other */
		fiber_sleep(drand(0.2));

		if (p->c.fd > 0)
			continue;

		int fd = tcp_connect(&p->addr, self_addr, 5);
		if (p->c.fd > 0) {
			assert(p->c.fd != fd);
			say_error("mesh peer %s already connected, closing fd:%i", p->name, fd);
			if (fd)
				close(fd);
			continue;
		}

		if (fd > 0) {
			peer_connect(p, fd);
			p->connect_err_said = false;
		} else {
			if (!p->connect_err_said)
				say_syserror("connect!");
			p->connect_err_said = true;
		}
	}

	fiber_sleep(1); /* no more then one reconnect to each member of cluster in second */
	goto loop;
}

static int
pair(const struct sockaddr_in *a, const struct sockaddr_in *b)
{
	int r = memcmp(&a->sin_addr, &b->sin_addr, sizeof(a->sin_addr) == 0);
	return r ? : (ntohs(a->sin_port) & ~1) == (ntohs(b->sin_port) & ~1);
}

static void
rendevouz_accept(int fd, void *data __attribute__((unused)))
{
	struct sockaddr_in peer_addr;
	socklen_t len = sizeof(peer_addr);

	if (getpeername(fd, &peer_addr, &len) < 0) {
		say_syserror("getpeername");
		close(fd);
		return;
	}

	foreach_peer(p) {
		if (pair(&p->addr, &peer_addr)) {
			if (p->c.fd > 0) {
				assert(p->c.fd != fd);
				say_error("mesh peer %s already connected, closing fd:%i", p->name, fd);
				close(fd);
				return;
			}

			peer_connect(p, fd);
			return;
		}
	}

	say_error("unknown mesh peer");
	close(fd);
}

static void
response_timeout(ev_timer *w, int events __attribute__((unused)))
{
	struct mesh_response *r = (void *)w - offsetof(struct mesh_response, timeout);
	fiber_wake(r->waiter, r);
}

struct mesh_response *
make_response(int quorum, ev_tstamp timeout)
{
	struct mesh_response *r = p0alloc(pool, sizeof(*r));
	r->sent = ev_now();
	r->seq = next_seq();
	r->quorum = quorum;
	ev_timer_init(&r->timeout, response_timeout, timeout, 0.);
	ev_timer_start(&r->timeout);
	r->waiter = fiber;
	u32 k = mh_i64_get(response_registry, r->seq);
	assert(k == mh_end(response_registry));
	(void)k;
	mh_i64_put(response_registry, r->seq, r, NULL);

	return r;
}

static void
delete_response(ev_timer *w, int events __attribute__((unused)))
{
	struct mesh_response *r = (void *)w - offsetof(struct mesh_response, timeout);
	u32 k = mh_i64_get(response_registry, r->seq);
	assert(k != mh_end(response_registry));
	mh_i64_del(response_registry, k);
}

void
release_response(struct mesh_response *r)
{
	ev_timer_stop(&r->timeout);
	ev_timer_init(&r->timeout, delete_response, 15., 0.);
	ev_timer_start(&r->timeout);
}

void
broadcast(int quorum, ev_tstamp timeout, struct mesh_msg *op)
{
	op->seq = make_response(quorum, timeout)->seq;

	foreach_peer (p) {
		if (p->c.fd < 0)
			continue;

		struct netmsg *m = netmsg_tail(&p->c.out_messages, pool);
		net_add_iov(&m, op, op->len);
		ev_io_start(&p->c.out);
	}
}

void
ping(va_list ap __attribute__((unused)))
{
	struct mesh_msg ping = { .len = sizeof(ping), .type = MESH_PING };
	struct mesh_response *r;

	for (;;) {
		fiber_sleep(1);

		ping.sent = ev_now();

		broadcast(mesh_peers, 2.0, &ping);
		r = yield();

		say_debug("ping seq:%i q:%i/c:%i %.4f%s",
			  (int)r->seq >> 8, r->quorum, r->count,
			  ev_now() - ping.sent,
			  r->count == 0 ? " TIMEOUT" : "");

		release_response(r);
	}

}


static bool
have_mesh_msg(struct conn *c)
{
	if (tbuf_len(c->rbuf) < sizeof(u32))
		return false;
	u32 len = *(u32 *)(void *)c->rbuf;
	return tbuf_len(c->rbuf) >= len;
}


static void
collect_response(struct mesh_msg *msg)
{
	u32 k = mh_i64_get(response_registry, msg->seq);
	if (k == mh_end(response_registry)) {
		say_warn("unregistered reply %i/%i/%i",
			 (int)msg->seq, (int)msg->seq >> 8, (int)hostid(msg->seq));
		return;
	}

	struct mesh_response *r = mh_i64_value(response_registry, k);
	if (r->closed > 0 && ev_now() - r->closed > 1) {
		say_warn("stale reply: q:%i/c:%i %.4f",
			 r->quorum, r->count,
			 ev_now() - r->closed);
		return;
	}

	r->reply[r->count++] = msg;
	if (r->count == r->quorum) {
		r->closed = ev_now();
		ev_timer_stop(&r->timeout);
		fiber_wake(r->waiter, r);
	}
}

static void
reply(struct mesh_msg *msg, struct conn *c,
		  void (*reply_callback)(struct mesh_peer *, struct mesh_msg *))
{
	struct netmsg *m = netmsg_tail(&c->out_messages, pool);
	struct mesh_peer *p = (void *)c - offsetof(struct mesh_peer, c);

	if (hostid(msg->seq) != p->id) {
		say_warn("id mismatch, dropping packet %i %i", hostid(msg->seq), p->id);
		return;
	}

	if (msg->type == MESH_PING) {
		if (ev_now() - msg->sent > 0.1)
			say_debug("pong %i to %s, delay: %.2f",
				  (int)msg->seq >> 8, p->name, ev_now() - msg->sent);
		net_add_iov_dup(&m, msg, msg->len);
		ev_io_start(&c->out);
		return;
	}

	reply_callback(p, msg);
}

static void
input_reader_aux(va_list ap)
{
	void (*reply_callback)(struct mesh_peer *, struct mesh_msg *) =
		va_arg(ap, void (*)(struct mesh_peer *, struct mesh_msg *));

	for (;;) {
		struct ev_watcher *w = yield();
		struct conn *c = w->data;
		struct mesh_peer *p = (void *)c - offsetof(struct mesh_peer, c);
		tbuf_ensure(c->rbuf, 16 * 1024);

		int r = read(c->fd, c->rbuf->data + tbuf_len(c->rbuf), c->rbuf->size - tbuf_len(c->rbuf));

		if (r > 0) {
			c->rbuf->len += r;
		} else if (r == 0 || /* r < 0 && */ (errno != EAGAIN && errno != EWOULDBLOCK)) {
			if (r < 0)
				say_debug("closing conn r:%i errno:%i", (int)r, errno);
			else
				say_debug("peer %s disconnected, fd:%i", p->name, c->fd);
			conn_close(c);
			continue;
		}

		while (have_mesh_msg(c)) {
			struct mesh_msg *msg = c->rbuf->data;
			tbuf_ltrim(c->rbuf, msg->len);
			if (hostid(msg->seq) == self_id)
				collect_response(msg);
			else
				reply(msg, c, reply_callback);
		}
	}
}

struct mesh_peer *
make_mesh_peer(int id, const char *name, const char *addr, struct mesh_peer *next)
{
	struct mesh_peer *p;

	p = calloc(1, sizeof(*p));
	if (atosin(addr, &p->addr) == -1) {
		free(p);
		return NULL;
	}
	p->id = id;
	p->name = strdup(name);
	p->next = next;

	return p;
}

struct mesh_peer *
mesh_peer(int id)
{
	foreach_peer (p)
		if (p->id == id)
			return p;
	return NULL;
}


void
mesh_init(struct mesh_peer *self_,
	  struct mesh_peer *peers_,
	  void (*reply_callback)(struct mesh_peer *, struct mesh_msg *))
{
	short accept_port;
	struct sockaddr_in *outgoing_addr;

	pool = palloc_create_pool("mesh");
	response_registry = mh_i64_init();

	peers = peers_;
	self_id = self_->id;
	accept_port = ntohs(self_->addr.sin_port);
	outgoing_addr = &self_->addr;
	outgoing_addr->sin_port = htons(ntohs(outgoing_addr->sin_port) + 1);

	output_flusher = fiber_create("mesh/output_flusher", service_output_flusher);
	input_reader = fiber_create("mesh/input_reader", input_reader_aux, reply_callback);

	foreach_peer (p) {
		say_debug("init mesh peer %s/%p", p->name, p);
		mesh_peers++;
		conn_init(&p->c, pool, -1, REF_STATIC);
		/* FIXME: meld into conn_init */
		ev_init(&p->c.out, (void *)output_flusher);
		ev_init(&p->c.in, (void *)input_reader);
	}


	fiber_create("mesh/rendevouz", rendevouz, outgoing_addr);
	fiber_create("mesh/rendevouz_accept", tcp_server, accept_port, rendevouz_accept, NULL, NULL);
	fiber_create("mesh/ping", ping);
}
