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
#import <iproto.h>
#import <full_mesh.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static struct palloc_pool *pool;
static struct fiber *output_flusher, *input_reader;


u32 self_id;
struct iproto_peer *mesh_peers;

#define foreach_peer(p) for (struct iproto_peer *p = mesh_peers; p; p = p->next)


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

	if (getpeername(fd, (struct sockaddr *)&peer_addr, &len) < 0) {
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

			iproto_peer_connect(p, fd);
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
broadcast(struct mesh_msg *op, u32 value_len, const char *value)
{
	assert(op->seq != 0);

	foreach_peer (p) {
		if (p->c.fd < 0)
			continue;

		struct netmsg *m = netmsg_tail(&p->c.out_messages);
		net_add_iov_dup(&m, op, op->len - value_len);
		if (value_len > 0)
			net_add_iov_dup(&m, value, value_len);
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
		ping.seq = make_response(mesh_peers, 2.0)->seq;

		broadcast(&ping, 0, NULL);
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
	struct netmsg *m = netmsg_tail(&c->out_messages);
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

		ssize_t r = tbuf_read(c->fd, c->rbuf);

		if (r == 0 || /* r < 0 && */ (errno != EAGAIN && errno != EWOULDBLOCK)) {
			if (r < 0)
				say_debug("closing conn r:%i errno:%i", (int)r, errno);
			else
				say_debug("peer %s disconnected, fd:%i", p->name, c->fd);
			conn_close(c);
			continue;
		}

		while (have_mesh_msg(c)) {
			struct mesh_msg *msg = c->rbuf->ptr;
			tbuf_ltrim(c->rbuf, msg->len);
			if (hostid(msg->seq) == self_id)
				collect_response(msg);
			else
				reply(msg, c, reply_callback);
		}
	}
}

struct mesh_peer *
make_mesh_peer(int id, const char *name, const char *addr, short primary_port, struct mesh_peer *next)
{
	struct mesh_peer *p;

	p = calloc(1, sizeof(*p));
	if (atosin(addr, &p->addr) == -1) {
		free(p);
		return NULL;
	}
	p->primary_addr = p->addr;
	p->primary_addr.sin_port = htons(primary_port);
	p->id = id;
	p->name = strdup(name);
	p->next = next;

	return p;
}

struct iproto_peer *
mesh_peer(int id)
{
	foreach_peer (p)
		if (p->id == id)
			return p;
	return NULL;
}


void
mesh_init(struct iproto_peer *self_,
	  struct iproto_peer *peers_,
	  void (*reply_callback)(struct iproto_peer *, struct iproto *, void *),
	  void *arg)
{
	short accept_port;
	struct sockaddr_in *outgoing_addr;

	pool = palloc_create_pool("mesh");

	mesh_peers = peers_;
	self_id = self_->id;
	accept_port = ntohs(self_->addr.sin_port);
	outgoing_addr = &self_->addr;
	outgoing_addr->sin_port = htons(ntohs(outgoing_addr->sin_port) + 1);

	output_flusher = fiber_create("mesh/output_flusher", service_output_flusher);
	input_reader = fiber_create("mesh/input_reader", iproto_input_reader, reply_callback, arg);

	foreach_peer (p) {
		say_debug("init mesh peer %s/%p", p->name, p);
		conn_init(&p->c, pool, -1, REF_STATIC);
		/* FIXME: meld into conn_init */
		ev_init(&p->c.out, (void *)output_flusher);
		ev_init(&p->c.in, (void *)input_reader);
	}


	fiber_create("mesh/rendevouz", iproto_rendevouz, outgoing_addr, mesh_peers);
	fiber_create("mesh/rendevouz_accept", tcp_server, accept_port, rendevouz_accept, NULL, NULL);
	fiber_create("mesh/ping", iproto_pinger, mesh_peers);
}

register_source(S_INFO);
