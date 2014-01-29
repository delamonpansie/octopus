/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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
#import <fiber.h>
#import <log_io.h>
#import <net_io.h>
#import <iproto.h>
#import <say.h>

#include <third_party/crc32.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

@interface XLogPuller ()
- (ssize_t) recv_with_timeout: (ev_tstamp)timeout;
- (const char *) establish_connection;
- (const char *) replication_compat: (i64)scn;
- (const char *) replication_handshake:(void*)hshake len:(size_t)len;
@end

@implementation XLogPuller
- (u32) version { return version; }
- (bool) eof { return false; }
- (struct palloc_pool *) pool { return NULL; }

- (XLogPuller *)
init
{
	say_debug("%s", __func__);
	conn_init(&c, fiber->pool, -1, fiber, fiber, MO_STATIC);
	palloc_register_gc_root(fiber->pool, &c, conn_gc);
	return [super init];
}

- (void)
set_addr:(struct sockaddr_in *)addr_
{
	memcpy(&addr, addr_, sizeof(addr));
}

- (int)
handshake:(struct sockaddr_in *)addr_ scn:(i64)scn err:(const char **)err_ptr
{
	memcpy(&addr, addr_, sizeof(addr));
	return [self handshake:scn err:err_ptr];
}

- (int)
handshake:(struct sockaddr_in *)addr_ scn:(i64)scn
{
	return [self handshake:addr_ scn:scn err:NULL];
}

- (const char *)
establish_connection
{
	int fd;

	abort = 0; /* must be set before connect */

	say_debug("%s: connect", __func__);
	if ((fd = tcp_connect(&addr, NULL, 5)) < 0) {
		return "can't connect to feeder";
	}

	int one = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0)
		say_syserror("setsockopt");

	int bufsize = 256 * 1024;
	if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0)
		say_syserror("setsockopt");

#ifdef HAVE_TCP_KEEPIDLE
	int keepidle = 20;
	if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle)) < 0)
		say_syserror("setsockopt");
	int keepcnt = 3;
	if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt)) < 0)
		say_syserror("setsockopt");
#endif

	assert(c.fd < 0);
	conn_set(&c, fd);
	return NULL;
}

- (const char *)
replication_compat:(i64)scn
{
	say_debug("%s: compat send scn", __func__);
	if (conn_write(&c, &scn, sizeof(scn)) != sizeof(scn)) {
		return "can't write initial lsn";
	}

	say_debug("%s: compat recv scn", __func__);
	if (conn_read(&c, &version, sizeof(version)) != sizeof(version)) {
		return "can't write initial lsn";
	}
	return NULL;
}

- (const char *)
replication_handshake:(void*)hshake len:(size_t)hsize
{
	struct tbuf *req = tbuf_alloc(fiber->pool);
	struct iproto ireq = { .msg_code = 0, .sync = 0, .data_len = hsize };
	tbuf_add_dup(req, &ireq);
	tbuf_append(req, hshake, hsize);

	say_debug("%s: send handshake, %u bytes", __func__, tbuf_len(req));
	if (conn_write(&c, req->ptr, tbuf_len(req)) != tbuf_len(req)) {
		return "can't write initial handshake";
	}

	do {
		tbuf_ensure(c.rbuf, 16 * 1024);
		ssize_t r = [self recv_with_timeout: 5];

		if (r < 0) {
			if (r == -2) {
				return "timeout";
			}
			if (errno == EAGAIN ||
			    errno == EWOULDBLOCK ||
			    errno == EINTR)
				continue;
			return "can'r read initial handshake";
		} else if (r == 0) {
			return "can'r read initial handshake, eof";
		}

		say_debug("%s: recv handshake part, %u bytes", __func__, tbuf_len(c.rbuf));
	} while (tbuf_len(c.rbuf) < sizeof(struct iproto_retcode) + sizeof(version));

	struct iproto_retcode *reply = (void *)iproto_parse(c.rbuf);
	if (reply == NULL) {
		return "can't read reply";
	}

	if (reply->ret_code != 0 ||
	    reply->sync != iproto(req)->sync ||
	    reply->msg_code != iproto(req)->msg_code ||
	    reply->data_len != sizeof(reply->ret_code) + sizeof(version))
	{
		return "bad reply";
	}

	say_debug("%s: iproto_reply data_len:%i, rbuf len:%i", __func__,
		  reply->data_len, tbuf_len(c.rbuf));

	memcpy(&version, reply->data, sizeof(version));
	return NULL;
}

- (int)
handshake:(i64)scn err:(const char **)err_ptr
{
	assert(scn >= 0);

	const char *err = [self establish_connection];
	if (err) goto err;

	if (cfg.replication_compat) {
		err = [self replication_compat: scn];
		if (err) goto err;
	} else {
		struct replication_handshake_v1 hshake = {1, scn, {0}};

		if (cfg.wal_feeder_filter != NULL) {
			if (strlen(cfg.wal_feeder_filter) + 1 > sizeof(hshake.filter))
				say_error("wal_feeder_filter too big, ignoring");
			else
				strcpy(hshake.filter, cfg.wal_feeder_filter);
		}
		err = [self replication_handshake: &hshake len: sizeof(hshake)];
		if (err) goto err;
	}

	if (version != default_version && version != version_11) {
		err = "unknown remote version";
		goto err;
	}

	say_info("succefully connected to feeder/%s, version:%i", sintoa(&addr), version);
	say_info("starting remote recovery from scn:%" PRIi64, scn);
	return 1;
err:
	if (err_ptr)
		*err_ptr = err;
	if (c.fd >= 0)
		conn_close(&c);
	return -1;
}

static bool
contains_full_row_v12(const struct tbuf *b)
{
	return tbuf_len(b) >= sizeof(struct row_v12) &&
		tbuf_len(b) >= sizeof(struct row_v12) + row_v12(b)->len;
}

static bool
contains_full_row_v11(const struct tbuf *b)
{
	return tbuf_len(b) >= sizeof(struct _row_v11) &&
		tbuf_len(b) >= sizeof(struct _row_v11) + _row_v11(b)->len;
}

- (ssize_t)
recv_with_timeout: (ev_tstamp)timeout
{
	ssize_t r = tbuf_recv(c.rbuf, c.fd);
	if (r == -1) {
		ev_io io = { .coro = 1 };
		ev_io_init(&io, (void *)fiber, c.fd, EV_READ);
		ev_io_start(&io);
		ev_timer timer = { .coro = 1 };
		ev_timer_init(&timer, (void *)fiber, timeout, 0);

		bool set_timer = timeout > 0.0;
		if (set_timer) {
			ev_now_update();
			ev_timer_start(&timer);
		}

		in_recv = fiber;
		void *w = yield();
		ev_io_stop(&io);
		in_recv = NULL;

		if (set_timer) {
			if (unlikely(w == &timer))
				return -2;
			ev_timer_stop(&timer);
		}

		if (w == &io) {
			r = tbuf_recv(c.rbuf, c.fd);
		}
	}

	return r;
}

- (ssize_t)
recv
{
	if (abort) {
		conn_close(&c);
		return -1;
	}

	tbuf_ensure(c.rbuf, 256 * 1024);
	ssize_t r = [self recv_with_timeout: cfg.wal_feeder_keepalive_timeout];
	if (abort) {
		conn_close(&c);
		errno = 0;
		return -1;
	}

	return r;
}

- (void)
abort_recv
{
	abort = 1;
	/* it's safe to wake conn_recv() with NULL */
	if (in_recv)
		fiber_wake(in_recv, NULL);
}

- (struct row_v12 *)
fetch_row
{
	struct tbuf *buf = NULL;
	struct row_v12 *row = NULL;
	u32 data_crc;

	switch (version) {
	case 12:
		if (!contains_full_row_v12(c.rbuf))
			return NULL;

		buf = tbuf_split(c.rbuf, sizeof(struct row_v12) + row_v12(c.rbuf)->len);
		buf->pool = c.rbuf->pool; /* FIXME: this is cludge */

		data_crc = crc32c(0, row_v12(buf)->data, row_v12(buf)->len);
		if (row_v12(buf)->data_crc32c != data_crc)
			raise("data crc32c mismatch");

		if (cfg.io12_hack && row_v12(buf)->scn == 0)
			row_v12(buf)->scn = row_v12(buf)->lsn;
		break;
	case 11:
		if (!contains_full_row_v11(c.rbuf))
				return NULL;

		buf = tbuf_split(c.rbuf, sizeof(struct _row_v11) + _row_v11(c.rbuf)->len);
		buf->pool = c.rbuf->pool;

		data_crc = crc32c(0, _row_v11(buf)->data, _row_v11(buf)->len);
		if (_row_v11(buf)->data_crc32c != data_crc)
			raise("data crc32c mismatch");

		buf = convert_row_v11_to_v12(buf);
		break;
	default:
		assert(false);
	}

	row = buf->ptr;
	if ((row->tag & ~TAG_MASK) == 0) /* old style row */
		row->tag = fix_tag(row->tag);

	say_debug("%s: scn:%"PRIi64 " tag:%s", __func__,
		  row->scn, xlog_tag_to_a(row->tag));

	return row;
}


- (int)
close
{
	if (c.fd < 0)
		return 0;
	return conn_close(&c);
}

- (id)
free
{
	say_debug("%s", __func__);
	palloc_unregister_gc_root(fiber->pool, &c);
	[self close];
	return [super free];
}

@end

register_source();
