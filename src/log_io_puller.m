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

- (int)
handshake:(i64)scn err:(const char **)err_ptr
{
	const char *err;
	int fd;

	abort = 0; /* must be set before connect */

	assert(scn >= 0);

	/* FIXME: do we need this ? */
	if (scn > 0) {
		scn -= 1024;
		if (scn < 1)
			scn = 1;
	}

	say_debug("%s: connect", __func__);
	if ((fd = tcp_connect(&addr, NULL, 5)) < 0) {
		err = "can't connect to feeder";
		goto err;
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

	if (cfg.replication_compat) {
		say_debug("%s: compat send scn", __func__);
		if (conn_write(&c, &scn, sizeof(scn)) != sizeof(scn)) {
			err = "can't write initial lsn";
			goto err;
		}

		say_debug("%s: compat recv scn", __func__);
		if (conn_read(&c, &version, sizeof(version)) != sizeof(version)) {
			err = "can't read version";
			goto err;
		}
	} else {
		struct replication_handshake_v1 hshake = {1, scn, {0}};

		if (cfg.wal_feeder_filter != NULL) {
			if (strlen(cfg.wal_feeder_filter) + 1 > sizeof(hshake.filter))
				say_error("wal_feeder_filter too big, ignoring");
			else
				strcpy(hshake.filter, cfg.wal_feeder_filter);
		}
		struct tbuf *req = tbuf_alloc(fiber->pool);
		tbuf_append(req, &(struct iproto){ .msg_code = 0, .sync = 0, .data_len = sizeof(hshake) },
			    sizeof(struct iproto));
		tbuf_append(req, &hshake, sizeof(hshake));

		say_debug("%s: send handshake, %u bytes", __func__, tbuf_len(req));
		if (conn_write(&c, req->ptr, tbuf_len(req)) != tbuf_len(req)) {
			err = "can't write initial handshake";
			goto err;
		}

		do {
			ev_tstamp timeout = 5;
			ev_timer timer = { .coro = 1 };
			ev_io io = { .coro = 1 };
			ev_io_init(&io, (void *)fiber, c.fd, EV_READ);
			ev_io_start(&io);
			ev_timer_init(&timer, (void *)fiber, timeout, 0.);
			ev_timer_start(&timer);
			void *w = yield();
			ev_io_stop(&io);
			ev_timer_stop(&timer);
			if (w == &timer) {
				err = "timeout";
				goto err;
			}
			tbuf_ensure(c.rbuf, 16 * 1024);

			ssize_t r = tbuf_recv(c.rbuf, c.fd);

			if (r < 0) {
				if (errno == EAGAIN ||
				    errno == EWOULDBLOCK ||
				    errno == EINTR)
					continue;
				err = "can'r read initial handshake";
				goto err;
			} else if (r == 0) {
				err = "can'r read initial handshake, eof";
				goto err;
			}

			say_debug("%s: recv handshake part, %u bytes", __func__, tbuf_len(c.rbuf));
		} while (tbuf_len(c.rbuf) < sizeof(struct iproto_retcode) + sizeof(version));

		struct iproto_retcode *reply = (void *)iproto_parse(c.rbuf);
		if (reply == NULL) {
			err = "can't read reply";
			goto err;
		}

		if (reply->ret_code != 0 ||
		    reply->sync != iproto(req)->sync ||
		    reply->msg_code != iproto(req)->msg_code ||
		    reply->data_len != sizeof(reply->ret_code) + sizeof(version))
		{
			err = "bad reply";
			goto err;
		}

		say_debug("%s: iproto_reply data_len:%i, rbuf len:%i", __func__,
			  reply->data_len, tbuf_len(c.rbuf));

		memcpy(&version, reply->data, sizeof(version));
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
recv
{
	if (abort) {
		conn_close(&c);
		return -1;
	}

	tbuf_ensure(c.rbuf, 256 * 1024);
	ssize_t r = tbuf_recv(c.rbuf, c.fd);
	if (r == -1) {
		ev_io io = { .coro = 1 };
		ev_io_init(&io, (void *)fiber, c.fd, EV_READ);
		ev_io_start(&io);
		in_recv = fiber;
		yield();
		in_recv = NULL;
		ev_io_stop(&io);

		if (abort) {
			conn_close(&c);
			errno = 0;
			return -1;
		}

		r = tbuf_recv(c.rbuf, c.fd);
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
