/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

bool
feeder_param_set_addr(struct feeder_param *feeder, const char *addr)
{
	feeder->addr.sin_family = AF_UNSPEC;
	feeder->addr.sin_addr.s_addr = INADDR_ANY;

	if (addr == NULL || *addr == 0) /* empty address is valid value */
		return true;

	if (strnlen(addr, 23) <= 22)
		if (atosin(addr, &feeder->addr) == 0)
			return true;

	say_error("invalid feeder address '%.*s'", 23, addr);
	return false;
}

enum feeder_cfg_e
feeder_param_fill_from_cfg(struct feeder_param *param, struct octopus_cfg *_cfg)
{
	if (_cfg == NULL) _cfg = &cfg;

	memset(param, 0, sizeof(*param));

	enum feeder_cfg_e e = 0;
	if (!feeder_param_set_addr(param, _cfg->wal_feeder_addr)) {
		say_error("replication feeder address wrong");
		e |= FEEDER_CFG_BAD_ADDR;
	}

	if (_cfg->wal_feeder_filter == NULL || *(_cfg->wal_feeder_filter) == 0) {
		param->filter.name = NULL;
	} else if (strnlen(_cfg->wal_feeder_filter, REPLICATION_FILTER_NAME_LEN+2) >=
			REPLICATION_FILTER_NAME_LEN) {
		say_error("replication filter name too long");
		e |= FEEDER_CFG_BAD_FILTER;
	} else {
		param->filter.name = _cfg->wal_feeder_filter;
	}

	if (_cfg->replication_compat) {
		if (param->filter.name != NULL) {
			say_error("replication_compat is incompatible with wal_feeder_filter");
			e |= FEEDER_CFG_BAD_VERSION;
		}
		param->ver = 0;
	} else {
		if (_cfg->wal_feeder_filter_type != NULL) {
			if (strncasecmp(_cfg->wal_feeder_filter_type, "id", 4) == 0)
				param->filter.type = FILTER_TYPE_ID;
			else if (strncasecmp(_cfg->wal_feeder_filter_type, "lua", 4) == 0)
				param->filter.type = FILTER_TYPE_LUA;
			else if (strncasecmp(_cfg->wal_feeder_filter_type, "c", 4) == 0)
				param->filter.type = FILTER_TYPE_C;
		} else if (param->filter.name == NULL)
			param->filter.type = FILTER_TYPE_ID;
		else
			param->filter.type = FILTER_TYPE_LUA;

		param->filter.arg = NULL;
		param->filter.arglen = 0;
		if (param->filter.type != FILTER_TYPE_ID) {
			if (_cfg->wal_feeder_filter_arg != NULL) {
				param->filter.arg = _cfg->wal_feeder_filter_arg;
				param->filter.arglen = strlen(_cfg->wal_feeder_filter_arg);
			}
		}

		if (param->filter.type == FILTER_TYPE_ID ||
		    (param->filter.type == FILTER_TYPE_LUA && param->filter.arg == NULL)) {
			param->ver = 1;
		} else {
			param->ver = 2;
		}
	}
	return e;
}

@interface XLogPuller (Helpers)
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
	feeder = NULL;
	return [super init];
}

- (XLogPuller *)
init:(struct feeder_param*)_feeder
{
	XLogPuller *e = [self init];
	[e feeder_param: _feeder];
	return e;
}

- (void)
feeder_param:(struct feeder_param*)_feeder
{
	feeder = _feeder;
}

- (const char *)
establish_connection
{
	int fd;

	abort = 0; /* must be set before connect */
	assert(feeder != NULL);

	say_debug("%s: connect", __func__);
	if ((fd = tcp_connect(&feeder->addr, NULL, 5)) < 0) {
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
				return "handshake timeout";
			}
			if (r == -3) {
				return "handshake aborted";
			}
			if (errno == EAGAIN ||
			    errno == EWOULDBLOCK ||
			    errno == EINTR)
				continue;
			return "can't read initial handshake";
		} else if (r == 0) {
			return "can't read initial handshake, eof";
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

	if (feeder->ver == 0) {
		err = [self replication_compat: scn];
		if (err) goto err;
	} else if (feeder->ver == 1) {
		struct replication_handshake_v1 hshake = {1, scn, {0}};
		if (feeder->filter.name)
			strncpy(hshake.filter, feeder->filter.name, sizeof(hshake.filter));

		err = [self replication_handshake: &hshake len: sizeof(hshake)];
		if (err) goto err;
	} else if (feeder->ver == 2) {
		struct tbuf *hbuf = tbuf_alloc(fiber->pool);
		struct replication_handshake_v2 hshake = {
			.ver = 2, .scn = scn, .filter = {0},
			.filter_type = feeder->filter.type,
		       	.filter_arglen = feeder->filter.arglen};
		if (feeder->filter.name)
			strncpy(hshake.filter, feeder->filter.name, sizeof(hshake.filter));
		strncpy(hshake.filter, feeder->filter.name, sizeof(hshake.filter));
		tbuf_add_dup(hbuf, &hshake);
		tbuf_append(hbuf, feeder->filter.arg, feeder->filter.arglen);

		err = [self replication_handshake: hbuf->ptr len: tbuf_len(hbuf)];
		if (err) goto err;
	}

	if (version != default_version && version != version_11) {
		err = "unknown remote version";
		goto err;
	}

	say_info("succefully connected to feeder/%s, version:%i", sintoa(&feeder->addr), version);
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

static ev_timer fake_abort;

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

		if (set_timer && w != &timer) {
			ev_timer_stop(&timer);
		}

		if (unlikely(abort)) {
			while (w != &fake_abort) {
				w = yield();
			}
			conn_close(&c);
			errno = 0;
			return -3;
		}

		if (unlikely(w == &timer)) {
			return -2;
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
		return -3;
	}

	tbuf_ensure(c.rbuf, 256 * 1024);
	ssize_t r = [self recv_with_timeout: cfg.wal_feeder_keepalive_timeout];

	if (r <= 0) {
		if (r == -2)
			raise("timeout");
		if (r == -3)
			raise("recv aborted");
		raise("unexpected EOF");
	}

	return r;
}

- (void)
abort_recv
{
	abort = 1;
	/* it's safe to wake conn_recv() with NULL */
	if (in_recv) {
		ev_timer_init(&fake_abort, NULL, 0, 0);
		fiber_wake(in_recv, (void*)&fake_abort);
	}
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

		fixup_row_v12(row_v12(buf));
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
