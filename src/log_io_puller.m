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
feeder_param_eq(struct feeder_param *this, struct feeder_param *that)
{
	bool equal =
		this->ver == that->ver &&
		this->addr.sin_family == that->addr.sin_family &&
		this->addr.sin_addr.s_addr == that->addr.sin_addr.s_addr &&
		this->addr.sin_port == that->addr.sin_port &&
		this->filter.type == that->filter.type &&
		this->filter.arglen == that->filter.arglen;
	if (!equal) return false;
	bool this_name_empty = this->filter.name == NULL || strlen(this->filter.name) == 0;
	bool that_name_empty = that->filter.name == NULL || strlen(that->filter.name) == 0;
	equal = (this_name_empty && that_name_empty) ||
		(!this_name_empty && !that_name_empty &&
		 strcmp(this->filter.name, that->filter.name) == 0);
	if (!equal) return false;
	equal = this->filter.arglen == 0 ||
		memcmp(this->filter.arg, that->filter.arg, this->filter.arglen) == 0;
	return equal;
}

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
- (int) establish_connection;
- (int) replication_compat: (i64)scn;
- (int) replication_handshake:(void*)hshake len:(size_t)len;
@end

@implementation XLogPuller
- (u32) version { return version; }
- (bool) eof { return false; }
- (struct palloc_pool *) pool { return NULL; }

- (id)
init
{
	[super init];
	fd = -1;
	rbuf = TBUF(NULL, 0, fiber->pool);
	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);
	return self;
}

- (id)
init:(struct feeder_param*)_feeder
{
	[self init];
	[self feeder_param: _feeder];
	return self;
}

- (void)
feeder_param:(struct feeder_param*)_feeder
{
	feeder = _feeder;
}

- (int)
establish_connection
{
	abort = 0; /* must be set before connect */
	assert(feeder != NULL);

	say_debug2("%s: connect", __func__);
	if ((fd = tcp_connect(&feeder->addr, NULL, 5)) < 0) {
		snprintf(errbuf, sizeof(errbuf), "can't connect, %s", strerror_o(errno));
		return -1;
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

	return 0;
}

- (int)
replication_compat:(i64)scn
{
	say_debug("%s: compat send scn", __func__);
	if (fiber_write(fd, &scn, sizeof(scn)) != sizeof(scn)) {
		snprintf(errbuf, sizeof(errbuf), "can't write initial lsn, %s", strerror_o(errno));
		return -1;
	}

	say_debug("%s: compat recv scn", __func__);
	if (fiber_read(fd, &version, sizeof(version)) != sizeof(version)) {
		snprintf(errbuf, sizeof(errbuf), "can't read initial lsn, %s", strerror_o(errno));
		return -1;
	}
	return 0;
}

- (int)
replication_handshake:(void*)hshake len:(size_t)hsize
{
	struct tbuf *req = tbuf_alloc(fiber->pool);
	struct iproto ireq = { .msg_code = MSG_REPLICA, .sync = 0, .data_len = hsize };
	tbuf_add_dup(req, &ireq);
	tbuf_append(req, hshake, hsize);

	say_debug("%s: send handshake, %u bytes", __func__, tbuf_len(req));
	if (fiber_write(fd, req->ptr, tbuf_len(req)) != tbuf_len(req)) {
		snprintf(errbuf, sizeof(errbuf), "can't write initial handshake, %s", strerror_o(errno));
		return -1;
	}

	do {
		tbuf_ensure(&rbuf, 16 * 1024);
		ssize_t r = [self recv_with_timeout: 5];

		if (r < 0) {
			if (errno == EAGAIN ||
			    errno == EWOULDBLOCK ||
			    errno == EINTR)
				continue;

			switch (r) {
			case -2:
				snprintf(errbuf, sizeof(errbuf), "handshake timeout");
				break;
			case -3:
				snprintf(errbuf, sizeof(errbuf), "handshake aborted");
				break;
			default:
				snprintf(errbuf, sizeof(errbuf), "can't read initial handshake, %s",
					 strerror_o(errno));
			}
			return -1;
		} else if (r == 0) {
			snprintf(errbuf, sizeof(errbuf), "can't read initial handshake, eof");
			return -1;
		}

		say_debug("%s: recv handshake part, %u bytes", __func__, tbuf_len(&rbuf));
	} while (tbuf_len(&rbuf) < sizeof(struct iproto_retcode) + sizeof(version));

	struct iproto_retcode *reply = (void *)iproto_parse(&rbuf);
	if (reply == NULL ||
	    reply->ret_code != 0 ||
	    reply->sync != iproto(req)->sync ||
	    reply->msg_code != iproto(req)->msg_code ||
	    reply->data_len != sizeof(reply->ret_code) + sizeof(version))
	{
		snprintf(errbuf, sizeof(errbuf), "can't parse reply: bad iproto packet");
		return -1;
	}

	say_debug("%s: iproto_reply data_len:%i, rbuf len:%i", __func__,
		  reply->data_len, tbuf_len(&rbuf));

	memcpy(&version, reply->data, sizeof(version));
	return 0;
}

- (int)
handshake:(i64)scn
{
	assert(scn >= 0);

	if ([self establish_connection] < 0)
		goto err;

	if (feeder->ver == 0) {
		if ([self replication_compat: scn] < 0)
			goto err;
	} else if (feeder->ver == 1) {
		struct replication_handshake_v1 hshake = {1, scn, {0}};
		if (feeder->filter.name)
			strncpy(hshake.filter, feeder->filter.name, sizeof(hshake.filter));

		if ([self replication_handshake: &hshake len: sizeof(hshake)] < 0)
			goto err;
	} else if (feeder->ver == 2) {
		struct tbuf *hbuf = tbuf_alloc(fiber->pool);
		struct replication_handshake_v2 hshake = {
			.ver = 2, .scn = scn, .filter = {0},
			.filter_type = feeder->filter.type,
		       	.filter_arglen = feeder->filter.arglen};
		if (feeder->filter.name)
			strncpy(hshake.filter, feeder->filter.name, sizeof(hshake.filter));
		tbuf_add_dup(hbuf, &hshake);
		tbuf_append(hbuf, feeder->filter.arg, feeder->filter.arglen);

		if ([self replication_handshake: hbuf->ptr len: tbuf_len(hbuf)] < 0)
			goto err;
	}

	if (version != default_version && version != version_11) {
		snprintf(errbuf, sizeof(errbuf), "unknown remote version");
		goto err;
	}

	say_info("succefully connected to feeder/%s, version:%i", sintoa(&feeder->addr), version);
	say_info("starting remote recovery from scn:%" PRIi64, scn);
	return 1;
err:
	tbuf_reset(&rbuf);
	if (fd >= 0) {
		close(fd);
		fd = -1;
	}
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
	ssize_t r = tbuf_recv(&rbuf, fd);
	if (r >= 0)
		return r;

	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_READ);
	ev_io_start(&io);

	ev_timer timer = { .coro = 1 };
	ev_timer_init(&timer, (void *)fiber, timeout, 0);
	if (timeout > 0)
		ev_timer_start(&timer);

	in_recv = fiber;
	void *w = yield();
	in_recv = NULL;

	ev_io_stop(&io);
	ev_timer_stop(&timer);

	if (unlikely(abort)) {
		/* cause we could awake by io or timer */
		fiber_cancel_wake(fiber);
		errno = 0;
		return -3;
	}

	if (unlikely(w == &timer))
		return -2;

	if (w == &io)
		return tbuf_recv(&rbuf, fd);

	assert(false);
}

- (ssize_t)
recv
{
	if (abort)
		raise_fmt("recv aborted");

	tbuf_ensure(&rbuf, 256 * 1024);
	ssize_t r = [self recv_with_timeout: cfg.wal_feeder_keepalive_timeout];

	if (r <= 0) {
		switch (r) {
		case 0: raise_fmt("unexpected EOF");
		case -2: raise_fmt("timeout");
		case -3: raise_fmt("recv aborted");
		default: raise_fmt("unknown error: %s", strerror_o(errno));
		}
	}

	return r;
}

- (void)
abort_recv
{
	abort = 1;
	if (in_recv) {
		fiber_wake(in_recv, NULL);
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
		if (!contains_full_row_v12(&rbuf))
			return NULL;

		buf = tbuf_split(&rbuf, sizeof(struct row_v12) + row_v12(&rbuf)->len);

		data_crc = crc32c(0, row_v12(buf)->data, row_v12(buf)->len);
		if (row_v12(buf)->data_crc32c != data_crc)
			raise_fmt("data crc32c mismatch");

		fixup_row_v12(row_v12(buf));
		break;
	case 11:
		if (!contains_full_row_v11(&rbuf))
				return NULL;

		buf = tbuf_split(&rbuf, sizeof(struct _row_v11) + _row_v11(&rbuf)->len);

		data_crc = crc32c(0, _row_v11(buf)->data, _row_v11(buf)->len);
		if (_row_v11(buf)->data_crc32c != data_crc)
			raise_fmt("data crc32c mismatch");

		buf = convert_row_v11_to_v12(buf);
		break;
	default:
		assert(false);
	}

	row = buf->ptr;

	int old_ushard = fiber->ushard;
	if (row->scn)
		fiber->ushard = row->shard_id;
	say_debug("%s: SCN:%"PRIi64 " tag:%s", __func__,
		  row->scn, xlog_tag_to_a(row->tag));
	fiber->ushard = old_ushard;

	/* feeder may send keepalive rows */
	if (row->lsn == 0 && row->scn == 0 && row->tag == (nop|TAG_SYS))
		return NULL;

	return row;
}

- (ssize_t)
recv_row
{
	switch (version) {
	case 12:
		while (!contains_full_row_v12(&rbuf))
			[self recv];
		break;
	case 11:
		while (!contains_full_row_v11(&rbuf))
			[self recv];
		break;
	default:
		assert(false);
	}
	return tbuf_len(&rbuf);
}

- (int)
close
{
	return fd < 0 ? 0 : close(fd);
}

- (id)
free
{
	[self close];
	palloc_unregister_gc_root(fiber->pool, &rbuf);
	return [super free];
}

- (const char *)
error
{
	return errbuf;
}

@end

register_source();
