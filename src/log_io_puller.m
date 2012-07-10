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

@implementation XLogPuller
- (XLogPuller *)
init
{
	conn_init(&c, fiber->pool, -1, REF_STATIC);
	palloc_register_gc_root(fiber->pool, &c, conn_gc);

	return [super init];
}

- (XLogPuller *)
init_addr:(struct sockaddr_in *)addr_
{
	memcpy(&addr, addr_, sizeof(addr));
	return [self init];
}

- (i64)
handshake:(struct sockaddr_in *)addr_ scn:(i64)scn err:(const char **)err_ptr
{
	memcpy(&addr, addr_, sizeof(addr));
	return [self handshake:scn err:err_ptr];
}

- (i64)
handshake:(struct sockaddr_in *)addr_ scn:(i64)scn
{
	return [self handshake:addr_ scn:scn err:NULL];
}

- (i64)
handshake:(i64)scn err:(const char **)err_ptr
{
	const char *err;
	struct tbuf *rep;
	assert(c.fd < 0);

	struct replication_handshake hshake = {1, scn, {0}};
	if (hshake.scn < 1)
		hshake.scn = 1;

	if (cfg.wal_feeder_filter != NULL) {
		if (strlen(cfg.wal_feeder_filter) + 1 > sizeof(hshake.filter)) {
			say_error("wal_feeder_filter too big");
			exit(EXIT_FAILURE);
		}
		strcpy(hshake.filter, cfg.wal_feeder_filter);
	}
	struct tbuf *req = tbuf_alloc(fiber->pool);
	tbuf_append(req, &(struct iproto){ .msg_code = 0, .sync = 0, .data_len = sizeof(hshake) },
		    sizeof(struct iproto));
	tbuf_append(req, &hshake, sizeof(hshake));

	rep = tbuf_alloc(fiber->pool);
	tbuf_ensure(rep, sizeof(struct iproto_retcode));

	if ((c.fd = tcp_connect(&addr, NULL, 0)) < 0) {
		err = "can't connect to feeder";
		goto err;
	}

	if (conn_write(&c, req->ptr, tbuf_len(req)) != tbuf_len(req)) {
		err = "can't write initial handshake";
		goto err;
	}

	while (tbuf_len(c.rbuf) < sizeof(struct iproto_retcode) + sizeof(version))
		conn_recv(&c);
	rep = iproto_parse(c.rbuf);
	if (rep == NULL) {
		err = "can't read reply";
		goto err;
	}

	if (iproto_retcode(rep)->ret_code != 0 ||
	    iproto_retcode(rep)->sync != iproto(req)->sync ||
	    iproto_retcode(rep)->msg_code != iproto(req)->msg_code ||
	    iproto_retcode(rep)->data_len != sizeof(iproto_retcode(rep)->ret_code) + sizeof(version))
	{
		err = "bad reply";
		goto err;
	}

	say_debug("go reply len:%i, rbuf %i", iproto_retcode(rep)->data_len, tbuf_len(c.rbuf));
	memcpy(&version, iproto_retcode(rep)->data, sizeof(version));
	if (version != default_version && version != version_11) {
		err = "unknown remote version";
		goto err;
	}

	say_crit("succefully connected to feeder");
	say_crit("starting remote recovery from scn:%" PRIi64, hshake.scn);
	return hshake.scn;
err:
	if (err_ptr)
		*err_ptr = err;
	conn_close(&c);
	return 0;
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

- (struct tbuf *)
fetch_row
{
	struct tbuf *row;
	u32 data_crc;

	switch (version) {
	case 12:
		while (!contains_full_row_v12(c.rbuf)) {
			if (pack) {
				pack = 0;
				return NULL;
			}
			if (conn_recv(&c) <= 0)
				raise("eof");
		}

		row = tbuf_split(c.rbuf, sizeof(struct row_v12) + row_v12(c.rbuf)->len);
		row->pool = c.rbuf->pool; /* FIXME: this is cludge */

		data_crc = crc32c(0, row_v12(row)->data, row_v12(row)->len);
		if (row_v12(row)->data_crc32c != data_crc)
			raise("data crc32c mismatch");
		break;
	case 11:
		while (!contains_full_row_v11(c.rbuf)) {
			if (pack) {
				pack = 0;
				return NULL;
			}
			if (conn_recv(&c) <= 0)
				raise("eof");
		}

		row = tbuf_split(c.rbuf, sizeof(struct _row_v11) + _row_v11(c.rbuf)->len);
		row->pool = c.rbuf->pool;

		data_crc = crc32c(0, _row_v11(row)->data, _row_v11(row)->len);
		if (_row_v11(row)->data_crc32c != data_crc)
			raise("data crc32c mismatch");

		row = convert_row_v11_to_v12(row);
		break;
	default:
		assert(false);
	}

	say_debug("%s: scn:%"PRIi64 " tag:%s", __func__,
		  row_v12(row)->scn, xlog_tag_to_a(row_v12(row)->tag));

	pack++;
	return row;
}

- (u32)
version
{
	return version;
}

- (void)
close
{
	conn_close(&c);
}

- (void)
free
{
	[self close];
	[super free];
}

@end

register_source();
