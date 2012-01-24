/*
 * Copyright (C) 2010, 2011 Mail.RU
 * Copyright (C) 2010, 2011 Yuriy Vostrikov
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

#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <fiber.h>
#include <util.h>
#include <tbuf.h>
#include <pickle.h>
#include <log_io.h>
#include <object.h>

@implementation Recovery (row)
- (int)
recover_row:(struct tbuf *)row
{

	struct tbuf *msg = tbuf_alloc(fiber->pool);

	if (row->len < sizeof(struct row_v11))
		return -1;

	tbuf_printf(msg, "lsn:%"PRIi64 " ", row_v11(row)->lsn);
	tbuf_peek(row, sizeof(struct row_v11));

	@try {
		u16 tag = read_u16(row);
		if (tag == wal_tag)
			tbuf_printf(msg, "wal_tag ");
		else if (tag == snap_tag)
			tbuf_printf(msg, "snap_tag ");
		else
			tbuf_printf(msg, "unknown_tag:%u", tag);
		// tbuf_printf(msg, "%s", tbuf_to_hex(row));
		say_crit("%.*s", msg->len, (char *)msg->data);
		return 0;
	}
	@catch (Error *e) {
		say_error("Got exception while handling row: %s", e->reason);
	}
	return -1;
}
@end

i32
check_config(struct tarantool_cfg *conf __attribute__((unused)))
{
	return 0;
}

void
reload_config(struct tarantool_cfg *old_conf __attribute__((unused)),
		  struct tarantool_cfg *new_conf __attribute__((unused)))
{
	return;
}

void
init(void)
{
	struct Recovery *r;

	set_proc_title("follower");

	r = [[Recovery alloc] init_snap_dir:"."
				    wal_dir:"."
			       rows_per_wal:10000
				fsync_delay:0
				 inbox_size:0
				      flags:RECOVER_READONLY
			 snap_io_rate_limit:0];

	ev_default_loop(0);
	[r recover:0];
	[r recover_follow: 10];
	ev_loop(0);
	exit(EXIT_SUCCESS);
}

struct tnt_module follower = {
        .name = "(silver)box",
        .init = init,
        .check_config = check_config,
        .reload_config = reload_config,
        .cat = NULL,
        .snapshot = NULL,
        .info = NULL,
        .exec = NULL
};

register_module(follower);
