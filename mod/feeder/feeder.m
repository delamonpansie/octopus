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
#import <pickle.h>
#import <say.h>

#include <third_party/crc32.h>

#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


@interface Feeder: Recovery {
	int fd;
	bool filtering;
}
@end

@implementation Feeder
- (id) init_snap_dir:(const char *)snap_dirname
             wal_dir:(const char *)wal_dirname
		  fd:(int)fd_
{
	[super init_snap_dir:snap_dirname
		     wal_dir:wal_dirname];
	fd = fd_;
	return self;
}

static void
writef(int fd, const char *b, size_t len)
{
	do {
		ssize_t r = write(fd, b, len);
		if (r <= 0) {
			say_syserror("write");
			_exit(EXIT_SUCCESS);
		}
		b += r;
		len -= r;
	} while (len > 0);
}


static struct row_v12 *
construct_row(const struct row_v12 *old, u16 tag, const char *data, size_t len)
{
	struct row_v12 *new = palloc(fiber->pool, sizeof(*new) + len);;
	memcpy(new, old, sizeof(*new));
	if (tag)
		new->tag = tag;

	memcpy(new->data, data, len);
	new->len = len;
	new->data_crc32c = crc32c(0, new->data, new->len);
	new->header_crc32c = crc32c(0, (u8 *)new + sizeof(new->header_crc32c),
				    sizeof(new) - sizeof(new->header_crc32c));
	return new;
}

- (void)
recover_row:(const struct row_v12 *)r
{
	if (filtering) {
		lua_pushvalue(fiber->L, 1);
		lua_pushlstring(fiber->L, (const char *)r, sizeof(*r) + r->len);

		if (lua_pcall(fiber->L, 1, 2, 0) != 0) {
			say_error("lua filter error: %s", lua_tostring(fiber->L, -1));
			_exit(EXIT_FAILURE);
		}
		if (lua_isnumber(fiber->L, -2)) {
			u16 tag = lua_tointeger(fiber->L, -1);
			size_t len;
			const char *new_data = lua_tolstring(fiber->L, -2, &len);

			r = construct_row(r, tag, new_data, len);
		} else if (lua_isstring(fiber->L, -2)) {
			size_t len;
			const char *new_data = lua_tolstring(fiber->L, -2, &len);
			r = construct_row(r, 0, new_data, len);
		} else if (lua_isnil(fiber->L, -2)) {
			;
		} else {
			say_warn("bad replication_filter return type");
		}
		lua_pop(fiber->L, 2);
	}

	say_debug("%s: lsn:%"PRIi64" scn:%"PRIi64" tag:%s", __func__,
		  r->lsn, r->scn, xlog_tag_to_a(r->tag));
	writef(fd, (const char *)r, sizeof(*r) + r->len);

	if (!dummy_tag(r->tag))
		lsn = r->lsn;
}

- (void)
wal_final_row
{
	[self recover_row:[self dummy_row_lsn:0 scn:0 tag:wal_final_tag]];
}

- (void)
recover_start_from_scn:(i64)initial_scn filter:(const char *)filter_name
{
	say_debug("%s initial_scn:%"PRIi64" filter:%s", __func__, initial_scn, filter_name);
	if (strlen(filter_name) > 0) {
		lua_getglobal(fiber->L, "replication_filter");
		lua_pushstring(fiber->L, filter_name);
		lua_gettable(fiber->L, -2);
		lua_remove(fiber->L, -2);
		if (lua_isnil(fiber->L, -1)) {
			say_error("nonexistent filter: %s", filter_name);
			_exit(EXIT_FAILURE);
		}
		filtering = true;
	}

	if (initial_scn == 0) {
		[self recover_snap];
		current_wal = [wal_dir containg_lsn:lsn];
	} else {
		i64 initial_lsn = [wal_dir containg_scn:initial_scn];
		if (initial_lsn <= 0)
			raise("unable to find WAL containing SCN:%"PRIi64, initial_scn);
		say_debug("%s: SCN:%"PRIi64" => LSN:%"PRIi64, __func__, initial_scn, initial_lsn);
		current_wal = [wal_dir containg_lsn:initial_lsn];
		lsn =  initial_lsn - 1; /* first row read by recovery process will be row
					   with lsn + 1 ==> equal to initial_lsn */
		scn = initial_scn;
	}
	[self recover_cont];
}

@end

static i64
handshake(int sock, char *filter)
{
	struct tbuf *rep, *input, *req;
	i64 scn;

	input = tbuf_alloc(fiber->pool);
	rep = tbuf_alloc(fiber->pool);

	for (;;) {
		tbuf_ensure(input, 4096);
		ssize_t r = tbuf_recv(input, sock);
		if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
			continue;
		if (r <= 0) {
			say_syserror("closing connection, recv");
			_exit(EXIT_SUCCESS);
		}
		if (tbuf_len(input) < sizeof(scn))
			continue;

		if ((req = iproto_parse(input)) != NULL)
			break;
	}

	if (iproto(req)->data_len != sizeof(struct replication_handshake)) {
		say_error("bad handshake len");
		_exit(EXIT_FAILURE);
	}

	struct replication_handshake *hshake = (void *)&iproto(req)->data;
	if (hshake->ver != 1) {
		say_error("bad replication version");
		_exit(EXIT_FAILURE);
	}
	scn = hshake->scn;
	memcpy(filter, hshake->filter, sizeof(hshake->filter));

	tbuf_append(rep, &(struct iproto_retcode)
			 { .msg_code = iproto(req)->msg_code,
			   .data_len = sizeof(default_version) +
				       field_sizeof(struct iproto_retcode, ret_code),
			   .sync = iproto(req)->sync,
			   .ret_code = 0 },
		    sizeof(struct iproto_retcode));

	tbuf_append(rep, &default_version, sizeof(default_version));
	writef(sock, rep->ptr, tbuf_len(rep));

	say_debug("remote requested scn:%"PRIi64, scn);
	return scn;
}

static void
eof_monitor(void)
{
	say_info("client gone, exiting");
	_exit(0);
}

static void
recover_feed_slave(int sock)
{
	Feeder *feeder;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	const char *peer_name = "<unknown>";
	ev_io io = { .coro = 0 };
	ev_timer tm = { .coro = 0 };
	char filter_name[field_sizeof(struct replication_handshake, filter)];

	if (getpeername(sock, (struct sockaddr *)&addr, &addrlen) != -1)
		peer_name = sintoa(&addr);

	set_proc_title("feeder:client_handler%s %s", custom_proc_title, peer_name);

	luaT_dofile("feeder_init.lua");
	luaT_dofile("init.lua");

	feeder = [[Feeder alloc] init_snap_dir:cfg.snap_dir
				       wal_dir:cfg.wal_dir
					    fd:sock];
	i64 initial_scn = handshake(sock, filter_name);
	say_info("initial scn:%"PRIi64" filter: '%s'", initial_scn, filter_name);
	[feeder recover_start_from_scn:initial_scn filter:filter_name];

	ev_io_init(&io, (void *)eof_monitor, sock, EV_READ);
	ev_io_start(&io);

	ev_timer_init(&tm, (void *)keepalive, 1, 1);
	ev_timer_start(&tm);

	ev_run(0);
}

static void
init(void)
{
	int server, client;
	struct sockaddr_in server_addr;
	int one = 1;

	if (cfg.wal_feeder_bind_addr == NULL) {
		say_info("WAL feeder is disabled");
		return;
	}

	if (tnt_fork() != 0)
		return;

	signal(SIGCLD, SIG_IGN);

	fiber->name = "feeder";
	fiber->pool = palloc_create_pool("feeder");
	fiber->L = root_L;

	lua_getglobal(fiber->L, "require");
        lua_pushliteral(fiber->L, "feeder");
	if (lua_pcall(fiber->L, 1, 0, 0) != 0)
		panic("feeder: %s", lua_tostring(fiber->L, -1));

	if (cfg.wal_dir == NULL || cfg.snap_dir == NULL)
		panic("can't start feeder without snap_dir or wal_dir");

	set_proc_title("feeder:acceptor%s %s",
		       custom_proc_title, cfg.wal_feeder_bind_addr);

	server = socket(AF_INET, SOCK_STREAM, 0);
	if (server < 0) {
		say_syserror("socket");
		goto exit;
	}

	if (atosin(cfg.wal_feeder_bind_addr, &server_addr) == -1)
		panic("bad wal_feeder_bind_addr: '%s'", cfg.wal_feeder_bind_addr);

	if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
		say_syserror("setsockopt");
		goto exit;
	}

	if (bind(server, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
		say_syserror("bind");
		goto exit;
	}

	listen(server, 5);

	struct timeval tm = { .tv_sec = 0, .tv_usec = 100000};
	setsockopt(server, SOL_SOCKET, SO_RCVTIMEO, &tm,sizeof(tm));
	say_info("WAL feeder initilized");

	for (;;) {
		pid_t child;
		keepalive();

		client = accept(server, NULL, NULL);
		if (unlikely(client < 0)) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				continue;
			say_syserror("accept");
			continue;
		}
		child = tnt_fork();
		if (child < 0) {
			say_syserror("fork");
			continue;
		}
		if (child == 0)
			recover_feed_slave(client);
		else
			close(client);
	}
      exit:
	_exit(EXIT_FAILURE);
}

static struct tnt_module feeder = {
        .name = "WAL feeder",
        .init = init,
        .check_config = NULL,
        .reload_config = NULL,
        .cat = NULL,
        .snapshot = NULL,
        .info = NULL,
        .exec = NULL
};

register_module(feeder);
register_source();
