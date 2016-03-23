/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014  Yuriy Vostrikov
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
#include <sys/ioctl.h>
#include <sys/types.h>

#import <mod/feeder/feeder.h>
#import <mod/feeder/feeder_version.h>

const char *filter_type_names[] = {
	"ID",
	"LUA",
	"C"
};

struct registered_callback {
	char name[REPLICATION_FILTER_NAME_LEN];
	filter_callback filter;
};

struct registered_callbacks {
	struct registered_callback *callbacks;
	int capa, count;
};

static struct registered_callbacks registered = {NULL, 0, 0};

@implementation Feeder
- (id) init_fd:(int)fd_
{
	[super init];
	fd = fd_;
	return self;
}

+ (void)
register_filter: (const char*)name call: (filter_callback)filter
{
	if (strlen(name) >= REPLICATION_FILTER_NAME_LEN) {
		panic("Filter callback name '%s' too long", name);
	}

	if (registered.capa == 0) {
		registered.callbacks = xcalloc(sizeof(*registered.callbacks), 4);
		registered.capa = 4;
	} else if (registered.count == registered.capa) {
		registered.callbacks = xrealloc(registered.callbacks, sizeof(*registered.callbacks) * registered.capa * 2);
		registered.capa *= 2;
	}

	int i;
	for(i=0; i < registered.count; i++) {
		if (strncmp(name, registered.callbacks[i].name,
				       	REPLICATION_FILTER_NAME_LEN) == 0) {
			panic("feeder filter callback '%s' already registered", name);
		}
	}

	strncpy(registered.callbacks[registered.count].name, name,
		       	REPLICATION_FILTER_NAME_LEN);
	registered.callbacks[registered.count].filter = filter;
	registered.count++;
}

static void
writef(int fd, const char *b, size_t len)
{
	do {
		ssize_t r = write(fd, b, len);
		if (r < 0 && errno == EINTR)
			continue;
		if (r <= 0) {
			say_syserror("write");
			_exit(EXIT_SUCCESS);
		}
		b += r;
		len -= r;
	} while (len > 0);
}


struct row_v12 *
id_filter(struct row_v12 *r, __attribute((unused)) const char *arg, __attribute__((unused)) int arglen)
{
	return r;
}

struct row_v12 *
lua_filter(struct row_v12 *r, __attribute((unused)) const char *arg, __attribute__((unused)) int arglen)
{
	struct lua_State *L = fiber->L;

	lua_pushvalue(L, 2);
	lua_pushvalue(L, 3);
	if (r) {
		lua_pushlightuserdata(L, r);
	} else {
		lua_pushnil(L);
	}
	lua_pushvalue(L, 4);

	if (lua_pcall(L, 3, 1, 1) != 0) {
		say_error("lua filter error: %s", lua_tostring(L, -1));
		_exit(EXIT_FAILURE);
	}

	if (lua_isboolean(L, -1) || lua_isnil(L, -1)) {
		if (!lua_toboolean(L, -1))
			r = NULL;
	} else {
		r = *(struct row_v12 **)lua_topointer(L, -1);
	}
	lua_pop(L, 1);

	if (r != NULL) {
		r->data_crc32c = crc32c(0, r->data, r->len);
		r->header_crc32c = crc32c(0, (u8 *)r + sizeof(r->header_crc32c),
					  sizeof(r) - sizeof(r->header_crc32c));
	}

	return r;
}

- (void)
send_row:(struct row_v12 *)row
{
	/* FIXME: we should buffer writes */
	writef(fd, (const char *)row, sizeof(*row) + row->len);
}

- (void)
recover_row:(struct row_v12 *)row
{
	if (row->scn != 0 && min_scn && row->scn < min_scn)
		return;

	if ((row = filter(row, NULL, 0)))
		[self send_row:row];
}

- (void)
wal_final_row
{
	[self recover_row:dummy_row(0, 0, wal_final|TAG_SYS)];
}


- (i64)
prepare_from_scn:(i64)initial_scn filter:(struct feeder_filter*)_filter
{
	int i, shard_id = 0;

	switch (_filter->type) {
	case FILTER_TYPE_ID:
		filter = id_filter;
		break;
	case FILTER_TYPE_LUA:
		luaT_pushtraceback(fiber->L);
		lua_getglobal(fiber->L, "__feederentrypoint");
		lua_getglobal(fiber->L, "replication_filter");
		lua_getfield(fiber->L, -1, _filter->name);
		lua_remove(fiber->L, -2);
		if (!lua_isfunction(fiber->L, -1)) {
			say_error("nonexistent lua filter: %s", _filter->name);
			_exit(EXIT_FAILURE);
		}
		if (_filter->arg) {
			lua_pushlstring(fiber->L, _filter->arg, _filter->arglen);
		} else {
			lua_pushnil(fiber->L);
		}
		filter = lua_filter;
		break;
	case FILTER_TYPE_C:
		for(i = 0; i < registered.count; i++) {
			if (strncmp(registered.callbacks[i].name, _filter->name,
					       	REPLICATION_FILTER_NAME_LEN) == 0)
				break;
		}
		if (i == registered.count) {
			say_error("nonexistent C filter: %s", _filter->name);
			_exit(EXIT_FAILURE);
		}
		filter = registered.callbacks[i].filter;
		break;
	}

	if (_filter->arg) {
		filter(NULL, _filter->arg, _filter->arglen);
	}

	if (_filter->type == FILTER_TYPE_ID)
		say_info("%s initial_scn:%"PRIi64" filter: type=ID", __func__, initial_scn);
	else
		say_info("%s initial_scn:%"PRIi64" shard:%i filter: type=%s name='%s'", __func__,
			 initial_scn, shard_id, filter_type_names[_filter->type], _filter->name);

	if (initial_scn != 0) {
		/* special case: newborn peer without WALs */
		if ([snap_dir containg_scn:initial_scn shard:shard_id]) {
			while ([wal_dir greatest_lsn] <= 0) {
				sleep(1);
				keepalive();
			}
		}

		min_scn = initial_scn;
		i64 initial_lsn = [wal_dir containg_scn:initial_scn shard:shard_id];
		if (initial_lsn <= 0)
			raise_fmt("unable to find WAL containing SCN:%"PRIi64, initial_scn);
		say_debug("%s: SCN:%"PRIi64" => LSN:%"PRIi64, __func__, initial_scn, initial_lsn);
		return initial_lsn;
	} else {
		/* load from last valid snapshot */
		return 0;
	}
}

- (void)
start_reader:(i64)lsn
{
	XLogReader *reader = [[XLogReader alloc] init_recovery:(id)self];

	[reader load_from_local:lsn];
	[self wal_final_row];
	[reader recover_follow:cfg.wal_dir_rescan_delay];
}

@end

static struct iproto * recv_req(int fd);

static i64
handshake(int sock, struct iproto *req, Feeder *feeder, struct feeder_filter *filter)
{
	struct tbuf *rep;
	i64 scn, lsn;

	rep = tbuf_alloc(fiber->pool);

	if (req->data_len < sizeof(struct replication_handshake_base)) {
		say_error("bad handshake len");
		_exit(EXIT_FAILURE);
	}

	filter->type = FILTER_TYPE_ID;
	filter->arglen = 0;
	filter->arg    = NULL;

	struct replication_handshake_base *hshake = (void *)&req->data;
	struct replication_handshake_v2 *hshake2 = (void *)&req->data;
	switch (hshake->ver) {
	case 1:
		if (req->data_len != sizeof(struct replication_handshake_v1)) {
			say_error("bad handshake len");
			_exit(EXIT_FAILURE);
		}
		if (strnlen(hshake->filter, sizeof(hshake->filter)) > 0) {
			filter->type = FILTER_TYPE_LUA;
			filter->name = hshake->filter;
		}
		break;
	case 2: {
		if (req->data_len != sizeof(*hshake2) + hshake2->filter_arglen) {
			say_error("bad handshake len");
			_exit(EXIT_FAILURE);
		}
		if (hshake2->filter_type >= FILTER_TYPE_MAX) {
			say_error("bad handshake filter type %d", hshake2->filter_type);
			_exit(EXIT_FAILURE);
		}
		if (strnlen(hshake2->filter, sizeof(hshake2->filter)) > 0) {
			filter->type = hshake2->filter_type;
			filter->name = hshake->filter;
			if (hshake2->filter_arglen > 0) {
				filter->arglen = hshake2->filter_arglen;
				filter->arg = hshake2->filter_arg;
			}
		}
		}
		break;
	default:
		say_error("bad replication version");
		_exit(EXIT_FAILURE);
	}
	scn = hshake->scn;
	lsn = [feeder prepare_from_scn:scn filter:filter];

	tbuf_append(rep, &(struct iproto_retcode)
			 { .msg_code = req->msg_code,
			   .data_len = sizeof(default_version) +
				       field_sizeof(struct iproto_retcode, ret_code),
			   .sync = req->sync,
			   .ret_code = 0 },
		    sizeof(struct iproto_retcode));

	tbuf_append(rep, &default_version, sizeof(default_version));
	writef(sock, rep->ptr, tbuf_len(rep));

	[feeder start_reader:lsn];

	return lsn;
}

static void
eof_monitor(void)
{
	say_info("client gone, exiting");
	_exit(0);
}

static void
keepalive_send(va_list ap)
{
	Feeder *feeder = va_arg(ap, typeof(feeder));
	struct row_v12* sysnop = dummy_row(0, 0, nop|TAG_SYS);

	for (;;) {
		if (cfg.wal_feeder_keepalive_timeout > 0.0) {
			[feeder send_row: sysnop]; /* this call skip filters: lua filter expect that
						      lua stack is non empty (it's filled in prepare_from_scn)
						      calling lua_filter from this fiber will result in crash */
			fiber_sleep(cfg.wal_feeder_keepalive_timeout / 3.0);
		} else {
			fiber_sleep(5.0);
		}
	}
}

static void
recover_feed_slave(int sock, struct iproto *req)
{
	Feeder *feeder;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	const char *peer_name = "<unknown>";
	ev_io io = { .coro = 0 };
	struct feeder_filter filter;
	memset(&filter, 0, sizeof(filter));

	if (getpeername(sock, (struct sockaddr *)&addr, &addrlen) != -1)
		peer_name = sintoa(&addr);
	say_info("connect peer:%s", peer_name);
	title("feeder_handler/%s", peer_name);

	luaT_require_or_panic("feeder", false, NULL);
	luaT_require_or_panic("feeder_init", false, NULL);
	luaT_require_or_panic("init", false, NULL);

	feeder = [[Feeder alloc] init_fd:sock];

	handshake(sock, req, feeder, &filter);
	ev_io_init(&io, (void *)eof_monitor, sock, EV_READ);
	ev_io_start(&io);

	fiber_create("feeder/keepalive_send", keepalive_send, feeder);

	ev_timer tm = { .coro = 0 };
	ev_timer_init(&tm, (void*)keepalive, 1, 1);
	if (!cfg.wal_feeder_debug_no_fork)
		ev_timer_start(&tm);

	ev_run(0);
}

static struct iproto *
recv_req(int fd)
{
	struct iproto *req;
	struct tbuf *input = tbuf_alloc(fiber->pool);

	for (;;) {
		tbuf_ensure(input, 4096);
		ssize_t r = tbuf_recv(input, fd);
		if (r < 0 && errno == EINTR)
			continue;
		if (r <= 0) {
			say_syserror("closing connection, recv");
			_exit(EXIT_SUCCESS);
		}
		if (tbuf_len(input) < sizeof(i64))
			continue;

		if ((req = iproto_parse(input)) != NULL)
			return req;
	}
}

static int
feeder_worker(int parent_fd, int fd, void *state, int len)
{
	close(parent_fd);
	recover_feed_slave(fd, len > 1 ? state : recv_req(fd));
	return 0;
}

static void
feeder_spawn_worker(int fd, void *req, int len)
{
	struct timeval tm = { .tv_sec = 120, .tv_usec = 0};
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tm,sizeof(tm));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tm,sizeof(tm));
	int zero = 0;
	if (ioctl(fd, FIONBIO, &zero) < 0)
		say_syserror("ioctl");

	struct child child = spawn_child("feeder/worker", feeder_worker, fd, req, len);
	if (child.pid > 0) {
		say_info("WAL feeder client");
		close(child.fd);
	}
}

static void
feeder_accept(int fd, void *data __attribute__((unused)))
{
	if (cfg.wal_feeder_debug_no_fork) {
		recover_feed_slave(fd, recv_req(fd));
		close(fd);
		return;
	}
	feeder_spawn_worker(fd, "\0", 1);
	close(fd);
}

static void
iproto_feeder_cb(struct netmsg_head *wbuf, struct iproto *req, void *arg __attribute__((unused)))
{
	struct netmsg_io *io = container_of(wbuf, struct netmsg_io, wbuf);
	feeder_spawn_worker(io->fd, req, sizeof(*req) + req->data_len);
	[io close];
}

void
feeder_service(struct iproto_service *s)
{
	if (cfg.wal_writer_inbox_size == 0)
		return;

	service_register_iproto(s, MSG_REPLICA, iproto_feeder_cb, IPROTO_LOCAL);
}

static void
init(void)
{
	struct sockaddr_in server_addr;

	if (cfg.wal_feeder_bind_addr == NULL || cfg.wal_writer_inbox_size == 0) {
		say_info("WAL feeder is disabled");
		return;
	}

	if (cfg.wal_dir == NULL || cfg.snap_dir == NULL)
		panic("can't start feeder without snap_dir or wal_dir");

	if (atosin(cfg.wal_feeder_bind_addr, &server_addr) == -1)
		panic("bad wal_feeder_bind_addr: '%s'", cfg.wal_feeder_bind_addr);

	fiber_create("feeder/acceptor", tcp_server, cfg.wal_feeder_bind_addr, feeder_accept, NULL, NULL);
}

static int
feeder_fixup_addr(struct octopus_cfg *cfg)
{
	extern void out_warning(int v, char *format, ...);
	if (net_fixup_addr(&cfg->wal_feeder_bind_addr, cfg->wal_feeder_bind_port) < 0)
		out_warning(0, "Option 'wal_feeder_bind_addr' is overridden by 'wal_feeder_bind_port'");

	return 0;
}

static struct tnt_module feeder = {
	.name = "feeder",
	.version = feeder_version_string,
	.check_config = feeder_fixup_addr,
	.init = init
};

register_module(feeder);
register_source();
