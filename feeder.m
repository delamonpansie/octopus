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

#import <mod/feeder/feeder_version.h>

const char *filter_type_names[] = {
	"ID",
	"LUA",
	"C"
};

typedef struct row_v12 *(*filter_callback)(struct row_v12 *r, const char *arg, int arglen);
struct registered_callback {
	char name[REPLICATION_FILTER_NAME_LEN];
	filter_callback filter;
};
struct registered_callbacks {
	struct registered_callback *callbacks;
	int capa, count;
};
static struct registered_callbacks registered = {NULL, 0, 0};

@interface Feeder: Recovery {
	int fd;
	filter_callback filter;
}
+ (void) register_filter: (const char*)name call: (filter_callback)filter;
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

	assert(r != NULL);
	lua_pushvalue(L, 1);
	lua_pushvalue(L, 2);
	if (r) {
		luaT_pushptr(L, r);
	} else {
		lua_pushnil(L);
	}
	lua_pushvalue(L, 3);

	if (lua_pcall(L, 3, 1, 0) != 0) {
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
recover_row:(struct row_v12 *)r
{
	struct row_v12 *n = filter(r, NULL, 0);

	/* FIXME: we should buffer writes */
	if (n)
		writef(fd, (const char *)n, sizeof(*n) + n->len);

	if (!dummy_tag(r->tag))
		lsn = r->lsn;
}

- (void)
wal_final_row
{
	[self recover_row:[self dummy_row_lsn:0 scn:0 tag:wal_final_tag]];
}

- (void)
write_row_direct: (struct row_v12*) row
{
	if (row)
		writef(fd, (const char *)row, sizeof(*row) + row->len);
}

- (void)
recover_start_from_scn:(i64)initial_scn filter:(struct feeder_filter*)_filter
{
	int i;
	say_debug("%s initial_scn:%"PRIi64" filter: type=%s name=%s", __func__, initial_scn, filter_type_names[_filter->type], _filter->name);
	switch (_filter->type) {
	case FILTER_TYPE_ID:
		filter = id_filter;
		break;
	case FILTER_TYPE_LUA:
		lua_getglobal(fiber->L, "__feederentrypoint");
		lua_getglobal(fiber->L, "replication_filter");
		lua_pushstring(fiber->L, _filter->name);
		lua_gettable(fiber->L, -2);
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
handshake(int sock, struct feeder_filter *filter)
{
	struct tbuf *rep, *input;
	struct iproto *req;
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

	tbuf_append(rep, &(struct iproto_retcode)
			 { .msg_code = req->msg_code,
			   .data_len = sizeof(default_version) +
				       field_sizeof(struct iproto_retcode, ret_code),
			   .sync = req->sync,
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
keepalive_send(va_list ap)
{
	Feeder *feeder = va_arg(ap, typeof(feeder));
	struct row_v12* sysnop = [feeder dummy_row_lsn: 0 scn: 0 tag: nop | TAG_SYS];

	for (;;) {
		[feeder write_row_direct: sysnop];
		fiber_sleep(cfg.wal_feeder_keepalive_interval);
	}
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
	struct feeder_filter filter;
	memset(&filter, 0, sizeof(filter));

	if (getpeername(sock, (struct sockaddr *)&addr, &addrlen) != -1)
		peer_name = sintoa(&addr);

	set_proc_title("feeder:client_handler%s %s", custom_proc_title, peer_name);

	if (luaT_require("feeder_init") == -1)
		panic("unable to load `feeder_init' lua module: %s", lua_tostring(fiber->L, -1));
	if (luaT_require("init") == -1)
		panic("unable to load `init' lua module: %s", lua_tostring(fiber->L, -1));

	feeder = [[Feeder alloc] init_snap_dir:cfg.snap_dir
				       wal_dir:cfg.wal_dir
					    fd:sock];
	i64 initial_scn = handshake(sock, &filter);
	say_info("connect peer:%s initial SCN:%"PRIi64" filter: type=%s name='%s'", peer_name, initial_scn, filter_type_names[filter.type], filter.name);
	[feeder recover_start_from_scn:initial_scn filter:&filter];

	ev_io_init(&io, (void *)eof_monitor, sock, EV_READ);
	ev_io_start(&io);

	ev_timer_init(&tm, (void *)keepalive, 1, 1);
	ev_timer_start(&tm);

	fiber_create("feeder/keepalive_send", keepalive_send, feeder);

	ev_run(0);
}

void fsleep(ev_tstamp t)
{
	struct timeval tv;
	tv.tv_sec = (long)t;
	tv.tv_usec = (long)((t - tv.tv_sec) * 1e6);
	select(0, NULL, NULL, NULL, &tv);
}

static void
init(void)
{
	int server, client;
	struct sockaddr_in server_addr;

	if (cfg.wal_feeder_bind_addr == NULL) {
		say_info("WAL feeder is disabled");
		return;
	}

	if (cfg.wal_feeder_fork_before_init)
		if (tnt_fork() != 0)
			return;

	signal(SIGCHLD, SIG_IGN);

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

	if (atosin(cfg.wal_feeder_bind_addr, &server_addr) == -1)
		panic("bad wal_feeder_bind_addr: '%s'", cfg.wal_feeder_bind_addr);

	server = server_socket(SOCK_STREAM, &server_addr, 0, NULL, fsleep);
	if (server == -1) {
		say_error("unable to create server socket");
		goto exit;
	}

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

		if (cfg.wal_feeder_debug_no_fork) {
			recover_feed_slave(client);
			continue;
		}

		child = tnt_fork();
		if (child < 0) {
			say_syserror("fork");
			close(client);
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
