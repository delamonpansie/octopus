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

#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


@interface Feeder: Recovery {
	int fd;
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
send_tbuf(int fd, struct tbuf *b)
{
	do {
		ssize_t r = write(fd, b->ptr, tbuf_len(b));
		if (r < 0) {
			say_syserror("write");
			exit(EXIT_SUCCESS);
		}
		tbuf_ltrim(b, r);
	} while (tbuf_len(b) > 0);
}

- (void)
recover_row:(struct tbuf *)row
{
	i64 row_lsn = row_v12(row)->lsn;
	u16 tag = row_v12(row)->tag;
	send_tbuf(fd, row);
	if (tag == snap_initial_tag || tag == snap_final_tag || tag == wal_tag)
		lsn = row_lsn;
}
@end

static i64
handshake(int sock)
{
	struct tbuf *rep, *input, *req;
	bool compat = 0;
	i64 lsn;

	input = tbuf_alloc(fiber->pool);
	rep = tbuf_alloc(fiber->pool);

	for (;;) {
		tbuf_ensure(input, 4096);
		if (tbuf_recv(input, sock) <= 0) {
			say_syserror("closing connection, recv");
			_exit(EXIT_SUCCESS);
		}
		if (tbuf_len(input) < sizeof(lsn))
			continue;

		if ((req = iproto_parse(input)) != NULL)
			break;

		if (*(u32 *)input->ptr != msg_replica) {
			compat = 1;
			break;
		}
	}

	if (compat) {
		lsn = read_u64(input);
	} else {
		if (iproto(req)->len != sizeof(struct replication_handshake)) {
			say_error("bad handshake len");
			_exit(EXIT_FAILURE);
		}

		struct replication_handshake *hshake = (void *)&iproto(req)->data;
		lsn = hshake->lsn;

		if (hshake->ver != 1) {
			say_error("bad replication version");
			_exit(EXIT_FAILURE);
		}

		tbuf_append(rep, &(struct iproto_header_retcode)
			    { .msg_code = iproto(req)->msg_code,
			      .len = sizeof(default_version) +
			             field_sizeof(struct iproto_header_retcode, ret_code),
			      .sync = iproto(req)->sync,
			      .ret_code = 0 },
			    sizeof(struct iproto_header_retcode));
	}

	tbuf_append(rep, &default_version, sizeof(default_version));
	send_tbuf(sock, rep);
	return lsn;
}

static void
eof_monitor(void)
{
	say_info("client gone, exiting");
	exit(0);
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

	if (getpeername(sock, (struct sockaddr *)&addr, &addrlen) != -1)
		peer_name = sintoa(&addr);

	set_proc_title("feeder:client_handler%s %s", custom_proc_title, peer_name);


	feeder = [[Feeder alloc] init_snap_dir:cfg.snap_dir
				       wal_dir:cfg.wal_dir
					    fd:sock];
	[feeder recover_local:handshake(sock)];

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
		if (client < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
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
