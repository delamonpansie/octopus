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

#import <config.h>
#import <fiber.h>
#import <util.h>
#import <log_io.h>

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
		ssize_t r = write(fd, b->data, b->len);
		if (r < 0) {
			say_syserror("write");
			exit(EXIT_SUCCESS);
		}
		tbuf_peek(b, r);
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
	struct tbuf *ver;
	ssize_t r;
	i64 lsn;

	r = read(sock, &lsn, sizeof(lsn));
	if (r != sizeof(lsn)) {
		if (r < 0)
			say_syserror("read");
		exit(EXIT_SUCCESS);
	}

	ver = tbuf_alloc(fiber->pool);
	tbuf_append(ver, &default_version, sizeof(default_version));
	send_tbuf(sock, ver);
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

	if (getpeername(sock, (struct sockaddr *)&addr, &addrlen) != -1)
		peer_name = inet_ntoa(addr.sin_addr);

	set_proc_title("feeder:client_handler%s %s", custom_proc_title, peer_name);


	feeder = [[Feeder alloc] init_snap_dir:cfg.snap_dir
				       wal_dir:cfg.wal_dir
					    fd:sock];
	[feeder recover_local:handshake(sock)];

	ev_io_init(&io, (void *)eof_monitor, sock, EV_READ);
	ev_io_start(&io);

	ev_run(0);
}

static void
init(void)
{
	int server, client;
	struct sockaddr_in server_addr;
	int one = 1;

	if (cfg.wal_feeder_bind_port <= 0) {
		say_info("WAL feeder is disabled");
		return;
	}

	if (tnt_fork() != 0)
		return;

	fiber->name = "feeder";
	fiber->pool = palloc_create_pool("feeder");

	if (cfg.wal_dir == NULL || cfg.snap_dir == NULL)
		panic("can't start feeder without snap_dir or wal_dir");

	set_proc_title("feeder:acceptor%s %s:%i",
		       custom_proc_title,
		       cfg.wal_feeder_bind_ipaddr == NULL ? "ANY" : cfg.wal_feeder_bind_ipaddr,
		       cfg.wal_feeder_bind_port);

	server = socket(AF_INET, SOCK_STREAM, 0);
	if (server < 0) {
		say_syserror("socket");
		goto exit;
	}

	memset(&server_addr, 0, sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	if (cfg.wal_feeder_bind_ipaddr == NULL) {
		server_addr.sin_addr.s_addr = INADDR_ANY;
	} else {
		server_addr.sin_addr.s_addr = inet_addr(cfg.wal_feeder_bind_ipaddr);
		if (server_addr.sin_addr.s_addr == INADDR_NONE)
			panic("inet_addr: %s'", cfg.wal_feeder_bind_ipaddr);
	}
	server_addr.sin_port = htons(cfg.wal_feeder_bind_port);

	if (setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
		say_syserror("setsockopt");
		goto exit;
	}

	if (bind(server, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
		say_syserror("bind");
		goto exit;
	}

	listen(server, 5);

	say_info("WAL feeder initilized");

	for (;;) {
		pid_t child;
		client = accept(server, NULL, NULL);
		if (client < 0) {
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
	exit(EXIT_FAILURE);
}

struct tnt_module feeder = {
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
