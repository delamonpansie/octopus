/*
 * Copyright (C) 2012 Mail.RU
 * Copyright (C) 2012 Yuriy Vostrikov
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

#import <net_io.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MAX_MESH_PEERS 7
#define MESH_PING 1

struct mesh_peer {
	int id;
	struct conn c;
	char *name;
	struct sockaddr_in addr;
	struct mesh_peer *next;
	bool connect_err_said;
};

struct mesh_msg {
	int len;
	int ver;
	i64 seq;
	int type;
	ev_tstamp sent;
};

struct mesh_response {
	int seq;
	int count, quorum;
	ev_timer timeout;
	struct fiber *waiter;
	ev_tstamp sent, closed;
	struct mesh_msg *reply[MAX_MESH_PEERS];
};

int hostid(u64 seq);
struct mesh_peer *mesh_peer(int id);
struct mesh_peer *make_mesh_peer(int id, const char *name, const char *addr,
				 struct mesh_peer *next);

void mesh_init(struct mesh_peer *self_,
	       struct mesh_peer *peers_,
	       void (*reply_callback)(struct mesh_peer *,
				      struct netmsg *, struct mesh_msg *));
void broadcast(int quorum, ev_tstamp timeout, struct mesh_msg *op);
void release_response(struct mesh_response *r);
