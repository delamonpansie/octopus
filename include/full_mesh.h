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
	struct sockaddr_in primary_addr;
	struct mesh_peer *next;
	bool connect_err_said;
};

struct mesh_msg {
	int len;
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
void mesh_set_seq(u64 seq);
struct mesh_peer *mesh_peer(int id);
struct mesh_peer *owner_of_seq(u64 seq);
struct mesh_peer *make_mesh_peer(int id, const char *name,
				 const char *addr, short primary_port,
				 struct mesh_peer *next);
struct netmsg *peer_netmsg_tail(struct mesh_peer *p);
void mesh_init(struct mesh_peer *self_,
	       struct mesh_peer *peers_,
	       void (*reply_callback)(struct mesh_peer *, struct mesh_msg *));
void broadcast(struct mesh_msg *op, u32 data_len, const char *data);
struct mesh_response *make_response(int quorum, ev_tstamp timeout);
void release_response(struct mesh_response *r);
