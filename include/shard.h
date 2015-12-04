/*
 * Copyright (C) 2015 Mail.RU
 * Copyright (C) 2015 Yuriy Vostrikov
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

#ifndef SHARD_H
#define SHARD_H

#include <util.h>

#define MAX_SHARD 4096

@class Shard;
@protocol Shard;
@protocol Executor;

enum shard_mode { SHARD_MODE_NONE,
		  SHARD_MODE_LOADING,
		  SHARD_MODE_LOCAL,
		  SHARD_MODE_PROXY,
		  SHARD_MODE_PARTIAL_PROXY };
enum shard_type { SHARD_TYPE_POR, SHARD_TYPE_PAXOS } ;


struct shard_route {
	enum shard_mode mode;
	struct iproto_egress *proxy;
	Shard<Shard> *shard;
	id<Executor> executor;
};

struct shard_conf {
	int id;
	const char *mod_name;
	enum shard_type type;
	enum shard_mode mode;
	const struct feeder_param *feeder_param;
};

struct shard_route shard_rt[MAX_SHARD];

void update_rt(int shard_id, enum shard_mode mode, Shard<Shard> *shard, const char *peer_name);

enum port_type { PORT_PRIMARY, PORT_REPLICATION };
const struct sockaddr_in *shard_addr(const char *name, enum port_type port_type);

#endif
