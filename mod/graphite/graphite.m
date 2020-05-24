/*
 * Copyright (C) 2016, 2017 Mail.RU
 * Copyright (C) 2016 Yura Sokolov
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

#include <cfg/defs.h>
#import <util.h>
#import <fiber.h>
#import <octopus_ev.h>
#import <tbuf.h>
#import <say.h>
#import <stat.h>
#include <unistd.h>
#include <stdio.h>
#include <net_io.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdarg.h>
#include <graphite.h>
#ifdef CFG_lua_path
#include <src-lua/octopus_lua.h>
#endif

#define mtu (1400)
static char graphite_head[256];
static int graphite_head_len = 0;
static struct sockaddr_in graphite_addr;
int graphite_sock = -1;
static char graphite_buffer[2048];
static int graphite_buflen;

static void reload_graphite_addr(struct octopus_cfg *old _unused_, struct octopus_cfg *new);

static ev_prepare graphite_flush_prepare;
static ev_periodic graphite_send_version_periodic;
static void graphite_flush_cb(ev_prepare *w _unused_, int revents _unused_);
static void graphite_send_version(ev_periodic *w _unused_, int revents _unused_);

void
graphite_init()
{
	char buf[65] = "unknown";
	int i;
	gethostname(buf, sizeof(buf));
	buf[64] = 0;
	char *addr = NULL;
#if CFG_primary_addr
	addr = cfg.primary_addr;
#endif
	graphite_head_len = snprintf(graphite_head, sizeof(graphite_head)-1,
			"my.octopus.%s%s%s%s",
			buf, addr?":":"", addr?:"",
			cfg.custom_proc_title?:"");
	if (graphite_head_len > sizeof(graphite_head)-1)
		graphite_head_len = sizeof(graphite_head)-1;
	/* between  s/[. ()]/_/ */
	for (i=strlen("my.octopus."); i<graphite_head_len; i++) {
		if (graphite_head[i] == '.' || graphite_head[i] == ' ' ||
			graphite_head[i] == '(' || graphite_head[i] == ')') {
			graphite_head[i] = '_';
		}
	}
	graphite_head[graphite_head_len] = 0;

	reload_graphite_addr(NULL, &cfg);
	ev_prepare_init(&graphite_flush_prepare, graphite_flush_cb);
	ev_periodic_init(&graphite_send_version_periodic, graphite_send_version, 59.999, 60, 0);
	ev_periodic_start(&graphite_send_version_periodic);
#if CFG_lua_path
	luaO_require_or_panic("graphite", true, NULL);
#endif
}

static void
reload_graphite_addr(struct octopus_cfg *old _unused_, struct octopus_cfg *new)
{
	const char *ga = new->graphite_addr;
	bool valid = false;
#ifdef GRAPHITE_ADDR
#define _str(x) #x
#define str(x) _str(x)
#define graddr str(GRAPHITE_ADDR)
	if (!ga || ga[0] == 0) {
		ga = graddr;
	}
#endif
	if (ga && ga[0] != 0) {
		int rc = atosin(ga, &graphite_addr);
		valid = !rc;
		if (!valid) {
			say_warn("graphite_addr: %s invalid", ga);
		}
	} else {
		say_warn("graphite_addr not given");
	}
	if (valid && graphite_sock == -1) {
		graphite_sock = socket(PF_INET, SOCK_DGRAM, 0);
		if (graphite_sock == -1)
			say_syserror("graphite socket");
	} else if (!valid && graphite_sock != -1) {
		close(graphite_sock);
		graphite_sock = -1;
	}
}

static void
graphite_flush(int addlen)
{
	if (graphite_sock == -1 || (graphite_buflen == 0 && addlen == 0))
		return;
	if (addlen > 0 && graphite_buflen + addlen < mtu) {
		graphite_buflen += addlen;
		ev_prepare_start(&graphite_flush_prepare);
		return;
	}
	int r = sendto(graphite_sock, graphite_buffer, graphite_buflen, MSG_DONTWAIT,
			(struct sockaddr*)&graphite_addr, sizeof(graphite_addr));
	(void)r;
	if (addlen > 0 && graphite_buflen + addlen < sizeof(graphite_buffer)) {
		memmove(graphite_buffer, graphite_buffer + graphite_buflen, addlen);
		graphite_buflen = addlen;
	} else {
		graphite_buflen = 0;
		ev_prepare_stop(&graphite_flush_prepare);
	}
}

static void
graphite_flush_cb(ev_prepare *w _unused_, int revents _unused_)
{
	graphite_flush(0);
}

void
graphite_flush_now()
{
	graphite_flush(0);
}

static void
graphite_send_version(ev_periodic *w _unused_, int revents _unused_) {
	static u64 vers = -1;
	if (vers == -1) {
		extern const char octopus_version_string[];
		vers = atoll(octopus_version_string);
	}
	if (vers > 0)
		graphite_send2("version", "version", vers);
}

void
graphite_send1(char const *name, double value)
{
	if (graphite_sock == -1)
		return;
	int oldlen = graphite_buflen;
	int addlen = snprintf(graphite_buffer+oldlen, sizeof(graphite_buffer)-oldlen,
			"%.*s.%s %.3f %d\n", graphite_head_len, graphite_head,
			name, value, (u32)ev_now());
	graphite_flush(addlen);
}

void
graphite_send2(char const *base, char const *name, double value)
{
	if (graphite_sock == -1)
		return;
	int oldlen = graphite_buflen;
	int addlen = snprintf(graphite_buffer+oldlen, sizeof(graphite_buffer)-oldlen,
			"%.*s.%s.%s %.3f %d\n", graphite_head_len, graphite_head,
			base, name, value, (u32)ev_now());
	graphite_flush(addlen);
}

void
graphite_send3(char const *base, char const *name, char const *suffix, double value)
{
	if (graphite_sock == -1)
		return;
	int oldlen = graphite_buflen;
	int addlen = snprintf(graphite_buffer+oldlen, sizeof(graphite_buffer)-oldlen,
			"%.*s.%s.%s.%s %.3f %d\n", graphite_head_len, graphite_head,
			base, name, suffix, value, (u32)ev_now());
	graphite_flush(addlen);
}

static struct tnt_module graphite_mod = {
	.reload_config = reload_graphite_addr,
};

register_module(graphite_mod);

