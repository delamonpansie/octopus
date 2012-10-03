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
#import <octopus_ev.h>
#import <coro.h>

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lauxlib.h>
#include <third_party/queue.h>

#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/uio.h>

struct fiber {
	struct octopus_coro coro;
	struct fiber *caller;
	struct palloc_pool *pool;
	uint32_t fid;

	SLIST_ENTRY(fiber) link, zombie_link, worker_link;
	TAILQ_ENTRY(fiber) wake_link;
	void *wake;

	lua_State *L;

	const char *name;
	void (*f)(va_list ap);
	va_list ap;

	bool reading_inbox;
};

SLIST_HEAD(, fiber) fibers, zombie_fibers;

struct child {
	pid_t pid;
	struct conn *c;
};

void fiber_init(void);
struct fiber *fiber_create(const char *name, void (*f)(va_list va), ...);
void fiber_destroy_all();
int wait_for_child(pid_t pid);

void resume(struct fiber *callee, void *w);
void *yield(void);
int fiber_wake(struct fiber *f, void *arg);

struct msg *read_inbox(void);
struct tbuf;
bool write_inbox(struct fiber *recipient, struct tbuf *msg);
int inbox_size(struct fiber *recipient);

void fiber_gc(void);
void fiber_sleep(ev_tstamp s);
void fiber_info(struct tbuf *out);
struct fiber *fid2fiber(int fid);

struct child *spawn_child(const char *name, struct fiber *in, struct fiber *out,
			  int (*handler)(int fd, void *state), void *state);

int luaT_openfiber(struct lua_State *L);
