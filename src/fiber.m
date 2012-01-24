/*
 * Copyright (C) 2010, 2011 Mail.RU
 * Copyright (C) 2010, 2011 Yuriy Vostrikov
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
#import <util.h>
#import <palloc.h>
#import <salloc.h>
#import <say.h>
#import <tarantool.h>
#import <tarantool_ev.h>
#import <tbuf.h>
#import <stat.h>
#import <pickle.h>
#import <assoc.h>
#import <net_io.h>

#include <third_party/queue.h>

#include <fiber.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sysexits.h>

struct fiber sched;
struct fiber *fiber = &sched;
coro_context *sched_ctx = &sched.coro.ctx;
void *watcher;
int events;
static uint32_t last_used_fid;

ev_prepare wake_prep;

static struct mhash_t *fibers_registry;

STAILQ_HEAD(, fiber) wake_list;

static void
update_last_stack_frame(struct fiber *fiber)
{
#ifdef BACKTRACE
	fiber->last_stack_frame = frame_addess();
#else
	(void)fiber;
#endif
}

void
resume(struct fiber *callee, void *w)
{
	assert(callee != &sched);
	struct fiber *caller = fiber;
	update_last_stack_frame(caller);
	callee->caller = caller;
	assert(callee->name);
	fiber = callee;
	callee->coro.w = w;
	coro_transfer(&caller->coro.ctx, &callee->coro.ctx);
	callee->caller = &sched;
}

void *
yield(void)
{
	struct fiber *callee = fiber;
	update_last_stack_frame(callee);
	fiber = callee->caller;
	coro_transfer(&callee->coro.ctx, &callee->caller->coro.ctx);
	return fiber->coro.w;
}

void
fiber_wake(struct fiber *f, void *arg)
{
	if (f->wake)
		return;
	if (arg == NULL)
		arg = (void *)1;
	f->wake = arg;
	STAILQ_INSERT_TAIL(&wake_list, f, wake_link);
}

void
fiber_sleep(ev_tstamp delay)
{
	ev_timer *s, w = { .coro = 1 };
	ev_timer_init(&w, (void *)fiber, delay, 0.);
	ev_timer_start(&w);
	s = yield();
	assert(s == &w);
	ev_timer_stop(&w);
}


/** Wait for a forked child to complete. */

int
wait_for_child(pid_t pid)
{
	ev_child w = { .coro = 1 };
	ev_child_init(&w, (void *)fiber, pid, 0);
	ev_child_start(&w);
	yield();
	ev_child_stop(&w);
	return WEXITSTATUS(w.rstatus);
}

static struct fiber *
fid2fiber(int fid)
{
	u32 k = mh_i32_get(fibers_registry, fid);

	if (k == mh_end(fibers_registry))
		return NULL;
	if (!mh_exist(fibers_registry, k))
		return NULL;
	return mh_i32_value(fibers_registry, k);
}

static void
register_fid(struct fiber *fiber)
{
	mh_i32_put(fibers_registry, fiber->fid, fiber, NULL);
}

static void
unregister_fid(struct fiber *fiber)
{
	u32 k = mh_i32_get(fibers_registry, fiber->fid);
	mh_i32_del(fibers_registry, k);
}

static void
clear_inbox(struct fiber *fiber)
{
	for (size_t i = 0; i < fiber->inbox->size; i++)
		fiber->inbox->ring[i] = NULL;
	fiber->inbox->head = fiber->inbox->tail = 0;
}

static void
fiber_do_gc(struct palloc_pool *pool, void *ptr)
{
	struct fiber *fiber = ptr;
	for (int i = 0; i < fiber->inbox->size; i++) {
		struct msg *ri = fiber->inbox->ring[i];
		if (ri != NULL) {
			fiber->inbox->ring[i] = palloc(pool, sizeof(*ri));
			fiber->inbox->ring[i]->sender_fid = ri->sender_fid;
			fiber->inbox->ring[i]->msg = tbuf_clone(fiber->pool, ri->msg);
		}
	}
}

static void
fiber_alloc(struct fiber *fiber, size_t inbox_size)
{
	if (fiber->pool == NULL) {
		fiber->pool = palloc_create_pool(fiber->name);
		palloc_register_gc_root(fiber->pool, fiber, fiber_do_gc);
		fiber->inbox = malloc((sizeof(*fiber->inbox) + inbox_size * sizeof(struct tbuf *)));
		fiber->inbox->size = inbox_size;
	}

	prelease(fiber->pool);
	clear_inbox(fiber);
}

void
fiber_gc()
{
	if (palloc_allocated(fiber->pool) < 128 * 1024)
		return;

	palloc_gc(fiber->pool);
}

static void
fiber_zombificate(struct fiber *f)
{
	f->name = NULL;
	f->f = NULL;
	unregister_fid(f);
	f->fid = 0;
	fiber_alloc(f, f->inbox->size);

	SLIST_INSERT_HEAD(&zombie_fibers, f, zombie_link);
}

static void
fiber_loop(void *data __attribute__((unused)))
{
	while (42) {
		assert(fiber != NULL && fiber->f != NULL && fiber->fid != 0);
		@try {
			fiber->f(fiber->ap);
		}
		@catch (id e) {
			panic("uncaught exception in fiber %s, exiting", fiber->name);
		}

		fiber_zombificate(fiber);
		yield();	/* give control back to scheduler */
	}
}


/* fiber never dies, just become zombie */
struct fiber *
fiber_create(const char *name, int inbox_size, void (*f)(va_list va), ...)
{
	struct fiber *new = NULL;
	static int reg_cnt = 0;

	if (inbox_size <= 0)
		inbox_size = 64;

	if (!SLIST_EMPTY(&zombie_fibers)) {
		new = SLIST_FIRST(&zombie_fibers);
		SLIST_REMOVE_HEAD(&zombie_fibers, zombie_link);
	} else {
		new = calloc(1, sizeof(*fiber));
		if (new == NULL)
			return NULL;

		if (tarantool_coro_create(&new->coro, fiber_loop, NULL) == NULL)
			return NULL;

		char lua_reg_name[16];
		sprintf(lua_reg_name, "_fiber:%i", reg_cnt++);
		new->L = lua_newthread(root_L);
		lua_setfield(root_L, LUA_REGISTRYINDEX, lua_reg_name);

		fiber_alloc(new, inbox_size);

		SLIST_INSERT_HEAD(&fibers, new, link);
	}

	new->name = name;
	palloc_name(new->pool, name);
	/* fids from 0 to 100 are reserved */
	do {
		last_used_fid++;
	} while (last_used_fid <= 100 || fid2fiber(last_used_fid) != NULL);
	new->fid = last_used_fid;
	register_fid(new);

	new->f = f;
	va_start(new->ap, f);
	resume(new, NULL);
	va_end(new->ap);

	return new;
}

/*
 * note, we can't release memory allocated via palloc(eter_pool, ...)
 * so, struct fiber and some of its members are leaked forever
 */

void
fiber_destroy_all()
{
	struct fiber *f;
	SLIST_FOREACH(f, &fibers, link) {
		if (f == fiber) /* do not destroy running fiber */
			continue;
		if (f->name != NULL && strcmp(f->name, "sched") == 0)
			continue;

		palloc_destroy_pool(f->pool);
		tarantool_coro_destroy(&f->coro);
	}
}


static int
ring_size(struct ring *inbox)
{
	return (inbox->size + inbox->head - inbox->tail) % inbox->size;
}

int
inbox_size(struct fiber *recipient)
{
	return ring_size(recipient->inbox);
}

bool
write_inbox(struct fiber *recipient, struct tbuf *msg)
{
	struct ring *inbox = recipient->inbox;
	if (ring_size(inbox) == inbox->size - 1)
		return false;

	inbox->ring[inbox->head] = palloc(recipient->pool, sizeof(struct msg));
	inbox->ring[inbox->head]->sender_fid = fiber->fid;
	if (msg->pool != recipient->pool)
		msg = tbuf_clone(recipient->pool, msg);
	inbox->ring[inbox->head]->msg = msg;
	inbox->head = (inbox->head + 1) % inbox->size;

	if (recipient->reading_inbox)
		fiber_wake(recipient, NULL);
	return true;
}

struct msg *
read_inbox(void)
{
	struct ring *inbox = fiber->inbox;
	while (ring_size(inbox) == 0) {
		fiber->reading_inbox = true;
		yield();
		fiber->reading_inbox = false;
	}

	struct msg *msg = inbox->ring[inbox->tail];
	inbox->ring[inbox->tail] = NULL;
	inbox->tail = (inbox->tail + 1) % inbox->size;

	return msg;
}

int
set_nonblock(int sock)
{
	int flags;
	if ((flags = fcntl(sock, F_GETFL, 0)) < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0)
		return -1;
	return sock;
}

static void
inbox2sock(va_list ap)
{
	int fd = va_arg(ap, int);
	struct conn *c = conn_create(fiber->pool, fd);
	palloc_register_gc_root(fiber->pool, c, conn_gc);

	for (;;) {
		struct netmsg *n = netmsg_tail(&c->out_messages, fiber->pool);
		do {
			struct msg *m = read_inbox();
			net_add_iov(&n, m->msg->data, tbuf_len(m->msg));
		} while (ring_size(fiber->inbox) > 0);

		if (conn_flush(c) < 0)
			panic("child is dead");
		fiber_gc();
	}
}

static void
sock2inbox(va_list ap)
{
	int fd = va_arg(ap, int);
	struct tbuf *msg;
	struct fiber *recipient;
	struct conn *c;
	u32 data_len, fid;
	void *data;

	c = conn_create(fiber->pool, fd);
	palloc_register_gc_root(fiber->pool, c, conn_gc);

	for (;;) {
		if (tbuf_len(c->rbuf) < sizeof(u32) * 2) {
			if (conn_readahead(c, sizeof(u32) * 2) < 0)
				panic("child is dead");
		}

		data_len = read_u32(c->rbuf);
		fid = read_u32(c->rbuf);

		if (tbuf_len(c->rbuf) < data_len) {
			if (conn_readahead(c, data_len) < 0)
				panic("child is dead");
		}

		data = read_bytes(c->rbuf, data_len);
		recipient = fid2fiber(fid);
		if (recipient == NULL) {
			say_error("recipient is lost");
			continue;
		}

		msg = tbuf_alloc(recipient->pool);
		tbuf_append(msg, data, data_len);
		write_inbox(recipient, msg);
		fiber_gc();
	}
}

struct child *
spawn_child(const char *name, int inbox_size, int (*handler)(int fd, void *state), void *state)
{
	char *proxy_name, *child_name;
	int socks[2];
	int pid;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, socks) == -1) {
		say_syserror("socketpair");
		return NULL;
	}

	if ((pid = tnt_fork()) == -1) {
		say_syserror("fork");
		return NULL;
	}

	if (pid) {
		close(socks[0]);
		if (set_nonblock(socks[1]) == -1)
			return NULL;

		struct child *c = malloc(sizeof(*c));
		c->pid = pid;
		c->sock = socks[1];

		if (inbox_size > 0) {
			proxy_name = malloc(64);
			snprintf(proxy_name, 64, "%s/sock2inbox", name);
			c->in = fiber_create(proxy_name, inbox_size, sock2inbox, socks[1]);
			proxy_name = malloc(64);
			snprintf(proxy_name, 64, "%s/inbox2sock", name);
			c->out = fiber_create(proxy_name, inbox_size, inbox2sock, socks[1]);
			c->out->reading_inbox = true;
		}
		return c;
	} else {
		salloc_destroy();
		close_all_xcpt(3, socks[0], stderrfd, sayfd);
		child_name = malloc(64);
		snprintf(child_name, 64, "%s/child", name);
		sched.name = child_name;
		set_proc_title(name);
		say_crit("%s initialized", name);
		exit(handler(socks[0], state));
	}
}

void
fiber_info(struct tbuf *out)
{
	struct fiber *fiber;

	tbuf_printf(out, "fibers:" CRLF);
	SLIST_FOREACH(fiber, &fibers, link) {
		void *stack_top = fiber->coro.stack + fiber->coro.stack_size;

		tbuf_printf(out, "  - fid: %4i" CRLF, fiber->fid);
		tbuf_printf(out, "    name: %s" CRLF, fiber->name);
		tbuf_printf(out, "    inbox: %i" CRLF, ring_size(fiber->inbox));
		tbuf_printf(out, "    stack: %p" CRLF, stack_top);
#ifdef BACKTRACE
		tbuf_printf(out, "    backtrace: %s" CRLF,
			    backtrace(fiber->last_stack_frame,
				      fiber->coro.stack, fiber->coro.stack_size));
#endif
	}
}

void
fiber_wakeup_pending(void)
{
	assert(fiber == &sched);
	struct fiber *f;
	STAILQ_FOREACH(f, &wake_list, wake_link) {
		void *arg = f->wake;
		f->wake = NULL;
		resume(f, arg);
	}
	STAILQ_INIT(&wake_list);
}

void
fiber_init(void)
{
	SLIST_INIT(&fibers);
	SLIST_INIT(&zombie_fibers);
	STAILQ_INIT(&wake_list);

	fibers_registry = mh_i32_init();

	memset(&sched, 0, sizeof(sched));
	sched.fid = 1;
	sched.name = "sched";
	fiber_alloc(&sched, 1);

	fiber = &sched;
	last_used_fid = 100;

	ev_prepare_init(&wake_prep, (void *)fiber_wakeup_pending);
	ev_prepare_start(&wake_prep);
	say_debug("fibers initialized");
}

static void
luaT_fiber_trampoline(va_list ap)
{
	struct lua_State *pL = va_arg(ap, struct lua_State *),
			  *L = fiber->L;

	lua_xmove(pL, L, 1);
	if (lua_pcall(L, 0, 0, 0) != 0) {
		say_error("lua_pcall(): %s", lua_tostring(L, -1));
		lua_pop(L, 1);
	}
}

static int
luaT_fiber_create(struct lua_State *L)
{
	if (!lua_isfunction(L, 1)) {
		lua_pushliteral(L, "fiber.create: arg is not a function");
		lua_error(L);
	}

	fiber_create("lua", -1, luaT_fiber_trampoline, L);
	return 0;
}

static int
luaT_fiber_sleep(struct lua_State *L)
{
	lua_Number delay = luaL_checknumber(L, 1);
	fiber_sleep(delay);
	return 0;
}


static const struct luaL_reg fiberlib [] = {
	{"create", luaT_fiber_create},
	{"sleep", luaT_fiber_sleep},
	{NULL, NULL}
};

int
luaT_openfiber(struct lua_State *L)
{
	luaL_register(L, "fiber", fiberlib);
	return 0;
}
