/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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
#import <palloc.h>
#import <salloc.h>
#import <say.h>
#import <octopus.h>
#import <octopus_ev.h>
#import <tbuf.h>
#import <stat.h>
#import <pickle.h>
#import <assoc.h>
#import <net_io.h>
#import <objc.h>

#include <third_party/queue.h>

#include <fiber.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sysexits.h>

struct Fiber* sched;
coro_context *sched_ctx = NULL;
#ifdef THREADS
__thread struct Fiber *fiber = nil;
__thread int coro_switch_cnt;
#else
struct Fiber *fiber = nil;
int coro_switch_cnt;
#endif
static uint32_t last_used_fid;

static ev_prepare wake_prep;
ev_async wake_async;

static struct mh_i32_t *fibers_registry;

TAILQ_HEAD(, Fiber) wake_list;

#if defined(FIBER_DEBUG) || defined(FIBER_EV_DEBUG)
void
fiber_ev_cb(void *arg)
{
	say_debug("%s: =<< arg:%p", __func__, arg);
}
#endif

void
resume(struct Fiber *callee, void *w)
{
	assert(callee != sched);
	Fiber *caller = fiber;
#ifdef FIBER_DEBUG
	say_debug("%s: %i/%s -> %i/%s arg:%p", __func__,
		  caller->fid, caller->name, callee->fid, callee->name, w);
#endif
	callee->caller = caller;
	fiber = callee;
	callee->coro.w = w;
	oc_coro_transfer(&caller->coro.ctx, &callee->coro.ctx);
	callee->caller = sched;
}

void *
yield(void)
{
	Fiber *callee = fiber;
#ifdef FIBER_DEBUG
	say_debug("%s: %i/%s -> %i/%s", __func__,
		  callee->fid, callee->name,
		  callee->caller->fid, callee->caller->name);
#endif
	fiber = callee->caller;
	oc_coro_transfer(&callee->coro.ctx, &callee->caller->coro.ctx);
#ifdef FIBER_DEBUG
	say_debug("%s: return arg:%p", __func__, fiber->coro.w);
#endif

	return fiber->coro.w;
}

int
fiber_wake(struct Fiber *f, void *arg)
{
	/* tqe_prev points to prev elem or tailq head => not null if member */
	if (f->wake_link.tqe_prev)
		return 0;
#ifdef FIBER_DEBUG
	say_debug("%s: %i/%s arg:%p", __func__, f->fid, f->name, arg);
#endif
	f->wake = arg;
	TAILQ_INSERT_TAIL(&wake_list, f, wake_link);
	return 1;
}

int
fiber_cancel_wake(struct Fiber *f)
{
	/* see fiber_wake() comment */
	if (f->wake_link.tqe_prev == NULL)
		return 0;
	TAILQ_REMOVE(&wake_list, f, wake_link);
	f->wake_link.tqe_prev = NULL;
	return 1;
}

void
fiber_sleep(ev_tstamp delay)
{
	assert(fiber != sched);
	ev_timer *s, w = { .coro = 1 };
	ev_timer_init(&w, (void *)fiber, delay, 0.);
	ev_timer_start(&w);
	s = yield();
	assert(s == &w);
	(void)s;
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

struct Fiber *
fid2fiber(int fid)
{
	u32 k = mh_i32_get(fibers_registry, fid);
	if (k == mh_end(fibers_registry))
		return NULL;
	return mh_i32_value(fibers_registry, k);
}

static void
register_fid(struct Fiber *fiber)
{
	mh_i32_put(fibers_registry, fiber->fid, fiber, NULL);
}

static void
unregister_fid(struct Fiber *fiber)
{
	mh_i32_remove(fibers_registry, fiber->fid, NULL);
}


static void
fiber_alloc(struct Fiber *fiber)
{
	if (fiber->pool == NULL)
		fiber->pool = palloc_create_pool((struct palloc_config){.name = fiber->name});
	if (fiber->autorelease.current == NULL) {
		fiber->autorelease.current = &fiber->autorelease.top;
		fiber->autorelease.top.prev = &fiber->autorelease.top;
	}

	prelease(fiber->pool);
}

void
fiber_gc()
{
	autorelease_top();

	if (palloc_allocated(fiber->pool) < 128 * 1024)
		return;

	palloc_gc(fiber->pool);
}

static void
fiber_zombificate(struct Fiber *f)
{
	autorelease_top();
	palloc_name(f->pool, "zombi_fiber");
	f->name = "zombi_fiber";
	f->f = NULL;
	unregister_fid(f);
	f->fid = 0;
	fiber_alloc(f);

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
		@catch (Error *e) {
			panic_exc(e);
		}
		@catch (id e) {
			panic("uncaught exception in fiber %s, exiting", fiber->name);
		}

		fiber_zombificate(fiber);
		yield();	/* give control back to scheduler */
	}
}


/* fiber never dies, just become zombie */
struct Fiber *
fiber_create(const char *name, void (*f)(va_list va), ...)
{
	Fiber *new = NULL;
	static int reg_cnt = 0;

	if (!SLIST_EMPTY(&zombie_fibers)) {
		new = SLIST_FIRST(&zombie_fibers);
		SLIST_REMOVE_HEAD(&zombie_fibers, zombie_link);
	} else {
		new = [Fiber alloc];
		if (octopus_coro_create(&new->coro, fiber_loop, NULL) == NULL)
			panic_syserror("fiber_create");

		char lua_reg_name[16];
		sprintf(lua_reg_name, "_fiber:%i", reg_cnt++);
		new->L = lua_newthread(root_L);
		lua_setfield(root_L, LUA_REGISTRYINDEX, lua_reg_name);

		fiber_alloc(new);

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

	if (new->fid == 0) /* f() exited without ever calling yield() */
		return NULL;

	return new;
}

#ifdef THREADS
/* create fake fiber structure for use in worker threads */
void
fiber_create_fake(const char *name)
{
	assert(fiber == nil);
	fiber = [Fiber alloc];
	fiber->name = name;
	fiber_alloc(fiber);
	fiber->fid = ~0;
	fiber->f = NULL;
}

void
fiber_destroy_fake()
{
	assert(fiber->fid == ~0 && fiber->f == NULL);
	autorelease_top();
	prelease(fiber->pool);
	palloc_destroy_pool(fiber->pool);
	[fiber free];
	fiber = NULL;
}
#endif

void
fiber_destroy_all()
{
	Fiber *f, *tmp;
	SLIST_FOREACH_SAFE(f, &fibers, link, tmp) {
		if (f == fiber) /* do not destroy running fiber */
			continue;
		if (f->name != NULL && strcmp(f->name, "sched") == 0)
			continue;

		palloc_destroy_pool(f->pool);
		octopus_coro_destroy(&f->coro);
		[f free];
	}
}

void
fiber_info(struct tbuf *out)
{
	Fiber *fiber;

	tbuf_printf(out, "fibers:" CRLF);
	SLIST_FOREACH(fiber, &fibers, link) {
		void *stack_top = fiber->coro.stack + fiber->coro.stack_size;

		tbuf_printf(out, "  - fid: %4i" CRLF, fiber->fid);
		tbuf_printf(out, "    name: %s" CRLF, fiber->name);
		tbuf_printf(out, "    stack: %p" CRLF, stack_top);
	}
}

void
fiber_wakeup_pending(void)
{
	assert(fiber == sched);
	Fiber *f, *tvar;

	for(int i=10; i && !TAILQ_EMPTY(&wake_list); i--) {
		TAILQ_FOREACH_SAFE(f, &wake_list, wake_link, tvar) {
			void *arg = f->wake;
			TAILQ_REMOVE(&wake_list, f, wake_link);
			f->wake_link.tqe_prev = NULL;
			resume(f, arg);
		}
	}

	if (!TAILQ_EMPTY(&wake_list)) {
		zero_io_collect_interval();
		ev_async_send(&wake_async);
	}
}

void
fiber_init(void)
{
	SLIST_INIT(&fibers);
	SLIST_INIT(&zombie_fibers);
	TAILQ_INIT(&wake_list);

	fibers_registry = mh_i32_init(xrealloc);

	sched = [Fiber alloc];
	sched->fid = 1;
	sched->name = "sched";
	fiber_alloc(sched);
	sched_ctx = &sched->coro.ctx;

	fiber = sched;
	last_used_fid = 100;

	ev_prepare_init(&wake_prep, (void *)fiber_wakeup_pending);
	ev_prepare_start(&wake_prep);
	ev_async_init(&wake_async, (void *)unzero_io_collect_interval);
	ev_async_start(&wake_async);
	say_debug("fibers initialized");
}

struct Fiber*
current_fiber()
{
	return fiber;
}

int
fiber_switch_cnt()
{
	return coro_switch_cnt;
}

static void
luaT_fiber_trampoline(va_list ap)
{
	struct lua_State *pL = va_arg(ap, struct lua_State *),
			  *L = fiber->L;

	lua_pushcfunction(L, luaT_traceback);
	lua_xmove(pL, L, 1);
	if (lua_pcall(L, 0, 0, -2) != 0) {
		say_error("lua_pcall(): %s", lua_tostring(L, -1));
		lua_pop(L, 1);
	}
	lua_pop(L, 1);
}

static int
luaT_fiber_create(struct lua_State *L)
{
	if (!lua_isfunction(L, 1)) {
		lua_pushliteral(L, "fiber.create: arg is not a function");
		lua_error(L);
	}

	fiber_create("lua", luaT_fiber_trampoline, L);
	return 0;
}

static int
luaT_fiber_sleep(struct lua_State *L)
{
	lua_Number delay = luaL_checknumber(L, 1);
	fiber_sleep(delay);
	return 0;
}

static int
luaT_fiber_gc(struct lua_State *L _unused_)
{
	fiber_gc();
	return 0;
}

static int
luaT_fiber_yield(struct lua_State *L _unused_)
{
	yield();
	return 0;
}


static const struct luaL_reg fiberlib [] = {
	{"create", luaT_fiber_create},
	{"sleep", luaT_fiber_sleep},
	{"gc", luaT_fiber_gc},
	{"yield", luaT_fiber_yield},
	{NULL, NULL}
};

int
luaT_openfiber(struct lua_State *L)
{
	luaL_register(L, "fiber", fiberlib);
	lua_pop(L, 1);
	return 0;
}

/* ObjectiveC autorelease functions */
id
autorelease(id obj)
{
	struct autorelease_chain *chain = fiber->autorelease.current;
	chain->objs[chain->cnt++] = obj;
	if (chain->cnt == AUTORELEASE_CHAIN_CAPA) {
		chain = xcalloc(1, sizeof(*chain));
		chain->prev = fiber->autorelease.current;
		fiber->autorelease.current = chain;
	}
	return obj;
}

static inline void
object_release(id obj) {
#ifdef OCT_OBJECT
	uintptr_t ptr = (uintptr_t)obj;
	if ((ptr & 1) != 0) {
		ptr &= ~(uintptr_t)1;
		struct tnt_object *tnt = (void*)ptr;
		object_decr_ref(tnt);
	} else
#endif
		[obj release];
}

void
autorelease_pop(struct autorelease_pool *pool)
{
	struct autorelease_chain *prev, *current = fiber->autorelease.current;
	u32 i;
	while (current != pool->chain) {
		i = current->cnt;
		while(i) {
			i--;
			@try {
				object_release(current->objs[i]);
			} @catch(Error* e) {
				panic_exc(e);
			} @catch(id e) {
				panic("exception during autorelease");
			}
		}
		fiber->autorelease.current = prev = current->prev;
		assert(current != &fiber->autorelease.top);
		free(current);
		current = prev;
	}
	i = current->cnt;
	while(i > pool->pos) {
		i--;
		@try {
			object_release(current->objs[i]);
		} @catch(Error* e) {
			panic_exc(e);
		} @catch(id e) {
			panic("exception during autorelease");
		}
	}
	current->cnt = i;
}

void
autorelease_pop_and_cut(struct autorelease_pool *pool)
{
	autorelease_pop(pool);
	palloc_cutoff(fiber->pool);
}

void
autorelease_top()
{
	struct autorelease_pool top = {.chain = &fiber->autorelease.top, .pos = 0};
	autorelease_pop(&top);
}

@implementation Fiber
-(void)
setValue: (id)val
{
	wake_flag = WAKE_VALUE;
	fiber_wake(self, [val retain]);
}

-(void)
setError: (id)err
{
	wake_flag = WAKE_ERROR;
	fiber_wake(self, [err retain]);
}

-(id)
yield
{
	id res = nil;
	if (wake_flag != 0) {
		fiber_cancel_wake(self);
		res = wake;
	} else {
		res = yield();
	}
	if (wake_flag == WAKE_VALUE) {
		wake_flag = 0;
		return [res autorelease];
	}
	if (wake_flag == WAKE_ERROR) {
		wake_flag = 0;
		@throw res;
	}
	panic("unknown fiber wake flag %d", wake_flag);
}
@end

register_source();
