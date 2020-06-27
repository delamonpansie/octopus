/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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
static ev_check wake_check;
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

static int fiber_async_sent = 1;
static void
fiber_async_send()
{
	if (fiber_async_sent == 0) {
		zero_io_collect_interval();
		ev_async_send(&wake_async);
		fiber_async_sent = 1;
	}
}

static void
fiber_async_check(ev_check* ev _unused_, int events _unused_)
{
	fiber_async_sent = 1;
}

void
#ifdef FIBER_DEBUG
fiber_resume(struct Fiber *callee, void *w)
#else
resume(struct Fiber *callee, void *w)
#endif
{
#ifdef FIBER_DEBUG
	assert(callee != sched && callee->caller == NULL);
#endif
	Fiber *caller = fiber;
	callee->caller = caller;
	fiber = callee;
	callee->coro.w = w;
	oc_coro_transfer(&caller->coro.ctx, &callee->coro.ctx);
}

void *
#ifdef FIBER_DEBUG
fiber_yield(void)
#else
yield(void)
#endif
{
	Fiber *callee = fiber;
#ifdef FIBER_DEBUG
	assert(callee->caller != NULL);
#endif
	fiber = callee->caller;
	callee->caller = NULL;
	oc_coro_transfer(&callee->coro.ctx, &fiber->coro.ctx);
	return fiber->coro.w;
}

int
fiber_wake(struct Fiber *f, void *arg)
{
	assert(f != sched);
	/* tqe_prev points to prev elem or tailq head => not null if member */
	if (f->wake_link.tqe_prev)
		return 0;
#ifdef FIBER_DEBUG
	say_debug("%s: %i/%s arg:%p", __func__, f->fid, f->name, arg);
#endif
	f->wake = arg;
	TAILQ_INSERT_TAIL(&wake_list, f, wake_link);
	fiber_async_send();
	return 1;
}

int
fiber_cancel_wake(struct Fiber *f)
{
	assert(f != sched);
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

#if CFG_lua_path
	extern void lua_fiber_init(struct Fiber *);
	if (fiber->L == NULL)
		lua_fiber_init(fiber);
#endif
#if CFG_caml_path
	if (fiber->ML.last_retaddr == 0)
		fiber->ML.last_retaddr = 1;
#endif

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
fiber_loop(void *data)
{
	while (42) {
		assert(fiber != NULL && fiber->f != NULL && fiber->fid != 0);
		@try {
			fiber->f(*(va_list *)data);
		}
		@catch (Error *e) {
			panic_exc(e);
		}
		@catch (id e) {
			panic("uncaught exception in fiber %s, exiting", fiber->name);
		}

		fiber_zombificate(fiber);
		data = yield();	/* give control back to scheduler */
	}
}


/* fiber never dies, just become zombie */
struct Fiber *
fiber_create(const char *name, void (*f)(va_list va), ...)
{
	Fiber *new = NULL;
	va_list ap;

	if (!SLIST_EMPTY(&zombie_fibers)) {
		new = SLIST_FIRST(&zombie_fibers);
		SLIST_REMOVE_HEAD(&zombie_fibers, zombie_link);
	} else {
		new = [Fiber alloc];
		if (octopus_coro_create(&new->coro, fiber_loop, &ap) == NULL)
			panic_syserror("fiber_create");

		fiber_alloc(new);

		SLIST_INSERT_HEAD(&fibers, new, link);
	}

	new->ushard = -1;
	new->name = name;
	palloc_name(new->pool, name);
	/* fids from 0 to 100 are reserved */
	do {
		last_used_fid++;
	} while (last_used_fid <= 100 || fid2fiber(last_used_fid) != NULL);
	new->fid = last_used_fid;
	register_fid(new);

	new->f = f;
	va_start(ap, f);
	resume(new, &ap);
	va_end(ap);

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
	Fiber *f;

	for(int i=10; i && !TAILQ_EMPTY(&wake_list); i--) {
		TAILQ_INSERT_TAIL(&wake_list, sched, wake_link);
		while (1) {
			f = TAILQ_FIRST(&wake_list);
			TAILQ_REMOVE(&wake_list, f, wake_link);
			if (f == sched) break;
			f->wake_link.tqe_prev = NULL;
#ifdef FIBER_DEBUG
			say_debug("%s: %i/%s arg:%p", __func__, f->fid, f->name, f->wake);
#endif
			resume(f, f->wake);
		}
	}

	fiber_async_sent = 0;
	if (!TAILQ_EMPTY(&wake_list)) {
		fiber_async_send();
	}
}

ssize_t
fiber_recv(int fd, struct tbuf *rbuf)
{
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_READ);
	ev_io_start(&io);
	yield();
	ev_io_stop(&io);
	tbuf_ensure(rbuf, 16 * 1024);
	return tbuf_recv(rbuf, fd);
}

ssize_t
fiber_read(int fd, void *buf, size_t count)
{
	ssize_t r, done = 0;
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_READ);
	ev_io_start(&io);

	while (count > done) {
		yield();
		r = read(fd, buf + done, count - done);

		if (unlikely(r <= 0)) {
			if (r < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					continue;
				say_syserror("%s: read", __func__);
				break;
			}
			if (r == 0) {
				say_debug("%s: fd:%i eof", __func__, fd);
				break;
			}
		}
		done += r;
	}
	ev_io_stop(&io);

	return done;
}

ssize_t
fiber_write(int fd, const void *buf, size_t count)
{
	int r;
	unsigned int done = 0;
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_WRITE);
	ev_io_start(&io);

	do {
		yield();
		if ((r = write(fd, buf + done, count - done)) < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				continue;
			say_syserror("%s: write", __func__);
			break;
		}
		done += r;
	} while (count != done);
	ev_io_stop(&io);

	return done;
}

ssize_t
fiber_writev(int fd, struct netmsg_head *head)
{
	int r;
	unsigned int done = 0;
	ev_io io = { .coro = 1 };
	ev_io_init(&io, (void *)fiber, fd, EV_WRITE);
	ev_io_start(&io);

	do {
		yield();
		if ((r = netmsg_writev(fd, head)) < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				continue;
			say_syserror("%s: write", __func__);
			break;
		}
		done += r;
	} while (head->bytes > 0);
	ev_io_stop(&io);

	return done;
}



void
fiber_init(const char *sched_name)
{
	SLIST_INIT(&fibers);
	SLIST_INIT(&zombie_fibers);
	TAILQ_INIT(&wake_list);

	fibers_registry = mh_i32_init(xrealloc);

	sched = [Fiber alloc];
	sched->fid = 1;
	sched->name = sched_name ?: "sched";
	sched->ushard = -1;
	fiber_alloc(sched);
	sched_ctx = &sched->coro.ctx;

	fiber = sched;
	last_used_fid = 100;

	ev_prepare_init(&wake_prep, (void *)fiber_wakeup_pending);
	ev_set_priority(&wake_prep, -1);
	ev_prepare_start(&wake_prep);
	ev_async_init(&wake_async, (void *)unzero_io_collect_interval);
	ev_async_start(&wake_async);
	ev_check_init(&wake_check, (void*)fiber_async_check);
	ev_check_start(&wake_check);

#if CFG_lua_path
	extern void luaO_init();
	luaO_init();
#endif
#if CFG_caml_path
	extern void fiber_caml_init();
	fiber_caml_init();
#endif
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

static void
rwcond_wait(struct rwlock *lock) {
	SLIST_INSERT_HEAD(&lock->wait, fiber, worker_link);
	yield();
}

static void
rwbroadcast(struct rwlock *lock) {
	struct Fiber *waiter = SLIST_FIRST(&lock->wait), *next;
	SLIST_INIT(&lock->wait);
	while (waiter) {
		next = SLIST_NEXT(waiter, worker_link);
		fiber_wake(waiter, NULL);
		waiter = next;
	}
}

void
wlock(struct rwlock *lock)
{
	while (lock->locked)
		rwcond_wait(lock);
	lock->locked = 1;
	while (lock->readers)
		rwcond_wait(lock);
}

void
wunlock(struct rwlock *lock)
{
	lock->locked = 0;
	rwbroadcast(lock);
}

void
rlock(struct rwlock *lock)
{
	while (lock->locked)
		rwcond_wait(lock);
	lock->readers++;
}

void
runlock(struct rwlock *lock)
{
	lock->readers--;
	if (lock->readers == 0 && !SLIST_EMPTY(&lock->wait))
		rwbroadcast(lock);
}

register_source();
