/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2016 Yuriy Vostrikov
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

#ifndef FIBER_H
#define FIBER_H

#include <util.h>
#include <coro.h>
#include <objc.h>
#include <cfg/defs.h>

#include <third_party/queue.h>

#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/uio.h>
#include <stdarg.h>

struct tbuf; /* forward declaration */

extern coro_context *sched_ctx;
#ifdef THREADS
extern __thread struct Fiber *fiber;
#else
extern struct Fiber* fiber;
#endif
extern struct Fiber* sched; /* fiber running ev callbacks */
static inline bool is_sched(struct Fiber* fib) { return fib == sched; }
static inline bool not_sched(struct Fiber* fib) { return fib != sched; }


@interface Fiber : Object <Waiter> {
@public
	struct octopus_coro coro;
	struct Fiber *caller;
	struct palloc_pool *pool;
	uint32_t fid;

	SLIST_ENTRY(Fiber) link, zombie_link, worker_link;
	TAILQ_ENTRY(Fiber) wake_link;
	void *wake;
	enum {WAKE_VALUE=1, WAKE_ERROR} wake_flag;
	int   ushard;
	void *txn;

#if CFG_lua_path
	struct lua_State *L;
#endif
#if CFG_caml_path
	struct caml_state {
		char * top_of_stack;          /* Top of stack for this thread (approx.) */
		char * bottom_of_stack;       /* Saved value of caml_bottom_of_stack */
		uintptr_t last_retaddr;         /* Saved value of caml_last_return_address */
		intptr_t * gc_regs;              /* Saved value of caml_gc_regs */
		char * exception_pointer;     /* Saved value of caml_exception_pointer */
		struct caml__roots_block * local_roots; /* Saved value of local_roots */

		int backtrace_pos;            /* Saved backtrace_pos */
		void ** backtrace_buffer;    /* Saved backtrace_buffer */
		intptr_t backtrace_last_exn;     /* Saved backtrace_last_exn (root) */
	} ML;
#endif
	struct {
		struct autorelease_chain *current;
		struct autorelease_chain top;
	} autorelease;

	const char *name;
	void (*f)(va_list ap);
	va_list ap;
}
- (void) setValue: (id)val;
- (void) setError: (id)err;
- (id) yield;
@end

SLIST_HEAD(, Fiber) fibers, zombie_fibers;

void fiber_init(const char *sched_name);
struct Fiber *fiber_create(const char *name, void (*f)(va_list va), ...);
void fiber_destroy_all();
int wait_for_child(pid_t pid);

#ifdef FIBER_DEBUG
void fiber_resume(struct Fiber *callee, void *w);
void *fiber_yield(void);
#define resume(callee, w) ({						\
	say_debug("resume: %i/%s -> %i/%s arg:%p",			\
		  fiber->fid, fiber->name, callee->fid, callee->name, w); \
	fiber_resume(callee, w);					\
	})
#define yield() ({						\
	say_debug("yield: %i/%s -> %i/%s",			\
		  fiber->fid, fiber->name,			\
		  fiber->caller->fid, fiber->caller->name);	\
	void *yield_ret = fiber_yield();			\
	say_debug("yield: return arg:%p", yield_ret);	\
	yield_ret;						\
	})
#else
void resume(struct Fiber *callee, void *w);
void *yield(void);
#endif
int fiber_wake(struct Fiber *f, void *arg);
int fiber_cancel_wake(struct Fiber *f);

void fiber_gc(void);
void fiber_sleep(double s);
void fiber_info(struct tbuf *out);
struct Fiber *fid2fiber(int fid);

struct Fiber* current_fiber();
int fiber_switch_cnt();
#ifdef THREADS
/* create and destroy fake fiber for working threads */
void fiber_create_fake(const char* name);
void fiber_destroy_fake();
#endif

/* "blocking" calls */
ssize_t fiber_recv(int fd, struct tbuf *rbuf);
ssize_t fiber_read(int fd, void *buf, size_t count);
ssize_t fiber_write(int fd, const void *buf, size_t count);
struct netmsg_head;
ssize_t fiber_writev(int fd, struct netmsg_head *head);

struct rwlock {
	SLIST_HEAD(, Fiber) wait;
	int readers;
	bool locked;
};
void wlock(struct rwlock *lock);
void wunlock(struct rwlock *lock);
void rlock(struct rwlock *lock);
void runlock(struct rwlock *lock);

#endif
