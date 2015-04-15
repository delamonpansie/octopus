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

#ifndef OCTOPUS_EV_H
#define OCTOPUS_EV_H

#define EV_MULTIPLICITY 0
#ifndef THREADS
#define EV_NO_THREADS 1
#endif
#define EV_CONFIG_H "include/config.h"

#include <coro.h>
extern coro_context *sched_ctx;
#ifdef THREADS
extern __thread struct fiber *fiber;
#else
extern struct fiber *fiber;
#endif

#define EV_STRINGIFY2(x) #x
#define EV_STRINGIFY(x) EV_STRINGIFY2(x)
#define EV_COMMON void *data; char coro; const char *cb_src;
#define ev_set_cb(ev,cb_) ev_cb (ev) = (cb_); (ev)->cb_src = __FILE__ ":" EV_STRINGIFY(__LINE__);
#define EV_CB_DECLARE(type) void (*cb)(struct type *w, int revents);

#if defined(FIBER_DEBUG) || defined(FIBER_EV_DEBUG)
extern void fiber_ev_cb(void *);
#define EV_CB_LOG(arg) fiber_ev_cb((arg))
#else
#define EV_CB_LOG(arg) (void)0
#endif

#define EV_CB_INVOKE(watcher, revents) ({			\
if ((watcher)->coro) {						\
	fiber = (struct fiber *)(watcher)->cb;			\
	((struct octopus_coro *)fiber)->w = (watcher);		\
	EV_CB_LOG((watcher));					\
	oc_coro_transfer(sched_ctx, (coro_context *)fiber);	\
} else								\
	(watcher)->cb((watcher), (revents));			\
})

#include "third_party/libev/ev.h"

#endif
