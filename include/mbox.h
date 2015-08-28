/*
 * Copyright (C) 2010, 2011, 2014 Mail.RU
 * Copyright (C) 2011 Teodor Sigaev
 * Copyright (C) 2014 Yuriy Vostrikov
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

#ifndef MBOX_H
#define MBOX_H

#include <third_party/queue.h>

#include <octopus_ev.h>
#include <util.h>
#include <fiber.h>

struct mbox_consumer {
	struct	Fiber			*fiber;
	LIST_ENTRY(mbox_consumer)	conslink;
	int msg_count;
};

#define MBOX(name, type)						\
struct name {								\
	LIST_HEAD(, mbox_consumer) consumer_list;			\
	TAILQ_HEAD(, type) msg_list;					\
	int msg_count;							\
}

#define MBOX_INITIALIZER(name)						\
	{ LIST_HEAD_INITIALIZER(name.consumer_list),			\
	  TAILQ_HEAD_INITIALIZER(name.msg_list),			\
	  0 }

#define mbox_init(mbox) ({						\
	LIST_INIT(&(mbox)->consumer_list);				\
	TAILQ_INIT(&(mbox)->msg_list);					\
	(mbox)->msg_count = 0;						\
})

#define mbox_put(mbox, msg, link) ({					\
	TAILQ_INSERT_TAIL(&(mbox)->msg_list, msg, link); 		\
	(mbox)->msg_count++;						\
	struct mbox_consumer *consumer;					\
	LIST_FOREACH(consumer, &(mbox)->consumer_list, conslink)	\
		if ((mbox)->msg_count >= consumer->msg_count)		\
			fiber_wake(consumer->fiber, msg);		\
})

#define mbox_msgtype(mbox) typeof((mbox)->msg_list.tqh_first)

#define mbox_peek(mbox) TAILQ_FIRST(&(mbox)->msg_list);

#define mbox_remove(mbox, msg) ({					\
	TAILQ_REMOVE(&(mbox)->msg_list, (msg), link);			\
	memset(&(msg)->link, 0, sizeof((msg)->link));			\
	(mbox)->msg_count--;						\
})

#define mbox_clear(mbox) ({						\
	struct msg_void_ptr *msg, *tmp;					\
	TAILQ_FOREACH_SAFE(msg, &(mbox)->msg_list, link, tmp)		\
		mbox_remove((mbox), msg);				\
})

#define mbox_get(mbox, link) ({						\
	mbox_msgtype(mbox) msg = TAILQ_FIRST(&(mbox)->msg_list);	\
	if (msg)							\
		mbox_remove(mbox, msg);					\
	msg;								\
})


#define mbox_wait(mbox) ({						\
	struct mbox_consumer consumer = { .fiber = fiber }; 		\
	mbox_msgtype(mbox) msg = TAILQ_FIRST(&(mbox)->msg_list);	\
	LIST_INSERT_HEAD(&(mbox)->consumer_list, &consumer, conslink);	\
	while ((mbox)->msg_count == 0)					\
		msg = yield();						\
	LIST_REMOVE(&consumer, conslink);				\
	msg;								\
})

#define mbox_timedwait(mbox, count, delay) ({				\
	struct mbox_consumer consumer = { .fiber = fiber,		\
					  .msg_count = count };		\
	ev_timer w;							\
	if (delay) {							\
		w = (ev_timer){ .coro = 1 };				\
		ev_timer_init(&w, (void *)fiber, delay, 0);		\
		ev_timer_start(&w);					\
	}								\
	mbox_msgtype(mbox) msg = TAILQ_FIRST(&(mbox)->msg_list);	\
	LIST_INSERT_HEAD(&(mbox)->consumer_list, &consumer, conslink);	\
	while ((mbox)->msg_count < count) {				\
		msg = yield();						\
		if (msg == (void *)&w) { /* timeout */			\
			msg = NULL;					\
			break;						\
		}							\
	}								\
	LIST_REMOVE(&consumer, conslink);				\
	if (delay)							\
		ev_timer_stop(&w);					\
	msg;								\
})

struct msg_void_ptr {
	void *ptr;
	TAILQ_ENTRY(msg_void_ptr) link;
};
MBOX(mbox_void_ptr, msg_void_ptr);

#endif
