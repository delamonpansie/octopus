/*
 * Copyright (C) 2010, 2011 Mail.RU
 * Copyright (C) 2011 Teodor Sigaev
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

#include <third_party/queue.h>

#import <octopus_ev.h>
#import <util.h>
#import <fiber.h>

#define MBOX_MAGICK_COOKIE	((void*)(uintptr_t)0xBADC0DEF)

struct mbox_consumer {
	struct	fiber			*fiber;
	TAILQ_ENTRY(mbox_consumer)	conslink;
};

struct mbox_msg {
	void				*msg;
	STAILQ_ENTRY(mbox_msg)		msglink;
};

struct mbox {
	STAILQ_HEAD(mbox_msg_list, mbox_msg)		msg_list;
	u32						msg_list_len;
	TAILQ_HEAD(mbox_consumer_list, mbox_consumer)	consumer_list;
};

static inline void
mbox_init(struct mbox *mbox) {
	STAILQ_INIT(&mbox->msg_list);
	TAILQ_INIT(&mbox->consumer_list);
	mbox->msg_list_len = 0;
}

static inline void
mbox_put(struct mbox *mbox, struct mbox_msg *msg) {
	struct mbox_consumer	*consumer;

	STAILQ_INSERT_TAIL(&mbox->msg_list, msg, msglink);
	mbox->msg_list_len++;

	TAILQ_FOREACH(consumer, &mbox->consumer_list, conslink)
	fiber_wake(consumer->fiber, MBOX_MAGICK_COOKIE);
}

static inline void *
mbox_get(struct mbox *mbox) {
	void *msg = NULL;

	if (mbox->msg_list_len > 0) {
		struct mbox_msg *mbox_msg = STAILQ_FIRST(&mbox->msg_list);
			
		assert(mbox_msg != NULL);

		STAILQ_REMOVE_HEAD(&mbox->msg_list, msglink);
		mbox->msg_list_len--;

		msg = mbox_msg->msg;
	} else {
		assert(STAILQ_FIRST(&mbox->msg_list) == NULL);
	}

	return msg;
}

static inline void
mbox_wait(struct mbox *mbox) {
	struct mbox_consumer	mbox_consumer = { .fiber = fiber };

	TAILQ_INSERT_TAIL(&mbox->consumer_list, &mbox_consumer,  conslink); 
	while(mbox->msg_list_len == 0) {
		void *r = yield();

		assert(r == MBOX_MAGICK_COOKIE);
		(void)r; /* keep compiler quiet */
	}
	TAILQ_REMOVE(&mbox->consumer_list, &mbox_consumer,  conslink);
}

static inline bool
mbox_timedwait(struct mbox *mbox, ev_tstamp delay) {
	struct mbox_consumer	mbox_consumer = { .fiber = fiber };
	ev_timer 		w = { .coro = 1 };
	bool			res = true;

	ev_timer_init(&w, (void *)fiber, delay, 0.);

	ev_timer_start(&w);
	TAILQ_INSERT_TAIL(&mbox->consumer_list, &mbox_consumer,  conslink);

	while(mbox->msg_list_len == 0) {
		void *r = yield();

		if (r == (void*)&w) {
			/* timeout */
			res = false;
			break;
		} else {
			assert(r == MBOX_MAGICK_COOKIE);
		}
	}

	TAILQ_REMOVE(&mbox->consumer_list, &mbox_consumer,  conslink);
	ev_timer_stop(&w);

	return res;
}

