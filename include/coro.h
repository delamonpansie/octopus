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

#ifndef	OCTOPUS_CORO_H
#define OCTOPUS_CORO_H

#include <stdlib.h>
#include <third_party/libcoro/coro.h>
#include <inttypes.h>

struct octopus_coro {
	coro_context ctx;
	void *stack, *mmap;
	size_t stack_size, mmap_size;
	void *w;
};

struct octopus_coro *
octopus_coro_create(struct octopus_coro *ctx, void (*f) (void *), void *data);
void octopus_coro_destroy(struct octopus_coro *ctx);

/* counter for context switches.
 * it has type `int` for fast retreiving from luajit.
 * located in fiber.m for performance issue. */
extern int coro_switch_cnt;
static inline void
oc_coro_transfer(coro_context *from, coro_context *to)
{
	/* do not rely on signed integer overflow. We believe, 30bit it is safe enough :) */
	coro_switch_cnt = (coro_switch_cnt & 0x3fffffff) + 1;
	coro_transfer(from, to);
}

#endif
