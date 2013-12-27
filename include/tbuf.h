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

#ifndef TBUF_H
#define TBUF_H

#include <util.h>

#include <third_party/luajit/src/lua.h>

#include <stdarg.h>
#include <string.h>


struct tbuf {
	void *ptr, *end;
	uint32_t free;
	struct palloc_pool *pool;
};

#define TBUF(d, l, p) (struct tbuf){ .free = 0, .ptr = (char *)(d), .end = (char *)(d) + (l), .pool = (p) }

static inline int __attribute__((pure)) tbuf_len(const struct tbuf *b)
{
#ifdef TBUF_PARANOIA
	assert(b != NULL);
	assert(b->end >= b->ptr);
#endif
	return b->end - b->ptr;
}
static inline int __attribute__((pure)) tbuf_size(const struct tbuf *b)
{
#ifdef TBUF_PARANOIA
	assert(b != NULL);
#endif
	return b->end - b->ptr + b->free;
}
static inline int __attribute__((pure)) tbuf_free(const struct tbuf *b)
{
#ifdef TBUF_PARANOIA
	assert(b != NULL);
#endif
	return b->free;
}

struct tbuf *tbuf_alloc(struct palloc_pool *pool);
void tbuf_ensure_resize(struct tbuf *e, size_t bytes_required);
static __attribute__((always_inline)) inline void
tbuf_ensure(struct tbuf *e, size_t required)
{
#ifdef TBUF_PARANOIA
	assert(tbuf_len(e) <= tbuf_size(e));
#endif
	if (unlikely(tbuf_free(e) < required))
		tbuf_ensure_resize(e, required);
}

void tbuf_willneed(struct tbuf *e, size_t required);

struct tbuf *tbuf_clone(struct palloc_pool *pool, const struct tbuf *orig);
void tbuf_gc(struct palloc_pool *pool, void *ptr);

struct tbuf *tbuf_split(struct tbuf *e, size_t at);
void tbuf_reset(struct tbuf *b);
void *tbuf_peek(struct tbuf *b, size_t count);
void tbuf_ltrim(struct tbuf *b, size_t diff);
void tbuf_rtrim(struct tbuf *b, size_t diff);

void tbuf_append(struct tbuf *b, const void *data, size_t len);
#define tbuf_add_dup(b, data) tbuf_append(b, (data), sizeof(*(data)))
void tbuf_append_field(struct tbuf *b, void *f);
void tbuf_vprintf(struct tbuf *b, const char *format, va_list ap)
	__attribute__ ((format(FORMAT_PRINTF, 2, 0)));
void tbuf_printf(struct tbuf *b, const char *format, ...)
	__attribute__ ((format(FORMAT_PRINTF, 2, 3)));

ssize_t tbuf_recv(struct tbuf *b, int fd);
char *tbuf_to_hex(const struct tbuf *x);

#endif
