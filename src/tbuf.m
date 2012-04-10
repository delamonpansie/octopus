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
#import <palloc.h>
#import <pickle.h>
#import <tbuf.h>
#import <fiber.h> // FIXME: remove dependency

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lauxlib.h>

#include <string.h>
#include <stddef.h>
#include <stdio.h>
#include <stdarg.h>

#ifdef POISON
#  define TBUF_POISON
#endif

#ifdef TBUF_POISON
#  define poison(ptr, len) memset((ptr), 'A', (len))
#else
#  define poison(ptr, len)
#endif

static void
tbuf_assert(const struct tbuf *b)
{
	(void)b;		/* arg used :-) */
	assert(tbuf_len(b) <= b->size);
}

struct tbuf *
tbuf_alloc(struct palloc_pool *pool)
{
	const size_t initial_size = 128 - sizeof(struct tbuf);
	struct tbuf *e = palloc(pool, sizeof(*e) + initial_size);
	e->len = 0;
	e->size = initial_size;
	e->data = (char *)e + sizeof(*e);
	e->pool = pool;
	poison(e->data, e->size);
	tbuf_assert(e);
	return e;
}

struct tbuf *
tbuf_alloc_fixed(struct palloc_pool *pool, void *data, u32 len)
{
	struct tbuf *e = palloc(pool, sizeof(*e));
	e->pool = NULL;
	e->len = 0;
	e->size = len;
	e->data = data;
	return e;
}

void __attribute__((regparm(2)))
tbuf_ensure_resize(struct tbuf *e, size_t required)
{
	tbuf_assert(e);
	assert(e->pool != NULL); /* attemp to resize fixed size tbuf */

	const size_t initial_size = MAX(e->size, 128 - sizeof(*e));
	size_t new_size = initial_size * 2;

	while (new_size - tbuf_len(e) < required)
		new_size *= 2;

	void *p = palloc(e->pool, new_size);

	poison(p, new_size);
	memcpy(p, e->data, e->len);
	poison(e->data, e->len);
	e->data = p;
	e->size = new_size;
	tbuf_assert(e);
}

struct tbuf *
tbuf_clone(struct palloc_pool *pool, const struct tbuf *orig)
{
	struct tbuf *clone = tbuf_alloc(pool);
	tbuf_assert(orig);
	tbuf_append(clone, orig->data, tbuf_len(orig));
	return clone;
}

void
tbuf_gc(struct palloc_pool *pool, void *ptr)
{
	struct tbuf *b = ptr;
	if (unlikely(b->size == 0))
		b->size = 128;
	void *data = palloc(pool, b->size);
	memcpy(data, b->data, b->len);
	b->data = data;
}

struct tbuf *
tbuf_split(struct tbuf *orig, size_t at)
{
	tbuf_assert(orig);
	assert(at <= tbuf_len(orig));

	struct tbuf *head = tbuf_alloc_fixed(orig->pool, orig->data, at);
	orig->data += at;
	orig->size -= at;
	orig->len -= at;
	head->len += at;
	return head;
}

void *
tbuf_peek(struct tbuf *b, size_t count)
{
	void *p = b->data;
	tbuf_assert(b);
	if (count <= tbuf_len(b)) {
		b->data += count;
		b->len -= count;
		b->size -= count;
		return p;
	}
	return NULL;
}

size_t
tbuf_reserve(struct tbuf *b, size_t count)
{
	tbuf_assert(b);
	tbuf_ensure(b, count);
	size_t offt = tbuf_len(b);
	b->len += count;
	return offt;
}

void
tbuf_reset(struct tbuf *b)
{
	tbuf_assert(b);
	poison(b->data, b->len);
	b->len = 0;
}

void
tbuf_append_field(struct tbuf *b, void *f)
{
	void *s = f;
	u32 size = LOAD_VARINT32(f);
	void *next = (u8 *)f + size;
	tbuf_append(b, s, next - s);
}

void
tbuf_ltrim(struct tbuf *b, size_t diff)
{
	tbuf_assert(b);
	assert(diff <= tbuf_len(b));

	b->data += diff;
	b->size -= diff;
	b->len -= diff;
}

void
tbuf_vprintf(struct tbuf *b, const char *format, va_list ap)
{
	int printed_len;
	size_t free_len = b->size - tbuf_len(b);
	va_list ap_copy;

	va_copy(ap_copy, ap);

	tbuf_assert(b);
	printed_len = vsnprintf(((char *)b->data) + tbuf_len(b), free_len, format, ap);

	/*
	 * if buffer too short, resize buffer and
	 * print it again
	 */
	if (free_len < printed_len + 1) {
		tbuf_ensure(b, printed_len + 1);
		free_len = b->size - tbuf_len(b);
		printed_len = vsnprintf(((char *)b->data) + tbuf_len(b), free_len, format, ap_copy);
	}

	b->len += printed_len;

	va_end(ap_copy);
}

void
tbuf_printf(struct tbuf *b, const char *format, ...)
{
	va_list args;

	va_start(args, format);
	tbuf_vprintf(b, format, args);
	va_end(args);
}

/* for debug printing */
char *
tbuf_to_hex(const struct tbuf *x)
{
	const unsigned char *data = x->data;
	size_t len = tbuf_len(x);
	char *out = palloc(x->pool, len * 3 + 1);
	out[len * 3] = 0;

	for (int i = 0; i < len; i++) {
		int c = *(data + i);
		sprintf(out + i * 3, "%02x ", c);
	}

	return out;
}
