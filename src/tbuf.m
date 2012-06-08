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

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lauxlib.h>

#include <string.h>
#include <stddef.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/socket.h>

#ifdef POISON
#  define TBUF_POISON
#endif

#if defined(TBUF_POISON) || defined(TBUF_PARANOIA)
#  define poison(ptr, len) memset((ptr), 'A', (len))
static void
tbuf_assert(const struct tbuf *b)
{
	(void)b;
	assert(tbuf_len(b) <= tbuf_size(b));
}
#else
#  define poison(ptr, len) (void)0
#  define tbuf_assert(b) (void)0
#endif


struct tbuf *
tbuf_alloc(struct palloc_pool *pool)
{
	const size_t initial_size = 128 - sizeof(struct tbuf);
	struct tbuf *e = palloc(pool, sizeof(*e) + initial_size);
	e->free = initial_size;
	e->ptr = e->end = (char *)e + sizeof(*e);
	e->pool = pool;
	poison(e->ptr, e->free);
	tbuf_assert(e);
	return e;
}

void __attribute__((regparm(2)))
tbuf_ensure_resize(struct tbuf *e, size_t required)
{
	tbuf_assert(e);
	assert(e->pool != NULL); /* attemp to resize fixed size tbuf */

	const size_t initial_size = MAX(tbuf_size(e), 128 - sizeof(*e));
	size_t new_size = initial_size * 2;

	while (new_size - tbuf_len(e) < required)
		new_size *= 2;

	void *p = palloc(e->pool, new_size);
	int len = tbuf_len(e);

	poison(p, new_size);
	memcpy(p, e->ptr, len);
	poison(e->ptr, len);
	e->ptr = p;
	e->end = p + len;
	e->free = new_size - len;
	tbuf_assert(e);
}

struct tbuf *
tbuf_clone(struct palloc_pool *pool, const struct tbuf *orig)
{
	struct tbuf *clone = tbuf_alloc(pool);
	tbuf_assert(orig);
	tbuf_append(clone, orig->ptr, tbuf_len(orig));
	return clone;
}

void
tbuf_gc(struct palloc_pool *pool, void *ptr)
{
	struct tbuf *b = ptr;
	tbuf_assert(b);
	if (unlikely(tbuf_size(b) == 0)) {
		b->free = 128;
		b->ptr = b->end = palloc(pool, b->free);
	} else {
		void *data = palloc(pool, tbuf_size(b));
		int len = tbuf_len(b);
		memcpy(data, b->ptr, len);
		b->ptr = data;
		b->end = data + len;
	}
}

struct tbuf *
tbuf_split(struct tbuf *orig, size_t at)
{
	tbuf_assert(orig);
	assert(at <= tbuf_len(orig));

	struct tbuf *h = palloc(orig->pool, sizeof(*h));
	h->ptr = orig->ptr;
	h->end = orig->ptr + at;
	h->free = 0;
	h->pool = orig->pool;

	tbuf_ltrim(orig, at);
	return h;
}

void
tbuf_reset(struct tbuf *b)
{
	tbuf_assert(b);
	poison(b->ptr, tbuf_len(b));
	b->end = b->ptr;
	b->free += tbuf_len(b);
}

void
tbuf_append(struct tbuf *b, const void *data, size_t len)
{
	tbuf_assert(b);
	if (likely(data != NULL)) {
		tbuf_ensure(b, len + 1);
		memcpy(b->end, data, len);
		*(((char *)b->end) + len) = '\0';
	}
	b->end += len;
	b->free -= len;
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

	b->ptr += diff;
}

void
tbuf_rtrim(struct tbuf *b, size_t diff)
{
	tbuf_assert(b);
	assert(diff <= tbuf_len(b));

	b->end -= diff;
	b->free += diff;
}

void
tbuf_vprintf(struct tbuf *b, const char *format, va_list ap)
{
	int printed_len;
	size_t free_len = tbuf_free(b);
	va_list ap_copy;

	va_copy(ap_copy, ap);

	tbuf_assert(b);
	printed_len = vsnprintf((char *)b->end, free_len, format, ap);

	/*
	 * if buffer too short, resize buffer and
	 * print it again
	 */
	if (free_len < printed_len + 1) {
		tbuf_ensure(b, printed_len + 1);
		free_len = tbuf_free(b);
		printed_len = vsnprintf((char *)b->end, free_len, format, ap_copy);
	}

	tbuf_append(b, NULL, printed_len);

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

ssize_t
tbuf_recv(struct tbuf *buf, int fd)
{
	ssize_t r = recv(fd, buf->end, tbuf_free(buf), 0);
	if (r > 0) {
		buf->end += r;
		buf->free -= r;
	}
	return r;
}

/* for debug printing */
char *
tbuf_to_hex(const struct tbuf *x)
{
	const unsigned char *data = x->ptr;
	size_t len = tbuf_len(x);
	char *out = palloc(x->pool, len * 3 + 1);
	out[len * 3] = 0;

	for (int i = 0; i < len; i++) {
		int c = *(data + i);
		sprintf(out + i * 3, "%02x ", c);
	}

	return out;
}
