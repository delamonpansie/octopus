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
	struct tbuf *e = palloc(pool, sizeof(*e));
	e->free = 16;
	e->ptr = palloc(pool, 16);
	e->end = e->ptr;
	e->pool = pool;
	return e;
}

void
tbuf_reserve_aux(struct tbuf *e, size_t required)
{
	tbuf_assert(e);
	assert(e->pool != NULL); /* attemp to resize fixed size tbuf */

	size_t size = tbuf_size(e);
	size_t len = tbuf_len(e);
	size_t req = required - e->free;
	size_t diff = size / 2;
	if (diff < req) {
		diff = req + required / 2;
	}
	assert(size + diff < 256 * 1024 * 1024);

	void *p = prealloc(e->pool, e->ptr, size, size + diff);

	e->ptr = p;
	e->end = p + len;
	e->free += diff;
	tbuf_assert(e);
}

void
tbuf_willneed(struct tbuf *e, size_t required)
{
	assert(tbuf_len(e) <= tbuf_size(e));
	if (unlikely(tbuf_free(e) < required))
		tbuf_reserve_aux(e, required);
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
		b->ptr = b->end = NULL;
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
	b->free += tbuf_len(b);
	b->end = b->ptr;
}

void
tbuf_reset_to(struct tbuf *b, size_t len)
{
	tbuf_assert(b);
	size_t oldlen = tbuf_len(b);
	assert(oldlen >= len);
	poison(b->ptr + len, oldlen - len);
	b->free += oldlen - len;
	b->end = b->ptr + len;
}

void
tbuf_append(struct tbuf *b, const void *data, size_t len)
{
	tbuf_assert(b);
	tbuf_reserve(b, len + 1);
	if (likely(data != NULL)) {
		memcpy(b->end, data, len);
		*(((char *)b->end) + len) = '\0';
	}
	b->end += len;
	b->free -= len;
}

void*
tbuf_expand(struct tbuf *b, size_t len)
{
	tbuf_assert(b);
	tbuf_reserve(b, len);
	void *res = b->end;
	b->end += len;
	b->free -= len;
	return res;
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
		tbuf_reserve(b, printed_len + 1);
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

static const char *hex = "0123456789ABCDEF";
void
tbuf_putx(struct tbuf *b, char c)
{
	tbuf_reserve(b, 3);
	char *end = b->end;
	*end = hex[(u8)c >> 4];
	*(end + 1) = hex[(u8)c & 0x0f];
	*(end + 2) = 0;
	b->end += 2;
	b->free -= 2;
}

void
tbuf_putxs(struct tbuf *b, const char *s, size_t len)
{
	tbuf_reserve(b, len*2+1);
	char *pos = b->end, *end = b->end + len*2;
	for (;pos != end; s++, pos+=2) {
		pos[0] = hex[(u8)(*s) >> 4];
		pos[1] = hex[(u8)(*s) & 0x0f];
	}
	*end = 0;
	b->end = end;
	b->free -= len*2;
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
	char *p = out;

	for (int i = 0; i < len; i++, p+=3) {
		u8 c = *(data + i);
		p[0] = hex[c>>4];
		p[1] = hex[c&15];
		p[2] = ' ';
	}
	p[0] = 0;

	return out;
}

static inline void
tbuf_putu_imp(struct tbuf *b, uint32_t u, bool zeropad)
{
	char d[] = "0000000000";
	uint32_t n;
	int p=9;
	do {
		n = u/10;
		d[p] = (u - n*10) + '0';
		u = n;
		p--;
	} while (u > 0);
	if (!zeropad)
		tbuf_append(b, d+p+1, 9-p);
	else
		tbuf_append(b, d+1, 9);
}

void
tbuf_putu(struct tbuf *b, uint32_t u)
{
	tbuf_putu_imp(b, u, false);
}

void
tbuf_puti(struct tbuf *b, int32_t i)
{
	if (i < 0) {
		tbuf_putc(b, '-');
		i = -i;
	}
	tbuf_putu_imp(b, (uint32_t)i, false);
}

void
tbuf_putul(struct tbuf *b, uint64_t u)
{
	const uint64_t e9 = (uint64_t)1e9;
	bool zeropad = false;
	if(u >= e9*e9) {
		uint64_t v = u/(e9*e9);
		u -= v*(e9*e9);
		tbuf_putu_imp(b, (uint32_t)v, false);
		zeropad = true;
	}
	if(u >= e9) {
		uint64_t v = u/e9;
		u -= v*e9;
		tbuf_putu_imp(b, (uint32_t)v, zeropad);
		zeropad = true;
	}
	tbuf_putu_imp(b, (uint32_t)u, zeropad);
}

void
tbuf_putl(struct tbuf *b, int64_t i)
{
	if (i < 0) {
		tbuf_putc(b, '-');
		i = -i;
	}
	tbuf_putul(b, (uint64_t)i);
}
