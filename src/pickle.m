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

#import <util.h>
#import <fiber.h>
#import <object.h>
#import <tbuf.h>
#import <palloc.h>
#import <iproto.h>		/* for err codes */
#import <pickle.h>

#include <stdlib.h>

void __attribute__((noreturn))
buffer_too_short()
{
	iproto_raise(ERR_CODE_UNKNOWN_ERROR, "tbuf too short");
}

/* caller must ensure that there is space in target */
u8 *
save_varint32(u8 *target, u32 value)
{

	if (value >= (1 << 7)) {
		if (value >= (1 << 14)) {
			if (value >= (1 << 21)) {
				if (value >= (1 << 28))
					*(target++) = (u8)(value >> 28) | 0x80;
				*(target++) = (u8)(value >> 21) | 0x80;
			}
			*(target++) = (u8)((value >> 14) | 0x80);
		}
		*(target++) = (u8)((value >> 7) | 0x80);
	}
	*(target++) = (u8)((value) & 0x7F);

	return target;
}

inline static void
append_byte(struct tbuf *b, u8 byte)
{
	*((u8 *)b->end++) = byte;
	b->free--;
}

void
write_varint32(struct tbuf *b, u32 value)
{
	tbuf_ensure(b, 5);
	if (value >= (1 << 7)) {
		if (value >= (1 << 14)) {
			if (value >= (1 << 21)) {
				if (value >= (1 << 28))
					append_byte(b, (u8)(value >> 28) | 0x80);
				append_byte(b, (u8)(value >> 21) | 0x80);
			}
			append_byte(b, (u8)((value >> 14) | 0x80));
		}
		append_byte(b, (u8)((value >> 7) | 0x80));
	}
	append_byte(b, (u8)((value) & 0x7F));
}

#define read_u(bits)							\
	u##bits read_u##bits(struct tbuf *b)				\
	{								\
		if (unlikely(tbuf_len(b) < bits/8))			\
			buffer_too_short();				\
		u##bits r = *(u##bits *)b->ptr;				\
		b->ptr += bits/8;					\
		return r;						\
	}

read_u(8)
read_u(16)
read_u(32)
read_u(64)

u32
read_varint32(struct tbuf *buf)
{
	void *p = buf->ptr;
	u32 r = load_varint32(&p);

	if (unlikely(p - buf->ptr > 4))
	    iproto_raise(ERR_CODE_UNKNOWN_ERROR, "bad varint32");
	if (unlikely(p >= buf->end))
		buffer_too_short();

	buf->ptr = p;
	return r;
}

u32
pick_u32(void *data, void **rest)
{
	u32 *b = data;
	if (rest != NULL)
		*rest = b + 1;
	return *b;
}

void *
read_field(struct tbuf *buf)
{
	void *p = buf->ptr;
	u32 data_len = load_varint32(&buf->ptr);
	buf->ptr += data_len;
	if (unlikely(buf->ptr > buf->end)) {
		buf->ptr = p;
		buffer_too_short();
	}

	return p;
}

void
read_push_field(lua_State *L, struct tbuf *buf)
{
	void *p = buf->ptr;
	u32 data_len = load_varint32(&buf->ptr);

	if (unlikely(buf->ptr + data_len > buf->end)) {
		buf->ptr = p;
		buffer_too_short();
	} else {
		lua_pushlstring(L, buf->ptr, data_len);
		buf->ptr += data_len;
	}
}


void *
read_bytes(struct tbuf *buf, u32 data_len)
{
	if (unlikely(data_len > tbuf_len(buf)))
		buffer_too_short();

	void *p = buf->ptr;
	buf->ptr += data_len;
	return p;
}

size_t
varint32_sizeof(u32 value)
{
	int s = 1;
	while (value >= 1 << 7) {
		value >>= 7;
		s++;
	}
	return s;
}

u32
load_varint32(void **pp)
{
	u8 *p = *pp;
	u32 v = 0;
	do v = (v << 7) | (*p & 0x7f); while (*p++ & 0x80 && p - (u8 *)*pp < 5);
	*pp = p;
	return v;
}

register_source(S_INFO);
