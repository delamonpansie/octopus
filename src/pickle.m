// vim: set sts=8 sw=8 noexpandtab:
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
#import <objc.h>
#import <tbuf.h>
#import <palloc.h>
#import <iproto.h>		/* for err codes */
#import <pickle.h>
#import <say.h>

#include <stdlib.h>

void __attribute__((noreturn))
tbuf_too_short()
{
	iproto_raise(ERR_CODE_UNKNOWN_ERROR, "tbuf too short");
}

static __attribute__((always_inline)) inline void
_read_must_have(struct tbuf *b, i32 n)
{
	if (unlikely(tbuf_len(b) < n)) {
		tbuf_too_short();
	}
}

void
read_must_have(struct tbuf *b, i32 n)
{
	_read_must_have(b, n);
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
		_read_must_have(b, bits/8);				\
		u##bits r = *(u##bits *)b->ptr;				\
		b->ptr += bits/8;					\
		return r;						\
	}

#define read_i(bits)							\
	i##bits read_i##bits(struct tbuf *b)				\
	{								\
		_read_must_have(b, bits/8);				\
		i##bits r = *(i##bits *)b->ptr;				\
		b->ptr += bits/8;					\
		return r;						\
	}

read_u(8)
read_u(16)
read_u(32)
read_u(64)
read_i(8)
read_i(16)
read_i(32)
read_i(64)

static u32
_safe_load_varint32(struct tbuf *buf)
{
	u8 *p = buf->ptr;
	u32 v = 0;
	for (;p < (u8*)buf->end && p - (u8*)buf->ptr < 5; v<<=7, p++) {
		v |= *p & 0x7f;
		if ((*p & 0x80) == 0) {
			buf->ptr = p + 1;
			return v;
		}
	}
	if (p == buf->end) {
		tbuf_too_short();
	}
	if (v > ((u32)0xffffffff >> 7) || ((*p & 0x80) != 0)) {
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "bad varint32");
	}
	v |= *p & 0x7f;
	buf->ptr = p + 1;
	return v;
}

static __attribute__((always_inline)) inline u32
safe_load_varint32(struct tbuf *buf)
{
	u8 *p = buf->ptr;
	if (buf->end > buf->ptr && (*p & 0x80) == 0) {
		buf->ptr++;
		return *p;
	} else {
		return _safe_load_varint32(buf);
	}
}

u32
read_varint32(struct tbuf *buf)
{
	return safe_load_varint32(buf);
}

void *
read_field(struct tbuf *buf)
{
	void *p = buf->ptr;
	u32 data_len = safe_load_varint32(buf);
	buf->ptr += data_len;
	if (unlikely(buf->ptr > buf->end)) {
		buf->ptr = p;
		tbuf_too_short();
	}

	return p;
}

void
read_push_field(lua_State *L, struct tbuf *buf)
{
	void *p = buf->ptr;
	u32 data_len = safe_load_varint32(buf);

	if (unlikely(buf->ptr + data_len > buf->end)) {
		buf->ptr = p;
		tbuf_too_short();
	} else {
		lua_pushlstring(L, buf->ptr, data_len);
		buf->ptr += data_len;
	}
}


void *
read_bytes(struct tbuf *buf, u32 data_len)
{
	_read_must_have(buf, data_len);
	void *p = buf->ptr;
	buf->ptr += data_len;
	return p;
}

void *
read_ptr(struct tbuf *buf)
{
	return *(void **)read_bytes(buf, sizeof(void *));
}

#define read_field_u(bits)						\
	u##bits read_field_u##bits(struct tbuf *b)			\
	{								\
		_read_must_have(b, bits/8 + 1);				\
		if (unlikely(*(u8*)b->ptr != bits/8))			\
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "bad field"); \
		u##bits r = *(u##bits *)(b->ptr + 1);			\
		b->ptr += bits/8 + 1;					\
		return r;						\
	}

#define read_field_i(bits)						\
	i##bits read_field_i##bits(struct tbuf *b)			\
	{								\
		_read_must_have(b, bits/8 + 1);				\
		if (unlikely(*(u8*)b->ptr != bits/8))			\
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "bad field"); \
		i##bits r = *(i##bits *)(b->ptr + 1);			\
		b->ptr += bits/8 + 1;					\
		return r;						\
	}

read_field_u(8)
read_field_u(16)
read_field_u(32)
read_field_u(64)
read_field_i(8)
read_field_i(16)
read_field_i(32)
read_field_i(64)

struct tbuf *
read_field_s(struct tbuf *b)
{
	void *p = b->ptr;
	u32 len = safe_load_varint32(b);
	if (b->end - b->ptr < len) {
		b->ptr = p;
		tbuf_too_short();
	}
	return tbuf_split(b, len);
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
_load_varint32(void **pp)
{
	u8 *p = *pp;
	u32 v = 0;
	do v = (v << 7) | (*p & 0x7f); while (*p++ & 0x80 && p - (u8 *)*pp < 5);
	*pp = p;
	return v;
}

#define write_i(bits)							\
	void write_i##bits(struct tbuf *b, i##bits i)			\
	{								\
		tbuf_ensure(b, bits/8);					\
		*(i##bits *)b->end = i;					\
		b->end += bits/8;					\
		b->free -= bits/8;					\
	}

#define write_field_i(bits)						\
	void write_field_i##bits(struct tbuf *b, i##bits i)		\
	{								\
		tbuf_ensure(b, bits/8 + 1);				\
		*(u8*)b->end = bits/8;					\
		*(i##bits *)(b->end+1) = i;				\
		b->end += bits/8 + 1;					\
		b->free -= bits/8 + 1;					\
	}

write_i(8)
write_i(16)
write_i(32)
write_i(64)
write_field_i(8)
write_field_i(16)
write_field_i(32)
write_field_i(64)

void
write_field_s(struct tbuf *b, const u8* s, u32 l)
{
	write_varint32(b, l);
	tbuf_append(b, s, l);
}

register_source();
