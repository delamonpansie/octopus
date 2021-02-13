/*
 * Copyright (C) 2010-2014, 2016 Mail.RU
 * Copyright (C) 2010-2014, 2016 Yury Vostrikov
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

#ifndef PICKLE_H
#define PICKLE_H

#include <util.h>
#include <string.h>
#include <tbuf.h>

struct tbuf;

__attribute__((noreturn)) void tbuf_too_short();
static __attribute__((always_inline)) inline void
_read_must_have(struct tbuf *b, i32 n)
{
	if (unlikely(tbuf_len(b) < n)) {
		tbuf_too_short();
	}
}

void read_must_have(struct tbuf *b, i32 len);
void read_must_end(struct tbuf *b, const char *err);

#define read_u(bits)							\
	static inline u##bits read_u##bits(struct tbuf *b)		\
	{								\
		_read_must_have(b, bits/8);				\
		u##bits r = *(u##bits *)b->ptr;				\
		b->ptr += bits/8;					\
		return r;						\
	}

#define read_i(bits)							\
	static inline i##bits read_i##bits(struct tbuf *b)		\
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

u32 read_varint32(struct tbuf *buf);
void *read_field(struct tbuf *buf);
void *read_bytes(struct tbuf *buf, u32 data_len);
void *read_ptr(struct tbuf *buf);
void read_to(struct tbuf *buf, void *p, u32 len);
static inline void _read_to(struct tbuf *buf, void *p, u32 len) {
	memcpy(p, read_bytes(buf, len), len);
}
#define read_into(buf, struct) _read_to((buf), (struct), sizeof(*(struct)))

u8 read_field_u8(struct tbuf *b);
u16 read_field_u16(struct tbuf *b);
u32 read_field_u32(struct tbuf *b);
u64 read_field_u64(struct tbuf *b);
i8 read_field_i8(struct tbuf *b);
i16 read_field_i16(struct tbuf *b);
i32 read_field_i32(struct tbuf *b);
i64 read_field_i64(struct tbuf *b);
struct tbuf* read_field_s(struct tbuf *b);

void write_i8(struct tbuf *b, i8 i);
void write_i16(struct tbuf *b, i16 i);
void write_i32(struct tbuf *b, i32 i);
void write_i64(struct tbuf *b, i64 i);

void write_varint32(struct tbuf *b, u32 value);
void write_field_i8(struct tbuf *b, i8 i);
void write_field_i16(struct tbuf *b, i16 i);
void write_field_i32(struct tbuf *b, i32 i);
void write_field_i64(struct tbuf *b, i64 i);
void write_field_s(struct tbuf *b, const u8* s, u32 l);

u32 pick_u32(void *data, void **rest);

size_t varint32_sizeof(u32);
u8 *save_varint32(u8 *target, u32 value);
u32 _load_varint32(void **data);
static inline u32 load_varint32(void **data)
{
	const u8* p = *data;
	if ((*p & 0x80) == 0) {
		(*data)++;
		return *p;
	} else {
		return _load_varint32(data);
	}
}

/* WARNING: this macro will decode BER intergers not larger than 2048383 */
#define LOAD_VARINT32(ptr) ({				\
	const unsigned char *p = (ptr);			\
	int v = *p & 0x7f;				\
	if (*p & 0x80) {				\
		v = (v << 7) | (*++p & 0x7f);		\
		if (*p & 0x80)				\
			v = (v << 7) | (*++p & 0x7f);	\
	}						\
	ptr = (typeof(ptr))(p + 1);			\
	v;						\
})

struct tlv {
	u16 tag;
	u32 len;
	u8 val[0];
} __attribute((packed));

static inline int
tlv_add(struct tbuf *buf, u16 tag)
{
	struct tlv tlv = { .tag = tag };
	int offt = buf->end - buf->ptr;
	tbuf_append(buf, &tlv, sizeof(tlv));
	return offt;
}

static inline void
tlv_end(struct tbuf *buf, int offt)
{
	struct tlv *tlv = buf->ptr + offt;
	tlv->len = buf->end - buf->ptr - offt - sizeof(struct tlv);
}

#endif
