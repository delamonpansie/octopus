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
#import <tbuf.h>

#include <third_party/luajit/src/luajit.h>

u8 *save_varint32(u8 *target, u32 value);
void write_varint32(struct tbuf *b, u32 value);

u8 read_u8(struct tbuf *b);
u16 read_u16(struct tbuf *b);
u32 read_u32(struct tbuf *b);
u64 read_u64(struct tbuf *b);

u32 read_varint32(struct tbuf *buf);
void *read_field(struct tbuf *buf);
void read_push_field(lua_State *L, struct tbuf *buf);
void *read_bytes(struct tbuf *buf, u32 data_len);

u32 pick_u32(void *data, void **rest);

size_t varint32_sizeof(u32);
u32 load_varint32(void **data);

#define LOAD_VARINT32(data) ({		\
	u8* __load_ptr = (data);	\
	(*__load_ptr & 0x80) == 0 ?	\
	((data)++, *__load_ptr) :	\
	load_varint32(&(data));	\
})
