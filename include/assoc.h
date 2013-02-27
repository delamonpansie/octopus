/*
 * Copyright (C) 2011 Mail.RU
 * Copyright (C) 2011 Yuriy Vostrikov
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
#import <pickle.h>

#include <stdlib.h>

typedef void* ptr_t;
typedef void* lstr;
typedef void* cstr;

/* All hashes use same layout
   {
      mh_val_t obj;
      mh_key_t key;
   }
*/

#define mh_name _i32
#define mh_key_t i32
#define mh_val_t ptr_t
#define mh_hash(a) ({ (a); })
#define mh_eq(a, b) ({ *(mh_key_t *)((a) + sizeof(mh_val_t)) == (b); })
#include <mhash.h>

#define mh_name _i64
#define mh_key_t i64
#define mh_val_t ptr_t
#define mh_hash(a) ({ (uint32_t)((a)>>33^(a)^(a)<<11); })
#define mh_eq(a, b) ({ *(mh_key_t *)((a) + sizeof(mh_val_t)) == (b); })
#include <mhash.h>

static inline int lstrcmp(void *a, void *b)
{
	unsigned int al, bl;
	u8 ac, bc;
	int r;

	ac = *(u8 *)a;
	bc = *(u8 *)b;

	if (((ac & 0x80) == 0 || (bc & 0x80) == 0) && ac != bc) {
		r = ac - bc;
	} else {
		al = LOAD_VARINT32(a);
		bl = LOAD_VARINT32(b);

		if (al != bl)
			r = al - bl;
		else
			r = memcmp(a, b, al);
	}
	return r;
}

#include <third_party/murmur_hash2.c>
#define mh_name _lstr
#define mh_key_t lstr
#define mh_val_t ptr_t
#define mh_hash(key) ({ void *_k = (key); unsigned l = LOAD_VARINT32(_k); MurmurHash2(_k, l, 13); })
#define mh_eq(a, b) ({ lstrcmp(*(mh_key_t *)((a) + sizeof(mh_val_t)), (b)) == 0; })
#include <mhash.h>


#define mh_name _cstr
#define mh_key_t cstr
#define mh_val_t ptr_t
#define mh_hash(key) ({ MurmurHash2((key), strlen(key), 13); })
#define mh_eq(a, b) ({ strcmp(*(mh_key_t *)((a) + sizeof(mh_val_t)), (b)) == 0; })
#include <mhash.h>
