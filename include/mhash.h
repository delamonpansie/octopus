/*
 * Copyright (C) 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

/* The MIT License

   Copyright (c) 2008, by Attractive Chaos <attractivechaos@aol.co.uk>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/


/* Examples:

#define mh_name _x
#define mh_key_t int
#define mh_val_t char
#define MH_STATIC
#include "mhash.h"

int main() {
	int ret, is_missing;
	uint32_t k;
	struct mh_x_t *h = mh_x_init(NULL);
	k = mh_x_iput(h, 5, &ret);
	if (!ret)
		mh_x_del(h, k);
	*mh_x_pvalue(h, k) = 10;
	k = mh_x_get(h, 10);
	is_missing = (k == mh_end(h));
	k = mh_x_get(h, 5);
	mh_x_del(h, k);

	for (k = mh_begin(h); k != mh_end(h); ++k)
		if (mh_x_slot_occupied(h, k))
			*mh_x_pvalue(h, k) = 1;

	mh_x_destroy(h);
	return 0;
}


// map: int -> int
#define mh_name _intmap
#define mh_key_t int
#define mh_val_t int
#include "m2hash.h"

// map2: int -> int, exisiting slot struct
#define mh_name _intmap2
struct intmap2_slot {
	int val;
	int val2;
	int key;
};
#define mh_slot_t intmap2_slot
#define mh_slot_key(slot) (slot)->key
#define mh_slot_value(slot) (slot)->val
#include "m2hash.h"

// set: int
#define mh_name _intset
#define mh_key_t int
#include "m2hash.h"


// string set with inline bitmap: low 2 bits of pointer used for hash housekeeping
#include "murmur_hash2.c"
#define mh_name _str
#define mh_slot_t cstr_slot
struct cstr_slot {
	union {
		const char *key;
		uintptr_t bits;
	};
	//int value;
};
#define mh_node_size sizeof(struct cstr_slot)

#define mh_hash(h, key) ({ MurmurHash2((key), strlen((key)), 13); })
#define mh_eq(h, a, b) ({ strcmp((a), (b)) == 0; })

# define mh_exist(h, i)		(h->slots[i].bits & 1)
# define mh_setfree(h, i)	h->slots[i].bits &= ~1
# define mh_setexist(h, i)	h->slots[i].bits |= 1
# define mh_dirty(h, i)		(h->slots[i].bits & 2)
# define mh_setdirty(h, i)	h->slots[i].bits |= 2

#define mh_slot_key(slot) ((const char *)((slot)->bits >> 2))
#define mh_slot_set_key(slot, key) (slot)->bits = ((slot)->bits & 3UL) | ((uintptr_t)key << 2)
#define mh_slot_copy(h, new, old) (new)->bits = (((old)->bits & ~3UL) | 1UL)
#include "m2hash.h"
*/

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>

#ifndef MH_HELPER_MACRO
#define MH_HELPER_MACRO
#define mh_cat(a, b) mh##a##_##b
#define mh_ecat(a, b) mh_cat(a, b)
#define _mh(x) mh_ecat(mh_name, x)
#define mh_unlikely(x)  __builtin_expect((x),0)
#endif

#ifndef MH_INCREMENTAL_RESIZE
#define MH_INCREMENTAL_RESIZE 0
/* incremental resize interacts badly with slot mutation and should be enabled explicitly */
#endif

#if MH_INCREMENTAL_RESIZE
# define MH_INCREMENTAL_CONST const
#else
# define MH_INCREMENTAL_CONST
#endif

#ifdef MH_STATIC
#define MH_SOURCE
#define MH_DECL static inline
#else
#define MH_DECL
#endif

#ifndef mh_slot_t
# ifndef mh_key_t
#  error Either mh_slot_t or mh_key_t must be defined
# endif
#define mh_slot_t struct _mh(slot)
struct _mh(slot) {
#ifdef mh_val_t
	mh_val_t val;
#define mh_slot_val(slot) (slot)->val
#endif
	mh_key_t key;
#ifndef mh_slot_key
# define mh_slot_key(h, slot) 	(slot)->key
#endif
} __attribute__((packed));
#else
# ifdef mh_key_t
#  error Either mh_slot_t or mh_key_t must be defined
# endif
# ifndef mh_slot_key
#  error mh_slot_key must be defined if mh_slot_t defined
# endif
# define mh_key_t typeof(mh_slot_key(NULL, (mh_slot_t *)0))
# ifdef mh_slot_val
#  define mh_val_t typeof(mh_slot_val((mh_slot_t *)0))
# endif
#endif

#ifndef mh_slot_size
#  define mh_slot_size(h) sizeof(mh_slot_t)
#endif

/* value of mh_hash(h, a) should be unsigned */
#ifndef mh_hash
# define mh_hash(h, a) mh_def_hash(a)
#endif

#ifndef mh_eq
# define mh_eq(h, a, b) mh_def_eq(a, b)
#endif

#ifndef mh_slot
# define mh_slot(h, i)		((h)->slots + i)
#endif

#ifndef mh_slot_key_eq
# define mh_slot_key_eq(h, i, key) mh_eq(h, mh_slot_key(h, mh_slot(h, i)), key)
#endif

#ifndef mh_slot_set_key
# define mh_slot_set_key(h, slot, key) memcpy(&mh_slot_key(h, slot), &(key), sizeof(mh_key_t))
#endif

#ifndef mh_slot_copy
# define mh_slot_copy(h, a, b) memcpy(a, b, mh_slot_size(h));
#endif

#ifndef mh_exist
# if SIZEOF_VOID_P == 8
/* assumes sizeof(void *) == sizeof(usigned long) */
# define mh_bitmap_t uint64_t
# define hash_t uint64_t
#  define mh_exist(h, i)	(h->bitmap[i >> 5] & (1UL << (i & 0x1f)))
#  define mh_setfree(h, i)	h->bitmap[i >> 5] &= ~(1UL << (i & 0x1f))
#  define mh_setexist(h, i)	h->bitmap[i >> 5] |= (1UL << (i & 0x1f))
#  define mh_dirty(h, i)	((h->bitmap[i >> 5] >> 0x20) & (1UL << (i & 0x1f)))
#  define mh_setdirty(h, i)	h->bitmap[i >> 5] |= (0x100000000UL << (i & 0x1f))
# else
# define hash_t uint32_t
# define mh_bitmap_t uint32_t
#  define mh_exist(h, i)	(h->bitmap[i >> 4] & (1UL << (i & 0xf)))
#  define mh_setfree(h, i)	h->bitmap[i >> 4] &= ~(1UL << (i & 0xf))
#  define mh_setexist(h, i)	h->bitmap[i >> 4] |= (1UL << (i & 0xf))
#  define mh_dirty(h, i)	((h->bitmap[i >> 4] >> 0x10) & (1UL << (i & 0xf)))
#  define mh_setdirty(h, i)	h->bitmap[i >> 4] |= (0x10000UL << (i & 0xf))
# endif
#endif

#ifndef __ac_HASH_PRIME_SIZE
static const uint32_t __ac_prime_list[] = {
	3ul,		11ul,		23ul,		53ul,
	97ul,		193ul,		389ul,		769ul,
	1543ul,		3079ul,		6151ul,		12289ul,
	24593ul,	49157ul,	98317ul,	196613ul,
	393241ul,	786433ul,	1572869ul,	3145739ul,
	6291469ul,	12582917ul,	25165843ul,	50331653ul,
	100663319ul,	201326611ul,	402653189ul,	805306457ul,
	1610612741ul
};
#define __ac_HASH_PRIME_SIZE (sizeof(__ac_prime_list)/sizeof(__ac_prime_list[0]))
#endif

#define mhash_t _mh(t)
struct _mh(t) {
	mh_slot_t *slots;
#ifdef mh_bitmap_t
	mh_bitmap_t *bitmap;
#endif
	uint32_t node_size;
	uint32_t n_buckets, n_occupied, size, upper_bound;

	uint32_t resize_position, resize_pending_put, resize_batch;
	struct mhash_t *shadow;
	void *(*realloc)(void *, size_t);
#ifdef mh_arg_t
	mh_arg_t arg;
#endif
};

/* public api */
MH_DECL struct mhash_t * _mh(init)(void *(*custom_realloc)(void *, size_t));
MH_DECL void _mh(initialize)(struct mhash_t *h);
MH_DECL void _mh(destroy)(struct mhash_t *h);
MH_DECL void _mh(destruct)(struct mhash_t *h); /* doesn't free hash itself */
MH_DECL void _mh(clear)(struct mhash_t *h);
MH_DECL size_t _mh(bytes)(struct mhash_t *h);
#define mh_size(h)		({ (h)->size; 		})
#define mh_begin(h)		({ 0;	})
#define mh_end(h)		({ (h)->n_buckets;	})
#define mh_foreach(name, h, x)	for (int x = 0; x < (h)->n_buckets; x++) if (mh_ecat(name, slot_occupied)(h, x))

/* basic */
static inline uint32_t _mh(get)(const struct mhash_t *h, const mh_key_t key);
/* it's safe (and fast) to set value via pvalue() pointer right after iput():
   uint32_t x = mh_name_iput(h, new_key, NULL);
   *mh_pvalue(h, x) = new_value;
 */
static inline uint32_t _mh(iput)(struct mhash_t *h, const mh_key_t key, int *ret);
static inline void _mh(del)(struct mhash_t *h, uint32_t x);
static inline int _mh(remove)(struct mhash_t *h, mh_key_t key);
static inline int _mh(exist)(struct mhash_t *h, mh_key_t key);

/*  slot */
static inline uint32_t _mh(sget)(const struct mhash_t *h, const mh_slot_t *slot);
static inline int _mh(sput)(struct mhash_t *h, const mh_slot_t *slot);
static inline int _mh(sremove)(struct mhash_t *h, const mh_slot_t *slot);

/* kv */
static inline mh_key_t _mh(key)(struct mhash_t *h, uint32_t x);
#ifdef mh_val_t
static inline int _mh(put)(struct mhash_t *h, const mh_key_t key, mh_val_t val, mh_val_t *prev_val);
/* as long as incremental resize is disabled and value is simple scalar (no mh_slot_set_val macro),
   it can be mutated via pvalue() pointer:

   uint32_t x = mh_name_get(h, key);
   if (x != mh_end(h))
	*mh_name_pvalue(h, x) = new_value;

   otherwise set_value() must be used:
   uint32_t x = mh_name_get(h, key);
   if (x != mh_end(h))
	mh_name_set_value(h, x, new_value);
*/
static inline void _mh(set_value)(struct mhash_t *h, uint32_t x, mh_val_t val);
static inline mh_val_t _mh(value)(struct mhash_t *h, uint32_t x);
static inline MH_INCREMENTAL_CONST mh_val_t * _mh(pvalue)(struct mhash_t *h, uint32_t x);

#endif

/* internal api */
static inline mh_slot_t * _mh(slot)(const struct mhash_t *h, uint32_t x) { return mh_slot(h, x); }
MH_DECL void _mh(slot_copy)(struct mhash_t *d, uint32_t dx, const mh_slot_t *source);
MH_DECL void _mh(slot_move)(struct mhash_t *h, uint32_t dx, uint32_t sx);
MH_DECL void _mh(resize_step)(struct mhash_t *h);
MH_DECL void _mh(start_resize)(struct mhash_t *h, uint32_t buckets);

#define mh_malloc(h, size) (h)->realloc(NULL, (size))
#define mh_calloc(h, nmemb, size) ({			\
	size_t __size = (size) * (nmemb);		\
	void *__ptr = (h)->realloc(NULL, __size);	\
	memset(__ptr, 0, __size);			\
	__ptr; })
#define mh_free(h, ptr) (h)->realloc((ptr), 0)

#ifdef MH_DEBUG
MH_DECL void _mh(dump)(struct mhash_t *h);
#endif


#ifndef mh_def_hash
static inline unsigned mh_u64_hash(uint64_t kk) { return  (kk >> 34) ^ ((kk  >> 17) & 0xffff8000) ^ kk; }
//-----------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

static inline unsigned int mh_MurmurHash2 ( const void * key, int len, unsigned int seed )
{
	// 'm' and 'r' are mixing constants generated offline.
	// They're not really 'magic', they just happen to work well.

	const unsigned int m = 0x5bd1e995;
	const int r = 24;

	// Initialize the hash to a 'random' value

	unsigned int h = seed ^ len;

	// Mix 4 bytes at a time into the hash

	const unsigned char * data = (const unsigned char *)key;

	while(len >= 4)
	{
		unsigned int k = *(unsigned int *)data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}

	// Handle the last few bytes of the input array

	switch(len)
	{
	case 3: h ^= data[2] << 16;
	case 2: h ^= data[1] << 8;
	case 1: h ^= data[0];
	        h *= m;
	};

	// Do a few final mixes of the hash to ensure the last few
	// bytes are well-incorporated.

	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}

static inline unsigned mh_str_hash(const char *kk) { return  mh_MurmurHash2(kk, strlen(kk), 13); }
#define mh_def_hash(key) ({						\
	unsigned ret;							\
	if (__builtin_types_compatible_p(mh_key_t, uint32_t) ||		\
	    __builtin_types_compatible_p(mh_key_t, int32_t))		\
		ret = (key);						\
	else if (__builtin_types_compatible_p(mh_key_t, uint64_t) ||	\
		 __builtin_types_compatible_p(mh_key_t, int64_t))	\
		ret = mh_u64_hash(key);					\
	else if (__builtin_types_compatible_p(mh_key_t, char *))	\
		ret = mh_str_hash((const char *)(uintptr_t)key);	\
	else								\
		abort();						\
	ret;								\
})

#define mh_def_eq(a, b) ({						\
	int ret;							\
	if (__builtin_types_compatible_p(mh_key_t, uint32_t) ||		\
	    __builtin_types_compatible_p(mh_key_t, int32_t) ||		\
	    __builtin_types_compatible_p(mh_key_t, uint64_t) ||		\
	    __builtin_types_compatible_p(mh_key_t, int64_t))		\
		ret = (a) == (b);					\
	else if (__builtin_types_compatible_p(mh_key_t, char *))	\
		ret = !strcmp((const char *)(uintptr_t)(a), (const char *)(uintptr_t)(b)); \
	else								\
		abort();						\
	ret;								\
})

#endif

static inline uint32_t
_mh(get)(const struct mhash_t *h, const mh_key_t key)
{
	int inc, i;
	unsigned n_buckets = h->n_buckets;
	unsigned k = mh_hash(h, key);
	i = k % n_buckets;
	inc = 1 + k % (n_buckets - 1);
	for (;;) {
		if (mh_exist(h, i) && mh_slot_key_eq(h, i, key))
			return i;

		if (!mh_dirty(h, i))
			return h->n_buckets;

		if ((i -= inc) < 0)
			i += h->n_buckets;
	}
}

static inline uint32_t
_mh(mark)(struct mhash_t *h, const mh_key_t key)
{
	int i, inc, p = -1;
	unsigned n_buckets = h->n_buckets;
	unsigned k = mh_hash(h, key);
	i = k % n_buckets;
	inc = 1 + k % (n_buckets - 1);

	do {
		if (mh_exist(h, i)) {
			if (mh_slot_key_eq(h, i, key))
				return i;
			else
				mh_setdirty(h, i);
		} else {
			if (!mh_dirty(h, i)) {
				h->n_occupied++;
				return i;
			} else {
				p = i;
			}
		}

		if ((i -= inc) < 0)
			i += n_buckets;
	} while (p < 0);

	for (;;) {
		if (!mh_exist(h, i)) {
			if (!mh_dirty(h, i)) {
				h->n_occupied++;
				return p;
			}
		} else {
			if (mh_slot_key_eq(h, i, key))
				return i;
		}
		if ((i -= inc) < 0)
			i += n_buckets;
	}
}


static inline uint32_t
_mh(iput)(struct mhash_t *h, const mh_key_t key, int *ret)
{
#if MH_INCREMENTAL_RESIZE
	if (mh_unlikely(h->resize_position))
		_mh(resize_step)(h);
	else
#endif
	if (mh_unlikely(h->n_occupied >= h->upper_bound))
		_mh(start_resize)(h, 0);

	uint32_t x = _mh(mark)(h, key);
#if MH_INCREMENTAL_RESIZE
	if (x < h->resize_position)
		h->resize_pending_put = x;
#endif
	int exist = mh_exist(h, x);
	if (!exist) {
		mh_setexist(h, x);
		h->size++;

		mh_slot_set_key(h, mh_slot(h, x), key);
	}

	if (ret)
		*ret = !exist;
	return x;
}

static inline void
_mh(del)(struct mhash_t *h, uint32_t x)
{
	mh_setfree(h, x);
	h->size--;
	if (!mh_dirty(h, x))
		h->n_occupied--;

#if MH_INCREMENTAL_RESIZE
	if (mh_unlikely(h->resize_position)) {
		if (x < h->resize_position) {
			mh_key_t key = mh_slot_key(h, mh_slot(h, x)); /* mh_setfree() MUST keep key valid */
			struct mhash_t *s = h->shadow;
			uint32_t y = _mh(get)(s, key);

			if (y != s->n_buckets)
				_mh(del)(s, y);

			if (h->resize_pending_put == x)
				h->resize_pending_put = -1;

			_mh(resize_step)(h);
		}
	}
#endif
}

static inline int
_mh(remove)(struct mhash_t *h, mh_key_t key)
{
	uint32_t x = _mh(get)(h, key);
	if (x != h->n_buckets)
		_mh(del)(h, x);
	return x != h->n_buckets;
}

/* slot variants */
static inline uint32_t
_mh(sget)(const struct mhash_t *h, const mh_slot_t *slot)
{
	return _mh(get)(h, mh_slot_key(h, slot));
}

static inline int
_mh(sput)(struct mhash_t *h, const mh_slot_t *slot)
{
#if MH_INCREMENTAL_RESIZE
	if (mh_unlikely(h->resize_position))
		_mh(resize_step)(h);
	else
#endif
	if (mh_unlikely(h->n_occupied >= h->upper_bound))
		_mh(start_resize)(h, 0);

	uint32_t x = _mh(mark)(h, mh_slot_key(h, slot));
#if MH_INCREMENTAL_RESIZE
	if (x < h->resize_position)
		h->resize_pending_put = x;
#endif
	int exist = mh_exist(h, x);
	if (!exist)
		h->size++; /* exists bit will be set by slot_copy() */

	_mh(slot_copy)(h, x, slot); /* always copy: overwrite old slot val if exists */

	return !exist;
}

static inline int
_mh(sremove)(struct mhash_t *h, const mh_slot_t *slot)
{
	uint32_t x = _mh(get)(h, mh_slot_key(h, slot));
	if (x != h->n_buckets)
		_mh(del)(h, x);
	return x != h->n_buckets;
}

static inline mh_key_t _mh(key)(struct mhash_t *h, uint32_t x)
{
	return mh_slot_key(h, mh_slot(h, x));
}

#ifdef mh_val_t
static inline int
_mh(put)(struct mhash_t *h, const mh_key_t key, mh_val_t val, mh_val_t *prev_val)
{

	int ret;
	uint32_t x = _mh(iput)(h, key, &ret);

	mh_slot_t *slot = mh_slot(h, x);
	if (!ret && prev_val)
		*prev_val = mh_slot_val(slot);

#ifndef mh_slot_set_val
	memcpy(&mh_slot_val(slot), &(val), sizeof(mh_val_t));
#else
	mh_slot_set_val(slot, val);
#endif
	return ret;
}

static inline void
_mh(set_value)(struct mhash_t *h, uint32_t x, mh_val_t val)
{
#if MH_INCREMENTAL_RESIZE
	if (mh_unlikely(h->resize_position))
		_mh(resize_step)(h);
#endif
	mh_slot_t *slot = mh_slot(h, x);
#ifndef mh_slot_set_val
	memcpy(&mh_slot_val(slot), &(val), sizeof(mh_val_t));
#else
	mh_slot_set_val(slot, val);
#endif

#if MH_INCREMENTAL_RESIZE
	if (x < h->resize_position)
		h->resize_pending_put = x;
#endif
}

static inline mh_val_t
_mh(value)(struct mhash_t *h, uint32_t x)
{
	return mh_slot_val(mh_slot(h, x));
}

static inline MH_INCREMENTAL_CONST mh_val_t *
_mh(pvalue)(struct mhash_t *h, uint32_t x)
{
	return (MH_INCREMENTAL_CONST mh_val_t*)&mh_slot_val(mh_slot(h, x));
}

#endif

static inline int
_mh(exist)(struct mhash_t *h, mh_key_t key)
{
	u_int32_t k;

	k = _mh(get)(h, key);
	return (k != mh_end(h));
}


static inline int
_mh(slot_occupied)(struct mhash_t *h, uint32_t x)
{
	return mh_exist(h, x);
}

#ifdef MH_SOURCE

#define load_factor 0.7

MH_DECL struct mhash_t *
_mh(init)(void *(*custom_realloc)(void *, size_t))
{
	custom_realloc = custom_realloc ?: realloc;
	struct mhash_t *h = custom_realloc(NULL, sizeof(*h));
	memset(h, 0, sizeof(*h));
	h->realloc = custom_realloc;
	_mh(initialize)(h);
	return h;
}

MH_DECL void
_mh(initialize)(struct mhash_t *h)
{
	h->realloc = h->realloc ?: realloc;
	h->node_size = h->node_size ?: sizeof(mh_slot_t);

	h->shadow = mh_calloc(h, 1, sizeof(*h));
	h->n_buckets = h->n_buckets ?: 3;

	h->slots = mh_calloc(h, h->n_buckets, mh_slot_size(h));
#ifdef mh_bitmap_t
	h->bitmap = mh_calloc(h, 1 + h->n_buckets / (4 * sizeof(*h->bitmap)), sizeof(*h->bitmap)); /* 4 maps per char */
#endif
	h->upper_bound = h->n_buckets * load_factor;
}

MH_DECL void
_mh(slot_copy)(struct mhash_t *d, uint32_t dx, const mh_slot_t *source)
{
	mh_slot_copy(d, mh_slot(d, dx), source);
#ifdef mh_bitmap_t
	/* mh_slot_copy must set exist bit for inline bitmaps */
	mh_setexist(d, dx);
#endif
}

MH_DECL void
_mh(slot_move)(struct mhash_t *h, uint32_t dx, uint32_t sx)
{
	_mh(slot_copy)(h, dx, mh_slot(h, sx));
	mh_setfree(h, sx);
}

static inline void
_mh(slot_copy_to_shadow)(struct mhash_t *h, uint32_t o)
{
	struct mhash_t *s = h->shadow;
	if (!mh_exist(h, o))
		return;

	mh_slot_t *slot = mh_slot(h, o);
	uint32_t n = _mh(mark)(s, mh_slot_key(s, slot));
	if (!mh_exist(s, n))
		s->size++;
	_mh(slot_copy)(s, n, slot);
}

MH_DECL void
_mh(resize_step)(struct mhash_t *h)
{
	struct mhash_t *s = h->shadow;
	uint32_t start = h->resize_position,
		   end = h->n_buckets;

#if MH_INCREMENTAL_RESIZE
	if (h->resize_pending_put != -1) {
		_mh(slot_copy_to_shadow)(h, h->resize_pending_put);
		h->resize_pending_put = -1;
	}

	uint32_t batch_end = h->resize_position + h->resize_batch;
	if (batch_end < end)
		end = batch_end;

	h->resize_position += h->resize_batch;
#endif

	uint32_t o;
	for (o = start; o < end; o++)
		_mh(slot_copy_to_shadow)(h, o);

	if (end == h->n_buckets) {
		mh_free(h, h->slots);
#ifdef mh_bitmap_t
		mh_free(h, h->bitmap);
#endif
		assert(s->size == h->size);
		memcpy(h, s, sizeof(*h));
		memset(s, 0, sizeof(*s));
	}
}

MH_DECL void
_mh(start_resize)(struct mhash_t *h, uint32_t want_size)
{
	if (h->resize_position)
		return;
	struct mhash_t *s = h->shadow;
	uint32_t n_buckets, upper_bound;
	int k = 0;

	if (h->size > want_size) want_size = h->size;
	n_buckets = want_size / (load_factor * 0.85) + 1;
	assert(n_buckets > want_size);

	while(k < __ac_HASH_PRIME_SIZE && __ac_prime_list[k] <= n_buckets)
		k++;

	if (k < __ac_HASH_PRIME_SIZE) {
		n_buckets = __ac_prime_list[k];
		upper_bound = n_buckets * load_factor;
	} else if (__ac_prime_list[k-1] > want_size) {
		n_buckets = __ac_prime_list[k-1];
		upper_bound = want_size + (n_buckets - want_size) / 2;
	} else {
		abort();
	}
#if MH_INCREMENTAL_RESIZE
	h->resize_batch = h->n_buckets / (256 * 1024);
	if (h->resize_batch < 256) /* minimum resize_batch is 3 */
		h->resize_batch = 256;
	h->resize_pending_put = -1;
#endif
	memcpy(s, h, sizeof(*h));
	s->resize_position = 0;
	s->n_buckets = __ac_prime_list[k];
	s->upper_bound = s->n_buckets * load_factor;
	s->n_occupied = 0;
	s->size = 0;
#ifdef mh_bitmap_t
	s->slots = mh_malloc(h, (size_t)s->n_buckets * mh_slot_size(h));
	s->bitmap = mh_calloc(h, 1 + s->n_buckets / (4 * sizeof(*s->bitmap)), sizeof(*s->bitmap)); /* 4 maps per char */
#else
	s->slots = mh_calloc(h, s->n_buckets, mh_slot_size(h));
#endif

	_mh(resize_step)(h);
}

MH_DECL size_t
_mh(bytes)(struct mhash_t *h)
{
	return h->resize_position ? _mh(bytes)(h->shadow) : 0 +
		sizeof(*h) +
		(size_t)h->n_buckets * mh_slot_size(h) +
		((size_t)h->n_buckets / 16 + 1) *  sizeof(uint32_t);

}

MH_DECL void
_mh(clear)(struct mhash_t *h)
{
	mh_free(h, h->slots);
#ifdef mh_bitmap_t
	mh_free(h, h->bitmap);
#endif
	h->n_buckets = 3;
	h->upper_bound = h->n_buckets * load_factor;
#ifdef mh_bitmap_t
	h->slots = mh_malloc(h, (size_t)h->n_buckets * mh_slot_size(h));
	h->bitmap = mh_calloc(h, h->n_buckets / 16 + 1, sizeof(uint32_t));
#else
	h->slots = mh_calloc(h, h->n_buckets, mh_slot_size(h));
#endif
}

MH_DECL void
_mh(destruct)(struct mhash_t *h)
{
#ifdef MH_INCREMENTAL_RESIZE
	if (h->shadow->slots) {
		mh_free(h, h->shadow->slots);
#ifdef mh_bitmap_t
		mh_free(h, h->shadow->bitmap);
#endif
	}
#endif
	mh_free(h, h->shadow);
#ifdef mh_bitmap_t
	mh_free(h, h->bitmap);
#endif
	mh_free(h, h->slots);
}

MH_DECL void
_mh(destroy)(struct mhash_t *h)
{
	_mh(destruct)(h);
	mh_free(h, h);
}

#ifdef MH_DEBUG
#include <stdio.h>
MH_DECL void
_mh(dump)(struct mhash_t *h)
{
	printf("slots:\n");
	int k = 0;
	for(int i = 0; i < h->n_buckets; i++) {
		if (mh_dirty(h, i) || mh_exist(h, i)) {
			printf("   [%i] ", i);
			if (mh_exist(h, i)) {
				printf("   -> %s", h->slots[i].key);
				k++;
			}
			if (mh_dirty(h, i))
				printf(" dirty");
			printf("\n");
		}
	}
	printf("end(%i)\n", k);
}
#  endif
#endif

#undef mh_key_t
#undef mh_val_t
#undef mh_slot_t

#undef mh_slot
#undef mh_slot_key
#undef mh_slot_val
#undef mh_slot_size

#undef mh_hash
#undef mh_eq
#undef mh_slot_key_eq
#undef mh_slot_set_key
#undef mh_slot_copy

#undef mh_arg_t

#undef mh_bitmap_t
#undef mh_exist
#undef mh_setfree
#undef mh_setexist
#undef mh_dirty
#undef mh_setdirty

#undef mh_malloc
#undef mh_calloc
#undef mh_free

#undef mh_name
#undef mhash_t
