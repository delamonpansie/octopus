/*
 * Copyright (C) 2011-2014 Mail.RU
 * Copyright (C) 2014 Sokolov Yuriy
 * Copyright (C) 2011, 2012, 2013 Yuriy Vostrikov
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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include <limits.h>
#include <unistd.h>
#include <sys/mman.h>
#include "ptr_hash.h"
#if !defined(USE_SSE) && __SSE2__ && HAVE_IMMINTRIN_H
#define USE_SSE 1
#endif
#if USE_SSE
#include <immintrin.h>
#endif

typedef struct ptr_hash hash_t;
typedef struct ptr_hash_desc desc_t;

#ifndef unlikely
#if HAVE__BUILTIN_EXPECT
#  define unlikely(x)  __builtin_expect(!!(x),0)
#  define likely(x)  __builtin_expect(!!(x),1)
#else
#  define unlikely(x)  (x)
#  define likely(x)  (x)
#endif
#endif

#ifdef min
#undef min
#endif
#define min(a, b) ({ \
		__typeof__(a) a_ = (a), b_ = (b); \
		a_ <= b_ ? a_ : b_; \
	})

#ifndef _unused_
#define _unused_ __attribute__((unused))
#endif

static inline size_t
PH_PAGE_SIZE() {
	static size_t page_size = 0;
	if (page_size == 0) page_size = sysconf(_SC_PAGESIZE);
	return page_size;
}

#ifndef PH_SMALL_PAGE
#define PH_SMALL_PAGE (1 << 15)
#define PH_DEFAULT_CAPA() min((PH_SMALL_PAGE / sizeof(bucket_t)), 32)
#endif
#ifndef PH_DEFAULT_CAPA
static inline size_t
PH_DEFAULT_CAPA() {
	static size_t default_capa = 0;
	if (default_capa == 0)
		default_capa = PH_PAGE_SIZE()/sizeof(bucket_t);
	return default_capa;
}
#endif

#define PH_FILLUPPER_PRCNT 68
#define PH_FILLLOWER_PRCNT 60
#define PH_FILLUPPER (PH_FILLUPPER_PRCNT / 100.0)
#define PH_FILLLOWER (PH_FILLLOWER_PRCNT / 100.0)
#if PH_FILLLOWER_PRCNT >= PH_FILLUPPER_PRCNT
#error "PH_FILLLOWER should be < PH_FILLUPPER"
#endif
#define PH_REINSERT_BACKOFF_AFTER 7
#define PH_SHRINK_BACKOFF_AFTER 5

#ifndef MMAP_HINT_ADDR
#define MMAP_HINT_ADDR ((void*)(sizeof(void*) == 8 ? (uintptr_t)1 << 40: 0))
#endif

static inline size_t
round_up(size_t sz)
{
	size_t m = PH_PAGE_SIZE() - 1;
	return (sz + m) & ~m;
}

static inline size_t
round_down(size_t sz)
{
	size_t m = PH_PAGE_SIZE() - 1;
	return sz & ~m;
}

static void*
default_realloc_page(void *h _unused_, void *ptr, size_t old_size, size_t size)
{
	void *newptr;
	if (size == 0) {
		if (ptr) {
			if (old_size <= PH_SMALL_PAGE)
				free(ptr);
			else
				munmap(ptr, old_size);
		}
		return NULL;
	}
	if (!ptr) {
		if (size <= PH_SMALL_PAGE) {
			newptr = calloc(1, size);
			assert(newptr);
		} else {
			newptr = mmap(MMAP_HINT_ADDR, round_up(size),
					PROT_READ | PROT_WRITE,
					MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			assert(newptr != MAP_FAILED);
		}
		return newptr;
	}
	if (old_size <= PH_SMALL_PAGE && size <= PH_SMALL_PAGE) {
		newptr = realloc(ptr, size);
		assert(newptr);
		if (size > old_size)
			memset(newptr + old_size, 0, size - old_size);
	} else if (old_size <= PH_SMALL_PAGE && size > PH_SMALL_PAGE) {
		newptr = mmap(MMAP_HINT_ADDR, round_up(size),
				PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		assert(newptr != MAP_FAILED);
		memcpy(newptr, ptr, old_size);
		free(ptr);
	} else if (old_size > PH_SMALL_PAGE && size <= PH_SMALL_PAGE) {
		newptr = malloc(size);
		assert(newptr);
		memcpy(newptr, ptr, size);
		munmap(ptr, round_up(old_size));
	} else if (old_size >= size) {
		size = round_up(size);
		old_size = round_up(old_size);
		munmap(ptr + size, old_size - size);
		newptr = ptr;
	} else {
		size = round_up(size);
		old_size = round_up(old_size);
#if HAVE_MREMAP
		newptr = mremap(ptr, old_size, size, MREMAP_MAYMOVE);
		assert(newptr != MAP_FAILED);
#else
		newptr = mmap(ptr + old_size, size - old_size,
				PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
		if (newptr != MAP_FAILED) {
			if (newptr == ptr + old_size) {
				return ptr;
			}
			munmap(newptr, size - old_size);
		}
		newptr = mmap(MMAP_HINT_ADDR, size,
				PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		assert(newptr != MAP_FAILED);
		memcpy(newptr, ptr, old_size);
		munmap(ptr, old_size);
#endif
	}
	return newptr;
}

typedef struct ptr ptr_t;
struct ptr {
	intptr_t ptr: 48;
} __attribute__((packed,aligned(2)));
#define TO_PTR(p) ((void*)(intptr_t)(p).ptr)
#define FROM_PTR(p) ((ptr_t){.ptr = (intptr_t)(p)})

#define BUCKET_SIZE 8
typedef uint16_t hshk;
typedef struct ptr_bucket bucket_t;
struct ptr_bucket {
	hshk     hsh[BUCKET_SIZE];
	ptr_t    ptr[BUCKET_SIZE];
} __attribute__((packed,aligned(64)));
static int ass[(sizeof(bucket_t)==(BUCKET_SIZE*8))-1] __attribute__((unused));

static void
ph_realloc_nodes(hash_t *h, size_t want_capa, void *arg)
{
	if (h->capa == want_capa)
		return;
	if (want_capa > 0 && h->backoff && want_capa < h->capa) {
		assert(want_capa - h->watermark >= h->backoff);
		memmove(h->buckets + want_capa - h->backoff,
			h->buckets + h->capa - h->backoff,
			h->backoff * sizeof(bucket_t));
	}
	h->buckets = default_realloc_page(arg, h->buckets,
			sizeof(bucket_t) * h->capa,
			sizeof(bucket_t) * want_capa);
	if (h->backoff && want_capa > h->capa) {
		memmove(h->buckets + want_capa - h->backoff,
			h->buckets + h->capa - h->backoff,
			h->backoff * sizeof(bucket_t));
		if (want_capa - h->backoff >= h->capa) {
			memset(h->buckets + h->capa - h->backoff, 0,
				h->backoff * sizeof(bucket_t));
		} else {
			memset(h->buckets + h->capa - h->backoff, 0,
				(want_capa - h->capa) * sizeof(bucket_t));
		}
	}
	h->capa = want_capa;
#if HAVE_MADVISE
	if (sizeof(bucket_t) * want_capa > PH_SMALL_PAGE) {
		size_t pw = round_up(h->watermark*sizeof(bucket_t));
		size_t pb = round_down((want_capa-h->backoff)*sizeof(bucket_t));
		if (pw < pb) {
			madvise((void*)h->buckets+pw, pb-pw, MADV_DONTNEED);
			h->maxwatermark = h->watermark;
		}
	}
#endif
}

static size_t
ph_upper_bound(hash_t *h)
{
	return (h->watermark + h->backoff) * BUCKET_SIZE * PH_FILLUPPER;
}

static size_t __attribute__((unused))
ph_lower_bound(hash_t *h)
{
	return (h->watermark + h->backoff) * BUCKET_SIZE * PH_FILLLOWER;
}

#define KNUTH_MULT 0x5851f42d4c957f2dULL
static inline uint32_t
ph_next_rand(hash_t *h)
{
	uint32_t k = ((h->rand >> 16) ^ h->rand) * 0x53215599;
	h->rand = h->rand * 9 + 0x53215599;
	return k ^ (k >> 16);
}

static inline hshk
ph_hashikof(uint64_t hash) {
	return hash % 65521 + 1;
}
static inline size_t
ph_getpos1(hash_t *h, uint64_t hash) {
	size_t pos = hash & (h->border - 1);
	if (pos >= h->watermark) pos -= h->border / 2;
	return pos;
}

static inline size_t
ph_getpos2(hash_t *h, uint64_t hash, hshk hashik __attribute__((unused))) {
	hash ^= (((hash >> 32) ^ (hashik << 1)) | 1) * KNUTH_MULT;
	size_t pos = hash & (h->border - 1);
	if (pos >= h->watermark) pos -= h->border / 2;
	return pos;
}

static void __attribute__((unused))
ph_check_table(hash_t *h, void *arg) {
	size_t bucket;
	size_t size = 0;
	for (bucket = 0; bucket < h->watermark; bucket++) {
		bucket_t* b = &h->buckets[bucket];
		int i;
		for (i = 0; i < BUCKET_SIZE; i++) {
			if (b->hsh[i] == 0) continue;
			size++;
			void* obj = TO_PTR(b->ptr[i]);
			uint64_t hash = h->desc->hash(arg, obj);
			uint64_t pos1 = ph_getpos1(h, hash);
			uint64_t pos2 = ph_getpos2(h, hash, ph_hashikof(hash));
			assert(pos1 == bucket || pos2 == bucket);
		}
	}
	for (bucket = h->capa-h->backoff; bucket < h->capa; bucket++) {
		bucket_t* b = &h->buckets[bucket];
		int i;
		for (i = 0; i < BUCKET_SIZE; i++) {
			if (b->hsh[i] == 0) continue;
			size++;
		}
	}
	assert(size == h->size);
}
#define PDEBUG 0
#if PDEBUG
#define check_table(h, a) ph_check_table((h), (a))
#else
#define check_table(h, a)
#endif

#define PH_CAPA_STEP_LOG 2
static inline size_t
ph_next_capa(size_t capa)
{
	if (capa != 0) {
#if SIZEOF_SIZE_T == SIZEOF_LONG
		int z = sizeof(long)*8 - __builtin_clzl(capa) - 1;
#else
		int z = sizeof(long long)*8 - __builtin_clzll(capa) - 1;
#endif
		z -= PH_CAPA_STEP_LOG;
		if (z < 0) z = 0;
		return capa + ((size_t)1 << z);
	} else {
		return PH_DEFAULT_CAPA();
	}
}

static inline size_t
ph_prev_capa(size_t capa)
{
	if (capa > PH_DEFAULT_CAPA()) {
#if SIZEOF_SIZE_T == SIZEOF_LONG
		int z = sizeof(long)*8 - __builtin_clzl(capa) - 1;
#else
		int z = sizeof(long long)*8 - __builtin_clzll(capa) - 1;
#endif
		size_t b = (size_t)1 << z;
		if (capa == b)
			return capa - (b >> (PH_CAPA_STEP_LOG + 1));
		else
			return capa - (b >> PH_CAPA_STEP_LOG);
	} else {
		return PH_DEFAULT_CAPA();
	}
}

static void ph_grow_bucket(hash_t *h, void *arg) {
	if (h->watermark + h->backoff == h->capa) {
		if (h->capa == 0) {
			ph_realloc_nodes(h, PH_DEFAULT_CAPA(), arg);
			h->border = h->capa;
			h->watermark = h->capa * PH_FILLUPPER;
			h->maxwatermark = h->watermark;
			h->reinsert_backoff_after = PH_REINSERT_BACKOFF_AFTER;
			return;
		} else {
			ph_realloc_nodes(h, ph_next_capa(h->capa), arg);
		}
	}
	assert(h->capa > h->watermark + h->backoff);
	if (h->watermark == h->border) {
		assert(h->border < h->capa);
		h->border *= 2;
	}
	int i = 0, j = 0;
	size_t move_from = h->watermark - h->border/2;
	bucket_t* from = &h->buckets[move_from];
	bucket_t* to   = &h->buckets[h->watermark];
	h->watermark++;
	if (h->watermark > h->maxwatermark) {
		h->maxwatermark = h->watermark;
	}
	for (i = 0; i < BUCKET_SIZE; i++) {
		if (from->hsh[i] == 0) continue;
		void* obj = TO_PTR(from->ptr[i]);
		uint64_t hash = h->desc->hash(arg, obj);
		uint64_t pos1 = ph_getpos1(h, hash);
		uint64_t pos2 = ph_getpos2(h, hash, ph_hashikof(hash));
		if (pos1 != move_from && pos2 != move_from) {
			to->hsh[j] = from->hsh[i];
			to->ptr[j] = from->ptr[i];
			from->hsh[i] = 0;
			from->ptr[i].ptr = 0;
			j++;
		}
	}
	check_table(h, arg);
}

static int ph_reinsert(hash_t *h, void *obj, uint64_t hash, hshk hashik, void *arg);
static void ph_shrink_bucket(hash_t *h, void *arg) {
	if (h->watermark <= 2) {
		return;
	}
	size_t prev_capa = ph_prev_capa(h->capa);
	if (prev_capa < h->capa &&
		(h->watermark + h->backoff) / PH_FILLLOWER < prev_capa) {
		ph_realloc_nodes(h, prev_capa, arg);
		if (h->capa - h->backoff < h->maxwatermark) {
			h->maxwatermark = h->capa - h->backoff;
		}
	}
#if HAVE_MADVISE
	else if (h->capa * sizeof(bucket_t) > PH_SMALL_PAGE &&
		round_up(h->watermark*sizeof(bucket_t)) + PH_PAGE_SIZE() * 128 < round_down(h->maxwatermark*sizeof(bucket_t))) {
		madvise((void*)h->buckets+round_up(h->watermark*sizeof(bucket_t)),
			round_down((h->capa-h->backoff)*sizeof(bucket_t))-
			round_up(h->watermark*sizeof(bucket_t)),
			MADV_DONTNEED);
		h->maxwatermark = h->watermark;
	}
#endif
	int i;
	check_table(h, arg);
	if (h->watermark == h->border/2) {
		h->border /= 2;
	}
	h->watermark--;
	bucket_t buc = h->buckets[h->watermark];
	memset(h->buckets + h->watermark, 0, sizeof(bucket_t));
#if PDEBUG
	for (i = 0; i < BUCKET_SIZE; i++) {
		if (buc.hsh[i] != 0) h->size--;
	}
#endif
	for (i = 0; i < BUCKET_SIZE; i++) {
		if (buc.hsh[i] == 0) continue;
		void* obj = TO_PTR(buc.ptr[i]);
		uint64_t hash = h->desc->hash(arg, obj);
		if (PDEBUG) h->size++;
		ph_reinsert(h, obj, hash, buc.hsh[i], arg);
	}
	check_table(h, arg);
}

#if USE_SSE
#define PH_ALL_EQ (0xffff)
static inline uint32_t
ph_check_bucket(hash_t *h, size_t pos, hshk hashik)
{
	uint32_t res = 0;
	__m128i cmp, hsh, eq;
	bucket_t *b = &h->buckets[pos];
	cmp = _mm_set1_epi16(hashik);
	hsh = _mm_load_si128((__m128i*)b->hsh);
	eq = _mm_cmpeq_epi16(hsh, cmp);
	res = _mm_movemask_epi8(eq);
	return res;
}

static inline int
ph_next_i(uint32_t *res)
{
	int i = __builtin_ctz(*res) / 2;
	*res ^= 0x3 << (i*2);
	return i;
}
#else
#define PH_ALL_EQ (0xff)
static inline uint32_t
ph_check_bucket(hash_t *h, size_t pos, hshk hashik)
{
	uint32_t res = 0;
	static const uint64_t m = ((uint64_t)(0x00010001) << 32) | 0x00010001;
	uint64_t *hsh, cmp, eq, eq1;
	hsh = (uint64_t*)h->buckets[pos].hsh;
	cmp = hashik * m;
	eq = (hsh[0] ^ cmp);
	eq = (eq - m) & ~eq;
	eq1 = (hsh[1] ^ cmp);
	eq1 = (eq1 - m) & ~eq1;
	res |= (eq >> 15) & 1;
	res |= (eq >> 30) & 2;
	res |= (eq >> 45) & 4;
	res |= (eq >> 60) & 8;
	res |= (eq1 >> 11) & 0x10;
	res |= (eq1 >> 26) & 0x20;
	res |= (eq1 >> 41) & 0x40;
	res |= (eq1 >> 56) & 0x80;
	return res;
}

static inline int
ph_next_i(uint32_t *res)
{
	int i = __builtin_ctz(*res);
	*res ^= 1 << i;
	return i;
}
#endif

static void
ph_put_to_backoff(hash_t *h, void* obj, uint64_t hash __attribute__((unused)), hshk hashik, void* arg)
{
	int i;
	size_t pos = h->capa - h->backoff;
	uint32_t res;
	if (h->backoff > 0 && (res = ph_check_bucket(h, pos, 0))) {
		i = ph_next_i(&res);
		h->buckets[pos].hsh[i] = hashik;
		h->buckets[pos].ptr[i] = FROM_PTR(obj);
		return;
	}
	if (h->watermark + h->backoff == h->capa) {
		if (PDEBUG) h->size--;
		ph_grow_bucket(h, arg);
		if (PDEBUG) h->size++;
	}
#if PDEBUG
	h->size--; check_table(h, arg); h->size++;
#endif
	h->backoff++;
	pos = h->capa - h->backoff;
	memset(h->buckets + pos, 0, sizeof(bucket_t));
	h->buckets[pos].hsh[0] = hashik;
	h->buckets[pos].ptr[0] = FROM_PTR(obj);
#if PDEBUG
	check_table(h, arg);
#endif
}

static void
ph_pop_from_backoff(hash_t *h, size_t buc, int i)
{
	uint32_t z;
	int j;
	size_t bak = h->capa - h->backoff;
	bucket_t *b = &h->buckets[buc];
	bucket_t *bc = &h->buckets[bak];
	assert(buc >= bak);
	if (buc > bak) {
		for (j = 0; j < BUCKET_SIZE && bc->hsh[j] == 0; j++);
		if (j < BUCKET_SIZE) {
			b->hsh[i] = bc->hsh[j];
			b->ptr[i] = bc->ptr[j];
			bc->hsh[j] = 0;
			bc->ptr[j] = FROM_PTR(NULL);
		}
	}
	z = ph_check_bucket(h, h->capa - h->backoff, 0);
	if (z == PH_ALL_EQ) {
		h->backoff--;
	}
}

static inline void
ph_mark_free(hash_t *h, size_t bucket, int pos)
{
	bucket_t *b = &h->buckets[bucket];
	b->hsh[pos] = 0;
	b->ptr[pos].ptr = 0;
	if (bucket >= h->capa - h->backoff)
		ph_pop_from_backoff(h, bucket, pos);
}

static inline void
ph_move_item(hash_t *h, size_t fbuc, int fpos, size_t tbuc, int tpos)
{
	h->buckets[tbuc].hsh[tpos] = h->buckets[fbuc].hsh[fpos];
	h->buckets[tbuc].ptr[tpos] = h->buckets[fbuc].ptr[fpos];
	ph_mark_free(h, fbuc, fpos);
}

static int
ph_reinsert(hash_t *h, void *obj, uint64_t hash, hshk hashik, void *arg)
{
	size_t pos1, pos2, pos;
	uint32_t i, res;
	hshk nhashik;
	void* nobj;
	uint64_t nhash;
	bucket_t *b;
	int cnt = 0;
	assert(h->desc);
restart: {}
#if PDEBUG
	h->size--; check_table(h, arg); h->size++;
#endif
	pos1 = ph_getpos1(h, hash);
	pos2 = ph_getpos2(h, hash, hashik);
	if ((res = ph_check_bucket(h, pos1, 0))) {
		i = ph_next_i(&res);
		h->buckets[pos1].hsh[i] = hashik;
		h->buckets[pos1].ptr[i] = FROM_PTR(obj);
		check_table(h, arg);
		return 0;
	}
	if ((res = ph_check_bucket(h, pos2, 0))) {
		i = ph_next_i(&res);
		h->buckets[pos2].hsh[i] = hashik;
		h->buckets[pos2].ptr[i] = FROM_PTR(obj);
		check_table(h, arg);
		return 0;
	}
	i = ph_next_rand(h) % (BUCKET_SIZE * 2);
	pos = i > BUCKET_SIZE ? pos1 : pos2;
	b = &h->buckets[pos];
	i %= BUCKET_SIZE;
	nhashik = b->hsh[i];
	nobj    = TO_PTR(b->ptr[i]);
	nhash   = h->desc->hash(arg, nobj);
	b->hsh[i] = hashik;
	b->ptr[i] = FROM_PTR(obj);
#if PDEBUG
	h->size--; check_table(h, arg); h->size++;
#endif
	hashik = nhashik;
	obj    = nobj;
	hash   = nhash;
	cnt++;
	if (cnt >= h->reinsert_backoff_after) {
		ph_put_to_backoff(h, obj, hash, hashik, arg);
		check_table(h, arg);
		return 1;
	}
	goto restart;
}

static int
ph_find_key(hash_t *h, uint64_t key, void *arg, size_t *bucket, int *pos)
{
	assert(h->desc);
	uint64_t hash = h->desc->hashKey(arg, key);
	hshk hashik = ph_hashikof(hash);
	size_t pos1 = ph_getpos1(h, hash);
	size_t pos2 = ph_getpos2(h, hash, hashik);
	size_t bac;
	void *ptr;
	int i;
	uint32_t res;
	res = ph_check_bucket(h, pos1, hashik);
	while (res) {
		i = ph_next_i(&res);
		ptr = TO_PTR(h->buckets[pos1].ptr[i]);
		if (h->desc->equalToKey(arg, ptr, key)) {
			*bucket = pos1;
			*pos    = i;
			return 1;
		}
	}
	res = ph_check_bucket(h, pos2, hashik);
	while (res) {
		i = ph_next_i(&res);
		ptr = TO_PTR(h->buckets[pos2].ptr[i]);
		if (h->desc->equalToKey(arg, ptr, key)) {
			if ((res = ph_check_bucket(h, pos1, 0))) {
				int j = ph_next_i(&res);
				ph_move_item(h, pos2, i, pos1, j);
				*bucket = pos1;
				*pos = j;
				return 1;
			}
			*bucket = pos2;
			*pos    = i;
			return 1;
		}
	}
	if (likely(h->backoff == 0))
		return 0;
	for (bac = h->backoff; bac; bac--) {
		size_t posb = h->capa - bac;
		res = ph_check_bucket(h, posb, hashik);
		while (res) {
			i = ph_next_i(&res);
			ptr = TO_PTR(h->buckets[posb].ptr[i]);
			if (h->desc->equalToKey(arg, ptr, key)) {
				if ((res = ph_check_bucket(h, pos1, 0))) {
					int j = ph_next_i(&res);
					ph_move_item(h, posb, i, pos1, j);
					*bucket = pos1;
					*pos = j;
					return 1;
				}
				if ((res = ph_check_bucket(h, pos2, 0))) {
					int j = ph_next_i(&res);
					ph_move_item(h, posb, i, pos2, j);
					*bucket = pos2;
					*pos = j;
					return 1;
				}
				*bucket = posb;
				*pos    = i;
				return 1;
			}
		}
	}
	return 0;
}

static void
ph_shrink_backoff(hash_t *h, void *arg)
{
	size_t buc;
	bucket_t b;
	hshk hashik;
	void *obj;
	uint64_t hash;
	int i;

	if (h->backoff == 0) {
		return;
	}
	buc = ph_next_rand(h) % h->backoff + 1;
	b = h->buckets[h->capa - buc];
	if (unlikely(buc != h->backoff)) {
		memmove(h->buckets + (h->capa - buc),
			h->buckets + (h->capa - h->backoff),
			sizeof(bucket_t));
	}
	memset(h->buckets + h->capa - h->backoff, 0, sizeof(bucket_t));
	h->backoff--;
#if PDEBUG
	for (i = 0; i < BUCKET_SIZE; i++) {
		if (b.hsh[i] != 0) h->size--;
	}
#endif
	h->reinsert_backoff_after = PH_SHRINK_BACKOFF_AFTER;
	for (i = 0; i < BUCKET_SIZE; i++) {
		if (b.hsh[i] == 0) continue;
		if (PDEBUG) h->size++;
		hashik = b.hsh[i];
		obj = TO_PTR(b.ptr[i]);
		hash = h->desc->hash(arg, obj);
		ph_reinsert(h, obj, hash, hashik, arg);
	}
	h->reinsert_backoff_after = PH_REINSERT_BACKOFF_AFTER;
}

void *
ph_insert(hash_t *h, void *obj, uint64_t key, void *arg)
{
	assert(h->desc);
	if (ph_upper_bound(h) <= h->size) {
		ph_grow_bucket(h, arg);
		ph_grow_bucket(h, arg);
	}
	if (unlikely(h->backoff != 0)) {
		ph_shrink_backoff(h, arg);
	}
	uint64_t hash = h->desc->hashKey(arg, key);
	hshk hashik = ph_hashikof(hash);
	size_t pos1 = ph_getpos1(h, hash);
	size_t pos2 = ph_getpos2(h, hash, hashik);
	size_t pos;
	void *ptr;
	int i;
	uint32_t res;
	res = ph_check_bucket(h, pos1, hashik);
	while (res) {
		i = ph_next_i(&res);
		ptr = TO_PTR(h->buckets[pos1].ptr[i]);
		if (ptr == obj || h->desc->equalToKey(arg, ptr, key)) {
			h->buckets[pos1].ptr[i] = FROM_PTR(obj);
			return ptr;
		}
	}
	res = ph_check_bucket(h, pos2, hashik);
	while (res) {
		i = ph_next_i(&res);
		ptr = TO_PTR(h->buckets[pos2].ptr[i]);
		if (ptr == obj || h->desc->equalToKey(arg, ptr, key)) {
			h->buckets[pos2].ptr[i] = FROM_PTR(obj);
			return ptr;
		}
	}
	for (pos = h->backoff; pos; pos--) {
		res = ph_check_bucket(h, h->capa-pos, hashik);
		while (res) {
			i = ph_next_i(&res);
			ptr = TO_PTR(h->buckets[h->capa-pos].ptr[i]);
			if (ptr == obj || h->desc->equalToKey(arg, ptr, key)) {
				h->buckets[h->capa-pos].ptr[i] = FROM_PTR(obj);
				return ptr;
			}
		}
	}
	h->size++;
	if ((res = ph_check_bucket(h, pos1, 0))) {
		i = ph_next_i(&res);
		h->buckets[pos1].hsh[i] = hashik;
		h->buckets[pos1].ptr[i] = FROM_PTR(obj);
		check_table(h, arg);
		return NULL;
	}
	if ((res = ph_check_bucket(h, pos2, 0))) {
		i = ph_next_i(&res);
		h->buckets[pos2].hsh[i] = hashik;
		h->buckets[pos2].ptr[i] = FROM_PTR(obj);
		check_table(h, arg);
		return NULL;
	}
	pos = pos1;
	bucket_t *b = &h->buckets[pos];
	i = ph_next_rand(h) % BUCKET_SIZE;
	hshk nhashik = b->hsh[i];
	void*    nobj    = TO_PTR(b->ptr[i]);
	uint64_t nhash   = h->desc->hash(arg, nobj);
	b->hsh[i] = hashik;
	b->ptr[i] = FROM_PTR(obj);
#if PDEBUG
	h->size--; check_table(h, arg); h->size++;
#endif
	ph_reinsert(h, nobj, nhash, nhashik, arg);
	check_table(h, arg);
	return NULL;
}

void *
ph_get_key(hash_t *h, uint64_t key, void *arg)
{
	if (h->buckets == NULL)
		return NULL;

	size_t bucket;
	int pos;
	if (ph_find_key(h, key, arg, &bucket, &pos)) {
		return TO_PTR(h->buckets[bucket].ptr[pos]);
	}
	return NULL;
}

void *
ph_delete_key(hash_t *h, uint64_t key, void *arg)
{
	if (h->buckets == NULL)
		return NULL;
	if (unlikely(h->backoff != 0)) {
		ph_shrink_backoff(h, arg);
	} else if (h->size < ph_lower_bound(h)) {
		ph_shrink_bucket(h, arg);
	}
	size_t bucket;
	int pos;
	if (ph_find_key(h, key, arg, &bucket, &pos)) {
		void* obj = TO_PTR(h->buckets[bucket].ptr[pos]);
		h->size--;
		ph_mark_free(h, bucket, pos);
		return obj;
	}
	return NULL;
}

size_t
ph_get_key_iter(hash_t *h, uint64_t key, void *arg)
{
	if (h->buckets == NULL)
		return SIZE_MAX;

	size_t bucket;
	int pos;
	if (ph_find_key(h, key, arg, &bucket, &pos)) {
		return bucket * BUCKET_SIZE + pos;
	}
	return SIZE_MAX;
}

size_t
ph_iter_first(hash_t *h)
{
	if (h->buckets[0].hsh[0] != 0)
		return 0;
	return ph_iter_next(h, 0);
}

size_t
ph_iter_next(hash_t *h, size_t i)
{
	size_t bucket = i / BUCKET_SIZE;
	int pos = i % BUCKET_SIZE;
	while (bucket < h->capa) {
		pos++;
		if (pos == BUCKET_SIZE) {
			pos = 0;
			bucket++;
		}
		if (bucket >= h->watermark &&
				bucket < h->capa-h->backoff) {
			pos = 0;
			bucket = h->capa - h->backoff;
		}
		if (bucket >= h->capa)
			return SIZE_MAX;
		if (h->buckets[bucket].hsh[pos] != 0)
			return bucket * BUCKET_SIZE + pos;
	}
	return SIZE_MAX;
}

void*
ph_iter_fetch(hash_t *h, size_t i)
{
	if (i/BUCKET_SIZE >= h->capa)
		return NULL;
	return TO_PTR(h->buckets[i/BUCKET_SIZE].ptr[i%BUCKET_SIZE]);
}

void
ph_destroy(hash_t *h, void* arg)
{
	desc_t const *desc = h->desc;
	ph_realloc_nodes(h, 0, arg);
	memset(h, 0, sizeof(*h));
	h->desc = desc;
}

void
ph_resize(hash_t *h, size_t size, void *arg)
{
	assert(h->desc);
	assert(h->size == 0);
	size_t capa = PH_DEFAULT_CAPA();
	while (capa * BUCKET_SIZE * PH_FILLUPPER < size) {
		capa *= 2;
	}
	if (capa > h->capa) {
		ph_realloc_nodes(h, capa, arg);
		h->border = h->capa;
		h->watermark = size / BUCKET_SIZE/ PH_FILLUPPER;
		if (h->watermark < h->border/2) {
			h->watermark = h->border/2;
		}
		h->maxwatermark = h->watermark;
		h->reinsert_backoff_after = PH_REINSERT_BACKOFF_AFTER;
	}
}

size_t
ph_capa(hash_t *h)
{
	return h->capa * BUCKET_SIZE;
}

size_t
ph_bytes(hash_t *h)
{
	return (h->maxwatermark + h->backoff)*sizeof(bucket_t) + sizeof(hash_t);
}
