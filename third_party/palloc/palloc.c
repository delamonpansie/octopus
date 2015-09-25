/*
 * Copyright (C) 2010, 2011, 2012, 2013 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013 Yuriy Vostrikov
 * Copyright (C) 2012, 2013 Roman Tokarev
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

#ifdef OCTOPUS
# import <util.h>
# import <palloc.h>
# import <say.h>
# import <tbuf.h>
# import <stat.h>
#else
# include "palloc.h"
#endif

#if HAVE_THIRD_PARTY_QUEUE_H
# include <third_party/queue.h>
#elif HAVE_UTIL
# include "../include/queue.h"
#else
# include "queue.h"
#endif

#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/mman.h>

#if HAVE_VALGRIND_VALGRIND_H && !defined(NVALGRIND)
# include <valgrind/valgrind.h>
# include <valgrind/memcheck.h>
#else
# define VALGRIND_MAKE_MEM_DEFINED(_qzz_addr,_qzz_len) (void)0
# define VALGRIND_MAKE_MEM_NOACCESS(_qzz_addr,_qzz_len) (void)0
# define VALGRIND_MAKE_MEM_UNDEFINED(_qzz_addr,_qzz_len) (void)0
# define VALGRIND_CREATE_MEMPOOL(pool, rzB, is_zeroed) (void)0
# define VALGRIND_DESTROY_MEMPOOL(pool) (void)0
# define VALGRIND_MEMPOOL_ALLOC(pool, addr, size) (void)0
# define VALGRIND_MEMPOOL_TRIM(pool, addr, size) (void)0
# define VALGRIND_MEMPOOL_CHANGE(pool, addrA, addrB, size) (void)0
#endif

#if defined(__SANITIZE_ADDRESS__)
void __asan_poison_memory_region(void const volatile *addr, size_t size, int magic);
void __asan_unpoison_memory_region(void const volatile *addr, size_t size);
#define ASAN_POISON_MEMORY_REGION(addr, size, magic) __asan_poison_memory_region((addr), (size), (magic))
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) __asan_unpoison_memory_region((addr), (size))
#else
#define ASAN_POISON_MEMORY_REGION(addr, size, magic) (void)0
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) (void)0
#endif

#ifndef PALLOC_CHUNK_TTL
# define PALLOC_CHUNK_TTL 4096
#endif

#ifndef __regparam
# define __regparam
#endif

#ifndef MMAP_HINT_ADDR
# define MMAP_HINT_ADDR NULL
#endif

#ifndef MAP_ANONYMOUS
# define MAP_ANONYMOUS MAP_ANON
#endif

#ifndef likely
# if HAVE__BUILTIN_EXPECT
#  define likely(x)	__builtin_expect((x),1)
#  define unlikely(x)  __builtin_expect((x),0)
# else
#  define likely(x)	(x)
#  define unlikely(x)  (x)
# endif
#endif

#ifndef nelem
# define nelem(x)     (sizeof((x))/sizeof((x)[0]))
#endif

#ifndef TYPEALIGN
# define TYPEALIGN(ALIGNVAL,LEN)  \
        (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#endif

#if defined(__SANITIZE_ADDRESS__)
# ifndef REDZONE
#  define REDZONE
# endif
# define PALLOC_ALIGN(ptr) (void *)TYPEALIGN(8, ptr)
# define RZMAX (PALLOC_REDZONE + (uintptr_t)PALLOC_ALIGN((void *)1))
#else
# define PALLOC_ALIGN(ptr) ptr
# define RZMAX PALLOC_REDZONE
#endif

#ifdef PALLOC_STAT
#include <stat.h>
#include <util.h>
#define STAT(_)					\
	_(PALLOC_CALL, 1)			\
	_(PALLOC_BYTES, 2)

enum stat_op ENUM_INITIALIZER(STAT);
static char * const stat_op[] = ENUM_STR_INITIALIZER(STAT);
static __thread int stat_base;
#endif

#ifdef REDZONE
#define PALLOC_REDZONE 8
#endif
#ifndef PALLOC_REDZONE
#define PALLOC_REDZONE 0
#endif
#ifdef POISON
#define PALLOC_POISON
#endif

struct chunk {
	uint32_t magic;
	void *brk;
	size_t free;
	size_t data_size;
	uint64_t last_use;

	struct chunk_class *class;
	TAILQ_ENTRY(chunk) link; /* chunk is either member of
				    palloc_pool->chunks or chunk_class->chunks */

#if PALLOC_REDZONE > 0 || (HAVE_VALGRIND_VALGRIND_H && !defined(NVALGRIND))
	/* initial chunk->brk must be 8-aligned (for asan) */
	char redzone[PALLOC_REDZONE] __attribute__ ((aligned (8)));
#endif
};

TAILQ_HEAD(chunk_list_head, chunk);

struct chunk_class {
	uint32_t size;
	int chunks_count;
	struct chunk_list_head chunks;
};

struct palloc_pool;
struct gc_root {
	void (*copy)(struct palloc_pool *, void *);
	void *ptr;
	SLIST_ENTRY(gc_root) link;
};
SLIST_HEAD(gc_list, gc_root);

struct palloc_cut_point {
	struct chunk *chunk;
	uint32_t chunk_free;
	size_t	allocated;
	SLIST_ENTRY(palloc_cut_point) link;
};
SLIST_HEAD(cut_list, palloc_cut_point);

struct palloc_pool {
	struct palloc_config cfg;
	struct chunk_list_head chunks;
	SLIST_ENTRY(palloc_pool) link;
	size_t allocated;
	struct gc_list gc_list;
	struct cut_list cut_list;
};

static __thread SLIST_HEAD(palloc_pool_head, palloc_pool) pools;
#define CHUNK_CLASS(i, kb) [(i)] = { .size = (kb) * 1024 - sizeof(struct chunk) }

#define almost_unlimited (uint32_t)-1
static __thread struct chunk_class classes[] = {
	CHUNK_CLASS(0, 32),
	CHUNK_CLASS(1, 64),
	CHUNK_CLASS(2, 128),
	CHUNK_CLASS(3, 256),
	CHUNK_CLASS(4, 512),
	CHUNK_CLASS(5, 1024),
	CHUNK_CLASS(6, 2048),
	CHUNK_CLASS(7, 4096),

	{ .size = almost_unlimited }
};

const uint32_t chunk_magic = 0xbb84fcf6;
#if !defined(NDEBUG) && defined(PALLOC_POISON)
static const char poison_char = 'P';
#endif

static __thread uint64_t release_count = 0;

static void
palloc_init(void)
{
	static __thread bool inited = false;

	if (inited)
		return;

#ifdef PALLOC_STAT
	stat_base = stat_register(stat_op, nelem(stat_op));
#endif

	for (uint32_t i = 0; i < nelem(classes); i++)
		TAILQ_INIT(&classes[i].chunks);
	SLIST_INIT(&pools);

	inited = true;
}

static void
chunk_poison(const struct chunk *chunk)
{
	(void)chunk;

	assert(chunk->magic == chunk_magic);
#if !defined(NDEBUG) && defined(PALLOC_POISON)
	(void)VALGRIND_MAKE_MEM_DEFINED(chunk->brk, chunk->free);
	memset(chunk->brk, poison_char, chunk->free);
	(void)VALGRIND_MAKE_MEM_NOACCESS(chunk->brk, chunk->free);
#endif
	VALGRIND_MAKE_MEM_NOACCESS(chunk->brk, chunk->free);
	ASAN_POISON_MEMORY_REGION(chunk->brk, chunk->free, 0xf7);
}

static void
unpoison(const char *ptr, size_t size)
{
	(void)ptr;
	(void)size;

	ASAN_UNPOISON_MEMORY_REGION(ptr, size);
#if !defined(NDEBUG) && defined(PALLOC_POISON)
	(void)VALGRIND_MAKE_MEM_DEFINED(ptr, size);
	for (int i = 0; i < (size); i++)
		assert(ptr[i] == poison_char);
#endif
	VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);
}

/* WARNING: may use extra RZMAX bytes for REDZONE and padding */
static inline void *
chunk_alloc(struct chunk *chunk, size_t size)
{
	void *brk = chunk->brk,
	     *ptr = brk;

	if (unlikely(size == 0))
		return NULL;

	brk = PALLOC_ALIGN(brk + size + PALLOC_REDZONE);
	chunk->free -= brk - ptr;
	chunk->brk = brk;

#ifdef PALLOC_PARANOIA
	assert(chunk->free < chunk->data_size);
#endif
	assert(PALLOC_ALIGN(chunk->brk) == chunk->brk);
	assert(PALLOC_ALIGN(ptr) == ptr);

	unpoison(ptr, size);
	VALGRIND_MEMPOOL_ALLOC(chunk, ptr, size);
	ASAN_POISON_MEMORY_REGION(ptr + size, chunk->brk - ptr - size, 0xfb);

	return ptr;
}


static struct chunk *
next_chunk_for(struct palloc_pool *pool, size_t size)
{
	struct chunk * chunk = TAILQ_FIRST(&pool->chunks);
	struct chunk_class *class;
	size_t chunk_size;

	if (chunk != NULL)
		class = chunk->class;
	else
		class = &classes[0];

	if (class->size == almost_unlimited) /* move to prev to almost_unlimited class */
		class--;

	while (class->size < size)
		class++;

	chunk = TAILQ_FIRST(&class->chunks);
	if (chunk != NULL)
		chunk_size = chunk->data_size;
	else if (class->size == almost_unlimited) {
		size_t mmap_size = (size + sizeof(struct chunk) + 4095) & ~4095;
		chunk_size = mmap_size - sizeof(struct chunk);
	} else
		chunk_size = class->size;

	if (pool->cfg.size != 0 &&
	    pool->allocated + chunk_size > pool->cfg.size)
		return NULL;

	if (chunk != NULL) {
		TAILQ_REMOVE(&class->chunks, chunk, link);
		goto found;
	}

	chunk = mmap(MMAP_HINT_ADDR, sizeof(struct chunk) + chunk_size,
		     PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (chunk == MAP_FAILED)
		return NULL;

	class->chunks_count++;
	chunk->magic = chunk_magic;
	chunk->data_size = chunk_size;
	chunk->free = chunk_size;
	chunk->brk = (void *)chunk + sizeof(struct chunk);
	chunk->class = class;

	chunk_poison(chunk);
	VALGRIND_MAKE_MEM_NOACCESS(chunk->redzone, sizeof(chunk->redzone));
	ASAN_POISON_MEMORY_REGION(chunk->redzone, sizeof(chunk->redzone), 0xfa);
found:
	assert(chunk != NULL && chunk->magic == chunk_magic);
	TAILQ_INSERT_HEAD(&pool->chunks, chunk, link);
	pool->allocated += chunk->data_size;
	VALGRIND_CREATE_MEMPOOL(chunk, PALLOC_REDZONE, 0); /* NOACCESS mark is set by chunk_poison() */

	return chunk;
}

void * __regparam
palloc_slow_path(struct palloc_pool *pool, size_t size)
{
	struct chunk *chunk;

	chunk = next_chunk_for(pool, size + RZMAX);
	if (chunk == NULL) {
		if (pool->cfg.nomem_cb != NULL)
			pool->cfg.nomem_cb(pool, (void *)pool->cfg.ctx);
		abort();
	}

	return chunk_alloc(chunk, size);
}

void * __regparam
palloc(struct palloc_pool *pool, size_t size)
{
	struct chunk *chunk = TAILQ_FIRST(&pool->chunks);

#ifdef PALLOC_STAT
	stat_collect(stat_base, PALLOC_CALL, 1);
	stat_collect(stat_base, PALLOC_BYTES, size);
#endif

	if (likely(chunk != NULL && chunk->free >= size + RZMAX))
		return chunk_alloc(chunk, size);
	return palloc_slow_path(pool, size);
}

void *
prealloc_slow_path(struct palloc_pool *pool, void *oldptr, size_t oldsize, size_t size)
{
	void *ptr = palloc(pool, size);
	memcpy(ptr, oldptr, oldsize);
	return ptr;
}

void *
prealloc(struct palloc_pool *pool, void *oldptr, size_t oldsize, size_t size)
{
	if (unlikely(size <= oldsize))
		return oldptr;
	if (unlikely(oldptr == NULL))
		return palloc(pool, size);

	const size_t diff_size = size - oldsize;
	struct chunk *chunk = TAILQ_FIRST(&pool->chunks);
	/* sizeof(redzone) >= 8 (sizeof(redzone) == PALLOC_REDZONE + align)
	   sizeof(chunk->free) >= 8 (chunk->brk is aligned when redzones enabled),
	   so there is always enough space to make new aligned redzone */
	if (likely(chunk != NULL && chunk->free >= diff_size &&
		   PALLOC_ALIGN(oldptr + oldsize + PALLOC_REDZONE) == chunk->brk)) {
		void *oldbrk = chunk->brk;

#ifdef PALLOC_STAT
		stat_collect(stat_base, PALLOC_CALL, 1);
		stat_collect(stat_base, PALLOC_BYTES, diff_size);
#endif
		chunk->brk = PALLOC_ALIGN(oldptr + size + PALLOC_REDZONE);
		chunk->free -= chunk->brk - oldbrk;
#ifdef PALLOC_PARANOIA
		assert(chunk->free < chunk->data_size);
#endif
		unpoison(oldptr + oldsize, diff_size);
		VALGRIND_MEMPOOL_CHANGE(chunk, oldptr, oldptr, size);
		ASAN_POISON_MEMORY_REGION(oldptr + size, chunk->brk - oldptr - size, 0xfb);
		return oldptr;
	} else {
		return prealloc_slow_path(pool, oldptr, oldsize, size);
	}
}

void *
p0alloc(struct palloc_pool *pool, size_t size)
{
	void *ptr;

	ptr = palloc(pool, size);
	memset(ptr, 0, size);
	return ptr;
}

void *
palloca(struct palloc_pool *pool, size_t size, size_t align)
{
	void *ptr;

	ptr = palloc(pool, size + align);
	return (void *)TYPEALIGN(align, (uintptr_t)ptr);
}

static void
release_chunk(struct chunk *chunk)
{
	VALGRIND_DESTROY_MEMPOOL(chunk);
	if (chunk->class->size != almost_unlimited) {
		chunk->free = chunk->data_size;
		chunk->brk = (void *)chunk + sizeof(struct chunk);
		/* chunk must be removed from palloc_pool->chunks already */
		TAILQ_INSERT_HEAD(&chunk->class->chunks, chunk, link);
		chunk_poison(chunk);
		chunk->last_use = release_count;
	} else {
		assert(chunk->class->chunks_count > 0);
		chunk->class->chunks_count--;
		ASAN_UNPOISON_MEMORY_REGION(chunk->redzone, sizeof(chunk->redzone));
		ASAN_UNPOISON_MEMORY_REGION(chunk->brk, chunk->free);
		munmap(chunk, chunk->data_size + sizeof(struct chunk));
	}
}

/* chunks is a garbage after call */
static void
release_chunks(struct chunk_list_head *chunks)
{
	struct chunk *chunk, *tvar;

	TAILQ_FOREACH_SAFE(chunk, chunks, link, tvar)
		release_chunk(chunk);

	if (++release_count % 256)
		return;

	for (uint32_t i = 0; i < nelem(classes); i++) {
		struct chunk_class *class = &classes[i];
		struct chunk *chunk, *tvar;

		if (class->size == almost_unlimited)
			continue;

		TAILQ_FOREACH_REVERSE_SAFE(chunk, &class->chunks, chunk_list_head, link, tvar) {
			if (release_count - chunk->last_use < PALLOC_CHUNK_TTL)
				break;

			TAILQ_REMOVE(&class->chunks, chunk, link);
			ASAN_UNPOISON_MEMORY_REGION(chunk->redzone, sizeof(chunk->redzone));
			ASAN_UNPOISON_MEMORY_REGION(chunk->brk, chunk->free);
			munmap(chunk, class->size + sizeof(struct chunk));
			class->chunks_count--;
		}
	}
}

void
prelease(struct palloc_pool *pool)
{
	release_chunks(&pool->chunks);
	TAILQ_INIT(&pool->chunks);
	SLIST_INIT(&pool->cut_list);
	pool->allocated = 0;
}

void
prelease_after(struct palloc_pool *pool, size_t after)
{
	if (pool->allocated > after)
		prelease(pool);
}

struct palloc_pool *
palloc_create_pool(struct palloc_config cfg)
{
	struct palloc_pool *pool;

	palloc_init();

	pool = malloc(sizeof(struct palloc_pool));
	assert(pool != NULL);
	pool->cfg = cfg;
	pool->allocated = 0;
	TAILQ_INIT(&pool->chunks);
	SLIST_INIT(&pool->gc_list);
	SLIST_INIT(&pool->cut_list);
	SLIST_INSERT_HEAD(&pools, pool, link);
	return pool;
}

void
palloc_destroy_pool(struct palloc_pool *pool)
{
	SLIST_REMOVE(&pools, pool, palloc_pool, link);
	prelease(pool);
	free(pool);
}

void
palloc_unmap_unused(void)
{
	for (uint32_t i = 0; i < nelem(classes); i++) {
		struct chunk_class *class = &classes[i];
		struct chunk *chunk, *tvar;

		TAILQ_FOREACH_SAFE(chunk, &class->chunks, link, tvar) {
			ASAN_UNPOISON_MEMORY_REGION(chunk, sizeof(struct chunk) + chunk->data_size);
			munmap(chunk, class->size + sizeof(struct chunk));
			class->chunks_count--;
		}
		TAILQ_INIT(&class->chunks);
	}
}

void
palloc_register_gc_root(struct palloc_pool *pool,
			void *ptr, void (*copy)(struct palloc_pool *, void *))
{
	struct gc_root *root = palloc(pool, sizeof(*root));
	root->ptr = ptr;
	root->copy = copy;
	SLIST_INSERT_HEAD(&pool->gc_list, root, link);
}

void
palloc_unregister_gc_root(struct palloc_pool *pool, void *ptr)
{
	struct gc_root *root;
	SLIST_FOREACH(root, &pool->gc_list, link)
		if (root->ptr == ptr) {
			SLIST_REMOVE(&pool->gc_list, root, gc_root, link);
			break;
		}
}

void
palloc_gc(struct palloc_pool *pool)
{
	struct chunk_list_head old_chunks = pool->chunks;

	TAILQ_INIT(&pool->chunks);
	pool->allocated = 0;

	struct gc_list new_list = SLIST_HEAD_INITIALIZER(list);
	struct gc_root *old, *new;
	SLIST_FOREACH(old, &pool->gc_list, link) {
		new = palloc(pool, sizeof(*new));
		memcpy(new, old, sizeof(*new));
		SLIST_INSERT_HEAD(&new_list, new, link);
	}
	memcpy(&pool->gc_list, &new_list, sizeof(new_list));

	struct gc_root *root;
	SLIST_FOREACH(root, &pool->gc_list, link)
		root->copy(pool, root->ptr);

	release_chunks(&old_chunks);
}

struct palloc_cut_point *
palloc_register_cut_point(struct palloc_pool *pool)
{
	struct chunk *chunk = TAILQ_FIRST(&pool->chunks);
	size_t allocated = pool->allocated;
	uint32_t chunk_free = 0;
	struct palloc_cut_point *cut_point;

	if (chunk != NULL)
		chunk_free = chunk->free;
	cut_point = palloc(pool, sizeof(*cut_point));
	cut_point->chunk = chunk;
	cut_point->chunk_free = chunk_free;
	cut_point->allocated = allocated;
	SLIST_INSERT_HEAD(&pool->cut_list, cut_point, link);

	return cut_point;
}

void
palloc_cutoff_to(struct palloc_pool *pool, struct palloc_cut_point *cut_point)
{
	struct chunk *chunk, *next_chunk;
	struct palloc_cut_point *cp;

	if (cut_point == NULL)
		cut_point = SLIST_FIRST(&pool->cut_list);
	if (cut_point == NULL)
		return prelease(pool);

	SLIST_FOREACH(cp, &pool->cut_list, link) {
		if (cp == cut_point)
			break;
	}
	assert(cp != NULL);
	// remove cut point and all previous ones
	SLIST_FIRST(&pool->cut_list) = SLIST_NEXT(cut_point, link);

	if (cut_point->chunk == NULL) {
		assert(SLIST_EMPTY(&pool->cut_list));
		return prelease(pool);
	}

	TAILQ_FOREACH_SAFE(chunk, &pool->chunks, link, next_chunk) {
		if (chunk == cut_point->chunk)
			break;

		TAILQ_REMOVE(&pool->chunks, chunk, link);
		release_chunk(chunk);
	}
	assert(chunk == cut_point->chunk);
	assert(chunk == TAILQ_FIRST(&pool->chunks));

	assert(cut_point->chunk_free >= chunk->free);
	chunk->brk -= cut_point->chunk_free - chunk->free;
	chunk->free = cut_point->chunk_free;
	pool->allocated = cut_point->allocated;

	chunk_poison(chunk);
	VALGRIND_MEMPOOL_TRIM(chunk, (void *)chunk + sizeof(*chunk), chunk->data_size - chunk->free);
}

void
palloc_cutoff(struct palloc_pool *pool)
{
	return palloc_cutoff_to(pool, NULL);
}

#ifdef OCTOPUS
void
palloc_stat_info(struct tbuf *buf)
{
	struct chunk_class *class;
	struct chunk *chunk;
	struct palloc_pool *pool;
	int chunks[nelem(classes)];

	tbuf_printf(buf, "palloc statistic:" CRLF);
	tbuf_printf(buf, "  classes:" CRLF);
	for (int i = 0; i < nelem(classes); i++) {
		class = &classes[i];

		int free_chunks = 0;
		TAILQ_FOREACH(chunk, &class->chunks, link)
		    free_chunks++;

		tbuf_printf(buf,
			    "    - { size: %"PRIu32
			    ", free_chunks: %- 6i, busy_chunks: %- 6i }" CRLF, class->size,
			    free_chunks, class->chunks_count - free_chunks);
	}
	tbuf_printf(buf, "  pools:" CRLF);

	SLIST_FOREACH(pool, &pools, link) {
		for (int i = 0; i < nelem(chunks); i++)
			chunks[i] = 0;

		tbuf_printf(buf, "    - name:  %s\n      alloc: %zu" CRLF,
			    pool->cfg.name, pool->allocated);

		if (pool->allocated > 0) {
			tbuf_printf(buf, "      busy chunks:" CRLF);

			TAILQ_FOREACH(chunk, &pool->chunks, link)
				chunks[chunk->class - &classes[0]]++;

			int indent = 0;
			for (int i = 0; i < nelem(classes); i++) {
				class = &classes[i];
				if (chunks[i] == 0)
					continue;
				tbuf_printf(buf, "        - { size: %"PRIu32", used: %i }" CRLF,
					    class->size, chunks[i]);

				if (indent == 0)
					indent = 19;
			}
		}
	}
}
#endif
const char *
palloc_name(struct palloc_pool *pool, const char *new_name)
{
	const char *old_name = pool->cfg.name;
	if (new_name != NULL)
		pool->cfg.name = new_name;
	return old_name;
}

void *
palloc_ctx(struct palloc_pool *pool, const void *new_ctx)
{
	void *old_ctx = (void *)pool->cfg.ctx;
	if (new_ctx != NULL)
		pool->cfg.ctx = new_ctx;
	return old_ctx;
}

palloc_nomem_cb_t
palloc_nomem_cb(struct palloc_pool *pool, palloc_nomem_cb_t new_cb)
{
	void *old_cb = pool->cfg.nomem_cb;
	if (new_cb != NULL)
		pool->cfg.nomem_cb = new_cb;
	return old_cb;
}

size_t
palloc_size(struct palloc_pool *pool, size_t *size)
{
	size_t old_size = pool->cfg.size;
	if (size != NULL)
		pool->cfg.size = *size;
	return old_size;
}

size_t
palloc_allocated(struct palloc_pool *pool)
{
	return pool->allocated;
}

bool
palloc_owner(struct palloc_pool *pool, void *ptr)
{
	struct chunk *chunk;

	TAILQ_FOREACH(chunk, &pool->chunks, link) {
		void *data_start = (void *)chunk + sizeof(struct chunk);
		void *data_end = data_start + chunk->data_size;
		if (data_start <= ptr && ptr < data_end)
			return true;
	}
	return false;
}
#ifdef OCTOPUS
register_source();
#endif
