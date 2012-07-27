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

#ifdef PALLOC_STAT_INFO
# import <util.h>
# import <palloc.h>
# import <say.h>
# import <tbuf.h>
# import <stat.h>
#endif

#if HAVE_VALGRIND_H
# include <valgrind/valgrind.h>
# include <valgrind/memcheck.h>
#elif HAVE_THIRD_PARTY_VALGRIND_H
# include <third_party/valgrind/valgrind.h>
# include <third_party/valgrind/memcheck.h>
#else
# define VALGRIND_MAKE_MEM_DEFINED(_qzz_addr,_qzz_len) 0
# define VALGRIND_MAKE_MEM_NOACCESS(_qzz_addr,_qzz_len) 0
# define VALGRIND_MAKE_MEM_UNDEFINED(_qzz_addr,_qzz_len) 0
# define VALGRIND_CREATE_MEMPOOL(pool, rzB, is_zeroed)
# define VALGRIND_DESTROY_MEMPOOL(pool)
# define VALGRIND_MEMPOOL_ALLOC(pool, addr, size)
# define VALGRIND_MEMPOOL_TRIM(pool, addr, size)
#endif

#if HAVE_QUEUE_H
# include "queue.h"
#elif HAVE_THIRD_PARTY_QUEUE_H
# include <third_party/queue.h>
#endif

#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/mman.h>

#ifndef likely
# if HAVE__BUILTIN_EXPECT
#  define likely(x)	__builtin_expect((x),1)
# else
#  define likely(x)	(x)
# endif
#endif

#ifndef nelem
# define nelem(x)     (sizeof((x))/sizeof((x)[0]))
#endif

#ifndef TYPEALIGN
# define TYPEALIGN(ALIGNVAL,LEN)  \
        (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#endif

#ifdef PALLOC_STAT
#include <stat.h>
#define STAT(_)					\
        _(PALLOC_CALL, 1)			\
	_(PALLOC_BYTES, 2)

ENUM(palloc_stat, STAT);
STRS(palloc_stat, STAT);
int stat_base;
#endif

struct chunk {
	uint32_t magic;
	void *brk;
	size_t free;
	size_t data_size;

	struct chunk_class *class;
	SLIST_ENTRY(chunk) busy_link;
	SLIST_ENTRY(chunk) free_link;
};

SLIST_HEAD(chunk_list_head, chunk);

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

struct palloc_pool {
	struct chunk_list_head chunks;
	SLIST_ENTRY(palloc_pool) link;
	size_t allocated;
	const char *name;
	struct gc_list gc_list;
};

SLIST_HEAD(palloc_pool_head, palloc_pool) pools;

#define CHUNK_SIZE(kb) (kb * 1024 - sizeof(struct chunk))
#define CHUNK_CLASS(kb) { .size = CHUNK_SIZE(kb) }
static struct chunk_class classes[] = {
	CHUNK_CLASS(32),
	CHUNK_CLASS(64),
	CHUNK_CLASS(128),
	CHUNK_CLASS(256),
	CHUNK_CLASS(512),
	CHUNK_CLASS(1024),
	CHUNK_CLASS(2048),
	CHUNK_CLASS(4096),

	{ .size = -1 } /* malloc fallback */
};

const uint32_t chunk_magic = 0xbb84fcf6;
static const char poison_char = 'P';

#ifdef REDZONE
#define PALLOC_REDZONE 4
#endif
#ifndef PALLOC_REDZONE
#define PALLOC_REDZONE 0
#endif
#ifdef POISON
#define PALLOC_POISON
#endif

void
palloc_init(void)
{
#ifdef PALLOC_STAT
	stat_base = stat_register(palloc_stat_strs, palloc_stat_MAX);
#endif

}

static void
poison_chunk(struct chunk *chunk)
{
	(void)chunk;
	assert(chunk->magic == chunk_magic);
#ifdef PALLOC_POISON
	(void)VALGRIND_MAKE_MEM_DEFINED((void *)chunk + sizeof(struct chunk), chunk->data_size);
	memset((void *)chunk + sizeof(struct chunk), poison_char, chunk->data_size);
#endif
	(void)VALGRIND_MAKE_MEM_NOACCESS((void *)chunk + sizeof(struct chunk), chunk->data_size);
}

static struct chunk *
next_chunk_for(struct palloc_pool *pool, size_t size)
{
	struct chunk * chunk = SLIST_FIRST(&pool->chunks);
	struct chunk_class *class;
	size_t chunk_size;

	if (chunk != NULL)
		class = chunk->class;
	else
		class = &classes[0];

	if (class->size == (uint32_t)-1)
		class--;

	while (class->size < size)
		class++;

	chunk = SLIST_FIRST(&class->chunks);
	if (chunk != NULL) {
		SLIST_REMOVE_HEAD(&class->chunks, free_link);
		goto found;
	}

	if (class->size == (uint32_t)-1) {
		chunk_size = size;
		chunk = malloc(sizeof(struct chunk) + chunk_size);
		if (chunk == NULL)
			return NULL;
	} else {
		chunk_size = class->size;
		chunk = mmap(NULL, sizeof(struct chunk) + chunk_size,
			     PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (chunk == MAP_FAILED)
			return NULL;
	}

	class->chunks_count++;
	chunk->magic = chunk_magic;
	chunk->data_size = chunk_size;
	chunk->free = chunk_size;
	chunk->brk = (void *)chunk + sizeof(struct chunk);
	chunk->class = class;

	poison_chunk(chunk);
found:
	assert(chunk != NULL && chunk->magic == chunk_magic);
	SLIST_INSERT_HEAD(&pool->chunks, chunk, busy_link);
	pool->allocated += chunk->data_size;
	return chunk;
}

void * __attribute__((regparm(2),noinline))
palloc_slow_path(struct palloc_pool *pool, size_t size)
{
	struct chunk *chunk;
	chunk = next_chunk_for(pool, size);
	if (chunk == NULL)
		abort();

	assert(chunk->free >= size);
	void *ptr = chunk->brk;
	chunk->brk += size;
	chunk->free -= size;
	return ptr;
}

void * __attribute__((regparm(2),malloc))
palloc(struct palloc_pool *pool, size_t size)
{
	const size_t rz_size = size + PALLOC_REDZONE * 2;
	struct chunk *chunk = SLIST_FIRST(&pool->chunks);
	void *ptr;

#ifdef PALLOC_STAT
	stat_collect(stat_base, PALLOC_CALL, 1);
	stat_collect(stat_base, PALLOC_BYTES, size);
#endif

	if (likely(chunk != NULL && chunk->free >= rz_size)) {
		ptr = chunk->brk;
		chunk->brk += rz_size;
		chunk->free -= rz_size;
	} else {
		ptr = palloc_slow_path(pool, rz_size);
	}

#if !defined(NDEBUG) && defined(PALLOC_POISON)
	const char *data_byte = ptr + PALLOC_REDZONE;
	(void)VALGRIND_MAKE_MEM_DEFINED(data_byte, size);
	for (int i = 0; i < size; i++)
		assert(data_byte[i] == poison_char);
	(void)VALGRIND_MAKE_MEM_UNDEFINED(data_byte, size);
#endif

	VALGRIND_MEMPOOL_ALLOC(pool, ptr + PALLOC_REDZONE, size);
	return ptr + PALLOC_REDZONE;
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
release_chunks(struct chunk_list_head *chunks)
{
	struct chunk *chunk, *next_chunk;

	for (chunk = SLIST_FIRST(chunks); chunk != NULL; chunk = next_chunk) {
		next_chunk = SLIST_NEXT(chunk, busy_link);

		(void)VALGRIND_MAKE_MEM_UNDEFINED((void *)chunk + sizeof(struct chunk),  chunk->data_size);
		if (chunk->class->size != (uint32_t)-1) {
			chunk->free = chunk->data_size;
			chunk->brk = (void *)chunk + sizeof(struct chunk);
			SLIST_INSERT_HEAD(&chunk->class->chunks, chunk, free_link);
			poison_chunk(chunk);
		} else {
			free(chunk);
		}
	}
}

void
prelease(struct palloc_pool *pool)
{
	release_chunks(&pool->chunks);
	SLIST_INIT(&pool->chunks);
	VALGRIND_MEMPOOL_TRIM(pool, NULL, 0);
	pool->allocated = 0;
}

void
prelease_after(struct palloc_pool *pool, size_t after)
{
	if (pool->allocated > after)
		prelease(pool);
}

struct palloc_pool *
palloc_create_pool(const char *name)
{
	struct palloc_pool *pool = malloc(sizeof(struct palloc_pool));
	assert(pool != NULL);
	memset(pool, 0, sizeof(*pool));
	pool->name = name;
	SLIST_INIT(&pool->chunks);
	SLIST_INSERT_HEAD(&pools, pool, link);
	VALGRIND_CREATE_MEMPOOL(pool, PALLOC_REDZONE, 0);
	return pool;
}

void
palloc_destroy_pool(struct palloc_pool *pool)
{
	SLIST_REMOVE(&pools, pool, palloc_pool, link);
	prelease(pool);
	VALGRIND_DESTROY_MEMPOOL(pool);
	free(pool);
}

void
palloc_unmap_unused(void)
{
	for (uint32_t i = 0; i < nelem(classes); i++) {
		struct chunk_class *class = &classes[i];
		struct chunk *chunk, *next_chunk;

		SLIST_FOREACH_SAFE(chunk, &class->chunks, free_link, next_chunk)
			munmap(chunk, class->size + sizeof(struct chunk));
		SLIST_INIT(&class->chunks);
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

	SLIST_INIT(&pool->chunks);
	pool->allocated = 0;
	VALGRIND_MEMPOOL_TRIM(pool, NULL, 0);

#ifndef NVALGRIND
	struct chunk *chunk;
	SLIST_FOREACH(chunk, &old_chunks, busy_link)
		(void)VALGRIND_MAKE_MEM_DEFINED((void *)chunk + sizeof(struct chunk),  chunk->data_size);
#endif

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

#ifdef PALLOC_STAT_INFO
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
		SLIST_FOREACH(chunk, &class->chunks, free_link)
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
			    pool->name, pool->allocated);

		if (pool->allocated > 0) {
			tbuf_printf(buf, "      busy chunks:" CRLF);

			SLIST_FOREACH(chunk, &pool->chunks, busy_link)
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
	const char *old_name = pool->name;
	if (new_name != NULL)
		pool->name = new_name;
	return old_name;
}

size_t
palloc_allocated(struct palloc_pool *pool)
{
	return pool->allocated;
}

bool
palloc_owner(struct palloc_pool *pool, void *ptr)
{
	struct chunk *chunk, *next_chunk;

	for (chunk = SLIST_FIRST(&pool->chunks); chunk != NULL; chunk = next_chunk) {
		next_chunk = SLIST_NEXT(chunk, busy_link);

		void *data_start = (void *)chunk + sizeof(struct chunk);
		void *data_end = data_start + chunk->data_size;
		if (data_start <= ptr && ptr < data_end)
			return true;
	}
	return false;
}

register_source();
