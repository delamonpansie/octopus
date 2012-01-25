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

#import <config.h>
#import <util.h>
#import <palloc.h>
#import <say.h>
#import <tbuf.h>
#import <stat.h>

#include <third_party/valgrind/valgrind.h>
#include <third_party/valgrind/memcheck.h>

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/mman.h>

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
	int i;
	u32 size;
	int chunks_count;
	struct chunk_list_head chunks;
	TAILQ_ENTRY(chunk_class) link;
};

TAILQ_HEAD(class_tailq_head, chunk_class) classes;

struct palloc_pool;
struct gc_root {
	void (*copy)(struct palloc_pool *, void *);
	void *ptr;
};

#define NROOT 8
struct palloc_pool {
	struct chunk_list_head chunks;
	SLIST_ENTRY(palloc_pool) link;
	size_t allocated;
	const char *name;
	struct gc_root gc_root[NROOT], *last_root;
};

SLIST_HEAD(palloc_pool_head, palloc_pool) pools;

static int class_count;
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

static size_t
palloc_greatest_size(void)
{
	return (1 << 22) - sizeof(struct chunk);
}


static struct chunk_class *
class_init(size_t size)
{
	struct chunk_class *class;

	class = malloc(sizeof(struct chunk_class));
	if (class == NULL)
		return NULL;

	class->i = class_count++;
	class->chunks_count = 0;
	class->size = size;
	SLIST_INIT(&class->chunks);
	TAILQ_INSERT_TAIL(&classes, class, link);

	return class;
}

int
palloc_init(void)
{
	struct chunk_class *class;

#ifdef PALLOC_STAT
	stat_base = stat_register(palloc_stat_strs, palloc_stat_MAX);
#endif

	class_count = 0;
	TAILQ_INIT(&classes);

	/*
	 * since chunks are allocated via mmap
	 * size must by multiply of 4096 minus sizeof(struct chunk)
	 * TODO: should we use sysconf(_SC_PAGESIZE)?
	 */

	for (size_t size = 4096 * 8; size <= 1 << 16; size *= 2) {
		if (class_init(size - sizeof(struct chunk)) == NULL)
			return 0;
	}

	if (class_init(palloc_greatest_size()) == NULL)
		return 0;

	if ((class = class_init(-1)) == NULL)
		return 0;

	TAILQ_NEXT(class, link) = NULL;

	return 1;
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
		class = TAILQ_FIRST(&classes);

	if (class->size == -1)
		class = TAILQ_PREV(class, class_tailq_head, link);

	while (class != NULL && class->size < size)
		class = TAILQ_NEXT(class, link);

	assert(class != NULL);

	chunk = SLIST_FIRST(&class->chunks);
	if (chunk != NULL) {
		SLIST_REMOVE_HEAD(&class->chunks, free_link);
		goto found;
	}

	if (size > palloc_greatest_size()) {
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

void * __attribute__((regparm(2),malloc,optimize("-fomit-frame-pointer")))
palloc(struct palloc_pool *pool, size_t size)
{
	const size_t rz_size = size + PALLOC_REDZONE * 2;
	struct chunk *chunk = SLIST_FIRST(&pool->chunks);
	void *ptr;

#ifdef PALLOC_STAT
	stat_collect(stat_base, PALLOC_CALL, 1);
	stat_collect(stat_base, PALLOC_BYTES, size);
#endif
	assert(size > 0);

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
		if (chunk->data_size <= palloc_greatest_size()) {
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
	pool->last_root = pool->gc_root;
	pool->last_root->ptr = (void *)1;
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
	struct chunk_class *class;
	struct chunk *chunk, *next_chunk;

	TAILQ_FOREACH(class, &classes, link) {
		SLIST_FOREACH_SAFE(chunk, &class->chunks, free_link, next_chunk)
			munmap(chunk, class->size + sizeof(struct chunk));
		SLIST_INIT(&class->chunks);
	}
}

void
palloc_register_gc_root(struct palloc_pool *pool,
			void *ptr, void (*copy)(struct palloc_pool *, void *))
{
	assert(pool->last_root < &pool->gc_root[NROOT]);

	pool->last_root++;
	pool->last_root->ptr = ptr;
	pool->last_root->copy = copy;
}

void
palloc_unregister_gc_root(struct palloc_pool *pool, void *ptr)
{
	assert(pool->gc_root + NROOT + 1 < pool->last_root);

	while (pool->last_root->ptr != ptr)
		pool->last_root--;

	while (pool->last_root->ptr != NULL) {
		*pool->last_root = *(pool->last_root + 1);
		pool->last_root++;
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
		VALGRIND_MAKE_MEM_DEFINED((void *)chunk + sizeof(struct chunk),  chunk->data_size);
#endif

	for(int i = 1; i < nelem(pool->gc_root); i++) {
		if (pool->gc_root[i].ptr == NULL)
			break;

		pool->gc_root[i].copy(pool, pool->gc_root[i].ptr);
	}

	release_chunks(&old_chunks);
	say_debug("palloc_gc survived %zi bytes", pool->allocated);
}

void
palloc_stat(struct tbuf *buf)
{
	struct chunk_class *class;
	struct chunk *chunk;
	struct palloc_pool *pool;
	int chunks[class_count];

	tbuf_printf(buf, "palloc statistic:" CRLF);
	tbuf_printf(buf, "  classes:" CRLF);
	TAILQ_FOREACH(class, &classes, link) {
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
		for (int i = 0; i < class_count; i++)
			chunks[i] = 0;

		tbuf_printf(buf, "    - name:  %s\n      alloc: %zu" CRLF,
			    pool->name, pool->allocated);

		if (pool->allocated > 0) {
			tbuf_printf(buf, "      busy chunks:" CRLF);

			SLIST_FOREACH(chunk, &pool->chunks, busy_link)
			    chunks[chunk->class->i]++;

			int indent = 0;
			TAILQ_FOREACH(class, &classes, link) {
				if (chunks[class->i] == 0)
					continue;
				tbuf_printf(buf, "        - { size: %"PRIu32", used: %i }" CRLF,
					    class->size, chunks[class->i]);

				if (indent == 0)
					indent = 19;
			}
		}
	}
}

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
