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
#import <salloc.h>
#import <tbuf.h>
#import <say.h>

#include <third_party/valgrind/valgrind.h>
#include <third_party/valgrind/memcheck.h>
#include <third_party/queue.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

#define SLAB_ALIGN_PTR(ptr) (void *)((uintptr_t)(ptr) & ~(SLAB_SIZE - 1))

#ifdef SLAB_DEBUG
#undef NDEBUG
u8 red_zone[4] = { 0xfa, 0xfa, 0xfa, 0xfa };
#else
u8 red_zone[0] = { };
#endif

const u32 SLAB_MAGIC = 0x51abface;
const size_t SLAB_SIZE = 1 << 22;
const size_t MAX_SLAB_ITEM = 1 << 20;

struct slab_item {
	struct slab_item *next;
};

struct slab {
	u32 magic;
	size_t used;
	size_t items;
	struct slab_item *free;
	struct slab_cache *cache;
	void *brk;
	SLIST_ENTRY(slab) link;
	SLIST_ENTRY(slab) free_link;
	TAILQ_ENTRY(slab) cache_partial_link;
	TAILQ_ENTRY(slab) cache_link;
};

SLIST_HEAD(slab_slist_head, slab);
TAILQ_HEAD(slab_tailq_head, slab);

struct slab_cache {
	size_t item_size;
	struct slab_tailq_head slabs, partial_populated_slabs;
};

struct arena {
	void *base;
	size_t size;
	size_t used;
};

size_t slab_active_caches;
struct slab_cache slab_caches[256];
struct arena arena;

struct slab_slist_head slabs, free_slabs;

static struct slab *
slab_of_ptr(void *ptr)
{
	struct slab *slab = SLAB_ALIGN_PTR(ptr);
	assert(slab->magic == SLAB_MAGIC);
	return slab;
}

void
slab_cache_init(struct slab_cache *cache, size_t item_size)
{
	cache->item_size = item_size;
	assert((cache->item_size & 1) == 0);

	TAILQ_INIT(&cache->slabs);
	TAILQ_INIT(&cache->partial_populated_slabs);
}

static void
slab_caches_init(size_t minimal, double factor)
{
	int i, size;
	const size_t ptr_size = sizeof(void *);

	for (i = 0, size = minimal & ~(ptr_size - 1);
	     i < nelem(slab_caches) - 1 && size <= MAX_SLAB_ITEM;
	     i++)
	{
		slab_cache_init(&slab_caches[i], size - sizeof(red_zone));

		size = MAX((size_t)(size * factor) & ~(ptr_size - 1),
			   (size + ptr_size) & ~(ptr_size - 1));
	}
	slab_cache_init(&slab_caches[i], MAX_SLAB_ITEM - sizeof(red_zone));
	i++;

	SLIST_INIT(&slabs);
	SLIST_INIT(&free_slabs);
	slab_active_caches = i;
}

static bool
arena_init(struct arena *arena, size_t size)
{
	size -= size % SLAB_SIZE; /* round to size of max slab */
	arena->used = 0;
	arena->size = size;

	arena->base = mmap(NULL, arena->size + SLAB_SIZE, /* add padding for later rounding */
			   PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (arena->base == MAP_FAILED) {
		say_syserror("mmap");
		return false;
	}

	void *aptr = SLAB_ALIGN_PTR(arena->base) + SLAB_SIZE;
	size_t pad_begin = aptr - arena->base,
		 pad_end = SLAB_SIZE - pad_begin;

	munmap(arena->base, pad_begin);
	arena->base += pad_begin;
	munmap(arena->base + arena->size, pad_end);
	return true;
}

static void *
arena_alloc(struct arena *arena)
{
	void *ptr;
	const size_t size = SLAB_SIZE;

	if (arena->size - arena->used < size)
		return NULL;

	ptr = (char *)arena->base + arena->used;
	arena->used += size;

	return ptr;
}

void
salloc_init(size_t size, size_t minimal, double factor)
{
	if (size < SLAB_SIZE * 2)
		panic("salloc_init: arena size is too small");

	if (!arena_init(&arena, size))
		panic_syserror("salloc_init: can't initialize arena");

	slab_caches_init(MAX(sizeof(void *), minimal), factor);
}

void
salloc_destroy(void)
{
	if (arena.base != NULL)
		munmap(arena.base, arena.size);

	memset(&arena, 0, sizeof(struct arena));
}

static void
format_slab(struct slab_cache *cache, struct slab *slab)
{
	assert(cache->item_size <= MAX_SLAB_ITEM);

	slab->magic = SLAB_MAGIC;
	slab->free = NULL;
	slab->cache = cache;
	slab->items = 0;
	slab->used = 0;
	slab->brk = (void *)CACHEALIGN((void *)slab + sizeof(struct slab));

	TAILQ_INSERT_HEAD(&cache->slabs, slab, cache_link);
	TAILQ_INSERT_HEAD(&cache->partial_populated_slabs, slab, cache_partial_link);
}

static bool
fully_populated(const struct slab *slab)
{
	return slab->brk + slab->cache->item_size >= (void *)slab + SLAB_SIZE &&
	       slab->free == NULL;
}

void
slab_validate(void)
{
	struct slab *slab;

	SLIST_FOREACH(slab, &slabs, link) {
		for (char *p = (char *)slab + sizeof(struct slab);
		     p + slab->cache->item_size < (char *)slab + SLAB_SIZE;
		     p += slab->cache->item_size + sizeof(red_zone)) {
			assert(memcmp(p + slab->cache->item_size, red_zone, sizeof(red_zone)) == 0);
		}
	}
}

static struct slab_cache *
cache_for(size_t size)
{
	for (int i = 0; i < slab_active_caches; i++)
		if (slab_caches[i].item_size >= size)
			return &slab_caches[i];

	return NULL;
}

static struct slab *
slab_of(struct slab_cache *cache)
{
	struct slab *slab;

	if (!TAILQ_EMPTY(&cache->partial_populated_slabs)) {
		slab = TAILQ_FIRST(&cache->partial_populated_slabs);
		assert(slab->magic == SLAB_MAGIC);
		return slab;
	}

	if (!SLIST_EMPTY(&free_slabs)) {
		slab = SLIST_FIRST(&free_slabs);
		assert(slab->magic == SLAB_MAGIC);
		SLIST_REMOVE_HEAD(&free_slabs, free_link);
		format_slab(cache, slab);
		return slab;
	}

	if ((slab = arena_alloc(&arena)) != NULL) {
		format_slab(cache, slab);
		SLIST_INSERT_HEAD(&slabs, slab, link);
		return slab;
	}

	return NULL;
}

#ifndef NDEBUG
static bool
valid_item(struct slab *slab, void *item)
{
	return (void *)item >= (void *)(slab) + sizeof(struct slab) &&
	    (void *)item < (void *)(slab) + sizeof(struct slab) + SLAB_SIZE;
}
#endif

void *
salloc(size_t size)
{
	struct slab_cache *cache;
	struct slab *slab;
	struct slab_item *item;

	if ((cache = cache_for(size)) == NULL)
		return NULL;

	if ((slab = slab_of(cache)) == NULL)
		return NULL;

	if (slab->free == NULL) {
		assert(valid_item(slab, slab->brk));
		item = slab->brk;
		memcpy((void *)item + cache->item_size, red_zone, sizeof(red_zone));
		slab->brk += cache->item_size + sizeof(red_zone);
	} else {
		assert(valid_item(slab, slab->free));
		item = slab->free;

		(void)VALGRIND_MAKE_MEM_DEFINED(item, sizeof(void *));
		slab->free = item->next;
		(void)VALGRIND_MAKE_MEM_UNDEFINED(item, sizeof(void *));
	}

	if (fully_populated(slab))
		TAILQ_REMOVE(&cache->partial_populated_slabs, slab, cache_partial_link);

	slab->used += cache->item_size + sizeof(red_zone);
	slab->items += 1;

	VALGRIND_MALLOCLIKE_BLOCK(item, cache->item_size, sizeof(red_zone), 0);
	return (void *)item;
}

void
sfree(void *ptr)
{
	assert(ptr != NULL);
	struct slab *slab = slab_of_ptr(ptr);
	struct slab_cache *cache = slab->cache;
	struct slab_item *item = ptr;

	if (fully_populated(slab))
		TAILQ_INSERT_TAIL(&cache->partial_populated_slabs, slab, cache_partial_link);

	assert(valid_item(slab, item));
	assert(slab->free == NULL || valid_item(slab, slab->free));

	item->next = slab->free;
	slab->free = item;
	slab->used -= cache->item_size + sizeof(red_zone);
	slab->items -= 1;

	if (slab->items == 0) {
		TAILQ_REMOVE(&cache->partial_populated_slabs, slab, cache_partial_link);
		TAILQ_REMOVE(&cache->slabs, slab, cache_link);
		SLIST_INSERT_HEAD(&free_slabs, slab, free_link);
	}

	VALGRIND_FREELIKE_BLOCK(item, sizeof(red_zone));
}

void
slab_stat(struct tbuf *t)
{
	struct slab *slab;
	int slabs, free_slabs_count = 0;
	i64 items, used, free, total_used = 0;
	tbuf_printf(t, "slab statistics:\n  caches:" CRLF);
	for (int i = 0; i < slab_active_caches; i++) {
		slabs = items = used = free = 0;
		TAILQ_FOREACH(slab, &slab_caches[i].slabs, cache_link) {
			free += SLAB_SIZE - slab->used - sizeof(struct slab);
			items += slab->items;
			used += sizeof(struct slab) + slab->used;
			total_used += sizeof(struct slab) + slab->used;
			slabs++;
		}

		if (slabs == 0)
			continue;

		tbuf_printf(t,
			    "     - { item_size: %- 5i, slabs: %- 3i, items: %- 11" PRIi64
			    ", bytes_used: %- 12" PRIi64 ", bytes_free: %- 12" PRIi64 " }" CRLF,
			    (int)slab_caches[i].item_size, slabs, items, used, free);

	}

	SLIST_FOREACH(slab, &free_slabs, free_link)
		free_slabs_count++;

	tbuf_printf(t, "  free_slabs: %i" CRLF, free_slabs_count);
	tbuf_printf(t, "  items_used: %.2f" CRLF, (double)total_used / arena.size * 100);
	tbuf_printf(t, "  arena_used: %.2f" CRLF, (double)arena.used / arena.size * 100);
}

void
slab_stat2(u64 *bytes_used, u64 *items)
{
	struct slab *slab;

	*bytes_used = *items = 0;
	SLIST_FOREACH(slab, &slabs, link) {
		*bytes_used += slab->used;
		*items += slab->items;
	}
}

register_source();
