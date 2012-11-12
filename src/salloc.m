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

#ifdef OCTOPUS
# import <util.h>
# import <tbuf.h>
# import <say.h>
# import <salloc.h>
#else
# include "salloc.h"
#endif

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>
#endif

#if HAVE_VALGRIND_VALGRIND_H
# include <valgrind/valgrind.h>
# include <valgrind/memcheck.h>
#else
# define VALGRIND_MAKE_MEM_DEFINED(_qzz_addr, _qzz_len) (void)0
# define VALGRIND_MAKE_MEM_UNDEFINED(_qzz_addr, _qzz_len) (void)0
# define VALGRIND_MALLOCLIKE_BLOCK(addr, sizeB, rzB, is_zeroed) (void)0
# define VALGRIND_FREELIKE_BLOCK(addr, rzB) (void)0
#endif

#ifndef MMAP_HINT_ADDR
# define MMAP_HINT_ADDR NULL
#endif
#ifndef MAX
# define MAX(a,b) (((a)>(b))?(a):(b))
#endif
#ifndef nelem
# define nelem(x) (sizeof((x))/sizeof((x)[0]))
#endif
#ifndef OCTOPUS
# define panic(x) abort()
# define panic_syserror(x) abort()
# define say_syserror(...) (void)0;
# define say_info(...) (void)0;
#endif

#define SLAB_ALIGN_PTR(ptr) (void *)((uintptr_t)(ptr) & ~(SLAB_SIZE - 1))

#ifdef SLAB_DEBUG
#undef NDEBUG
uint8_t red_zone[4] = { 0xfa, 0xfa, 0xfa, 0xfa };
#else
uint8_t red_zone[0] = { };
#endif

static const uint32_t SLAB_MAGIC = 0x51abface;
static const size_t SLAB_SIZE = 1 << 22;
static const size_t MAX_SLAB_ITEM = 1 << 20;
static const size_t GROW_ARENA_SIZE = 1 << 22;
static size_t page_size;

struct slab_item {
	struct slab_item *next;
};

struct slab {
	uint32_t magic;
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
SLIST_HEAD(slab_cache_head, slab_cache) slab_cache = SLIST_HEAD_INITIALIZER(&slab_cache);

struct arena {
	void *base;
	size_t size;
	size_t used;
	struct slab_slist_head slabs, free_slabs;
	struct { void *base; size_t size; } mmaps[64];
	int i;
};

static uint32_t slab_active_caches;
static struct slab_cache slab_caches[256];
static struct arena arena[2], *fixed_arena = &arena[0], *grow_arena = &arena[1];

static struct slab *
slab_of_ptr(void *ptr)
{
	struct slab *slab = SLAB_ALIGN_PTR(ptr);
	assert(slab->magic == SLAB_MAGIC);
	return slab;
}

void
slab_cache_init(struct slab_cache *cache, size_t item_size, enum arena_type type, const char *name)
{
	assert(item_size <= MAX_SLAB_ITEM);
	assert((item_size & 1) == 0);
	cache->item_size = item_size;
	cache->name = name;

	switch (type) {
	case SLAB_FIXED:
		cache->arena = fixed_arena; break;
	case SLAB_GROW:
		cache->arena = grow_arena; break;
	}

	TAILQ_INIT(&cache->slabs);
	TAILQ_INIT(&cache->partial_populated_slabs);
	if (name)
		SLIST_INSERT_HEAD(&slab_cache, cache, link);
}

static void
slab_caches_init(size_t minimal, double factor)
{
	uint32_t i;
	size_t size;
	const size_t ptr_size = sizeof(void *);

	for (i = 0, size = minimal & ~(ptr_size - 1);
	     i < nelem(slab_caches) - 1 && size <= MAX_SLAB_ITEM;
	     i++)
	{
		slab_cache_init(&slab_caches[i], size - sizeof(red_zone), SLAB_FIXED, NULL);

		size = MAX((size_t)(size * factor) & ~(ptr_size - 1),
			   (size + ptr_size) & ~(ptr_size - 1));
	}
	slab_cache_init(&slab_caches[i], MAX_SLAB_ITEM - sizeof(red_zone), SLAB_FIXED, NULL);
	i++;

	slab_active_caches = i;
}

static void *
mmapa(size_t size, size_t align)
{
	void *ptr, *aptr;
	assert (size % align == 0);

	ptr = mmap(MMAP_HINT_ADDR, size + align, /* add padding for later rounding */
		   PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (ptr == MAP_FAILED) {
		say_syserror("mmap");
		return NULL;
	}

	aptr = (void *)(((uintptr_t)(ptr) & ~(align - 1)) + align);
	size_t pad_begin = aptr - ptr,
		 pad_end = align - pad_begin;

	munmap(ptr, pad_begin);
	ptr += pad_begin;
	munmap(ptr + size, pad_end);
	return ptr;
}

static bool
arena_add_mmap(struct arena *arena, size_t size)
{
	if (arena->i == nelem(arena->mmaps))
		return false;

	void *ptr = mmapa(size, SLAB_SIZE);
	if (!ptr)
		return false;

	arena->used = 0;
	arena->size = arena->mmaps[arena->i].size = size;
	arena->base = arena->mmaps[arena->i].base = ptr;
	arena->i++;
	return true;
}

static bool
arena_init(struct arena *arena, size_t size)
{
	memset(arena, 0, sizeof(*arena));

	if (!arena_add_mmap(arena, size))
		return false;

	SLIST_INIT(&arena->slabs);
	SLIST_INIT(&arena->free_slabs);
	return true;
}

static void *
arena_alloc(struct arena *arena)
{
	void *ptr;
	const size_t size = SLAB_SIZE;

	if (arena->size - arena->used < size) {
		if (arena == fixed_arena)
			return NULL;

		if (!arena_add_mmap(grow_arena, GROW_ARENA_SIZE))
			panic("arena_alloc: can't enlarge grow_arena");

		return arena_alloc(grow_arena);
	}

	ptr = (char *)arena->base + arena->used;
	arena->used += size;

	return ptr;
}

void
salloc_init(size_t size, size_t minimal, double factor)
{
#if HAVE_PAGE_SIZE
	page_size = PAGE_SIZE;
#elif HAVE_SYSCONF
	page_size = sysconf(_SC_PAGESIZE);
#else
	page_size = 0x1000;
#endif
	assert(sizeof(struct slab) <= page_size);

	size -= size % SLAB_SIZE; /* round to size of max slab */
	if (size < SLAB_SIZE * 2)
		size = SLAB_SIZE * 2;


	if (!arena_init(fixed_arena, size))
		panic_syserror("salloc_init: can't initialize arena");

	if (!arena_init(grow_arena, GROW_ARENA_SIZE))
		panic_syserror("salloc_init: can't initialize arena");

	slab_caches_init(MAX(sizeof(void *), minimal), factor);
	say_info("slab allocator configured, fixed_arena:%.1fGB",
		 size / (1024. * 1024 * 1024));
}

void
salloc_destroy(void)
{
	for (uint32_t i = 0; i < nelem(arena); i++) {
		for (uint32_t j = 0; j < nelem(arena->mmaps); j++) {
			if (arena[i].mmaps[j].base == NULL)
				break;
			munmap(arena[i].mmaps[j].base, arena[i].mmaps[j].size);
		}
		memset(&arena[i], 0, sizeof(struct arena));
	}
}

static void
format_slab(struct slab_cache *cache, struct slab *slab)
{
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

	for (uint32_t i = 0; i < nelem(arena); i++) {
		SLIST_FOREACH(slab, &arena[i].slabs, link) {
			for (char *p = (char *)slab + sizeof(struct slab);
			     p + slab->cache->item_size < (char *)slab + SLAB_SIZE;
			     p += slab->cache->item_size + sizeof(red_zone))
			{
				assert(memcmp(p + slab->cache->item_size, red_zone, sizeof(red_zone)) == 0);
			}
		}
	}
}

static struct slab_cache *
cache_for(size_t size)
{
	for (uint32_t i = 0; i < slab_active_caches; i++)
		if (slab_caches[i].item_size >= size)
			return &slab_caches[i];

	return NULL;
}

static struct slab *
slab_of(struct slab_cache *cache)
{
	struct slab *slab;

	slab = TAILQ_LAST(&cache->partial_populated_slabs, slab_tailq_head);
	if (slab != NULL) {
		assert(slab->magic == SLAB_MAGIC);
		return slab;
	}

	if (!SLIST_EMPTY(&cache->arena->free_slabs)) {
		slab = SLIST_FIRST(&cache->arena->free_slabs);
		assert(slab->magic == SLAB_MAGIC);
		SLIST_REMOVE_HEAD(&cache->arena->free_slabs, free_link);
		format_slab(cache, slab);
		return slab;
	}

	if ((slab = arena_alloc(cache->arena)) != NULL) {
		SLIST_INSERT_HEAD(&cache->arena->slabs, slab, link);
		format_slab(cache, slab);
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
slab_cache_alloc(struct slab_cache *cache)
{
	struct slab *slab;
	struct slab_item *item;

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

void *
salloc(size_t size)
{
	struct slab_cache *cache;

	if ((cache = cache_for(size)) == NULL)
		return NULL;

	return slab_cache_alloc(cache);
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
		SLIST_INSERT_HEAD(&cache->arena->free_slabs, slab, free_link);

#ifdef HAVE_MADVISE
		int r;
		r = madvise((void *)slab + page_size, SLAB_SIZE - page_size, MADV_DONTNEED);
		assert(r == 0);
#endif
	}

	VALGRIND_FREELIKE_BLOCK(item, sizeof(red_zone));
}

void
slab_cache_free(struct slab_cache *cache __attribute__((unused)), void *ptr)
{
	sfree(ptr);
}

#ifdef OCTOPUS
static int64_t
cache_stat(struct slab_cache *cache, struct tbuf *out)
{
	struct slab *slab;
	int slabs = 0;
	int64_t items = 0, used = 0, free = 0;

	TAILQ_FOREACH(slab, &cache->slabs, cache_link) {
		free += SLAB_SIZE - slab->used - sizeof(struct slab);
		items += slab->items;
		used += sizeof(struct slab) + slab->used;
		slabs++;
	}

	if (slabs == 0 && cache->name == NULL)
		return 0;

	tbuf_printf(out,
		    "     - { name: %-16s, item_size: %- 5i, slabs: %- 3i, items: %- 11" PRIi64
		    ", bytes_used: %- 12" PRIi64 ", bytes_free: %- 12" PRIi64 " }" CRLF,
		    cache->name, (int)cache->item_size, slabs, items, used, free);

	return used;
}

void
slab_stat(struct tbuf *t)
{
	struct slab *slab;
	struct slab_cache *cache;

	int64_t total_used = 0;
	tbuf_printf(t, "slab statistics:" CRLF);

	tbuf_printf(t, "  arenas:" CRLF);
	for (int i = 0; i < nelem(arena); i++) {
		if (arena[i].size == 0)
			break;
		int free_slabs = 0;
		SLIST_FOREACH(slab, &arena[i].free_slabs, free_link)
			free_slabs++;

		int64_t arena_size = 0;
		for (uint32_t j = 0; j < nelem(arena[i].mmaps); j++)
			arena_size += arena[i].mmaps[j].size;

		tbuf_printf(t, "    - { type: %s, used: %.2f, size: %"PRIi64", free_slabs: %i }" CRLF,
			    &arena[i] == fixed_arena ? "fixed" :
			    &arena[i] == grow_arena ? "grow" : "unknown",
			    (double)arena[i].used / arena_size * 100,
			    arena_size, free_slabs);
	}

	tbuf_printf(t, "  caches:" CRLF);
	for (uint32_t i = 0; i < slab_active_caches; i++)
		total_used += cache_stat(&slab_caches[i], t);

	SLIST_FOREACH(cache, &slab_cache, link)
		cache_stat(cache, t);


	int fixed_free_slabs = 0;
	SLIST_FOREACH(slab, &fixed_arena->free_slabs, free_link)
			fixed_free_slabs++;

	tbuf_printf(t, "  free_slabs: %i" CRLF, fixed_free_slabs);
	tbuf_printf(t, "  items_used: %.2f" CRLF, (double)total_used / fixed_arena->size * 100);
	tbuf_printf(t, "  arena_used: %.2f" CRLF, (double)fixed_arena->used / fixed_arena->size * 100);
}

register_source();
#endif

void
slab_stat2(uint64_t *bytes_used, uint64_t *items)
{
	struct slab *slab;

	*bytes_used = *items = 0;
	for (uint32_t i = 0; i < nelem(arena); i++) {
		SLIST_FOREACH(slab, &arena[i].slabs, link) {
			*bytes_used += slab->used;
			*items += slab->items;
		}
	}
}
