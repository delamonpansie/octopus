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

#ifndef _SALLOC_H_
#define _SALLOC_H_

#ifdef OCTOPUS
# import <util.h>
# import <tbuf.h>
#endif

#include <stddef.h>
#include <stdint.h>

#if HAVE_THIRD_PARTY_QUEUE_H
# include <third_party/queue.h>
#else
# include "queue.h"
#endif

TAILQ_HEAD(slab_tailq_head, slab);
struct arena;
struct slab_cache {
	size_t item_size;
	struct slab_tailq_head slabs, partial_populated_slabs;
	struct arena *arena;
	const char *name;
	void (*ctor)(void *);
	void (*dtor)(void *);
	SLIST_ENTRY(slab_cache) link;
};

enum arena_type {
	SLAB_FIXED,
	SLAB_GROW
};

enum salloc_error {
	ESALLOC_NOCACHE,
	ESALLOC_NOMEM
};

extern int salloc_error;

void salloc_init(size_t size, size_t minimal, double factor);
void salloc_destroy(void);
void slab_cache_init(struct slab_cache *cache, size_t item_size, enum arena_type type, const char *name);
void *slab_cache_alloc(struct slab_cache *cache);
void slab_cache_free(struct slab_cache *cache, void *ptr);
void *salloc(size_t size);
void sfree(void *ptr);
void slab_validate();
#ifdef OCTOPUS
void slab_stat(struct tbuf *buf);
#endif
void slab_total_stat(uint64_t *bytes_used, uint64_t *items);
void slab_cache_stat(struct slab_cache *cache, uint64_t *bytes_used, uint64_t *items);
struct slab_cache *slab_cache_of_ptr(const void *ptr);
size_t salloc_usable_size(const void *ptr);

#endif // _SALLOC_H_
