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

#ifndef _PALLOC_H_
#define _PALLOC_H_

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

struct palloc_pool;
struct palloc_cut_point;
typedef void (* palloc_nomem_cb_t)(struct palloc_pool *, void *);
struct palloc_config {
	const char *name;
	const void *ctx;

	size_t size;

	palloc_nomem_cb_t nomem_cb;
};

void *palloc(struct palloc_pool *pool, size_t size);
void *prealloc(struct palloc_pool *pool, void *oldptr, size_t oldsize, size_t size);
void *p0alloc(struct palloc_pool *pool, size_t size);
void *palloca(struct palloc_pool *pool, size_t size, size_t align);
void prelease(struct palloc_pool *pool);
void prelease_after(struct palloc_pool *pool, size_t after);
struct palloc_pool *palloc_create_pool(struct palloc_config);
void palloc_destroy_pool(struct palloc_pool *);
void palloc_unmap_unused(void);
const char *palloc_name(struct palloc_pool *, const char *);
void *palloc_ctx(struct palloc_pool *, const void *);
palloc_nomem_cb_t palloc_nomem_cb(struct palloc_pool *, palloc_nomem_cb_t);
size_t palloc_size(struct palloc_pool *, size_t *size);
size_t palloc_allocated(struct palloc_pool *);

void palloc_register_gc_root(struct palloc_pool *pool,
			     void *ptr, void (*copy)(struct palloc_pool *, void *));
void palloc_unregister_gc_root(struct palloc_pool *pool, void *ptr);
void palloc_gc(struct palloc_pool *pool);

struct palloc_cut_point *palloc_register_cut_point(struct palloc_pool *pool);
// cut off to the latest cut point
void palloc_cutoff(struct palloc_pool *pool);
// palloc_cutoff_to(pool, NULL) == palloc_cutoff
void palloc_cutoff_to(struct palloc_pool *pool, struct palloc_cut_point *cut_point);

struct tbuf;
void palloc_stat_info(struct tbuf *buf);
bool palloc_owner(struct palloc_pool *pool, void *ptr);

#endif // _PALLOC_H_
