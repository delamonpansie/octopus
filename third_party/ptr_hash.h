#ifndef PTR_HASH_H
#define PTR_HASH_H
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

struct ptr_hash;
struct ptr_hash_desc {
	uint64_t (*hash)      (void *arg, void* ptr);
	uint64_t (*hashKey)  (void *arg, uint64_t key);
	int      (*equalToKey)(void *arg, void* ptr, uint64_t key);
};

struct ptr_hash {
	struct ptr_hash_desc const *desc;
	struct ptr_bucket *buckets;
	size_t size, capa;
	size_t watermark, border;
	size_t backoff, maxwatermark;
	uint32_t rand;
	uint8_t reinsert_backoff_after;
};

/* key pattern match */
void* ph_get_key(struct ptr_hash *hash, uint64_t key, void *arg);
void* ph_delete_key(struct ptr_hash *hash, uint64_t key, void *arg);
/* returns previous value, if any */
void* ph_insert(struct ptr_hash *hash, void *obj, uint64_t key, void *arg);

void  ph_destroy(struct ptr_hash *hash, void *arg);
void  ph_resize(struct ptr_hash *hash, size_t size, void *arg);

/* exact match */
size_t ph_get_iter(struct ptr_hash *hash, void *obj, void *arg);
/* key pattern match */
size_t ph_get_key_iter(struct ptr_hash *hash, uint64_t key, void *arg);
size_t ph_iter_first(struct ptr_hash *hash);
size_t ph_iter_next(struct ptr_hash *hash, size_t i);
void* ph_iter_fetch(struct ptr_hash *hash, size_t i);
#define ph_iter_end(i) ((i) == SIZE_MAX)
#define ph_foreach(h, i) \
	for(size_t (i) = ph_iter_first(h); !ph_iter_end(i); (i) = ph_iter_next(h, (i)))

size_t ph_capa(struct ptr_hash *hash);
size_t ph_bytes(struct ptr_hash *hash);

#endif
