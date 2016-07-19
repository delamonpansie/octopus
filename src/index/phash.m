/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
 * Copyright (C) 2014 Sokolov Yurii
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
#import <fiber.h>
#import <iproto.h>
#import <assoc.h>
#import <index.h>
#import <pickle.h>
#include <third_party/qsort_arg.h>
#include <third_party/ptr_hash.h>
#include <stdio.h>

struct hash {
	struct ptr_hash h;
	struct ptr_hash_desc d;
};

@interface PHashImp: Hash {
@public
	struct ptr_hash h;
}
@end

@implementation PHash
+ (PHashImp*) alloc
{
	return [PHashImp alloc];
}
@end

PHashImp *phash_debug;

@implementation PHashImp
- (struct tnt_object *)
find:(const char *)key
{
	switch (conf.field[0].type) {
	case SNUM16:
	case UNUM16: node_a.key.u16 = *(u16 *)key; break;
	case SNUM32:
	case UNUM32: node_a.key.u32 = *(u32 *)key; break;
	case SNUM64:
	case UNUM64: node_a.key.u64 = *(u64 *)key; break;
	case STRING: node_a.key.ptr = key; break;
	default: abort();
	}
	node_a.obj = (void *)(uintptr_t)1; /* cardinality */
	return ph_get_key(&h, (uintptr_t)&node_a, self);
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	return ph_get_key(&h, (uintptr_t)&node_a, self);
}

- (struct tnt_object *)
find_obj:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	return ph_get_key(&h, (uintptr_t)&node_a, self);
}

- (struct tnt_object *)
find_node:(const struct index_node *)node
{
	return ph_get_key(&h, (uintptr_t)node, self);
}

- (void)
resize:(u32)buckets
{
	ph_resize(&h, buckets, self);
}

- (u32)
size
{
	return h.size;
}

- (u32)
slots
{
	return ph_capa(&h);
}

- (size_t)
bytes
{
	return ph_bytes(&h);
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	ph_insert(&h, obj, (uintptr_t)&node_a, self);
}

- (int)
remove:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	return ph_delete_key(&h, (uintptr_t)&node_a, self) != NULL;
}

- (void)
iterator_init
{
	iter = 0;
}

- (struct tnt_object *)
iterator_next
{
	struct tnt_object* obj = NULL;
	while (iter != SIZE_MAX && obj == NULL) {
		obj = ph_iter_fetch(&h, iter);
		iter = ph_iter_next(&h, iter);
	}
	return obj;
}



- (void)
iterator_init_with_object: (struct tnt_object*)obj
{
	dtor(obj, &node_a, dtor_arg);
	iter = ph_get_key_iter(&h, (uintptr_t)&node_a, self);
}

- (void)
iterator_init_with_node: (const struct index_node*)node
{
	iter = ph_get_key_iter(&h, (uintptr_t)node, self);
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	iter = ph_get_key_iter(&h, (uintptr_t)&node_a, self);
}

- (struct tnt_object*)
get:(u32)i
{
	return ph_iter_fetch(&h, i);
}

- (u32)
cardinality
{
	struct index_conf *ic = dtor_arg;
	return ic->cardinality;
}

static u64
hash_key(void *h, u64 key)
{
	PHashImp *ph = h;
	struct index_node *n = (typeof(n))(uintptr_t)key;
	return gen_hash_node(n, &ph->conf);
}

static u64
hash_object(void *h, void *a)
{
	PHashImp *ph = h;
	struct tnt_object *o = (typeof(o))a;
	ph->dtor(o, &ph->search_pattern, ph->dtor_arg);
	return gen_hash_node(&ph->search_pattern, &ph->conf);
}

static int
equal_key(void *h, void *a, u64 key)
{
	PHashImp *ph = h;
	struct tnt_object *o = (typeof(o))a;
	struct index_node *n = (typeof(n))(uintptr_t)key;
	ph->dtor(o, &ph->search_pattern, ph->dtor_arg);
	return ph->eq(n, &ph->search_pattern, &ph->conf);
}

#define KNUTH_MULT 0x5851f42d4c957f2dULL
static u64
hash_key32(void *h _unused_, u64 key)
{
	struct index_node *n = (typeof(n))(uintptr_t)key;
	u64 s = 0xbada5515bad ^ n->key.u32;
	s ^= s >> 11; s ^= s >> 13;
	s *= KNUTH_MULT;
	return s;
}

static u64
hash_object32(void *h, void *a)
{
	PHashImp *ph = h;
	struct tnt_object *o = (typeof(o))a;
	ph->dtor(o, &ph->search_pattern, ph->dtor_arg);
	u64 s = 0xbada5515bad ^ ph->search_pattern.key.u32;
	s ^= s >> 11; s ^= s >> 13;
	s *= KNUTH_MULT;
	return s;
}

static int
equal_key32(void *h, void *a, u64 key)
{
	PHashImp *ph = h;
	struct tnt_object *o = (typeof(o))a;
	struct index_node *n = (typeof(n))(uintptr_t)key;
	ph->dtor(o, &ph->search_pattern, ph->dtor_arg);
	return n->key.u32 == ph->search_pattern.key.u32;
}

static const struct ptr_hash_desc generic = {
	.hash = hash_object,
	.hashKey = hash_key,
	.equalToKey = equal_key
};

static const struct ptr_hash_desc num32 = {
	.hash = hash_object32,
	.hashKey = hash_key32,
	.equalToKey = equal_key32
};

- (id)
init:(struct index_conf*)ic dtor:(const struct dtor_conf*)dc
{
	[super init:ic dtor:dc];
	memset(&h, 0, sizeof(h));
	if (ic->cardinality == 1 && (ic->field[0].type == UNUM32 || ic->field[0].type == SNUM32)) {
		h.desc = &num32;
	} else {
		h.desc = &generic;
	}
	return self;
}
@end
