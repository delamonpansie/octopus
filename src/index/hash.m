/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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
#import <index.h>
#import <pickle.h>
#include <third_party/qsort_arg.h>


typedef const void* lstr;
typedef const void* cstr;

/* All hashes use same layout
   {
      mh_val_t obj;
      mh_key_t key;
   }
*/

#define MH_INCREMENTAL_RESIZE 1
#define MH_STATIC 1

#define mh_var_slot(h,i) ((typeof((h)->slots))((char *)(h)->slots + (i) * mh_slot_size(h)))

#define mh_may_skip 1
#define mh_name _i32
#define mh_slot_t struct index_node
#define mh_slot_key(h, slot) (slot)->key.u32
#define mh_slot_val(slot) (slot)->obj
#define mh_slot_size(h) (sizeof(void *) + sizeof(u32))
#define mh_slot mh_var_slot
#include <mhash.h>
#undef mh_slot_t
#undef mh_slot_key

#define mh_name _i64
#define mh_slot_t struct index_node
#define mh_slot_key(h, slot) (slot)->key.u64
#define mh_slot_val(slot) (slot)->obj
#define mh_slot_size(h) (sizeof(void *) + sizeof(u64))
#define mh_slot mh_var_slot
#define mh_hash(h, a) ({ (uint32_t)((a)>>33^(a)^(a)<<11); })
#include <mhash.h>


#include <third_party/murmur_hash2.c>
#define mh_name _cstr
#define mh_slot_t struct index_node
#define mh_slot_key(h, slot) (slot)->key.ptr
#define mh_slot_val(slot) (slot)->obj
#define mh_slot_size(h) (sizeof(void *) + sizeof(void *))
#define mh_slot mh_var_slot
#define mh_hash(h, key) ({ MurmurHash2((key), strlen(key), 13); })
#define mh_eq(h, a, b) ({ strcmp((a), (b)) == 0; })
#include <mhash.h>


@implementation Hash
- (void)
iterator_init
{
	iter = 0;
}

@end

#define DEFINE_METHODS(type)						\
- (id)									\
init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc          \
{									\
	[super init:ic dtor:dc];					\
	h = mh_##type##_init(xrealloc);					\
	return self;							\
}									\
- (void)								\
clear									\
{									\
	mh_##type##_clear(h);						\
}									\
- (id)									\
free									\
{									\
	mh_##type##_destroy(h);						\
	return [super free];						\
}									\
- (struct tnt_object *)							\
get:(u32)i								\
{									\
	if (i > mh_end(h) || !mh_##type##_slot_occupied(h, i))		\
		return NULL;						\
	return mh_##type##_value(h, i);					\
}									\
- (void)								\
resize:(u32)buckets							\
{									\
	mh_##type##_start_resize(h, buckets);				\
}									\
- (struct tnt_object *)							\
find_obj:(struct tnt_object *)obj					\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
	u32 k = mh_##type##_sget(h, (void *)node_);			\
	if (k != mh_end(h)) 						\
		return mh_##type##_value(h, k);				\
	return NULL;							\
}									\
- (struct tnt_object *)							\
find_node:(const struct index_node *)node				\
{									\
	u32 k = mh_##type##_sget(h, (void *)node);			\
	if (k != mh_end(h)) 						\
		return mh_##type##_value(h, k);				\
	return NULL;							\
}									\
- (void)								\
replace:(struct tnt_object *)obj					\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
        mh_##type##_sput(h, (void *)node_, NULL);			\
}									\
- (int)									\
remove:(struct tnt_object *)obj						\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
	u32 k = mh_##type##_sget(h, (void *)node_);			\
	if (k != mh_end(h)) {						\
		node_->obj = NULL;					\
		mh_##type##_del(h, k);					\
		return 1;						\
	}								\
	return 0;							\
}									\
- (void)								\
iterator_init_with_object:(struct tnt_object *)obj			\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
	iter = mh_##type##_sget(h, (void *)node_);			\
}									\
- (void)								\
iterator_init_with_node:(const struct index_node *)node			\
{									\
	iter = mh_##type##_sget(h, (void *)node);			\
}									\
- (struct tnt_object *)							\
iterator_next								\
{									\
	for (; iter < mh_end(h); iter++) {				\
		if (!mh_##type##_slot_occupied(h, iter))		\
			continue;					\
		return mh_##type##_value(h, iter++);			\
	}								\
	return NULL;							\
}									\
- (void)								\
ordered_iterator_init							\
{									\
	int j = 0;							\
	[self iterator_init];						\
	/* assert(j + 1 == ..) assumes that hash has at least one elem */ \
	if (mh_size(h) == 0)						\
		return;							\
	for (int i = 1; i < mh_end(h); i++) {				\
		if (!mh_##type##_slot_occupied(h, i))			\
			continue;					\
		while (mh_##type##_slot_occupied(h, j) && j < i)	\
			j++;						\
		if (j == i)						\
			continue;					\
		assert(!mh_##type##_slot_occupied(h, j));		\
		mh_##type##_slot_move(h, j, i);				\
	}								\
	assert(j + 1 == mh_size(h));					\
	qsort_arg(h->slots, mh_size(h), node_size, compare, self);	\
}									\
- (u32) size { return mh_size(h); }					\
- (u32) slots { return mh_end(h); }					\
- (size_t) bytes { return mh_##type##_bytes(h); }


@implementation Int32Hash
DEFINE_METHODS(i32)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node node_b;
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return na->key.u32 == nb->key.u32;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
		index_raise("hashed key has cardinality != 1");
	u32 key_size = read_u8(key_data); /* key_size is actually varint */
	if (key_size != sizeof(i32))
		index_raise("key is not i32");

	i32 num = read_u32(key_data);

        u32 k = mh_i32_get(h, num);
        if (k != mh_end(h))
		return mh_i32_value(h, k);
	return NULL;
}

- (struct tnt_object *)
find:(const char *)key
{
	i32 num = *(i32 *)key;
	u32 k = mh_i32_get(h, num);
	if (k != mh_end(h))
		return mh_i32_value(h, k);
	return NULL;
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	if (cardinality != 1)
		index_raise("cardinality too big");
	u32 len = read_varint32(key_data);
	if (len != sizeof(i32))
		index_raise("key is not u32");
	iter = mh_i32_get(h, read_u32(key_data));
}

@end

@implementation Int64Hash
DEFINE_METHODS(i64)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node node_b;
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return na->key.u64 == nb->key.u64;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
                index_raise("hashed key has cardinality != 1");
	u32 key_size = read_u8(key_data); /* key_size is actually varint */
	if (key_size != sizeof(i64))
		index_raise("key is not i64");

	i64 num = read_u64(key_data);

        u32 k = mh_i64_get(h, num);
        if (k != mh_end(h))
		return mh_i64_value(h, k);
	return NULL;
}

- (struct tnt_object *)
find:(const char *)key
{
	i64 num = *(i64 *)key;
	u32 k = mh_i64_get(h, num);
	if (k != mh_end(h))
		return mh_i64_value(h, k);
	return NULL;
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	if (cardinality != 1)
		index_raise("cardinality too big");
	u32 len = read_varint32(key_data);
	if (len != sizeof(i64))
		index_raise("key is not i64");
	iter = mh_i64_get(h, read_u64(key_data));
}

@end

@implementation CStringHash
DEFINE_METHODS(cstr)

- (id)
init:(void *)ic
{
	[super init];
	(void)ic;
	h = mh_cstr_init(xrealloc);
	node_size = sizeof(struct tnt_object *) + sizeof(cstr);
	compare = (index_cmp)cstr_compare;
	return self;
}

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node node_b;
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return strcmp(na->key.ptr, nb->key.ptr) == 0;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
                index_raise("hashed key has cardinality != 1");

	void *f = read_field(key_data);
        u32 k = mh_cstr_get(h, f);
        if (k != mh_end(h))
		return mh_cstr_value(h, k);
	return NULL;
}

- (struct tnt_object *)
find:(const char *)key
{
	u32 k = mh_cstr_get(h, key);
	if (k != mh_end(h))
		return mh_cstr_value(h, k);
	return NULL;
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	if (cardinality != 1)
		index_raise("cardinality too big");
	iter = mh_cstr_get(h, read_field(key_data));
}

@end

#define mh_byte_map 1
#define mh_may_skip 1
#define mh_name _gen
#define mh_slot_t tnt_ptr
#define mh_arg_t GenHash*
static const struct index_node* gen_hash_slot_key(struct mh_gen_t const * h, tnt_ptr const * slot);
#define mh_slot_key(h, slot) gen_hash_slot_key(h, slot)
#define mh_slot_key_eq(h, i, key) ({ \
		GenHash *hs = (h)->arg; \
		hs->dtor(tnt_ptr2obj(*mh_slot(h, i)), &hs->search_pattern, hs->dtor_arg); \
		hs->eq(key, &hs->search_pattern, &hs->conf); \
		})
#define mh_slot_set_key(h, slot, key)
#define mh_hash(h, key) ({ gen_hash_node((key), &(h)->arg->conf); })
#include <mhash.h>

static const struct index_node*
gen_hash_slot_key(struct mh_gen_t const * h, tnt_ptr const * slot)
{
	GenHash *hs = (h)->arg;
	hs->dtor(tnt_ptr2obj(*slot), &hs->node_a, hs->dtor_arg);
	return &hs->node_a;
}

@implementation GenHash
- (id)
init:(struct index_conf*)ic dtor:(const struct dtor_conf*)dc
{
	[super init:ic dtor:dc];
	h = mh_gen_init(xrealloc);
	h->arg = self;
	return self;
}
- (void)
clear
{
	mh_gen_clear(h);
}
- (id)
free
{
	mh_gen_destroy(h);
	return [super free];
}
- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node node_b;
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return eq(na, nb, self->dtor_arg);
}

- (struct tnt_object*)
get:(u32)i
{
	if (i > mh_end(h) || !mh_gen_slot_occupied(h, i)) {
		return NULL;
	}
	return tnt_ptr2obj(*mh_gen_slot(h, i));
}
- (void)
resize:(u32)buckets
{
	mh_gen_start_resize(h, buckets);
}
- (struct tnt_object*)
find_obj:(struct tnt_object*)obj
{
	tnt_ptr p = tnt_obj2ptr(obj);
	u32 k = mh_gen_sget(h, &p);
	if (k != mh_end(h))
		return tnt_ptr2obj(*mh_gen_slot(h, k));
	return NULL;
}
- (struct tnt_object*)
find_node:(const struct index_node *)node
{
	u32 k = mh_gen_sget_by_key(h, node);
	if (k != mh_end(h))
		return tnt_ptr2obj(*mh_gen_slot(h, k));
	return NULL;
}
- (void)
replace:(struct tnt_object *)obj
{
	tnt_ptr p = tnt_obj2ptr(obj);
	mh_gen_sput(h, &p, NULL);
}
- (int)
remove:(struct tnt_object *)obj
{
	tnt_ptr p = tnt_obj2ptr(obj);
	return mh_gen_sremove(h, &p, NULL);
}
- (void)
iterator_init_with_object:(struct tnt_object*)obj
{
	tnt_ptr p = tnt_obj2ptr(obj);
	iter = mh_gen_sget(h, &p);
}
- (void)
iterator_init_with_node:(const struct index_node*)node
{
	iter = mh_gen_sget_by_key(h, node);
}
- (struct tnt_object*)
iterator_next
{
	for (; iter < mh_end(h); iter++) {
		if (!mh_gen_slot_occupied(h, iter))
			continue;
		return tnt_ptr2obj(*mh_gen_slot(h, iter++));
	}
	return NULL;
}
- (void)
ordered_iterator_init
{
	int i = 0;
	[self iterator_init];
	/* assert(j + 1 == ..) assumes that hash has at least one elem */
	if (mh_size(h) == 0)
		return;
	char *slots = xcalloc(mh_size(h), node_size);
	char *current = slots;
	for (i = 0; i < mh_end(h); i++) {
		if (!mh_gen_slot_occupied(h, i))
			continue;

		dtor(tnt_ptr2obj(*mh_gen_slot(h, i)), (struct index_node*)current, dtor_arg);
		current += node_size;
	}
	qsort_arg(slots, mh_size(h), node_size, compare, self);
	current = slots;
	for (i = 0; i < mh_size(h); i++) {
		tnt_ptr ptr = tnt_obj2ptr(*(void**)current);
		mh_gen_hijack_slot_put(h, i, &ptr);
		current += node_size;
	}
	for (; i < mh_end(h); i++) {
		mh_gen_hijack_slot_free(h, i);
	}
	free(slots);
}
- (u32) size { return mh_size(h); }
- (u32) slots { return mh_end(h); }
- (size_t) bytes { return mh_gen_bytes(h); }

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
	return [self find_node: &node_a];
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	if (cardinality != conf.cardinality)
		index_raise("cardinality should match");
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	return [self find_node: &node_a];
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	if (cardinality != conf.cardinality)
		index_raise("cardinality should match");
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	[self iterator_init_with_node: &node_a];
}
@end
