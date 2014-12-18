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

static inline int lstrcmp(const void *a, const void *b)
{
	int al, bl;
	int r;

	al = LOAD_VARINT32(a);
	bl = LOAD_VARINT32(b);

	if (al != bl)
		r = al - bl;
	else
		r = memcmp(a, b, al);

	return r;
}

#include <third_party/murmur_hash2.c>
#define mh_name _lstr
#define mh_slot_t struct index_node
#define mh_slot_key(h, slot) (slot)->key.ptr
#define mh_slot_val(slot) (slot)->obj
#define mh_slot_size(h) (sizeof(void *) + sizeof(void *))
#define mh_slot mh_var_slot
#define mh_hash(h, key) ({ const void *_k = (key); int l = LOAD_VARINT32(_k); MurmurHash2(_k, l, 13); })
#define mh_eq(h, a, b) ({ lstrcmp((a), (b)) == 0; })
#include <mhash.h>


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
	[super init:ic dtor:dc];						\
	h = mh_##type##_init(xrealloc);					\
	return self;							\
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

@implementation LStringHash
DEFINE_METHODS(lstr)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node node_b;
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return lstrcmp(na->key.ptr, nb->key.ptr) == 0;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
                index_raise("hashed key has cardinality != 1");

	void *f = read_field(key_data);
        u32 k = mh_lstr_get(h, f);
        if (k != mh_end(h))
		return mh_lstr_value(h, k);
	return NULL;
}

- (struct tnt_object *)
find:(const char *)key
{
	u32 k = mh_lstr_get(h, key);
	if (k != mh_end(h))
		return mh_lstr_value(h, k);
	return NULL;
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	if (cardinality != 1)
		index_raise("cardinality too big");
	iter = mh_lstr_get(h, read_field(key_data));
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
	compare = pattern_compare = (index_cmp)cstr_compare;
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
