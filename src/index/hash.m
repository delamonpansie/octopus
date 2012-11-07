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

#import <util.h>
#import <fiber.h>
#import <iproto.h>
#import <say.h>
#import <assoc.h>
#import <index.h>
#import <pickle.h>
#include <third_party/qsort_arg.h>

@implementation Hash
- (Hash *)init
{
	[super init];
	type = HASH;
	unique = true;
	return self;
}

- (struct tnt_object *)
get:(u32)i
{
	if (i > mh_end(h) || !mh_exist(h, i))
		return NULL;
	return *(struct tnt_object **)mh_slot(h, i);
}

- (u32)
size
{
	return mh_size(h);
}

- (u32)
slots
{
	return mh_end(h);
}

- (size_t)
bytes
{
	return mh_bytes(h);
}

- (void)
iterator_init
{
	iter = 0;
}

- (struct tnt_object *)
iterator_next
{
	for (; iter < mh_end(h); iter++) {
		if (!mh_exist(h, iter))
			continue;
		return *(struct tnt_object **)mh_slot(h, iter++);
	}
	return NULL;
}

- (void)
ordered_iterator_init
{
	int j = 0;
	for (int i = 1; i < mh_end(h); i++) {
		if (!mh_exist(h, i))
		    continue;

		while (mh_exist(h, j) && j < i)
			j++;

		if (j == i)
			continue;

		assert(!mh_exist(h, j));
		memcpy(mh_slot(h, j), mh_slot(h, i), node_size);
		mh_setfree(h, i);
		mh_setexist(h, j);
	}
	assert(j + 1 == mh_size(h));
	qsort_arg(h->nodes, mh_size(h), node_size, compare, self);
	[self iterator_init];
}

@end

#define DEFINE_METHODS(type)						\
- (id)									\
init									\
{									\
	[super init];							\
	h = mh_##type##_init(NULL);					\
	node_size = sizeof(struct index_node) + sizeof(type);		\
	lua_ctor = luaT_##type##_ctor;					\
	compare = ucompare = (index_cmp)type##_compare;			\
	return self;							\
}									\
- (void)								\
resize:(u32)buckets							\
{									\
	mh_##type##_start_resize(h, buckets, 0);			\
}									\
- (struct tnt_object *)							\
find_by_obj:(struct tnt_object *)obj					\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
	u32 k = mh_##type##_get_node(h, (void *)node_);			\
	if (k != mh_end(h)) 						\
		return mh_##type##_value(h, k);				\
	return NULL;							\
}									\
- (void)								\
replace:(struct tnt_object *)obj					\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
        mh_##type##_put_node(h, (void *)node_);				\
}									\
- (void)								\
remove:(struct tnt_object *)obj						\
{									\
	struct index_node *node_ = GET_NODE(obj, node_a);		\
        u32 k = mh_##type##_get_node(h, (void *)node_);			\
	node_->obj = NULL;						\
        mh_##type##_del(h, k);						\
}


@implementation Int32Hash
DEFINE_METHODS(i32)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return *(i32 *)na->key == *(i32 *)nb->key;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
		index_raise("hashed key has cardinality != 1");
	u32 key_size = read_varint32(key_data);
	if (key_size != sizeof(u32))
		index_raise("key is not i32");

	i32 num = read_u32(key_data);

        u32 k = mh_i32_get(h, num);
        if (k != mh_end(h))
		return mh_i32_value(h, k);
	return NULL;
}

- (struct tnt_object *)
find:(void *)key
{
	u32 key_size = ((u8 *)key)[0];
	if (key_size != sizeof(i32))
		index_raise("key is not i32");

	i32 num = ((i32 *)(key + 1))[0];
	u32 k = mh_i32_get(h, num);
	if (k != mh_end(h))
		return mh_i32_value(h, k);
	return NULL;
}

@end

@implementation Int64Hash
DEFINE_METHODS(i64)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return *(i64 *)na->key == *(i64 *)nb->key;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
                index_raise("hashed key has cardinality != 1");
	u32 key_size = read_varint32(key_data);
	if (key_size != sizeof(i64))
		index_raise("key is not i64");

	i64 num = read_u64(key_data);

        u32 k = mh_i64_get(h, num);
        if (k != mh_end(h))
		return mh_i64_value(h, k);
	return NULL;
}

- (struct tnt_object *)
find:(void *)key
{
	u32 key_size = ((u8 *)key)[0];
	if (key_size != sizeof(i64))
		index_raise("key is not i64");

	i64 num = ((i64 *)(key + 1))[0];
	u32 k = mh_i64_get(h, num);
	if (k != mh_end(h))
		return mh_i64_value(h, k);
	return NULL;
}

@end

@implementation StringHash
DEFINE_METHODS(lstr)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return lstrcmp(*(void **)na->key, *(void **)nb->key) == 0;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality
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
find:(void *)key
{
	u32 k = mh_lstr_get(h, key);
	if (k != mh_end(h))
		return mh_lstr_value(h, k);
	return NULL;
}

@end
