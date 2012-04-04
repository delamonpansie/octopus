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

#include <config.h>
#include <alloca.h>

#import <fiber.h>
#import <iproto.h>
#import <say.h>
#import <assoc.h>
#import <index.h>
#import <pickle.h>

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

@end

#define DEFINE_METHODS(type)						\
- (id)									\
init									\
{									\
	[super init];							\
	h = mh_##type##_init();						\
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
	if (find_obj_cache_q == obj)					\
		return find_obj_cache;					\
	dtor(obj, &node, dtor_arg);					\
									\
	find_obj_cache = obj;						\
									\
	u32 k = mh_##type##_get_node(h, (void *)&node);			\
	if (k != mh_end(h)) {						\
		struct tnt_object *r = mh_##type##_value(h, k);		\
		if (likely(!ghost(r)))					\
			return (find_obj_cache = r);			\
	}								\
	find_obj_cache = NULL;						\
	return NULL;							\
}									\
- (void)								\
replace:(struct tnt_object *)obj					\
{									\
	find_obj_cache_q = NULL;					\
	dtor(obj, &node, dtor_arg);					\
        mh_##type##_put_node(h, (void *)&node);				\
}									\
- (void)								\
remove:(struct tnt_object *)obj						\
{									\
	find_obj_cache_q = NULL;					\
	dtor(obj, &node, dtor_arg);					\
        u32 k = mh_##type##_get_node(h, (void *)&node);			\
        mh_##type##_del(h, k);						\
}


@implementation Int32Hash
DEFINE_METHODS(i32)

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
		index_raise("hashed key has cardinality != 1");
	u32 key_size = read_varint32(key_data);
	if (key_size != sizeof(type))
		index_raise("key is not i32");

	i32 num = read_u32(key_data);

        u32 k = mh_i32_get(h, num);
        if (k != mh_end(h)) {
		struct tnt_object *r = mh_i32_value(h, k);
		if (likely(!ghost(r)))
			return r;
	}
	return NULL;
}

@end

@implementation Int64Hash
DEFINE_METHODS(i64)

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
        if (k != mh_end(h)) {
		struct tnt_object *r = mh_i64_value(h, k);
		if (likely(!ghost(r)))
			return r;
        }
	return NULL;
}

@end

@implementation StringHash
DEFINE_METHODS(lstr)

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality
{
        if (key_cardinality != 1)
                index_raise("hashed key has cardinality != 1");

	void *f = read_field(key_data);
        u32 k = mh_lstr_get(h, f);
        if (k != mh_end(h)) {
		struct tnt_object *r = mh_lstr_value(h, k);
		if (likely(!ghost(r)))
		    return r;
	}
	return NULL;
}

@end
