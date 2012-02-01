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
#include <stdbool.h>

#import <object.h>
#import <third_party/sptree.h>

struct tnt_object;
struct index_node {
	struct tnt_object *obj;
	char key[0];
};

struct field {
	u32 len;
	union {
		u32 u32;
		u64 u64;
		u8 data[sizeof(u64)];
		void *data_ptr;
	};
};

enum field_data_type { NUM, NUM64, STRING };
struct gen_dtor {
	u32 min_tuple_cardinality;
	u32 index_field[8];
	u32 cardinality;
	enum field_data_type type[8];
};

struct tree_node {
	struct tnt_object *obj;
	struct field key[];
};

union {
	struct index_node index;
	struct tree_node tree;
} index_nodes;

typedef void (index_dtor)(struct tnt_object *obj, struct index_node *node, void *arg);

@protocol BasicIndex
- (struct tnt_object *)find_by_obj:(struct tnt_object *)obj;
- (struct tnt_object *) find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality;
- (void) remove: (struct tnt_object *)obj;
- (void) replace: (struct tnt_object *)obj;
- (void) valid_object: (struct tnt_object *)obj;

- (void)iterator_init;
- (struct tnt_object *)iterator_next;
- (u32)size;
@end


@interface Index: Object {
@public
	unsigned n;
	bool unique;
	enum { HASH, TREE } type;
	u32 index_cardinality;

	struct tnt_object *find_obj_cache_q, *find_obj_cache;
	index_dtor *dtor;
	void *dtor_arg;

	struct index_node node;
	char __padding[64]; /* FIXME: check for overflow */
}

- (void) valid_object:(struct tnt_object*)obj;
@end

@interface DummyIndex: Index <BasicIndex> {
@public
	Index *index;
}
- (id) init_with_index:(Index *)_index;
- (bool) is_wrapper_of:(Class)some_class;
- (id) unwrap;
@end

@interface Index (Tuple)
+ (Index<BasicIndex> *)new_with_n:(int)n_
			      cfg:(struct tarantool_cfg_object_space_index *)cfg;
@end

@protocol HashIndex <BasicIndex>
- (void) resize:(u32)buckets;
- (struct tnt_object *) get:(u32)i;
- (u32)buckets;
@end

@interface Hash: Index {
	size_t iter;
	struct mhash_t *h;
}
@end

@interface StringHash: Hash <HashIndex>
@end
@interface Int32Hash: Hash <HashIndex>
@end
@interface Int64Hash: Hash <HashIndex>
@end



SPTREE_DEF(str_t, realloc);

@interface Tree: Index <BasicIndex> {
@public
        sptree_str_t *tree;
	void *nodes;
	size_t node_size;

	int (*compare)(const void *a, const void *b, void *);
	void (*init_pattern)(struct tbuf *key, int cardinality,
			     struct index_node *pattern, void *);

	struct sptree_str_t_iterator *iterator;
	struct index_node search_pattern;
	char __tree_padding[64]; /* FIXME: overflow */
}
- (id)init_with_unique:(bool)_unque;
- (void) set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated;

- (void)iterator_init:(struct tbuf *)key_data with_cardinalty:(u32)cardinality;
- (void)iterator_init_with_object:(struct tnt_object *)obj;
- (struct tnt_object *)iterator_next;
- (struct tnt_object *)iterator_next_verify_pattern;
@end


@interface Int32Tree: Tree
@end
@interface Int64Tree: Tree
@end
@interface StringTree: Tree
@end

@interface GenTree: Tree
@end

#define foreach_index(ivar, obj_space)					\
	for (Index<BasicIndex>						\
		     *__foreach_idx = (void *)0,			\
		     *ivar = (id)(obj_space)->index[(uintptr_t)__foreach_idx]; \
	     (ivar = (id)(obj_space)->index[(uintptr_t)__foreach_idx]);	\
	     __foreach_idx = (void *)((uintptr_t)__foreach_idx + 1))

@interface IndexError: Error
@end
