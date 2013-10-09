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

#ifndef INDEX_H
#define INDEX_H

#include <util.h>
#include <objc.h>

#include <stdbool.h>


struct index_node {
	struct tnt_object *obj;
	union {
		char key[0];
		u32 u32;
		u64 u64;
		void *str;
	};
};

union index_field {
	u16 u16;
	u32 u32;
	u64 u64;
	struct {
		i16 len;
		union {
			u8 bytes[sizeof(u64)];
			void *ptr;
		} data;
	} str __attribute__((packed));
};

struct index_conf {
	int field_index[8];
	int cmp_order[8];
	int offset[8];
	enum index_field_type { NUM16, NUM32, NUM64, STRING } field_type[8];
	int min_tuple_cardinality, cardinality;
	enum index_type { HASH, TREE } type;
	bool unique;
	int n;
};

typedef struct index_node *(index_dtor)(struct tnt_object *obj, struct index_node *node, void *arg);
struct lua_State;
typedef struct tbuf *(index_lua_ctor)(struct lua_State *L, int i);
typedef int (*index_cmp)(const void *, const void *, void *);

struct dtor_conf {
	index_dtor *u32, *u64, *lstr, *generic;
};

@protocol BasicIndex
- (int)eq:(struct tnt_object *)a :(struct tnt_object *)b;
- (struct tnt_object *)find_by_obj:(struct tnt_object *)obj;
- (struct tnt_object *) find_key:(struct tbuf *)key_data with_cardinalty:(u32)key_cardinality;
- (int) remove: (struct tnt_object *)obj;
- (void) replace: (struct tnt_object *)obj;
- (void) valid_object: (struct tnt_object *)obj;

- (void)iterator_init;
- (void)iterator_init:(struct tbuf *)key_data with_cardinalty:(u32)cardinality;
- (void)iterator_init_with_object:(struct tnt_object *)obj;
- (struct tnt_object *)iterator_next;
- (u32)size;
- (u32)slots;
- (size_t) bytes;
- (u32)cardinality;
@end

#define GET_NODE(obj, node) ({ dtor(obj, &node, dtor_arg); &node; })
@interface Index: Object {
@public
	struct index_conf conf;

	size_t node_size;
	index_dtor *dtor;
	void *dtor_arg;
	index_lua_ctor *lua_ctor;

	int (*compare)(const void *a, const void *b, void *);
	int (*pattern_compare)(const void *a, const void *b, void *);

	struct index_node node_a;
	char __padding_a[512]; /* FIXME: check for overflow */
	struct index_node node_b;
	char __padding_b[512];
}

+ (Index *)new_conf:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc;
- (Index *)init:(struct index_conf *)ic;
- (void) valid_object:(struct tnt_object*)obj;
- (u32)cardinality;
@end

@interface DummyIndex: Index <BasicIndex> {
@public
	Index *index;
}
- (id) init_with_index:(Index *)_index;
- (bool) is_wrapper_of:(Class)some_class;
- (id) unwrap;
@end

@protocol HashIndex <BasicIndex>
- (void) resize:(u32)buckets;
- (struct tnt_object *) get:(u32)i;
- (struct tnt_object *) find:(void *)key;
- (void) ordered_iterator_init; /* WARNING! after this the index become corrupt! */
@end

@interface Hash: Index {
	size_t iter;
	struct mhash_t *h;
}
@end

@interface LStringHash: Hash <HashIndex>
@end
@interface CStringHash: Hash <HashIndex>
@end
@interface Int32Hash: Hash <HashIndex>
@end
@interface Int64Hash: Hash <HashIndex>
@end


@interface Tree: Index <BasicIndex> {
@public
        struct sptree_t *tree;
	void *nodes;

	void (*init_pattern)(struct tbuf *key, int cardinality,
			     struct index_node *pattern, void *);

	struct sptree_iterator *iterator;
	struct index_node search_pattern;
	char __tree_padding[256]; /* FIXME: overflow */
}
- (void)set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated;

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
void gen_set_field(union index_field *f, enum index_field_type type, int len, void *data);

#define foreach_index(ivar, obj_space)					\
	for (Index<BasicIndex>						\
		     *__foreach_idx = (void *)0,			\
		     *ivar = (id)(obj_space)->index[(uintptr_t)__foreach_idx]; \
	     (ivar = (id)(obj_space)->index[(uintptr_t)__foreach_idx]);	\
	     __foreach_idx = (void *)((uintptr_t)__foreach_idx + 1))

@interface IndexError: Error
@end


int luaT_indexinit(struct lua_State *L);
void luaT_pushindex(struct lua_State *L, Index *index);
struct tbuf *luaT_i32_ctor(struct lua_State *L, int i);
struct tbuf *luaT_i64_ctor(struct lua_State *L, int i);
struct tbuf *luaT_lstr_ctor(struct lua_State *L, int i);
struct tbuf *luaT_cstr_ctor(struct lua_State *L, int i);

void index_raise_(const char *file, int line, const char *msg)
	__attribute__((noreturn)) oct_cold;
#define index_raise(msg) index_raise_(__FILE__, __LINE__, (msg))


int i32_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int i32_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int i64_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int i64_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int lstr_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int lstr_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int cstr_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));
int cstr_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)));

#endif
