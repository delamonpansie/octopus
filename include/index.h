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

#ifndef INDEX_H
#define INDEX_H

#include <util.h>
#include <pickle.h>
#include <objc.h>

#include <stdbool.h>
#include <string.h>
#include <third_party/twltree/twltree.h>


union index_field {
	u16 u16;
	u32 u32;
	u64 u64;
	const void *ptr;
	char chr[12]; /* for LuaJIT casts */
	union {
		struct {
			i16 len;
			union {
				u8 bytes[sizeof(u64)];
				const void *ptr;
			} data;
		} str __attribute__((packed));
	};
};

struct index_node {
	struct tnt_object *obj;
	union index_field key;
};


struct index_conf {
	int field_index[8];
	int fill_order[8];
	int offset[8];
	enum sort_order { ASC = 1, DESC = -1 } sort_order[8];
	enum index_field_type { UNDEF, SIGNFLAG=1,
		UNUM16=2, SNUM16=3,
		UNUM32=4, SNUM32=5,
		UNUM64=6, SNUM64=7,
		STRING=8 } field_type[8];
	int min_tuple_cardinality, cardinality;
	enum index_type { HASH, NUMHASH, SPTREE, FASTTREE, COMPACTTREE } type;
	bool unique;
	int n;
};

typedef struct index_node *(index_dtor)(struct tnt_object *obj, struct index_node *node, void *arg);
struct lua_State;
typedef int (*index_cmp)(const void *, const void *, void *);

struct dtor_conf {
	index_dtor *i32, *i64, *u32, *u64, *lstr, *generic;
};

#ifdef __amd64__
struct ptr_for_index {
	intptr_t ptr : 48;
};
typedef struct ptr_for_index tnt_ptr;
#define tnt_ptr2obj(p) ( (struct tnt_object*)(intptr_t)(p).ptr )
#define tnt_obj2ptr(o) ((tnt_ptr){ .ptr = (intptr_t)o })
#else
typedef struct tnt_object* tnt_ptr;
#define tnt_ptr2obj(p) (p)
#define tnt_obj2ptr(o) (o)
#endif

/* Following selectors used by LuaJIT bindings and MUST NOT throw exceptions:
   find:
   find_by_node:
   get:
   iterator_init
   iterator_init_with_node:
   iterator_init_with_object:
   iterator_next
*/

@protocol BasicIndex
- (int)eq:(struct tnt_object *)a :(struct tnt_object *)b;
- (struct tnt_object *)find:(const char *)key;
- (struct tnt_object *)find_obj:(struct tnt_object *)obj;
- (struct tnt_object *)find_node:(const struct index_node *)obj;
- (struct tnt_object *) find_key:(struct tbuf *)key_data cardinalty:(u32)key_cardinality;
- (int) remove: (struct tnt_object *)obj;
- (void) replace: (struct tnt_object *)obj;
- (void) valid_object: (struct tnt_object *)obj;

- (void)iterator_init;
- (void)iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality;
- (void)iterator_init_with_object:(struct tnt_object *)obj;
- (void)iterator_init_with_node:(const struct index_node *)node;
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

	int (*eq)(const void *a, const void *b, void *);
	int (*compare)(const void *a, const void *b, void *);
	int (*pattern_compare)(const void *a, const void *b, void *);
	void (*init_pattern)(struct tbuf *key, int cardinality,
			     struct index_node *pattern, void *);

	struct index_node node_a;
	struct index_node __padding_a[7];
	struct index_node search_pattern;
	struct index_node __tree_padding[7];
}

+ (Index *)new_conf:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc;
- (Index *)init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc;
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
- (void) ordered_iterator_init; /* WARNING! after this the index become corrupt! */
@end

@interface Hash: Index {
	size_t iter;
}
@end

@interface CStringHash: Hash <HashIndex> {
	struct mh_cstr_t *h;
}
@end
@interface Int32Hash: Hash <HashIndex> {
	struct mh_i32_t *h;
}
@end
@interface Int64Hash: Hash <HashIndex> {
	struct mh_i64_t *h;
}
@end
@interface GenHash: Hash <HashIndex> {
	struct mh_gen_t *h;
}
@end

/* must be same as sptree_direction_t */
enum iterator_direction {
	iterator_forward = 1,
	iterator_backward = -1
};

@protocol IterIndex
- (void)iterator_init_with_direction:(enum iterator_direction)direction;
- (void)iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality direction:(enum iterator_direction)direction;
- (void)iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction;
- (void)iterator_init_with_node:(const struct index_node *)node direction:(enum iterator_direction)direction;

- (struct tnt_object *)iterator_next_check:(index_cmp)check;

- (void)set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated;
- (index_cmp) pattern_compare;
@end

@interface Tree: Index <BasicIndex, IterIndex>
- (void)set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated;
- (index_cmp) pattern_compare;
@end

@interface SPTree: Tree {
@public
        struct sptree_t *tree;
	struct sptree_iterator *iterator;
}
@end

@interface TWLTree : Tree {
	struct twltree_t tree;
	struct twliterator_t iter;
}
@end

@interface TWLFastTree : TWLTree
@end

@interface TWLCompactTree : TWLTree
@end

#define foreach_index(ivar, obj_space)					\
	for (Index<BasicIndex>						\
		     *__foreach_idx = (void *)0,			\
		     *ivar = (id)(obj_space)->index[(uintptr_t)__foreach_idx]; \
	     (ivar = (id)(obj_space)->index[(uintptr_t)__foreach_idx]);	\
	     __foreach_idx = (void *)((uintptr_t)__foreach_idx + 1))

@interface IndexError: Error
@end

void index_raise_(const char *file, int line, const char *msg)
	__attribute__((noreturn)) oct_cold;
#define index_raise(msg) index_raise_(__FILE__, __LINE__, (msg))


int u32_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u32_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u32_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u32_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
void u32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)));
void i32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)));
int u64_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u64_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u64_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u64_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
void u64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)));
void i64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)));
int lstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int lstr_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int lstr_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int lstr_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
void lstr_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)));
int cstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int cstr_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int cstr_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int cstr_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));

int u32_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u32_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u64_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int u64_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int lstr_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int lstr_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int cstr_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));
int cstr_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)));

int tree_node_compare(struct index_node *na, struct index_node *nb, struct index_conf *ic);
int tree_node_compare_with_addr(struct index_node *na, struct index_node *nb, struct index_conf *ic);
int tree_node_eq(struct index_node *na, struct index_node *nb, struct index_conf *ic);
int tree_node_eq_with_addr(struct index_node *na, struct index_node *nb, struct index_conf *ic);
void gen_init_pattern(struct tbuf *key_data, int cardinality, struct index_node *pattern_, void *arg);
void gen_set_field(union index_field *f, enum index_field_type type, int len, const void *data);
u32 gen_hash_node(const struct index_node *n, struct index_conf *ic);

static inline int llexstrcmp(const void *a, const void *b)
{
	int al, bl;
	int r;

	al = LOAD_VARINT32(a);
	bl = LOAD_VARINT32(b);

	r = memcmp(a, b, al <= bl ? al : bl);

	return r != 0 ? r : al - bl;
}

#endif
