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
#import <say.h>
#import <assoc.h>
#import <index.h>
#import <third_party/sptree.h>

@implementation Tree
- (void)
set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated
{
	assert(nodes == NULL);

	if (nodes_ == NULL) {
		if (allocated == 0)
			allocated = 64;
		nodes = xmalloc(allocated * node_size);
	} else {
		nodes = nodes_;
	}

	sptree_init(tree, node_size, nodes, count, allocated,
		    compare, self->dtor_arg);
}

- (Tree *)
init_with_unique:(bool)_unique
{
	[super init];
	type = TREE;
	tree = xmalloc(sizeof(*tree));
	unique = _unique;
	return self;
}

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return compare(na, nb, NULL) == 0;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)cardinality
{
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	struct index_node *r = sptree_find(tree, &node_a);
	return r != NULL ? r->obj : NULL;
}

- (struct tnt_object *)
find_by_obj:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	struct index_node *r = sptree_find(tree, &node_a);
	return r != NULL ? r->obj : NULL;
}

- (u32)
size
{
	return tree->size;
}

- (u32)
slots
{
	return tree->ntotal;
}

- (size_t)
bytes
{
	return sptree_bytes(tree);
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	sptree_insert(tree, &node_a);
}

- (int)
remove:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	return sptree_delete(tree, &node_a);
}

- (void)
iterator_init
{
	[self iterator_init:NULL with_cardinalty:0];
}

- (void)
iterator_init:(struct tbuf *)key_data with_cardinalty:(u32)cardinality
{
        init_pattern(key_data, cardinality, &search_pattern, dtor_arg);
        sptree_iterator_init_set(tree, &iterator, &search_pattern);
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj
{
        dtor(obj, &search_pattern, dtor_arg);
	sptree_iterator_init_set(tree, &iterator, &search_pattern);
}

- (struct tnt_object *)
iterator_next
{
	struct index_node *r = sptree_iterator_next(iterator);
	return likely(r != NULL) ? r->obj : NULL;
}

- (struct tnt_object *)
iterator_next_verify_pattern
{
	struct index_node *r = sptree_iterator_next(iterator);

	if (r != NULL) {
		if (pattern_compare(&search_pattern, r, self->dtor_arg) != 0)
			return NULL;
		return r->obj;
	}
	return NULL;
}
@end

@implementation Int32Tree

static void
i32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->u32 = INT32_MIN;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u32))
			index_raise("key is not u32");
		pattern->u32 = read_u32(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	node_size = sizeof(struct tnt_object *) + sizeof(i32);
	lua_ctor = luaT_i32_ctor;
	init_pattern = i32_init_pattern;
	pattern_compare = (index_cmp)i32_compare;
	compare = unique ? (index_cmp)i32_compare : (index_cmp)i32_compare_with_addr;
	return self;
}
@end

@implementation Int64Tree

static void
i64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->u64 = INT64_MIN;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u64))
			index_raise("key is not u64");
		pattern->u64 = read_u64(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	node_size = sizeof(struct tnt_object *) + sizeof(i64);
	lua_ctor = luaT_i64_ctor;
	init_pattern = i64_init_pattern;
	pattern_compare = (index_cmp)i64_compare;
	compare = unique ? (index_cmp)i64_compare : (index_cmp)i64_compare_with_addr;
	return self;
}
@end

@implementation StringTree

static void
lstr_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;
	static u8 empty[] = {0};
	void *f;

	if (cardinality == 1)
		f = read_field(key);
	else if (cardinality == 0)
		f = &empty;
	else
		index_raise("cardinality too big");

	pattern->str = f;
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	node_size = sizeof(struct tnt_object *) + sizeof(void *);
	lua_ctor = luaT_lstr_ctor;
	init_pattern = lstr_init_pattern;
	pattern_compare = (index_cmp)lstr_compare;
	compare = unique ? (index_cmp)lstr_compare : (index_cmp)lstr_compare_with_addr;
	return self;
}
@end


@implementation GenTree
- (u32)
cardinality
{
	struct gen_dtor *desc = dtor_arg;
	return desc->cardinality;
}

static int
field_compare(struct field *f1, struct field *f2, enum field_data_type type)
{
	void *d1, *d2;
	int r;

	switch (type) {
	case NUM16:
		return f1->u16 > f2->u16 ? 1 : f1->u16 == f2->u16 ? 0 : -1;
	case NUM32:
		return f1->u32 > f2->u32 ? 1 : f1->u32 == f2->u32 ? 0 : -1;
	case NUM64:
		return f1->u64 > f2->u64 ? 1 : f1->u64 == f2->u64 ? 0 : -1;
	case STRING:
		d1 = f1->len <= sizeof(f1->data) ? f1->data : f1->data_ptr;
		d2 = f2->len <= sizeof(f2->data) ? f2->data : f2->data_ptr;
		r = memcmp(d1, d2, MIN(f1->len, f2->len));
		if (r != 0)
			return r;

		return f1->len > f2->len ? 1 : f1->len == f2->len ? 0 : -1;
	}
	abort();
}

static int
tree_node_compare(struct tree_node *na, struct tree_node *nb, struct gen_dtor *desc)
{
	/* if pattern is partialy specified compare only significant fields.
	   it's ok to return 0 here: sptree_iterator_init_set() will select
	   leftmost node in case of equality.
	   it is guaranteed that pattern is a first arg.
	*/

	int n = (uintptr_t)na->obj < nelem(desc->index_field) ? (uintptr_t)na->obj : desc->cardinality;

	for (int i = 0; i < n; ++i) {
		int j = desc->cmp_order[i];
		int r = field_compare(&na->key[j], &nb->key[j], desc->type[j]);
		if (r != 0)
			return r;
	}
	return 0;
}

static int
tree_node_compare_with_addr(struct tree_node *na, struct tree_node *nb, struct gen_dtor *desc)
{
	int r = tree_node_compare(na, nb, desc);
	if (r != 0)
		return r;

	if ((uintptr_t)na->obj < nelem(desc->index_field)) /* `na' is a pattern */
		return r;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

static void
gen_init_pattern(struct tbuf *key_data, int cardinality, struct index_node *pattern_, void *arg)
{
	struct tree_node *pattern = (void *)pattern_;
	struct gen_dtor *desc = arg;

	if (cardinality > desc->cardinality || cardinality > nelem(desc->index_field))
                index_raise("cardinality too big");

        for (int i = 0; i < desc->cardinality; i++)
		pattern->key[i].len = -1;

	for (int i = 0; i < cardinality; i++) {
		u32 len = read_varint32(key_data);
                void *key = read_bytes(key_data, len);
		int j = desc->cmp_order[i];

		if (desc->type[j] == NUM16 && len != sizeof(u16))
			index_raise("key size mismatch, expected u16");
		else if (desc->type[j] == NUM32 && len != sizeof(u32))
			index_raise("key size mismatch, expected u32");
		else if (desc->type[j] == NUM64 && len != sizeof(u64))
			index_raise("key size mismatch, expected u64");

		pattern->key[j].len = len;
		if (len <= sizeof(pattern->key[j].data))
			memcpy(pattern->key[j].data, key, len);
		else
			pattern->key[j].data_ptr = key;
		key += len;
	}

	pattern->obj = (void *)(uintptr_t)cardinality;
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	struct gen_dtor *desc = dtor_arg;
	node_size = sizeof(struct tnt_object *) + desc->cardinality * sizeof(struct field);
	init_pattern = gen_init_pattern;
	pattern_compare = (index_cmp)tree_node_compare;
	compare = unique ? (index_cmp)tree_node_compare : (index_cmp)tree_node_compare_with_addr;
	return self;
}
@end
