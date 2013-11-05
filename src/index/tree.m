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
	/* WARNING: set_nodes will drop any previously allocated data
	   and reinitialize tree */

	assert(node_size > 0);

	free(nodes);
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
init:(struct index_conf *)ic
{
	[super init:ic];
	tree = xmalloc(sizeof(*tree));
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
find:(const u8 *)key
{
	const u8 *p = key;
	int len = LOAD_VARINT32(p);
	init_pattern(&TBUF(key, p - key + len, NULL), 1, &node_a, dtor_arg);
	struct index_node *r = sptree_find(tree, &node_a);
	return r != NULL ? r->obj : NULL;
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

- (struct tnt_object *)
find:(u8 *)key
{
	u32 key_size = key[0];
	if (key_size != sizeof(i32))
		index_raise("key is not i32");
	init_pattern(&TBUF(key, 1 + sizeof(i32), NULL), 1, &node_a, dtor_arg);
	struct index_node *r = sptree_find(tree, &node_a);
	return r != NULL ? r->obj : NULL;
}

- (id)
init:(struct index_conf *)ic
{
	[super init:ic];
	node_size = sizeof(struct tnt_object *) + sizeof(i32);
	init_pattern = i32_init_pattern;
	pattern_compare = (index_cmp)i32_compare;
	compare = conf.unique ? (index_cmp)i32_compare : (index_cmp)i32_compare_with_addr;
	[self set_nodes:NULL count:0 allocated:0];
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

- (struct tnt_object *)
find:(u8 *)key
{
	u32 key_size = key[0];
	if (key_size != sizeof(i64))
		index_raise("key is not i64");
	init_pattern(&TBUF(key, 1 + sizeof(i64), NULL), 1, &node_a, dtor_arg);
	struct index_node *r = sptree_find(tree, &node_a);
	return r != NULL ? r->obj : NULL;
}

- (id)
init:(struct index_conf *)ic
{
	[super init:ic];
	node_size = sizeof(struct tnt_object *) + sizeof(i64);
	init_pattern = i64_init_pattern;
	pattern_compare = (index_cmp)i64_compare;
	compare = conf.unique ? (index_cmp)i64_compare : (index_cmp)i64_compare_with_addr;
	[self set_nodes:NULL count:0 allocated:0];
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
init:(struct index_conf *)ic
{
	[super init:ic];
	node_size = sizeof(struct tnt_object *) + sizeof(void *);
	init_pattern = lstr_init_pattern;
	pattern_compare = (index_cmp)lstr_compare;
	compare = conf.unique ? (index_cmp)lstr_compare : (index_cmp)lstr_compare_with_addr;
	[self set_nodes:NULL count:0 allocated:0];
	return self;
}
@end


@implementation GenTree
- (u32)
cardinality
{
	struct index_conf *ic = dtor_arg;
	return ic->cardinality;
}

static int
field_compare(union index_field *f1, union index_field *f2, enum index_field_type type)
{
	const void *d1, *d2;
	int r;

	switch (type) {
	case NUM16:
		return f1->u16 > f2->u16 ? 1 : f1->u16 == f2->u16 ? 0 : -1;
	case NUM32:
		return f1->u32 > f2->u32 ? 1 : f1->u32 == f2->u32 ? 0 : -1;
	case NUM64:
		return f1->u64 > f2->u64 ? 1 : f1->u64 == f2->u64 ? 0 : -1;
	case STRING:
		d1 = f1->str.len <= sizeof(f1->str.data) ? f1->str.data.bytes : f1->str.data.ptr;
		d2 = f2->str.len <= sizeof(f2->str.data) ? f2->str.data.bytes : f2->str.data.ptr;
		r = memcmp(d1, d2, MIN(f1->str.len, f2->str.len));
		if (r != 0)
			return r;

		return f1->str.len > f2->str.len ? 1 : f1->str.len == f2->str.len ? 0 : -1;
	}
	abort();
}

static int
tree_node_compare(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	/* if pattern is partialy specified compare only significant fields.
	   it's ok to return 0 here: sptree_iterator_init_set() will select
	   leftmost node in case of equality.
	   it is guaranteed that pattern is a first arg.
	*/

	int n = (uintptr_t)na->obj < nelem(ic->field_index) ? (uintptr_t)na->obj : ic->cardinality;

	for (int i = 0; i < n; ++i) {
		int j = ic->cmp_order[i];
		union index_field *akey = (void *)na->key + ic->offset[j];
		union index_field *bkey = (void *)nb->key + ic->offset[j];
		int r = field_compare(akey, bkey, ic->field_type[j]);
		if (r != 0)
			return r;
	}
	return 0;
}

static int
tree_node_compare_with_addr(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	int r = tree_node_compare(na, nb, ic);
	if (r != 0)
		return r;

	if ((uintptr_t)na->obj < nelem(ic->field_index)) /* `na' is a pattern */
		return r;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

void
gen_set_field(union index_field *f, enum index_field_type type, int len, const void *data)
{
	switch (type) {
	case NUM16:
		if (len != sizeof(u16))
			index_raise("key size mismatch, expected u16");
		f->u16 = *(u16 *)data;
		break;
	case NUM32:
		if (len != sizeof(u32))
			index_raise("key size mismatch, expected u32");
		f->u32 = *(u32 *)data;
		break;
	case NUM64:
		if (len != sizeof(u64))
			index_raise("key size mismatch, expected u64");
		f->u64 = *(u64 *)data;
		break;
	case STRING:
		if (len > 0xffff)
			index_raise("string key too long");
		f->str.len = len;
		if (len <= sizeof(f->str.data))
			memcpy(f->str.data.bytes, data, len);
		else
			f->str.data.ptr = data;
		break;
	}
}
static void
gen_init_pattern(struct tbuf *key_data, int cardinality, struct index_node *pattern_, void *arg)
{
	struct index_node *pattern = (void *)pattern_;
	struct index_conf *ic = arg;

	if (cardinality > ic->cardinality || cardinality > nelem(ic->field_index))
                index_raise("cardinality too big");

	for (int i = 0; i < cardinality; i++) {
		u32 len = read_varint32(key_data);
		void *key = read_bytes(key_data, len);
		int j = ic->cmp_order[i];

		union index_field *f = (void *)pattern->key + ic->offset[j];
		gen_set_field(f, ic->field_type[j], len, key);
		key += len;
	}

	pattern->obj = (void *)(uintptr_t)cardinality;
}

- (id)
init:(struct index_conf *)ic
{
	[super init:ic];
	node_size = sizeof(struct tnt_object *);
	for (int i = 0; i < ic->cardinality; i++)
		switch (ic->field_type[i]) {
		case NUM16: node_size += field_sizeof(union index_field, u16); break;
		case NUM32: node_size += field_sizeof(union index_field, u32); break;
		case NUM64: node_size += field_sizeof(union index_field, u64); break;
		case STRING: node_size += field_sizeof(union index_field, str); break;
		}

	init_pattern = gen_init_pattern;
	pattern_compare = (index_cmp)tree_node_compare;
	compare = conf.unique ? (index_cmp)tree_node_compare : (index_cmp)tree_node_compare_with_addr;
	[self set_nodes:NULL count:0 allocated:0];
	return self;
}
@end
