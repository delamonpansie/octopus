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
#import <assoc.h>
#import <index.h>
#import <salloc.h>
#import <say.h>

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
	case UNDEF:
		abort();
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
		union index_field *akey = (void *)&na->key + ic->offset[j];
		union index_field *bkey = (void *)&nb->key + ic->offset[j];
		int r = field_compare(akey, bkey, ic->field_type[j]);
		if (r != 0)
			return r * ic->sort_order[i];
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

		union index_field *f = (void *)&pattern->key + ic->offset[j];
		gen_set_field(f, ic->field_type[j], len, key);
		key += len;
	}

	pattern->obj = (void *)(uintptr_t)cardinality;
}

static void
i32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->key.u32 = INT32_MIN;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u32))
			index_raise("key is not u32");
		pattern->key.u32 = read_u32(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

static void
i64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->key.u64 = INT64_MIN;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u64))
			index_raise("key is not u64");
		pattern->key.u64 = read_u64(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

static int
twl_index_key_cmp(const void *a, const void *b, void *arg)
{
	TWLTree* t = (TWLTree*)arg;
	struct index_node const *an = (typeof(an))a;
	struct index_node const *bn = (typeof(bn))b;
	return t->compare(an, bn, t->dtor_arg);
}

static void*
twl_realloc(void *old, size_t new_size)
{
	if (old == NULL) {
		if (new_size > 0)
			return salloc(new_size);
		return NULL;
	}
	if (new_size == 0) {
		sfree(old);
		return NULL;
	}
	struct slab_cache *old_slab = slab_cache_of_ptr(old);
	void *new = salloc(new_size);
	if (new == NULL)
		return NULL;
	memcpy(new, old, MIN(new_size, old_slab->item_size));
	sfree(old);
	return new;
}

static struct twltree_conf_t twltree_gen_conf = {
	.index_key_free = NULL,
	.tuple_key_2_index_key = NULL,
	.tuple_key_cmp = twl_index_key_cmp,
	.index_key_cmp = NULL,
	.page_sizes = NULL,
	.page_sizes_n = 0,
};

static void
twl_raise(twlerrcode_t r)
{
	/*
	switch(r) {
	case TWL_OK: break;
	case TWL_WRONG_CONFIG: index_raise("Wrong config"); break;
	case TWL_NOMEMORY: index_raise("No memory"); break;
	case TWL_SUPPORT: index_raise("twl support"); break;
	case TWL_DUPLICATE: index_raise("Duplicate"); break;
	case TWL_NOTFOUND: index_raise("Not found"); break;
	}
	*/
	if (r != TWL_OK)
		assert(r == TWL_OK);
}

@implementation TWLTree
- (u32)
cardinality
{
	struct index_conf *ic = dtor_arg;
	return ic->cardinality;
}
#define COMPARE(type)										\
	conf.sort_order[0] == ASC ?								\
	(conf.unique ? (index_cmp)type##_compare : (index_cmp)type##_compare_with_addr) :	\
	(conf.unique ? (index_cmp)type##_compare_desc : (index_cmp)type##_compare_with_addr_desc)

- (TWLTree *)
init:(struct index_conf *)ic
{
	[super init:ic];
	if (ic->cardinality == 1) {
		switch(ic->field_type[0]) {
		case NUM32:
			node_size = sizeof(struct tnt_object *) + sizeof(i32);
			init_pattern = i32_init_pattern;
			pattern_compare = (index_cmp)i32_compare;
			compare = COMPARE(i32);
			break;
		case NUM64:
			node_size = sizeof(struct tnt_object *) + sizeof(i64);
			init_pattern = i64_init_pattern;
			pattern_compare = (index_cmp)i64_compare;
			compare = COMPARE(i64);
			break;
		default:
			break;
		}
	}
	if (node_size == 0) {
		node_size = sizeof(struct tnt_object *);
		for (int i = 0; i < ic->cardinality; i++)
			switch (ic->field_type[i]) {
			case NUM16: node_size += field_sizeof(union index_field, u16); break;
			case NUM32: node_size += field_sizeof(union index_field, u32); break;
			case NUM64: node_size += field_sizeof(union index_field, u64); break;
			case STRING: node_size += field_sizeof(union index_field, str); break;
			case UNDEF: abort();
			}

		init_pattern = gen_init_pattern;
		pattern_compare = (index_cmp)tree_node_compare;
		compare = conf.unique ? (index_cmp)tree_node_compare : (index_cmp)tree_node_compare_with_addr;
	}

	tree.conf = &twltree_gen_conf;
	tree.arg = self;
	tree.tlrealloc = twl_realloc;
	tree.sizeof_index_key = node_size;
	tree.sizeof_tuple_key = node_size;
	enum twlerrcode_t r = twltree_init(&tree);
	twl_raise(r);
	return self;
}

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b);
	return compare(na, nb, self->dtor_arg) == 0;
}

- (struct tnt_object *)
find:(const char *)key
{
#if 0
	switch (conf.field_type[0]) {
	case NUM16: node_a.key.u16 = *(u16 *)key; break;
	case NUM32: node_a.key.u32 = *(u32 *)key; break;
	case NUM64: node_a.key.u64 = *(u64 *)key; break;
	case STRING: node_a.key.ptr = key; break;
	default: abort();
	}
	node_a.obj = (void *)(uintptr_t)1; /* cardinality */
	struct index_node *r = sptree_find(tree, &node_a);
	return r != NULL ? r->obj : NULL;
#else
	(void)key;
	return NULL;
#endif
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	struct index_node* r = twltree_find_by_index_key(&tree, &node_a, NULL);
	return r != NULL ? r->obj : NULL;
}

- (struct tnt_object *)
find_obj:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	struct index_node* r = twltree_find_by_index_key(&tree, &node_a, NULL);
	return r != NULL ? r->obj : NULL;
}

- (struct tnt_object *)
find_node:(const struct index_node *)node
{
	struct index_node* r = twltree_find_by_index_key(&tree, node, NULL);
	return r != NULL ? r->obj : NULL;
}

- (u32)
size
{
	return tree.n_tuple_keys;
}

- (u32)
slots
{
	return tree.n_tuple_space;
}

- (size_t)
bytes
{
	return tree.n_tuple_space * sizeof(struct tnt_object*);
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	twlerrcode_t r = twltree_insert(&tree, &node_a, true);
	if (r != TWL_OK)
		twl_raise(r);
}

- (int)
remove:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	twlerrcode_t r = twltree_delete(&tree, &node_a);
	if (r != TWL_OK && r != TWL_NOTFOUND)
		twl_raise(r);
	return r == TWL_OK;
}

- (void)
iterator_init
{
	[self iterator_init_with_key:NULL cardinalty:0 direction:iterator_forward];
}

- (void)
iterator_init_with_direction:(enum iterator_direction)direction
{
	[self iterator_init_with_key:NULL cardinalty:0 direction:direction];
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	[self iterator_init_with_key:key_data cardinalty:cardinality direction:iterator_forward];
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj
{
	[self iterator_init_with_object:obj direction:iterator_forward];
}

- (void)
iterator_init_with_node:(const struct index_node *)node
{
	[self iterator_init_with_node:node direction:iterator_forward];
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data
	    cardinalty:(u32)cardinality
	     direction:(enum iterator_direction)direction
{
	if (cardinality == 0) {
		twltree_iterator_init(&tree, &iter,
				direction == iterator_forward ? twlscan_forward : twlscan_backward);
	} else {
		init_pattern(key_data, cardinality, &search_pattern, dtor_arg);
		twltree_iterator_init_set_index_key(&tree, &iter, &search_pattern,
				direction == iterator_forward ? twlscan_forward : twlscan_backward);
	}
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
	twltree_iterator_init_set(&tree, &iter, &obj,
				direction == iterator_forward ? twlscan_forward : twlscan_backward);
}

- (void)
iterator_init_with_node:(const struct index_node *)node direction:(enum iterator_direction)direction
{
	memcpy(&search_pattern, node, node_size);
	twltree_iterator_init_set_index_key(&tree, &iter, &search_pattern,
			direction == iterator_forward ? twlscan_forward : twlscan_backward);
}

- (struct tnt_object *)
iterator_next
{
	struct index_node *r = twltree_iterator_next(&iter);
	return likely(r != NULL) ? r->obj : NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	struct index_node *r;
	while ((r = twltree_iterator_next(&iter))) {
		switch (check(&search_pattern, r, self->dtor_arg)) {
		case 0: return r->obj;
		case -1:
		case 1: return NULL;
		case 2: continue;
		}
	}
	return NULL;
}

- (index_cmp)
pattern_compare
{
	return pattern_compare;
}

@end

register_source();
