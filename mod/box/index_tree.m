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
#import <fiber.h>
#import <say.h>
#import <assoc.h>

#import <mod/box/box.h>
#import <mod/box/index.h>
#import <cfg/tarantool_box_cfg.h>


@implementation Tree
- (void)
set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated
{
	assert(nodes == NULL);

	if (nodes_ == NULL) {
		if (allocated == 0)
			allocated = 64;
		nodes = malloc(allocated * node_size);
	} else {
		nodes = nodes_;
	}

	sptree_str_t_init(tree, node_size, nodes, count, allocated,
			  compare, self->dtor_arg);
}

- (id)
init_with_unique:(bool)_unique;
{
	[super init];
	type = TREE;
	tree = malloc(sizeof(*tree));
	unique = _unique;
	return self;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)cardinality
{
        init_pattern(key_data, cardinality, &node, dtor_arg);
 	struct index_node *r = sptree_str_t_find(tree, &node);
	return likely(r && !ghost(r->obj)) ? r->obj : NULL;
}

- (struct tnt_object *)
find_by_obj:(struct tnt_object *)obj
{
	dtor(obj, &node, dtor_arg);
	struct index_node *r = sptree_str_t_find(tree, &node);
	return likely(r && !ghost(r->obj)) ? r->obj : NULL;
}

- (u32)
size
{
	return tree->size;
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node, dtor_arg);
	sptree_str_t_insert(tree, &node);
}

- (void)
remove:(struct tnt_object *)obj
{
        dtor(obj, &node, dtor_arg);
	sptree_str_t_delete(tree, &node);
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
        sptree_str_t_iterator_init_set(tree, &iterator, &search_pattern);
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj
{
        dtor(obj, &search_pattern, dtor_arg);
	sptree_str_t_iterator_init_set(tree, &iterator, &search_pattern);
}

- (struct tnt_object *)
iterator_next
{
	struct index_node *r = sptree_str_t_iterator_next(iterator);
	return likely(r && !ghost(r->obj)) ? r->obj : NULL;
}

- (struct tnt_object *)
iterator_next_verify_pattern
{
	struct index_node *r = sptree_str_t_iterator_next(iterator);

	if (r != NULL) {
		search_pattern.obj = r->obj;
		if (compare(&search_pattern, r, self->dtor_arg) != 0)
			return NULL;
		return r->obj;
	}
	return NULL;
}

+ (void)
build_object_space_trees:(struct object_space *)object_space
{
	say_info("Building tree indexes of object space %i", object_space->n);

	Index<BasicIndex> *pk = object_space->index[0];
	size_t n_tuples = [pk size];
        size_t estimated_tuples = n_tuples * 1.2;

	Tree *ts[MAX_IDX] = { nil, };
	void *nodes[MAX_IDX] = { NULL, };
	int i = 0, tree_count = 0;

	for (int j = 0; object_space->index[j]; j++)
		if ([object_space->index[j] isKindOf:[DummyIndex class]]) {
			DummyIndex *dummy = (id)object_space->index[j];
			if ([dummy is_wrapper_of:[Tree class]]) {
				object_space->index[j] = [dummy unwrap];
				ts[i++] = (id)object_space->index[j];
			}
		}
	tree_count = i;

        if (n_tuples > 0) {
		for (int i = 0; i < tree_count; i++) {
                        nodes[i] = malloc(estimated_tuples * ts[i]->node_size);
			if (nodes[i] == NULL)
                                panic("can't allocate node array");
                }

		struct tnt_object *obj;
		u32 t = 0;
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			for (int i = 0; i < tree_count; i++) {
                                struct index_node *node = nodes[i] + t * ts[i]->node_size;
                                ts[i]->dtor(obj, node, ts[i]->dtor_arg);
                        }
                        t++;
		}
	}

	for (int i = 0; i < tree_count; i++) {
		say_info("  %i:%s", ts[i]->n, [ts[i] class]->name);
		[ts[i] set_nodes:nodes[i]
			   count:n_tuples
		       allocated:estimated_tuples];
	}
}
@end

@implementation Int32Tree
static int
i32_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i32 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;
	else
		return 0;
}

static int
i32_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i32 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

static void
i32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	i32 min = INT32_MIN;
	u32 len;
	switch (cardinality) {
	case 0: memcpy(pattern->key, &min, sizeof(u32));
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u32))
			box_raise(ERR_CODE_ILLEGAL_PARAMS, "key is not u32");
		*(u32 *)pattern->key = read_u32(key);
		break;
	default:
		box_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality too big");
	}
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	node_size = sizeof(struct index_node) + sizeof(i32);
	init_pattern = i32_init_pattern;
	compare = unique ? (void *)i32_compare : i32_compare_with_addr;
	return self;
}
@end

@implementation Int64Tree
static int
i64_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i64 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;
	else
		return 0;
}

static int
i64_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i64 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

static void
i64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	i64 min = INT64_MIN;
	u32 len;
	switch (cardinality) {
	case 1: len = read_varint32(key);
		if (len != sizeof(u64))
			box_raise(ERR_CODE_ILLEGAL_PARAMS, "key is not u64");
		*(u64 *)pattern->key = read_u64(key);
		break;
	case 0: memcpy(pattern->key, &min, sizeof(u64));
		break;
	default:
		box_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality too big");
	}
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	node_size = sizeof(struct index_node) + sizeof(i64);
	init_pattern = i64_init_pattern;
	compare = unique ? (void *)i64_compare : i64_compare_with_addr;
	return self;
}
@end

@implementation StringTree
static int
lstr_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	return lstrcmp(*(void **)na->key, *(void **)nb->key);
}

static int
lstr_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{

	int r = lstrcmp(*(void **)na->key, *(void **)nb->key);
	if (r != 0)
		return r;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

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
		box_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality too big");

	memcpy(&pattern->key, &f, sizeof(void *));
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	node_size = sizeof(struct index_node) + sizeof(void *);
	init_pattern = lstr_init_pattern;
	compare = unique ? (void *)lstr_compare : lstr_compare_with_addr;
	return self;
}
@end


@implementation GenTree
static i8
field_compare(struct field *f1, struct field *f2, enum field_data_type type)
{
        /* When data_ptr eq NULL it is an star field , match any in other words */
	if (f1->data_ptr == NULL)
		return 0;
	if (f2->data_ptr == NULL)
		return 0;

	if (type == NUM) {
		assert(f1->len == f2->len);
		assert(f1->len == sizeof(f1->u32));

		return f1->u32 >f2->u32 ? 1 : f1->u32 == f2->u32 ? 0 : -1;
	} else if (type == NUM64) {
		assert(f1->len == f2->len);
		assert(f1->len == sizeof(f1->u64));

		return f1->u64 >f2->u64 ? 1 : f1->u64 == f2->u64 ? 0 : -1;
	} else if (type == STRING) {
		int cmp;
		void *f1_data, *f2_data;

		f1_data = f1->len <= sizeof(f1->data) ? f1->data : f1->data_ptr;
		f2_data = f2->len <= sizeof(f2->data) ? f2->data : f2->data_ptr;

		cmp = memcmp(f1_data, f2_data, MIN(f1->len, f2->len));

		if (cmp > 0)
			return 1;
		else if (cmp < 0)
			return -1;
		else if (f1->len == f2->len)
			return 0;
		else if (f1->len > f2->len)
			return 1;
		else
			return -1;
	}

	assert(false);
	return 0;
}

static int
tree_node_compare(struct tree_node *na, struct tree_node *nb, struct gen_dtor *desc)
{
	for (int i = 0, end = desc->cardinality; i < end; ++i) {
		int r = field_compare(&na->key[i], &nb->key[i],
				      desc->type[i]);
		if (r != 0)
			return r;
	}

	return 0;
}

static int
tree_node_compare_with_addr(struct tree_node *na, struct tree_node *nb, void *x)
{
	int r = tree_node_compare(na, nb, x);
	if (r != 0)
		return r;

	if (na->obj == NULL || nb->obj == NULL)
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

	if (cardinality > desc->cardinality)
                box_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality too big");

        for (int i = 0; i < desc->cardinality; i++)
		pattern->key[i].data_ptr = NULL;

	for (int i = 0; i < cardinality; i++) {
		u32 len = read_varint32(key_data);
                void *key = read_bytes(key_data, len);

		if (desc->type[i] == NUM && len != sizeof(u32))
			box_raise(ERR_CODE_ILLEGAL_PARAMS, "key size mismatch, expected u32");
		else if (desc->type[i] == NUM64 && len != sizeof(u64))
			box_raise(ERR_CODE_ILLEGAL_PARAMS, "key size mismatch, expected u64");

                pattern->key[i].len = len;
		if (len <= sizeof(pattern->key[i].data))
			memcpy(pattern->key[i].data, key, len);
		else
			pattern->key[i].data_ptr = key;
		key += len;
	}

	pattern->obj = NULL;
}

- (id)
init_with_unique:(bool)_unique
{
	[super init_with_unique:_unique];
	struct gen_dtor *desc = dtor_arg;
	node_size = sizeof(struct index_node) + desc->cardinality * sizeof(struct field);
	init_pattern = gen_init_pattern;
	compare = unique ? (void *)tree_node_compare : tree_node_compare_with_addr;
	return self;
}
@end
