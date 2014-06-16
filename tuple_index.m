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
#import <say.h>
#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>
#import <index.h>

#include <stdbool.h>

#import <mod/box/box.h>
#import <cfg/octopus.h>

static struct index_node *
box_tuple_u32_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	int n = (uintptr_t)arg;
	struct box_tuple *tuple = box_tuple(obj);
	if (tuple->cardinality <= n)
		index_raise("cardinality too small");
	u8 *f = tuple_field(tuple, n);
	u32 size = LOAD_VARINT32(f);
	if (size != sizeof(u32))
		index_raise("expected u32");

	node->obj = obj;
	memcpy(&node->key, f, sizeof(u32));
	return node;
}
static struct index_node *
box_tuple_u64_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	int n = (uintptr_t)arg;
	struct box_tuple *tuple = box_tuple(obj);
	if (tuple->cardinality <= n)
		index_raise("cardinality too small");
	const u8 *f = tuple_field(tuple, n);
	u32 size = LOAD_VARINT32(f);
	if (size != sizeof(u64))
		index_raise("expected u64");

	node->obj = obj;
	memcpy(&node->key, f, sizeof(u64));
	return node;
}
static struct index_node *
box_tuple_lstr_dtor(struct tnt_object *obj, struct index_node *node, void  *arg)
{
	int n = (uintptr_t)arg;
	struct box_tuple *tuple = box_tuple(obj);
	if (tuple->cardinality <= n)
		index_raise("cardinality too small");
	void *f = tuple_field(tuple, n);
	node->obj = obj;
	memcpy(&node->key, &f, sizeof(void *));
	return node;
}
static struct index_node *
box_tuple_gen_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	struct index_conf *desc = arg;
	struct box_tuple *tuple = box_tuple(obj);
	const u8 *tuple_data = tuple->data;

	if (tuple->cardinality < desc->min_tuple_cardinality)
		index_raise("tuple cardinality too small");

	for (int i = 0, j = 0; i < desc->cardinality; j++) {
		assert(tuple_data < (const u8 *)tuple->data + tuple->bsize);
		u32 len = LOAD_VARINT32(tuple_data);
		while (desc->field_index[i] == j) {
			union index_field *f = (void *)&node->key + desc->offset[i];
			gen_set_field(f, desc->field_type[i], len, tuple_data);
			i++;
		}
		tuple_data += len;
	}

	node->obj = obj;
	return (struct index_node *)node;
}

struct dtor_conf box_tuple_dtor = {
	.u32 = box_tuple_u32_dtor,
	.u64 = box_tuple_u64_dtor,
	.lstr = box_tuple_lstr_dtor,
	.generic = box_tuple_gen_dtor
};

struct index_conf *
cfg_box2index_conf(struct octopus_cfg_object_space_index *c)
{
	struct index_conf *d = xcalloc(1, sizeof(*d));

	for (int i = 0; i < nelem(d->field_index); i++)
		d->field_index[i] = d->cmp_order[i] = d->offset[i] = -1;

	d->unique = c->unique;
	if (strcmp(c->type, "HASH") == 0)
		d->type = HASH;
	else if (strcmp(c->type, "TREE") == 0)
		d->type = TREE;
	else
		panic("unknown index type");

	int offset = 0;
	for (int k = 0; c->key_field[k] != NULL; k++) {
		if (c->key_field[k]->fieldno == -1)
			break;

		assert(d->cmp_order[d->cardinality] == -1);
		assert(d->field_index[d->cardinality] == -1);

		d->cmp_order[d->cardinality] = d->cardinality;
		d->field_index[d->cardinality] = c->key_field[k]->fieldno;
		d->offset[d->cardinality] = offset;

		if (strcmp(c->key_field[k]->sort_order, "ASC") == 0)
			d->sort_order[d->cardinality] = ASC;
		else if (strcmp(c->key_field[k]->sort_order, "DESC") == 0)
			d->sort_order[d->cardinality] = DESC;
		else
			panic("unknown sort order");

		if (strcmp(c->key_field[k]->type, "NUM") == 0) {
			d->field_type[d->cardinality] = NUM32;
			offset += field_sizeof(union index_field, u32);
		} else if (strcmp(c->key_field[k]->type, "NUM16") == 0) {
			d->field_type[d->cardinality] = NUM16;
			offset += field_sizeof(union index_field, u16);
		} else if (strcmp(c->key_field[k]->type, "NUM32") == 0) {
			d->field_type[d->cardinality] = NUM32;
			offset += field_sizeof(union index_field, u32);
		} else if (strcmp(c->key_field[k]->type, "NUM64") == 0) {
			d->field_type[d->cardinality] = NUM64;
			offset += field_sizeof(union index_field, u64);
		} else if (strcmp(c->key_field[k]->type, "STR") == 0) {
			d->field_type[d->cardinality] = STRING;
			offset += field_sizeof(union index_field, str);
		} else
			panic("unknown field data type: `%s'", c->key_field[k]->type);

		if (c->key_field[k]->fieldno + 1 > d->min_tuple_cardinality)
			d->min_tuple_cardinality = c->key_field[k]->fieldno + 1;
		d->cardinality++;
	}

	if (d->cardinality > nelem(d->field_index))
		panic("index cardinality is too big");

	if (d->cardinality == 0)
		panic("index cardinality is 0");

	for (int i = 0; i < d->cardinality; i++)
		for (int j = 0; j < d->cardinality; j++)
			if (d->field_index[i] < d->field_index[j]) {
#define swap(f) ({ int t = d->f[i]; d->f[i] = d->f[j]; d->f[j] = t; })
				swap(field_index);
				swap(offset);
				swap(cmp_order);
				swap(sort_order);
				swap(field_type);
#undef swap
			}

	return d;
}


register_source();
