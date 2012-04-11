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
#import <say.h>
#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>
#import <index.h>

#include <stdbool.h>

#import <mod/box/box.h>
#import <cfg/tarantool_cfg.h>

@implementation Index (Tuple)
static void
box_tuple_u32_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	int n = (uintptr_t)arg;
	struct box_tuple *tuple = box_tuple(obj);
	if (tuple->cardinality <= n)
		index_raise("cardinality too small");
	void *f = tuple_field(tuple, n);
	u32 size = LOAD_VARINT32(f);
	if (size != sizeof(u32))
		index_raise("expected u32");

	node->obj = obj;
	memcpy(node->key, f, sizeof(u32));
}
static void
box_tuple_u64_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	int n = (uintptr_t)arg;
	struct box_tuple *tuple = box_tuple(obj);
	if (tuple->cardinality <= n)
		index_raise("cardinality too small");
	void *f = tuple_field(tuple, n);
	u32 size = LOAD_VARINT32(f);
	if (size != sizeof(u64))
		index_raise("expected u64");

	node->obj = obj;
	memcpy(node->key, f, sizeof(u64));
}
static void
box_tuple_lstr_dtor(struct tnt_object *obj, struct index_node *node, void  *arg)
{
	int n = (uintptr_t)arg;
	struct box_tuple *tuple = box_tuple(obj);
	if (tuple->cardinality <= n)
		index_raise("cardinality too small");
	void *f = tuple_field(tuple, n);
	node->obj = obj;
	memcpy(node->key, &f, sizeof(void *));
}
static void
box_tuple_gen_dtor(struct tnt_object *obj, struct index_node *node_, void *arg)
{
	struct tree_node *node = (void *)node_;
	struct gen_dtor *desc = arg;
	struct box_tuple *tuple = box_tuple(obj);
	void *tuple_data = tuple->data;

	if (tuple->cardinality < desc->min_tuple_cardinality)
		index_raise("tuple cardinality too small");

	for (int i = 0, j = 0, n = 0; i < desc->cardinality; j++) {
		assert(tuple_data < (void *)tuple->data + tuple->bsize);
		u32 len = LOAD_VARINT32(tuple_data);
		if (desc->index_field[i] == j) {
			if (desc->type[i] == NUM && len != sizeof(u32))
				index_raise("key size mismatch, expected u32");
			else if (desc->type[i] == NUM64 && len != sizeof(u64))
				index_raise("key size mismatch, expected u64");

			struct field *f = &node->key[n++];

			f->len = len;
			if (len <= sizeof(f->data))
				memcpy(f->data, tuple_data, len);
			else
				f->data_ptr = tuple_data;
			i++;
		}
		tuple_data += len;
	}

	node->obj = obj;
}

static struct gen_dtor *
cfg_box_tuple_gen_dtor(struct tarantool_cfg_object_space_index *c)
{
	struct gen_dtor *d = calloc(1, sizeof(*d));

	for (int k = 0; c->key_field[k] != NULL; k++) {
		if (c->key_field[k]->fieldno == -1)
			break;

		d->index_field[d->cardinality] = c->key_field[k]->fieldno;

		if (strcmp(c->key_field[k]->type, "NUM") == 0)
			d->type[d->cardinality] = NUM;
		else if (strcmp(c->key_field[k]->type, "NUM64") == 0)
			d->type[d->cardinality] = NUM64;
		else if (strcmp(c->key_field[k]->type, "STR") == 0)
			d->type[d->cardinality] = STRING;
		else
			panic("unknown field data type: `%s'", c->key_field[k]->type);

		if (c->key_field[k]->fieldno > d->min_tuple_cardinality)
			d->min_tuple_cardinality = c->key_field[k]->fieldno + 1;
		d->cardinality++;
	}

	if (d->cardinality > nelem(d->type))
		panic("index cardinality is too big");

	if (d->cardinality == 0)
		panic("index cardinality is 0");

	for (int i = 0; i < d->cardinality; i++)
		for (int j = 0; j < d->cardinality; j++)
			if (d->index_field[i] < d->index_field[j]) {
				int t;
				t = d->index_field[i];
				d->index_field[i] = d->index_field[j];
				d->index_field[j] = t;

				t = d->type[i];
				d->type[i] = d->type[j];
				d->type[j] = t;
			}

	return d;
}

+ (Index *)
new_with_n:(int)n cfg:(struct tarantool_cfg_object_space_index *)cfg
{
	Index *i;

	if (strcmp(cfg->type, "HASH") == 0) {
		if (cfg->key_field[0] == NULL || cfg->key_field[1] != NULL)
			panic("hash index must habve exactly one key_field");

		if (cfg->unique == false)
			panic("hash index must be unique");

		if (strcmp(cfg->key_field[0]->type, "NUM") == 0) {
			i = [Int32Hash alloc];
			i->dtor = box_tuple_u32_dtor;
		} else if (strcmp(cfg->key_field[0]->type, "NUM64") == 0) {
			i = [Int64Hash alloc];
			i->dtor = box_tuple_u64_dtor;
		} else {
			i = [StringHash alloc];
			i->dtor = box_tuple_lstr_dtor;
		}
		i->dtor_arg = (void *)(uintptr_t)cfg->key_field[0]->fieldno;
		i->n = n;
		[i init];
	} else if (strcmp(cfg->type, "TREE") == 0) {
		struct gen_dtor *d = cfg_box_tuple_gen_dtor(cfg);
		if (d->cardinality > 1) {
			i = [GenTree alloc];
			i->dtor = box_tuple_gen_dtor;
			i->dtor_arg = (void *)d;
		} else {
			free(d);
			if (strcmp(cfg->key_field[0]->type, "NUM") == 0) {
				i = [Int32Tree alloc];
				i->dtor = box_tuple_u32_dtor;
			} else if (strcmp(cfg->key_field[0]->type, "NUM64") == 0) {
				i = [Int64Tree alloc];
				i->dtor = box_tuple_u64_dtor;
			} else {
				i = [StringTree alloc];
				i->dtor = box_tuple_lstr_dtor;
			}
			i->dtor_arg = (void *)(uintptr_t)cfg->key_field[0]->fieldno;
		}
		i->n = n;
		[(Tree *)i init_with_unique:cfg->unique];
		if (n > 0) {
			Index *dummy = [[DummyIndex alloc] init_with_index:i];
			i = dummy;
		} else {
			[(id)i set_nodes:NULL count:0 allocated:0];
		}


	} else {
		return nil;
	}

	return i;
}
@end
