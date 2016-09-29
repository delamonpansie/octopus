/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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
#import <cfg/octopus.h>

#include <stdbool.h>

#import <mod/box/box.h>

static struct index_node *
box_tuple_u32_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	int n = (uintptr_t)arg;
	u8 *f = tuple_field(obj, n);
	if (f == NULL)
		index_raise("cardinality too small");
	if (*f != sizeof(u32))
		index_raise("expected u32");

	node->obj = obj;
	node->key.u32 = *(u32*)(f + 1);
	return node;
}
static struct index_node *
box_tuple_u64_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	int n = (uintptr_t)arg;
	const u8 *f = tuple_field(obj, n);
	if (f == NULL)
		index_raise("cardinality too small");
	if (*f != sizeof(u64))
		index_raise("expected u64");

	node->obj = obj;
	node->key.u64 = *(u64*)(f + 1);
	return node;
}
static struct index_node *
box_tuple_lstr_dtor(struct tnt_object *obj, struct index_node *node, void  *arg)
{
	int n = (uintptr_t)arg;
	const u8 *f = tuple_field(obj, n);
	if (f == NULL)
		index_raise("cardinality too small");
	size_t size = LOAD_VARINT32(f);
	if (size > 0xffff)
		index_raise("string key too long");
	node->obj = obj;
	set_lstr_field(&node->key, size, f);
	return node;
}
static struct index_node *
box_tuple_gen_dtor(struct tnt_object *obj, struct index_node *node, void *arg)
{
	const struct index_conf *desc = arg;

	node->obj = obj;

	if (desc->cardinality == 1) {
		const u8 *f = tuple_field(obj, desc->field[0].index);
		if (f == NULL)
			index_raise("cardinality too small");
		u32 len = LOAD_VARINT32(f);
		gen_set_field(&node->key, desc->field[0].type, len, f);
		return node;
	}

	if (tuple_cardinality(obj) < desc->min_tuple_cardinality)
		index_raise("cardinality too small");

	int i = 0, j = 0;
	int indi = desc->fill_order[i];
	const struct index_field_desc *field = &desc->field[indi];
	const u8 *data = tuple_data(obj);

	for (;;j++) {
		u32 len = LOAD_VARINT32(data);
		if (field->index == j) {
			union index_field *f = (void *)&node->key + field->offset;
			gen_set_field(f, field->type, len, data);
			if (++i == desc->cardinality)
				goto end;
			indi = desc->fill_order[i];
			field = &desc->field[indi];
		}
		data += len;
	}
end:
	return (struct index_node *)node;
}

struct dtor_conf box_tuple_dtor = {
	.u32 = box_tuple_u32_dtor,
	.u64 = box_tuple_u64_dtor,
	.lstr = box_tuple_lstr_dtor,
	.generic = box_tuple_gen_dtor
};

typedef struct {
	int type;
	char** name;
} typenames;

#define eq(t, s) (strcmp((t),(s)) == 0)
static typenames *one_column_types = (typenames[]){
	{SNUM32, (char *[]){"NUM", "SNUM", "NUM32", "SNUM32", NULL}},
	{SNUM64, (char *[]){"NUM64", "SNUM64", NULL}},
	{STRING, (char *[]){"STR", "STRING", NULL}},
	{UNUM32, (char *[]){"UNUM", "UNUM32", NULL}},
	{UNUM64, (char *[]){"UNUM64", NULL}},
	{SNUM16, (char *[]){"NUM16", "SNUM16", NULL}},
	{UNUM16, (char *[]){"UNUM16", NULL}},
	{UNDEF, (char *[]){NULL}}
};

static typenames *many_column_types = (typenames[]){
	{UNUM32, (char *[]){"NUM", "UNUM", "NUM32", "UNUM32", NULL}},
	{UNUM64, (char *[]){"NUM64", "UNUM64", NULL}},
	{STRING, (char *[]){"STR", "STRING", NULL}},
	{SNUM32, (char *[]){"SNUM", "SNUM32", NULL}},
	{SNUM64, (char *[]){"SNUM64", NULL}},
	{UNUM16, (char *[]){"NUM16", "UNUM16", NULL}},
	{SNUM16, (char *[]){"SNUM16", NULL}},
	{UNDEF, (char *[]){NULL}},
};


struct index_conf *
cfg_box2index_conf(struct octopus_cfg_object_space_index *c, int sno, int ino, int do_panic)
{
	extern void out_warning(int v, char *format, ...);
	struct index_conf *d = calloc(1, sizeof(*d));
#define exception(fmt, ...) \
	do { if (do_panic) { panic("space %d index %d " fmt, sno, ino, ##__VA_ARGS__); } \
	     else { out_warning(0, "space %d index %d " fmt, sno, ino, ##__VA_ARGS__); return NULL; } } while(0)

	for (int i = 0; i < nelem(d->field); i++)
		d->field[i].index = d->fill_order[i] = d->field[i].offset = -1;

	d->unique = c->unique;
	if (strcmp(c->type, "HASH") == 0)
		d->type = HASH;
	else if (strcmp(c->type, "NUMHASH") == 0)
		d->type = NUMHASH;
	else if (strcmp(c->type, "TREE") == 0)
		d->type = COMPACTTREE;
	else if (strcmp(c->type, "FASTTREE") == 0)
		d->type = FASTTREE;
	else if (strcmp(c->type, "SPTREE") == 0)
		d->type = SPTREE;
	else if (strcmp(c->type, "POSTREE") == 0)
		d->type = POSTREE;
	else if (strcmp(c->type, "HUGEHASH") == 0)
		d->type = PHASH;
	else
		exception("unknown index type");

	if (d->unique == false && (d->type == HASH || d->type == NUMHASH || d->type == PHASH))
		exception("hash index should be unique");

	__typeof__(c->key_field[0]) key_field;
	for (d->cardinality = 0; c->key_field[(int)d->cardinality] != NULL; d->cardinality++) {
		key_field = c->key_field[(int)d->cardinality];
		if (key_field->fieldno == -1)
			exception("key %d fieldno should be set", d->cardinality);
		if (key_field->fieldno > 255)
			exception("key %d fieldno must be between 0 and 255", d->cardinality);
		if (!eq(key_field->sort_order, "ASC") && !eq(key_field->sort_order, "DESC"))
			exception("key %d unknown sort order", d->cardinality);
		if (d->cardinality > nelem(d->field))
			exception("key %d index cardinality is too big", d->cardinality);
	}

	if (d->cardinality == 0)
		exception("index cardinality is 0");

	for (int k = 0; k < d->cardinality; k++) {
		key_field = c->key_field[k];
		d->fill_order[k] = k;
		d->field[k].index = key_field->fieldno;
		d->field[k].sort_order = eq(key_field->sort_order, "ASC") ? ASC : DESC;

		const char *typename = key_field->type;
		int type = UNDEF;
		typenames *names = d->cardinality == 1 ? one_column_types : many_column_types;
		for (;type == UNDEF && names->type != UNDEF; names++) {
			char **name = names->name;
			for(;*name != NULL; name++) {
				if (eq(typename, *name)) {
					type = names->type;
					break;
				}
			}
		}
		if (type == UNDEF) {
			exception("key %d unknown field data type: `%s'", k, typename);
		}
		d->field[k].type = type;
		if (key_field->fieldno + 1 > d->min_tuple_cardinality)
			d->min_tuple_cardinality = key_field->fieldno + 1;
	}

	index_conf_sort_fields(d);

	return d;
}

#undef eq

register_source();
