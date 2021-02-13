/*
 * Copyright (C) 2010-2016 Mail.RU
 * Copyright (C) 2010-2016 Yury Vostrikov
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
#include <stdbool.h>

#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>
#import <index.h>
#import <say.h>

@implementation IndexError
@end

@implementation Index

+ (id)
new_conf:(const struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	Index *i;
	if (ic->cardinality == 1 && ic->type == NUMHASH) {
		if (ic->unique == false)
			index_raise("NUMHASH index must be unique");

		switch (ic->field[0].type) {
		case SNUM8:
		case UNUM8:
		case SNUM16:
		case UNUM16:
		case SNUM32:
		case UNUM32:
			i = [Int32Hash alloc];
			break;
		case SNUM64:
		case UNUM64:
			i = [Int64Hash alloc];
			break;
		default:
			abort();
		}
	} else if (ic->type == HASH || ic->type == PHASH) {
		if (ic->unique == false)
			return nil;
		i = ic->type == HASH ? [GenHash alloc] : [PHash alloc];
	} else if (ic->type == SPTREE) {
		i = [SPTree alloc];
	} else if (ic->type == FASTTREE) {
		i = [TWLFastTree alloc];
	} else if (ic->type == COMPACTTREE) {
		i = [TWLCompactTree alloc];
	} else if (ic->type == POSTREE) {
		i = [NIHCompactTree alloc];
	} else {
		abort();
	}

	return [i init:ic dtor:dc];
}

#define COMPARE(type)										\
	conf.field[0].sort_order != DESC ?								\
	(conf.unique ? (index_cmp)type##_compare : (index_cmp)type##_compare_with_addr) :	\
	(conf.unique ? (index_cmp)type##_compare_desc : (index_cmp)type##_compare_with_addr_desc)
#define EQ(type) \
	conf.unique ? (index_cmp)type##_eq : (index_cmp)type##_eq_with_addr;

- (id)
init:(const struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	[super init];
	if (ic == NULL)
		return self;
	memcpy(&conf, ic, sizeof(*ic));
	if (ic->cardinality == 1) {
		int ftype = ic->field[0].type;
		conf.field[0].offset = 0;
		switch(ftype) {
		case SNUM32:
		case UNUM32:
			node_size = sizeof(struct tnt_object *) + sizeof(u32);
			eq = EQ(u32);
			dtor = dc->u32;
			dtor_arg = (void *)(uintptr_t)ic->field[0].index;
			break;
		case SNUM64:
		case UNUM64:
			node_size = sizeof(struct tnt_object *) + sizeof(u64);
			eq = EQ(u64);
			dtor = dc->u64;
			dtor_arg = (void *)(uintptr_t)ic->field[0].index;
			break;
		case STRING:
			node_size = sizeof(struct tnt_object *) + field_sizeof(union index_field, str);
			init_pattern = lstr_init_pattern;
			eq = EQ(lstr);
			compare = COMPARE(lstr);
			dtor = dc->lstr;
			dtor_arg = (void *)(uintptr_t)ic->field[0].index;
		default:
			break;
		}

		switch(ftype) {
		case SNUM32:
			init_pattern = i32_init_pattern;
			compare = COMPARE(i32);
			break;
		case UNUM32:
			init_pattern = u32_init_pattern;
			compare = COMPARE(u32);
			break;
		case SNUM64:
			init_pattern = i64_init_pattern;
			compare = COMPARE(i64);
			break;
		case UNUM64:
			init_pattern = u64_init_pattern;
			compare = COMPARE(u64);
			break;
		default:
			break;
		}

	}

	if (node_size == 0) {
		int offset = 0;
		for (int i = 0; i < ic->cardinality; i++) {
			conf.field[i].offset = offset;
			switch (ic->field[i].type) {
			case SNUM8:
			case UNUM8:
			case SNUM16:
			case UNUM16:
			case SNUM32:
			case UNUM32:
				offset += field_sizeof(union index_field, u32); break;
			case SNUM64:
			case UNUM64:
				offset += field_sizeof(union index_field, u64); break;
			case STRING:
				offset += field_sizeof(union index_field, str); break;
			case UNDEF: abort();
			}
		}
		node_size = sizeof(struct tnt_object *) + offset;

		init_pattern = gen_init_pattern;
		eq = conf.unique ? (index_cmp)tree_node_eq : (index_cmp)tree_node_eq_with_addr;
		compare = conf.unique ? (index_cmp)tree_node_compare : (index_cmp)tree_node_compare_with_addr;
		dtor = dc->generic;
		dtor_arg = &conf;
	}
	return self;
}

- (void)
valid_object:(struct tnt_object*)obj
{
	dtor(obj, &node_a, dtor_arg);
}

- (u32)
cardinality
{
	return conf.cardinality;
}

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	struct index_node node_b[8];
	dtor(obj_a, &node_a, dtor_arg);
	dtor(obj_b, node_b, dtor_arg);
	return eq(&node_a, node_b, self->dtor_arg);
}

- (struct tnt_object *)
find:(const char *)key
{
	switch (conf.field[0].type) {
	case SNUM8:  node_a.key.i32 = (i32)*(i8 *)key; break;
	case UNUM8:  node_a.key.u32 = (u32)*(u8 *)key; break;
	case SNUM16: node_a.key.i32 = (i32)*(i16 *)key; break;
	case UNUM16: node_a.key.u32 = (u32)*(u16 *)key; break;
	case SNUM32:
	case UNUM32: node_a.key.u32 = *(u32 *)key; break;
	case SNUM64:
	case UNUM64: node_a.key.u64 = *(u64 *)key; break;
	case STRING: set_lstr_field(&node_a.key, strlen(key), (const u8*)key); break;
	default: abort();
	}
	node_a.obj = (void *)(uintptr_t)1; /* cardinality */
	return [(id<BasicIndex>)self find_node: &node_a];
}

- (u32)
size
{
	raise_fmt("Subclass responsibility");
	return 0;
}

- (const char *)
info
{
	struct tbuf *b = tbuf_alloc(fiber->pool);
	tbuf_printf(b, "%s", [[self class] name]);
	index_conf_print(b, &conf);
	return b->ptr;
}
@end

void __attribute__((noreturn)) oct_cold
index_raise_(const char *file, int line, const char *msg)
{
	@throw [[IndexError with_reason: msg] init_line:line file:file];
}

register_source()
