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
#include <stdbool.h>

#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>
#import <index.h>

@implementation IndexError
@end

@implementation Index

+ (Index *)
new_conf:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	Index *i;
	if (ic->cardinality == 1 && ic->type == NUMHASH) {
		if (ic->type == HASH && ic->unique == false)
			return nil;

		switch (ic->field_type[0]) {
		case SNUM32:
		case UNUM32:
			i = [Int32Hash alloc];
			break;
		case SNUM64:
		case UNUM64:
			i = [Int64Hash alloc];
			break;
		case SNUM16:
		case UNUM16:
			index_raise("NUM16 single column indexes unupported");
		default:
			abort();
		}
	} else if (ic->type == HASH) {
		i = [GenHash alloc];
	} else if (ic->type == SPTREE) {
		i = [SPTree alloc];
	} else if (ic->type == FASTTREE) {
		i = [TWLFastTree alloc];
	} else if (ic->type == COMPACTTREE) {
		i = [TWLCompactTree alloc];
	} else {
		abort();
	}

	return [i init:ic dtor:dc];
}

#define COMPARE(type)										\
	conf.sort_order[0] != DESC ?								\
	(conf.unique ? (index_cmp)type##_compare : (index_cmp)type##_compare_with_addr) :	\
	(conf.unique ? (index_cmp)type##_compare_desc : (index_cmp)type##_compare_with_addr_desc)
#define EQ(type) \
	conf.unique ? (index_cmp)type##_eq : (index_cmp)type##_eq_with_addr;

- (Index *)
init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	[super init];
	if (ic == NULL)
		return self;
	memcpy(&conf, ic, sizeof(*ic));
	if (ic->cardinality == 1) {
		int ftype = ic->field_type[0];
		switch(ftype) {
		case SNUM32:
		case UNUM32:
			node_size = sizeof(struct tnt_object *) + sizeof(u32);
			init_pattern = ftype == SNUM32 ? i32_init_pattern : u32_init_pattern;
			pattern_compare = (index_cmp)u32_compare;
			eq = EQ(u32);
			compare = COMPARE(u32);
			dtor = ftype == SNUM32 ? dc->i32 : dc->u32;
			dtor_arg = (void *)(uintptr_t)ic->field_index[0];
			break;
		case SNUM64:
		case UNUM64:
			node_size = sizeof(struct tnt_object *) + sizeof(u64);
			init_pattern = ftype == SNUM64 ? i64_init_pattern : u64_init_pattern;
			pattern_compare = (index_cmp)u64_compare;
			eq = EQ(u64);
			compare = COMPARE(u64);
			dtor = ftype == SNUM64 ? dc->i64 : dc->u64;
			dtor_arg = (void *)(uintptr_t)ic->field_index[0];
			break;
		case STRING:
			node_size = sizeof(struct tnt_object *) + sizeof(void *);
			init_pattern = lstr_init_pattern;
			pattern_compare = (index_cmp)lstr_compare;
			eq = EQ(lstr);
			compare = COMPARE(lstr);
			dtor = dc->lstr;
			dtor_arg = (void *)(uintptr_t)ic->field_index[0];
		default:
			break;
		}
	}
	if (node_size == 0) {
		node_size = sizeof(struct tnt_object *);
		for (int i = 0; i < ic->cardinality; i++)
			switch (ic->field_type[i]) {
			case SNUM16:
			case UNUM16:
				node_size += field_sizeof(union index_field, u16); break;
			case SNUM32:
			case UNUM32:
				node_size += field_sizeof(union index_field, u32); break;
			case SNUM64:
			case UNUM64:
				node_size += field_sizeof(union index_field, u64); break;
			case STRING: node_size += field_sizeof(union index_field, str); break;
			case UNDEF: abort();
			}

		init_pattern = gen_init_pattern;
		pattern_compare = (index_cmp)tree_node_compare;
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
	return memcmp(&node_a.key, &node_b[0].key, node_size - sizeof(struct tnt_object *)) == 0;
}

@end

void __attribute__((noreturn)) oct_cold
index_raise_(const char *file, int line, const char *msg)
{
	@throw [[IndexError palloc] init_line:line
					 file:file
				    backtrace:NULL
				       reason:msg];
}

