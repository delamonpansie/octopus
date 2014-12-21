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
#import <say.h>

@implementation Tree
#define COMPARE(type)										\
	conf.sort_order[0] == ASC ?								\
	(conf.unique ? (index_cmp)type##_compare : (index_cmp)type##_compare_with_addr) :	\
	(conf.unique ? (index_cmp)type##_compare_desc : (index_cmp)type##_compare_with_addr_desc)

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	static struct index_node node_b[8];
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b[0]);
	return eq(na, nb, self->dtor_arg) == 0;
}

- (struct tnt_object *)
find:(const char *)key
{
	switch (conf.field_type[0]) {
	case NUM16: node_a.key.u16 = *(u16 *)key; break;
	case NUM32: node_a.key.u32 = *(u32 *)key; break;
	case NUM64: node_a.key.u64 = *(u64 *)key; break;
	case STRING: node_a.key.ptr = key; break;
	default: abort();
	}
	node_a.obj = (void *)(uintptr_t)1; /* cardinality */
	return [self find_node: &node_a];
}

- (index_cmp)
pattern_compare
{
	return pattern_compare;
}

- (u32)
size
{
	raise("Subclass responsibility");
	return 0;
}

- (u32)
slots
{
	raise("Subclass responsibility");
	return 0;
}

- (size_t)
bytes
{
	raise("Subclass responsibility");
	return 0;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	init_pattern(key_data, cardinality, &node_a, dtor_arg);
	return [self find_node: &node_a];
}

- (struct tnt_object *)
find_obj:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	return [self find_node: &node_a];
}

- (struct tnt_object *)
find_node:(const struct index_node *)node
{
	raise("Subclass responsibility");
	(void)node;
	return 0;
}

- (void)
replace:(struct tnt_object *)obj
{
	raise("Subclass responsibility");
	(void)obj;
}

- (int)
remove:(struct tnt_object *)obj
{
	raise("Subclass responsibility");
	(void)obj;
	return 0;
}

- (void)
iterator_init
{
	[self iterator_init_with_direction:iterator_forward];
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
iterator_init_with_direction:(enum iterator_direction)direction
{
	(void)direction;
	raise("Subclass responsibility");
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data
	    cardinalty:(u32)cardinality
	     direction:(enum iterator_direction)direction
{
	if (cardinality == 0) {
		[self iterator_init_with_direction: direction];
	} else {
		init_pattern(key_data, cardinality, &search_pattern, dtor_arg);
		[self iterator_init_with_node: &search_pattern direction: direction];
	}
}

- (void)
iterator_init_with_node:(const struct index_node *)node direction:(enum iterator_direction)direction
{
	raise("Subclass responsibility");
	(void)node; (void)direction;
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
	raise("Subclass responsibility");
	(void)obj; (void)direction;
}

- (struct tnt_object *)
iterator_next
{
	raise("Subclass responsibility");
	return NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	raise("Subclass responsibility");
	(void)check;
	return NULL;
}

- (void)
set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated
{
	raise("Subclass responsibility");
	(void)nodes_; (void)count; (void)allocated;
}

@end

register_source()
