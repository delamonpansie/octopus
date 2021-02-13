/*
 * Copyright (C) 2010-2015 Mail.RU
 * Copyright (C) 2010-2015 Yury Vostrikov
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
#import <third_party/qsort_arg.h>

@implementation Tree

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	static struct index_node node_b[8];
	struct index_node *na = GET_NODE(obj_a, node_a),
			  *nb = GET_NODE(obj_b, node_b[0]);
	return eq(na, nb, self->dtor_arg);
}

- (index_cmp)
compare
{
	return compare;
}

- (u32)
size
{
	raise_fmt("Subclass responsibility");
	return 0;
}

- (u32)
slots
{
	raise_fmt("Subclass responsibility");
	return 0;
}

- (size_t)
bytes
{
	raise_fmt("Subclass responsibility");
	return 0;
}

- (void)
clear
{
	raise_fmt("Subclass responsibility");
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
	raise_fmt("Subclass responsibility");
	(void)node;
	return 0;
}

- (void)
replace:(struct tnt_object *)obj
{
	raise_fmt("Subclass responsibility");
	(void)obj;
}

- (int)
remove:(struct tnt_object *)obj
{
	raise_fmt("Subclass responsibility");
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
	raise_fmt("Subclass responsibility");
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
	raise_fmt("Subclass responsibility");
	(void)node; (void)direction;
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
	raise_fmt("Subclass responsibility");
	(void)obj; (void)direction;
}

- (struct tnt_object *)
iterator_next
{
	raise_fmt("Subclass responsibility");
	return NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	raise_fmt("Subclass responsibility");
	(void)check;
	return NULL;
}

- (void)
set_sorted_nodes:(void *)nodes_ count:(size_t)count
{
	raise_fmt("Subclass responsibility");
	(void)nodes_; (void)count;
}

- (bool)
sort_nodes:(void *)nodes_ count:(size_t)count onduplicate:(ixsort_on_duplicate)ondup arg:(void*)arg
{
	int i;
	bool no_dups = true;
	qsort_arg(nodes_, count, node_size, compare, dtor_arg);
	for (i = 1; i < count; i++) {
		struct index_node *node = nodes_ + i * node_size;
		struct index_node *prev = nodes_ + (i-1) * node_size;
		int cmp = compare(node, prev, dtor_arg);
		assert(cmp >= 0);
		if (conf.unique && cmp == 0) {
			no_dups = false;
			if(ondup != NULL) {
				ondup(arg, prev, node, i);
			}
		}
	}
	return no_dups;
}
@end

register_source()
