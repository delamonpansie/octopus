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
#import <third_party/sptree.h>

@implementation SPTree

- (void)
set_nodes:(void *)nodes_ count:(size_t)count allocated:(size_t)allocated
{
	assert(node_size > 0);
	sptree_destroy(tree);
	if (nodes_ == NULL) {
		if (allocated == 0)
			allocated = 64;
		nodes_ = xmalloc(allocated * node_size);
	}

	sptree_init(tree, node_size, nodes_, count, allocated,
		    compare, self->dtor_arg);
}

- (SPTree*)
init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	[super init:ic dtor:dc];
	tree = xmalloc(sizeof(*tree));
	sptree_init(tree, node_size, NULL, 0, 0, compare, self->dtor_arg);
	return self;
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

- (struct tnt_object *)
find_node:(const struct index_node *)node
{
	struct index_node *r = sptree_find(tree, node);
	return r != NULL ? r->obj : NULL;
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
iterator_init_with_direction:(enum iterator_direction)direction
{
	sptree_iterator_init(tree, &iterator, direction);
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
        dtor(obj, &search_pattern, dtor_arg);
	sptree_iterator_init_set(tree, &iterator, &search_pattern, direction);
}

- (void)
iterator_init_with_node:(const struct index_node *)node direction:(enum iterator_direction)direction
{
	if (node != &search_pattern)
		memcpy(&search_pattern, node, node_size);
	sptree_iterator_init_set(tree, &iterator, &search_pattern, direction);
}

- (struct tnt_object *)
iterator_next
{
	struct index_node *r = sptree_iterator_next(iterator);
	return likely(r != NULL) ? r->obj : NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	struct index_node *r;
	while ((r = sptree_iterator_next(iterator))) {
		switch (check(&search_pattern, r, self->dtor_arg)) {
		case 0: return r->obj;
		case -1:
		case 1: return NULL;
		case 2: continue;
		}
	}
	return NULL;
}
@end

