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
#import <third_party/qsort_arg.h>

static bool
nih_tuple_2_index_key(const void *tuple_key, void *index_key, void *arg)
{
	NIHTree* t = (NIHTree*)arg;
	tnt_ptr *obj = (typeof(obj))tuple_key;
	struct index_node *node = (typeof(node))index_key;
	t->dtor(tnt_ptr2obj(*obj), node, t->dtor_arg);
	return true;
}

static int
nih_index_key_cmp(const void *a, const void *b, void *arg)
{
	NIHTree* t = (NIHTree*)arg;
	struct index_node const *an = (typeof(an))a;
	struct index_node const *bn = (typeof(bn))b;
	return t->compare(an, bn, t->dtor_arg);
}

static int nih_slabs_n = 0;
static struct slab_cache **nih_slabs = NULL;
static void*
nih_realloc(void *old, size_t new_size, void *arg _unused_)
{
	int i;
	if (new_size == 0) {
		if (old)
			sfree(old);
		return NULL;
	}
	struct slab_cache *cache = NULL;
	for (i = 0; i < nih_slabs_n; i++) {
		if (new_size == nih_slabs[i]->item_size) {
			cache = nih_slabs[i];
			break;
		} else if (new_size > nih_slabs[i]->item_size) {
			break;
		}
	}
	if (cache == NULL) {
		cache = xmalloc(sizeof(struct slab_cache));
		slab_cache_init(cache, new_size, SLAB_GROW, "nih_tree");
		nih_slabs = xrealloc(nih_slabs, sizeof(nih_slabs[0])*(nih_slabs_n+1));
		memmove(nih_slabs+i+1, nih_slabs+i, sizeof(nih_slabs[0])*(nih_slabs_n-i));
		nih_slabs[i] = cache;
		nih_slabs_n++;
	}
	void *new = slab_cache_alloc(cache);
	if (new == NULL)
		panic("Out of memory");
	if (old != NULL) {
		struct slab_cache *old_slab = slab_cache_of_ptr(old);
		memcpy(new, old, MIN(new_size, old_slab->item_size));
		sfree(old);
	}
	return new;
}

static void
nih_raise(niherrcode_t r)
{
	switch(r) {
	case NIH_OK: break;
	case NIH_WRONG_CONFIG: index_raise("Wrong config"); break;
	case NIH_NOMEMORY: panic("Out of memory"); break;
	case NIH_SUPPORT: index_raise("nih support"); break;
	case NIH_DUPLICATE: index_raise("Duplicate"); break;
	case NIH_NOTFOUND: index_raise("Not found"); break;
	}
}

@implementation NIHTree
- (u32)
size
{
	return nihtree_count(&tree);
}

- (u32)
slots
{
	return nihtree_tuple_space(&tree);
}

- (size_t)
bytes
{
	return nihtree_bytes(&tree, &tconf);
}

- (void)
iterator_init_with_direction:(enum iterator_direction)direction
{
	nihtree_iter_init(&tree, &tconf, &iter,
				direction == iterator_forward ? nihscan_forward : nihscan_backward);
}

- (void)
iterator_init_with_node:(const struct index_node *)node direction:(enum iterator_direction)direction
{
	if (node != &search_pattern)
		memcpy(&search_pattern, node, node_size);
	nihtree_iter_init_set_buf(&tree, &tconf, &iter, &search_pattern,
			direction == iterator_forward ? nihscan_forward : nihscan_backward,
			&node_b);
}

- (uint32_t)
position_with_node:(const struct index_node *)key
{
	return nihtree_key_position_buf(&tree, &tconf, key, NULL, &node_b);
}

- (uint32_t)
position_with_object:(struct tnt_object*)obj
{
	dtor(obj, &node_a, dtor_arg);
	return nihtree_key_position_buf(&tree, &tconf, &node_a, NULL, &node_b);
}

- (void)
clear
{
	nihtree_release(&tree, &tconf);
}

- (id)
free
{
	nihtree_release(&tree, &tconf);
	return [super free];
}
@end

@implementation NIHCompactTree
- (void)
set_nodes:(void *)nodes count:(size_t)count allocated:(size_t)allocated
{
	assert(node_size > 0);
	(void)allocated;
	nihtree_release(&tree, &tconf);
	if (nodes != NULL && count > 0) {
		niherrcode_t r = NIH_OK;
		qsort_arg(nodes, count, node_size, compare, dtor_arg);
		/* compress index nodes to pointers */
		@try {
			int i;
			for (i=0; r == NIH_OK && i < count; i++) {
				struct index_node *node = nodes + i * node_size;
				tnt_ptr tuple = tnt_obj2ptr(node->obj);
				r = nihtree_insert_key_buf(&tree, &tconf, &tuple, false, node, &node_a);
			}
			if (r != NIH_OK) {
				nihtree_release(&tree, &tconf);
				nih_raise(r);
			}
		} @finally {
			free(nodes);
		}
	}
}

- (NIHCompactTree*) init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	[super init:ic dtor:dc];
	tconf.sizeof_key = node_size;
	tconf.sizeof_tuple = sizeof(tnt_ptr);
	tconf.nhrealloc = nih_realloc;
	tconf.inner_max = 128;
	tconf.leaf_max = 32;
	tconf.tuple_2_key = nih_tuple_2_index_key;
	tconf.key_cmp = nih_index_key_cmp;
	tconf.key_tuple_cmp = NULL;
	tconf.arg = self;
	niherrcode_t r = nihtree_conf_init(&tconf);
	nih_raise(r);
	assert(sizeof(iter)+sizeof(__iter_padding) >= nihtree_iter_need_size(6));
	iter.max_height = 6;
	nihtree_init(&tree);
	return self;
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	tnt_ptr ptr = tnt_obj2ptr(obj);
	niherrcode_t r = nihtree_insert_key_buf(&tree, &tconf, &ptr, true, &node_a, &search_pattern);
	if (r != NIH_OK)
		nih_raise(r);
}

- (int)
remove:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	niherrcode_t r = nihtree_delete_buf(&tree, &tconf, &node_a, &search_pattern);
	if (r != NIH_OK && r != NIH_NOTFOUND)
		nih_raise(r);
	return r == NIH_OK;
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
	dtor(obj, &search_pattern, dtor_arg);
	nihtree_iter_init_set_buf(&tree, &tconf, &iter, &search_pattern,
				direction == iterator_forward ? nihscan_forward : nihscan_backward, &node_a);
}

- (struct tnt_object *)
iterator_next
{
	tnt_ptr* r = nihtree_iter_next(&iter);
	return r ? tnt_ptr2obj(*r) : NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	tnt_ptr *o;
	while ((o = nihtree_iter_next(&iter))) {
		struct index_node *r = GET_NODE(tnt_ptr2obj(*o), node_b);
		switch (check(&search_pattern, r, self->dtor_arg)) {
		case 0: return tnt_ptr2obj(*o);
		case -1:
		case 1: return NULL;
		case 2: continue;
		}
	}
	return NULL;
}

- (struct tnt_object *)
find_node:(const struct index_node *)node
{
	tnt_ptr* r = nihtree_find_by_key_buf(&tree, &tconf, node, NULL, &node_b);
	return r != NULL ? tnt_ptr2obj(*r) : NULL;
}

@end

register_source();
