/*
 * Copyright (C) 2010-2016 Mail.RU
 * Copyright (C) 2010-2014, 2016 Yury Vostrikov
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

static bool
twl_tuple_2_index_key(void *index_key, const void *tuple_key, void *arg)
{
	TWLTree* t = (TWLTree*)arg;
	tnt_ptr *obj = (typeof(obj))tuple_key;
	struct index_node *node = (typeof(node))index_key;
	t->dtor(tnt_ptr2obj(*obj), node, t->dtor_arg);
	return true;
}

static int
twl_index_key_cmp(const void *a, const void *b, void *arg)
{
	TWLTree* t = (TWLTree*)arg;
	struct index_node const *an = (typeof(an))a;
	struct index_node const *bn = (typeof(bn))b;
	return -t->compare(bn, an, t->dtor_arg);
}

static struct twltree_conf_t twltree_fast_conf = {
	.index_key_free = NULL,
	.tuple_key_2_index_key = NULL,
	.tuple_key_cmp = twl_index_key_cmp,
	.index_key_cmp = NULL,
	.page_sizes = NULL,
	.page_sizes_n = 0,
};

static struct twltree_conf_t twltree_compact_conf = {
	.index_key_free = NULL,
	.tuple_key_2_index_key = twl_tuple_2_index_key,
	.tuple_key_cmp = NULL,
	.index_key_cmp = twl_index_key_cmp,
	.page_sizes = NULL,
	.page_sizes_n = 0,
};

static int twl_slabs_n = 0;
static struct slab_cache **twl_slabs = NULL;
void*
twl_realloc(void *old, size_t new_size)
{
	int i;
	if (new_size == 0) {
		if (old)
			sfree(old);
		return NULL;
	}
	struct slab_cache *cache = NULL;
	for (i = 0; i < twl_slabs_n; i++) {
		if (new_size == twl_slabs[i]->item_size) {
			cache = twl_slabs[i];
			break;
		} else if (new_size > twl_slabs[i]->item_size) {
			break;
		}
	}
	if (cache == NULL) {
		cache = xmalloc(sizeof(struct slab_cache));
		slab_cache_init(cache, new_size, SLAB_GROW, "twl_common");
		twl_slabs = xrealloc(twl_slabs, sizeof(twl_slabs[0])*(twl_slabs_n+1));
		memmove(twl_slabs+i+1, twl_slabs+i, sizeof(twl_slabs[0])*(twl_slabs_n-i));
		twl_slabs[i] = cache;
		twl_slabs_n++;
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

static struct slab_cache *twl_compact_slabs = NULL;
static void*
twl_compact_realloc(void *old, size_t new_size)
{
	int i;
	if (new_size == 0) {
		if (old)
			sfree(old);
		return NULL;
	}
	if (unlikely(twl_compact_slabs == NULL)) {
		assert(twltree_compact_conf.page_sizes_n != 0);
		twl_compact_slabs = xcalloc(twltree_compact_conf.page_sizes_n, sizeof(*twl_compact_slabs));
		for (i = 0; i < twltree_compact_conf.page_sizes_n; i++) {
			slab_cache_init(&twl_compact_slabs[i],
					twltree_page_header_size()+sizeof(tnt_ptr)*twltree_compact_conf.page_sizes[i],
					SLAB_GROW, "twl_compact");
		}
	}
	void *new = NULL;
	for (i = 0; i < twltree_compact_conf.page_sizes_n; i++) {
		if (new_size == twl_compact_slabs[i].item_size) {
			new = slab_cache_alloc(&twl_compact_slabs[i]);
			if (new == NULL)
				panic("Out of memory");
			break;
		}
	}
	if (new == NULL)
		return twl_realloc(old, new_size);
	if (old != NULL) {
		struct slab_cache *old_slab = slab_cache_of_ptr(old);
		memcpy(new, old, MIN(new_size, old_slab->item_size));
		sfree(old);
	}
	return new;
}


static void
twl_raise(twlerrcode_t r)
{
	switch(r) {
	case TWL_OK: break;
	case TWL_WRONG_CONFIG: index_raise("Wrong config"); break;
	case TWL_NOMEMORY: panic("Out of memory"); break;
	case TWL_SUPPORT: index_raise("twl support"); break;
	case TWL_DUPLICATE: index_raise("Duplicate"); break;
	case TWL_NOTFOUND: index_raise("Not found"); break;
	}
}

@implementation TWLTree
- (u32)
size
{
	return tree.n_tuple_keys;
}

- (u32)
slots
{
	return tree.n_tuple_space;
}

- (size_t)
bytes
{
	return twltree_bytes(&tree);
}

- (void)
iterator_init_with_direction:(enum iterator_direction)direction
{
	twltree_iterator_init(&tree, &iter,
				direction == iterator_forward ? twlscan_forward : twlscan_backward);
}

- (void)
iterator_init_with_node:(const struct index_node *)node direction:(enum iterator_direction)direction
{
	if (node != &search_pattern)
		memcpy(&search_pattern, node, node_size);
	twltree_iterator_init_set_index_key(&tree, &iter, &search_pattern,
			direction == iterator_forward ? twlscan_forward : twlscan_backward);
}

- (void)
clear
{
	twltree_free(&tree);
	enum twlerrcode_t r = twltree_init(&tree);
	twl_raise(r);
}

- (id)
free
{
	twltree_free(&tree);
	return [super free];
}
@end

@implementation TWLFastTree
- (void)
set_sorted_nodes:(void *)nodes_ count:(size_t)count
{
	assert(node_size > 0);
	twltree_free(&tree);
	if (nodes_ == NULL) {
		twltree_init(&tree);
	} else {
		if (count > 0) {
			enum twlerrcode_t r = twltree_bulk_load(&tree, nodes_, count);
			if (r != TWL_OK) {
				@try {
					twl_raise(r);
				} @finally {
					/* free nodes only if exception is caught somewhere */
					free(nodes_);
				}
			}
			free(nodes_);
		}
	}
}

- (TWLFastTree*) init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	[super init:ic dtor:dc];
	tree.conf = &twltree_fast_conf;
	tree.arg = self;
	tree.tlrealloc = twl_realloc;
	tree.sizeof_index_key = node_size;
	tree.sizeof_tuple_key = node_size;
	enum twlerrcode_t r = twltree_init(&tree);
	twl_raise(r);
	return self;
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	twlerrcode_t r = twltree_insert(&tree, &node_a, true);
	if (r != TWL_OK)
		twl_raise(r);
}

- (int)
remove:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	twlerrcode_t r = twltree_delete(&tree, &node_a);
	if (r != TWL_OK && r != TWL_NOTFOUND)
		twl_raise(r);
	return r == TWL_OK;
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
	dtor(obj, &node_a, dtor_arg);
	twltree_iterator_init_set(&tree, &iter, &node_a,
				direction == iterator_forward ? twlscan_forward : twlscan_backward);
}

- (struct tnt_object *)
iterator_next
{
	struct index_node *r = twltree_iterator_next(&iter);
	return likely(r != NULL) ? r->obj : NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	struct index_node *r;
	while ((r = twltree_iterator_next(&iter))) {
		switch (check(&search_pattern, r, self->dtor_arg)) {
		case 0: return r->obj;
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
	struct index_node* r = twltree_find_by_index_key(&tree, node, NULL);
	return r != NULL ? r->obj : NULL;
}

@end

@implementation TWLCompactTree
- (void)
set_sorted_nodes:(void *)nodes_ count:(size_t)count
{
	assert(node_size > 0);
	twltree_free(&tree);
	if (nodes_ == NULL) {
		twltree_init(&tree);
	} else {
		if (count > 0) {
			/* compress index nodes to pointers */
			tnt_ptr* nodes = (tnt_ptr*)nodes_;
			size_t i;
			for (i = 0; i < count; i++) {
				struct index_node *node = nodes_ + i * node_size;
				nodes[i] = tnt_obj2ptr(node->obj);
			}
			tnt_ptr* _nodes = realloc(nodes, sizeof(nodes[0])*count);
			if (_nodes != NULL) nodes = _nodes;
			enum twlerrcode_t r = twltree_bulk_load(&tree, nodes, count);
			@try {
				twl_raise(r);
			} @finally {
				/* free nodes only if exception is caught somewhere */
				free(nodes);
			}
		}
	}
}

- (TWLCompactTree*) init:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	[super init:ic dtor:dc];
	tree.conf = &twltree_compact_conf;
	tree.arg = self;
	tree.tlrealloc = twl_compact_realloc;
	tree.sizeof_index_key = node_size;
	tree.sizeof_tuple_key = sizeof(tnt_ptr);
	enum twlerrcode_t r = twltree_init(&tree);
	twl_raise(r);
	return self;
}

- (void)
replace:(struct tnt_object *)obj
{
	dtor(obj, &node_a, dtor_arg);
	tnt_ptr ptr = tnt_obj2ptr(obj);
	twlerrcode_t r = twltree_insert(&tree, &ptr, true);
	if (r != TWL_OK)
		twl_raise(r);
}

- (int)
remove:(struct tnt_object *)obj
{
	tnt_ptr ptr = tnt_obj2ptr(obj);
	twlerrcode_t r = twltree_delete(&tree, &ptr);
	if (r != TWL_OK && r != TWL_NOTFOUND)
		twl_raise(r);
	return r == TWL_OK;
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj direction:(enum iterator_direction)direction
{
	tnt_ptr ptr = tnt_obj2ptr(obj);
	twltree_iterator_init_set(&tree, &iter, &ptr,
				direction == iterator_forward ? twlscan_forward : twlscan_backward);
}

- (struct tnt_object *)
iterator_next
{
	tnt_ptr* r = twltree_iterator_next(&iter);
	return r ? tnt_ptr2obj(*r) : NULL;
}

- (struct tnt_object *)
iterator_next_check:(index_cmp)check
{
	tnt_ptr *o;
	while ((o = twltree_iterator_next(&iter))) {
		static struct index_node node_b[8];
		struct index_node *r = GET_NODE(tnt_ptr2obj(*o), node_b[0]);
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
	tnt_ptr* r = twltree_find_by_index_key(&tree, node, NULL);
	return r != NULL ? tnt_ptr2obj(*r) : NULL;
}

@end

register_source();
