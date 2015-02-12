/*
 * Copyright (C) 2014 Mail.RU
 * Copyright (C) 2014 Teodor Sigaev <teodor@sigaev.ru>
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

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <third_party/twltree/twltree.h>
#include <third_party/bsearch.h>

#ifndef offsetof
#define offsetof(type, member) ((size_t) &((type *)0)->member)
#endif

#ifndef TYPEALIGN
#define TYPEALIGN(ALIGNVAL,LEN)  \
		(((long) (LEN) + ((ALIGNVAL) - 1)) & ~((long) ((ALIGNVAL) - 1)))

#define SHORTALIGN(LEN)                 TYPEALIGN(sizeof(int16_t), (LEN))
#define INTALIGN(LEN)                   TYPEALIGN(sizeof(int32_t), (LEN))
#define MAXALIGN(LEN)                   TYPEALIGN(sizeof(int64_t), (LEN))
#define PTRALIGN(LEN)                   TYPEALIGN(sizeof(void*), (LEN))
#endif

typedef struct twlpage_t twlpage_t;

struct twlpage_t {
	u_int32_t	n_tuple_keys;
	u_int32_t	page_n;
	twlpage_t	*left;
	twlpage_t	*right;

	char		data[1];
};

#define TWLPAGEHDRSZ	(offsetof(twlpage_t, data))
#define TUPITH(tt, page, i)	((page)->data + ((tt)->sizeof_tuple_key * (i))) 
#define TUPMOVE(tt, page, to, from, cnt) do { \
	memmove(TUPITH((tt), (page), (to)), TUPITH((tt), (page), (from)), (cnt) * (tt)->sizeof_tuple_key); \
} while(0)
#define TUPCPY(tt, pageto, to, pagefrom, from, cnt) do { \
	memmove(TUPITH((tt), (pageto), (to)), TUPITH((tt), (pagefrom), (from)), (cnt) * (tt)->sizeof_tuple_key); \
} while(0)

size_t
twltree_page_header_size() {
	return TWLPAGEHDRSZ;
}

typedef struct index_key_t {
	twlpage_t 	*page;
	char		key[1];
} index_key_t;
#define  TWLIKTHDRSZ	(offsetof(index_key_t, key))

static int
inner_tuple_key_t_cmp(const void* a, const void* b, void* arg) {
	twltree_t	*tt = arg;

	return tt->conf->index_key_cmp(((index_key_t*)a)->key, ((index_key_t*)b)->key, tt->arg);
}

static bool
inner_tuple_key_2_index_key(void* index_key, const void* tuple_key, void* arg) {
	twltree_t	*tt = arg;
	memcpy(index_key, ((index_key_t*)tuple_key)->key, tt->sizeof_index_key);
	return true;
}

static u_int32_t default_sizes[] = {29, 33, 37, 41, 45, 49, 53, 57};
#define SMALLEST(tt) ((tt)->conf->page_sizes[0])
#define LARGEST(tt) ((tt)->conf->page_sizes[(tt)->conf->page_sizes_n - 1])
static inline u_int32_t
next_size(twltree_t *tt, u_int32_t size, int maybe_resize_first) {
	int i;
	if (maybe_resize_first && size < SMALLEST(tt) && tt->page_index == NULL) {
		i = 7;
		while (i <= size) i *= 2;
		if (i > SMALLEST(tt)) i = SMALLEST(tt);
		return i;
	}
	for (i = 0; size >= tt->conf->page_sizes[i]; i++) {
		assert(i < tt->conf->page_sizes_n-1);
	}
	return tt->conf->page_sizes[i];
}

static twlpage_t* twltree_new_page(twltree_t *tt, u_int32_t page_n);

twlerrcode_t
twltree_init(twltree_t *tt) {
	size_t index_key_alloc;

	if ((tt->flags & TWL_FLAGS_MASK) != tt->flags)
		return TWL_WRONG_CONFIG;
	if ((tt->flags & TWL_INNER) == 0)
		tt->child = NULL;
	else if (tt->child == NULL)
		return TWL_WRONG_CONFIG;

	if (tt->sizeof_index_key == 0 || tt->sizeof_tuple_key == 0)
		return TWL_WRONG_CONFIG;

	if (tt->conf->tuple_key_2_index_key == NULL) {
		if (tt->conf->index_key_cmp == NULL)
			tt->conf->index_key_cmp = tt->conf->tuple_key_cmp;
		if (tt->conf->tuple_key_cmp == NULL)
			tt->conf->tuple_key_cmp = tt->conf->index_key_cmp;
		if (!(tt->sizeof_index_key == tt->sizeof_tuple_key &&
				tt->conf->tuple_key_cmp == tt->conf->index_key_cmp &&
				tt->conf->index_key_free == NULL))
			return TWL_WRONG_CONFIG;
	}

	if (tt->conf->index_key_cmp == NULL)
		return TWL_WRONG_CONFIG;

	if (tt->conf->page_sizes_n == 0 || tt->conf->page_sizes == NULL) {
		tt->conf->page_sizes = default_sizes;
		tt->conf->page_sizes_n = sizeof(default_sizes) / sizeof(default_sizes[0]);
	}

	if (tt->tlrealloc == NULL)
		tt->tlrealloc = realloc;

	index_key_alloc = (TWLIKTHDRSZ + tt->sizeof_index_key + 8) / 8 * 8;
	tt->search_index_key = tt->tlrealloc(NULL, index_key_alloc * 3);
	if (!tt->search_index_key) {
		return TWL_NOMEMORY;
	}
	memset(tt->search_index_key, 0, index_key_alloc * 3);
	tt->stored_index_key = (index_key_t*)((char*)tt->search_index_key + index_key_alloc);
	tt->firstpage = (index_key_t*)((char*)tt->stored_index_key + index_key_alloc);
		 
	tt->firstpage->page = twltree_new_page(tt, next_size(tt, 0, 1));
	assert(tt->firstpage->page->page_n > 0);
	if (tt->firstpage->page == NULL) {
		tt->tlrealloc(tt->search_index_key, 0);
		return TWL_NOMEMORY;
	}
	tt->page_index = NULL;
	memset(&tt->pi_iterator, 0, sizeof(tt->pi_iterator));

	tt->n_tuple_keys = tt->n_index_keys = 0;
	tt->n_tuple_space = 0;

	return TWL_OK;
}

#define FIRST_INNER_INDEX_KEY(tt) ((index_key_t*)tt->page_index->firstpage->page->data)
static u_int32_t inner_sizes[] = {500, 600, 750, 900};
static twltree_conf_t inner_conf = {
	.index_key_free = NULL,
	.tuple_key_2_index_key = inner_tuple_key_2_index_key,
	.tuple_key_cmp = inner_tuple_key_t_cmp,
	.index_key_cmp = (void*)abort,
	.page_sizes = inner_sizes,
	.page_sizes_n = sizeof(inner_sizes) / sizeof(inner_sizes[0]),
};
static twltree_t*
twltree_alloc_inner(twltree_t *tt) {
	twltree_t* inner = tt->tlrealloc(0, sizeof(*inner));
	twltree_t* outter = tt->child ? tt->arg : tt;
	if (inner == NULL) {
		return NULL;
	}
	memset(inner, 0, sizeof(*inner));
	inner->sizeof_index_key = outter->sizeof_index_key;
	inner->sizeof_tuple_key = TWLIKTHDRSZ + outter->sizeof_index_key;
	inner->conf = &inner_conf;
	inner->arg = outter;
	inner->tlrealloc = tt->tlrealloc;
	inner->child = tt;
	inner->flags = TWL_INNER;
	if (twltree_init(inner) != TWL_OK) {
		tt->tlrealloc(inner, 0);
		return NULL;
	}
	return inner;
}

void
twltree_free(twltree_t *tt) {
	twlpage_t *page, *tp;
	if (tt->page_index != NULL && tt->conf->index_key_free != NULL) {
		index_key_t *key;
		twltree_iterator_init(tt->page_index, &tt->pi_iterator, twlscan_forward);
		while( (key = twltree_iterator_next(&tt->pi_iterator)) != NULL) {
			tt->conf->index_key_free(key->key, tt->arg);
		}
	}
	if (tt->page_index != NULL) {
		twltree_free(tt->page_index);
		tt->tlrealloc(tt->page_index, 0);
		tt->page_index = NULL;
	}
	page = tt->firstpage->page;
	while (page != NULL) {
		tp = page;
		page = page->right;
		tt->tlrealloc(tp, 0);
	}
	tt->tlrealloc(tt->search_index_key, 0);
}

static twlpage_t*
twltree_new_page(twltree_t *tt, u_int32_t page_n) {
	twlpage_t	*page;

	page = tt->tlrealloc(NULL, TWLPAGEHDRSZ + page_n * tt->sizeof_tuple_key);
	if (page) {
		memset(page, 0, TWLPAGEHDRSZ);
		page->page_n = page_n;
		tt->n_tuple_space += page_n;
	}

	return page;
}

static int
twltree_realloc_page(twltree_t *tt, index_key_t *key, u_int32_t page_n) {
	twlpage_t	*page;

	page = tt->tlrealloc(key->page, TWLPAGEHDRSZ + page_n * tt->sizeof_tuple_key);
	if (page == NULL)
		return TWL_NOMEMORY;
	key->page = page;
	tt->n_tuple_space -= page->page_n;
	page->page_n = page_n;
	tt->n_tuple_space += page_n;
	if (page->left)
		page->left->right = page;
	else
		tt->firstpage->page = page;
	if (page->right)
		page->right->left = page;
	return TWL_OK;
}

static inline void
free_index_key(twltree_t *tt, index_key_t* key) {
	if (tt->conf->index_key_free != NULL)
		tt->conf->index_key_free(key->key, tt->arg);
}

static void
twltree_free_page(twltree_t *tt, index_key_t *key) {
	twlpage_t	*page;
	int rd;

	if (tt->conf->index_key_free != NULL)
		memcpy(tt->stored_index_key->key, key->key, tt->sizeof_tuple_key);
	page = key->page;
	tt->n_tuple_space -= page->page_n;
	rd = twltree_delete(tt->page_index, key);
	assert(rd == TWL_OK);
	(void)rd;
	free_index_key(tt, tt->stored_index_key);

	if (page->left)
		page->left->right = page->right;
	else {
		assert(page->right != NULL);
		tt->firstpage->page = page->right;
		if (page->right->right == NULL) {
			assert(tt->page_index->page_index == NULL);
			free_index_key(tt, FIRST_INNER_INDEX_KEY(tt));
			twltree_free(tt->page_index);
			tt->page_index = NULL;
			tt->n_index_keys--;
		}
	}
	if (page->right)
		page->right->left = page->left;

	tt->tlrealloc(page, 0);
	assert(tt->n_index_keys > 0);
	tt->n_index_keys--;
}

static inline twlerrcode_t
make_search_key2(twltree_t *tt, void *tuple_key, index_key_t *key) {
	if (tt->conf->tuple_key_2_index_key) {
		if (tt->conf->tuple_key_2_index_key(key->key, tuple_key, tt->arg) == false)
			return TWL_SUPPORT;
	} else {
		memcpy(key->key, tuple_key, tt->sizeof_tuple_key);
	}

	return TWL_OK;
}

#define make_search_key(tt, tk) make_search_key2((tt), (tk), (tt)->search_index_key)

#define free_search_key(tt) free_index_key((tt), (tt)->search_index_key)

#define iterator_fetch(it) ((index_key_t*)TUPITH((it), (it)->page, (it)->ith))

typedef struct search_result_t {
	index_key_t	*key;
	twlpage_t	*page;
	int32_t		pos;
	bool		isEq;
} search_result_t;

static inline void
fixup_iterator(twliterator_t *it, twlscan_direction_t direction) {
	if (direction == twlscan_forward) {
		if (it->ith == it->page->n_tuple_keys) {
			it->ith = 0;
			it->page = it->page->right;
		}
	} else if (direction == twlscan_backward) {
		if (it->ith == 0) {
			it->page = it->page->left;
			if (it->page)
				it->ith = it->page->n_tuple_keys - 1;
		} else {
			it->ith--;
		}
	}
}

static inline twlerrcode_t
binary_search_i(twltree_t *tt, search_result_t *res, void const *tuple_key, void const *index_key, twlscan_direction_t direction) {
	bs_t		bs;
	twlpage_t	*page = res->page;
	int		diff;
	twlerrcode_t	r = TWL_OK;
	void*		stored_tuple_key;

	res->isEq = false;
	bs_init(&bs, page->n_tuple_keys);
	if (direction == twlscan_search && page->right == NULL && page->n_tuple_keys > 3) {
		bs.mid = page->n_tuple_keys - 2;
	}
	do {
		stored_tuple_key = TUPITH(tt, page, bs.mid);

		if (tt->conf->tuple_key_cmp != NULL && tuple_key != NULL) {
			diff = tt->conf->tuple_key_cmp(stored_tuple_key, tuple_key, tt->arg);
		} else {
			r = make_search_key2(tt, stored_tuple_key, tt->stored_index_key);
			if (r != TWL_OK)
				return r;

			diff = tt->conf->index_key_cmp(tt->stored_index_key->key, index_key,
					tt->arg);
			free_index_key(tt, tt->stored_index_key);
		}
		if (direction == twlscan_search) {
			bs_step_to_equal_may_break(&bs, -diff);
		} else if (direction == twlscan_forward) {
			bs_step_to_first_ge(&bs, -diff);
		} else {
			bs_step_to_first_gt(&bs, -diff);
		}
	} while (bs_not_found(&bs));
	res->isEq = bs.equal;
	res->pos = bs.mid;
	return TWL_OK;
}

static twlerrcode_t
binary_search(twltree_t *tt, search_result_t *res, void const *tuple_key, void const *index_key) {
	return binary_search_i(tt, res, tuple_key, index_key, twlscan_search);
}

static twlerrcode_t
binary_search_forward(twltree_t *tt, search_result_t *res, void const *tuple_key, void const *index_key) {
	return binary_search_i(tt, res, tuple_key, index_key, twlscan_forward);
}

static twlerrcode_t
binary_search_backward(twltree_t *tt, search_result_t *res, void const *tuple_key, void const *index_key) {
	return binary_search_i(tt, res, tuple_key, index_key, twlscan_backward);
}

static inline index_key_t*
find_index_key_t_i(twltree_t *top, void const *index_key, twlscan_direction_t direction) {
	twliterator_t	*it;
	index_key_t 	*key;
	twlpage_t	*page;
	bs_t		bs;
	int		diff;
	twltree_t	*tt, *outter;
	u_int32_t	sizeof_index_key;
	outter = top->child != NULL ? (twltree_t*)top->arg : top;

	tt = top;
	while (tt->page_index != NULL) {
		tt = tt->page_index;
	}
	sizeof_index_key = tt->sizeof_tuple_key;

	key = tt->firstpage;
	while(tt != top) {
		page = key->page;
		it = &tt->child->pi_iterator;
		it->page = NULL;
		it->sizeof_tuple_key = tt->sizeof_tuple_key;
		it->direction = direction;

		bs_init(&bs, page->n_tuple_keys);
		if (direction == twlscan_search && page->right == NULL && page->n_tuple_keys > 3) {
			bs.mid = page->n_tuple_keys - 2;
		}
		do {
			key = (index_key_t*)(page->data + sizeof_index_key * bs.mid);
			diff = outter->conf->index_key_cmp(key->key, index_key, outter->arg);

			if (direction == twlscan_search) {
				bs_step_to_equal_may_break(&bs, -diff);
			} else if (direction == twlscan_forward) {
				bs_step_to_first_ge(&bs, -diff);
			} else {
				bs_step_to_first_gt(&bs, -diff);
			}
		} while (bs_not_found(&bs));
		if (bs.mid == page->n_tuple_keys) {
			assert(page->right == NULL);
			if (direction == twlscan_forward)
				return NULL;
			bs.mid--;
		}
		it->page = page;
		it->ith = bs.mid;

		tt = tt->child;
		key = iterator_fetch(it);
	}
	return key;
}

static index_key_t*
find_index_key_t_search(twltree_t *top, void const *index_key) {
	return find_index_key_t_i(top, index_key, twlscan_search);
}

static index_key_t*
find_index_key_t_forward(twltree_t *top, void *index_key) {
	return find_index_key_t_i(top, index_key, twlscan_forward);
}

static index_key_t*
find_index_key_t_backward(twltree_t *top, void *index_key) {
	return find_index_key_t_i(top, index_key, twlscan_backward);
}

static inline twlerrcode_t
search_tuple(twltree_t *tt, search_result_t *search, void const *tuple_key, void const *index_key) {
	search->page = NULL;
	search->isEq = false;
	search->pos = 0;
	search->key = find_index_key_t_search(tt, index_key);
	search->page = search->key->page;
	assert(search->page->n_tuple_keys > 0);

	return binary_search(tt, search, tuple_key, index_key);
}

static inline twlerrcode_t
search_border(twltree_t *tt, search_result_t *search, void *tuple_key, void *index_key, twlscan_direction_t direction) {
	assert(direction == twlscan_forward || direction == twlscan_backward);
	search->page = NULL;
	search->isEq = false;
	search->pos = 0;
	if (direction == twlscan_forward) {
		search->key = find_index_key_t_forward(tt, index_key);
		if (search->key == NULL)
			return TWL_OK;
	} else {
		search->key = find_index_key_t_backward(tt, index_key);
	}
	search->page = search->key->page;
	assert(search->page->n_tuple_keys > 0);

	if (direction == twlscan_forward) {
		return binary_search_forward(tt, search, tuple_key, index_key);
	} else {
		return binary_search_backward(tt, search, tuple_key, index_key);
	}
}

static twlerrcode_t
split_page(twltree_t *tt, index_key_t *key) {
	search_result_t s;
	twlpage_t	*page, *leftpage;
	u_int32_t	limit, shrinked;
	twlerrcode_t	r, rt = TWL_SUPPORT;

	page = key->page;

	limit = (page->page_n-1)/2;
	if (page->right == NULL)
		limit = tt->conf->page_sizes[tt->n_index_keys % (tt->conf->page_sizes_n/2 + 1)]-1;
	else if (tt->flags & TWL_OVERLEFT)
		limit = next_size(tt, limit+1, 0) - 1;
	else if (tt->flags & TWL_OVERRIGHT)
		limit = page->page_n - next_size(tt, limit+1, 0) + 1;

	assert(limit >= 1 && limit < LARGEST(tt));

	leftpage = twltree_new_page(tt, next_size(tt, limit, 0));
	if (leftpage == NULL)
		return TWL_NOMEMORY;

	/* fill new left page */
	TUPCPY(tt, leftpage, 0, page, 0, limit);
	leftpage->n_tuple_keys = limit;
	r = make_search_key(tt, TUPITH(tt, leftpage, limit-1));
	if (r != TWL_OK) {
		tt->n_tuple_space -= leftpage->page_n;
		tt->tlrealloc(leftpage, 0);
		return r;
	}
	tt->search_index_key->page = leftpage;

	/* update old page (it becomes right page) */
	TUPMOVE(tt, page, 0, limit, page->page_n - limit);
	assert(page->n_tuple_keys == page->page_n);
	page->n_tuple_keys -= limit;

	if (tt->page_index == NULL) {
		assert(key == tt->firstpage);
		tt->page_index = twltree_alloc_inner(tt);
		if (tt->page_index == NULL) {
			r = TWL_NOMEMORY;
			goto rollback_split;
		}
		r = make_search_key2(tt, TUPITH(tt, page, page->n_tuple_keys-1), tt->firstpage);
		if (r != TWL_OK) {
			goto rollback_split;
		}
		twltree_insert(tt->page_index, tt->firstpage, false);
		tt->n_index_keys++;
		key = FIRST_INNER_INDEX_KEY(tt);
		assert(key->page == page);
	}

	memcpy(tt->stored_index_key, key, tt->sizeof_index_key + TWLIKTHDRSZ);

	rt = twltree_insert(tt->page_index, tt->search_index_key, false);

	r = search_tuple(tt->page_index, &s, tt->stored_index_key, tt->stored_index_key->key);
	assert(r == TWL_OK && s.isEq == true);
	key = (index_key_t*)TUPITH(tt->page_index, s.page, s.pos);
	assert(key->page == page);

	if (rt != TWL_OK) {
		r = rt;
		goto rollback_split;
	}

	shrinked = next_size(tt, page->n_tuple_keys, 0);
	if (shrinked < page->page_n && page->right != NULL) {
		/* try compact old page */
		r = twltree_realloc_page(tt, key, shrinked);
		if (r != TWL_OK) {
rollback_split:
			/* restore old page */
			TUPMOVE(tt, page, limit, 0, page->page_n - limit);
			TUPCPY(tt, page, 0, leftpage, 0, limit);
			page->n_tuple_keys += limit;
			/* delete new left page */
			if (rt == TWL_OK) {
				r = twltree_delete(tt->page_index, tt->search_index_key);
				assert(r == TWL_OK);
			}
			free_search_key(tt);
			tt->n_tuple_space -= leftpage->page_n;
			tt->tlrealloc(leftpage, 0);
			/* delete page index, if page is single */
			if (tt->firstpage->page == page && page->right == NULL && tt->page_index != NULL) {
				free_index_key(tt, key);
				twltree_free(tt->page_index);
				tt->page_index = NULL;
			}
			return r;
		}
		page = key->page;
	}

	tt->n_index_keys++;

	/* update links */
	leftpage->left = page->left;
	if (leftpage->left)
		leftpage->left->right = leftpage;
	else
		tt->firstpage->page = leftpage;
	leftpage->right = page;
	page->left = leftpage;

	return TWL_OK;
}

static void
copy_current_index_key_to_upper(twltree_t *tt, index_key_t *key) {
	twliterator_t *it = &tt->pi_iterator;
	index_key_t *upper;
	assert((char*)key == TUPITH(it, it->page, it->ith));
	if (it->ith < it->page->n_tuple_keys-1) {
		return;
	}
	if (tt->page_index->page_index == NULL) {
		assert(it->page == tt->page_index->firstpage->page);
		return;
	}
	it = &tt->page_index->pi_iterator;
	upper = (__typeof__(upper))(TUPITH(it, it->page, it->ith));
	assert(upper->page == tt->pi_iterator.page);
	memcpy(upper->key, key->key, tt->page_index->sizeof_index_key);
	copy_current_index_key_to_upper(tt->page_index, upper);
}

twlerrcode_t
twltree_insert(twltree_t *tt, void *tuple_key, bool replace) {
	search_result_t s;
	twlerrcode_t	r;

	if (tt->n_tuple_keys == 0) {
		s.page = tt->firstpage->page;
		memcpy(TUPITH(tt, s.page, 0), tuple_key, tt->sizeof_tuple_key);
		s.page->n_tuple_keys++;
		tt->n_tuple_keys++;
		return TWL_OK;
	}

restart:
	if ((r = make_search_key(tt, tuple_key)) != TWL_OK)
		return r;

	r = search_tuple(tt, &s, tuple_key, tt->search_index_key->key);
	if (s.isEq) {
		if (r == TWL_OK && replace) {
			memcpy(TUPITH(tt, s.page, s.pos), tuple_key, tt->sizeof_tuple_key);

			if (s.pos == s.page->n_tuple_keys-1 && tt->page_index != NULL) {
				/* update key of upper index */
				free_index_key(tt, s.key);
				memcpy(s.key->key, tt->search_index_key->key, tt->sizeof_index_key);
				copy_current_index_key_to_upper(tt, s.key);
			} else {
				free_search_key(tt);
			}
			return TWL_OK;
		}
		r = TWL_DUPLICATE;
	}
	if (r != TWL_OK) {
		free_search_key(tt);
		return r;
	}

	if (s.page->n_tuple_keys >= s.page->page_n) {
		assert(s.page->n_tuple_keys == s.page->page_n);
		if (s.page->page_n < LARGEST(tt)) {
			u_int32_t page_n = next_size(tt, s.page->page_n, 1);

			r = twltree_realloc_page(tt, s.key, page_n);
			if (r != TWL_OK) {
				free_search_key(tt);
				return r;
			}
			s.page = s.key->page;
		} else { /* split page for two */
			free_search_key(tt);

			if ((r = split_page(tt, s.key)) != TWL_OK)
				return r;

			/* restart insert: it's simple and cheap */
			goto restart;
		}
	}

	if (s.pos != s.page->n_tuple_keys)
		TUPMOVE(tt, s.page, s.pos+1, s.pos, s.page->n_tuple_keys - s.pos);

	memcpy(TUPITH(tt, s.page, s.pos), tuple_key, tt->sizeof_tuple_key);

	if (s.page->right == NULL && s.pos == s.page->n_tuple_keys && tt->page_index != NULL) {
		/* update key of right-most page */
		free_index_key(tt, s.key);
		memcpy(s.key->key, tt->search_index_key->key, tt->sizeof_index_key);
		copy_current_index_key_to_upper(tt, s.key);
	} else {
		free_search_key(tt);
	}

	s.page->n_tuple_keys++;
	tt->n_tuple_keys++;

	return TWL_OK;
}

twlerrcode_t
twltree_bulk_load(twltree_t *tt, void *tuple_key, u_int32_t nkeys) {
	twlpage_t *page, *left;
	u_int32_t page_size, i;
	twlerrcode_t r;

	if ((r = twltree_init(tt)) != TWL_OK) {
		return r;
	}
	page_size = SMALLEST(tt) > (nkeys+1) ? next_size(tt, nkeys, 1) : SMALLEST(tt);
	page = twltree_new_page(tt, page_size);
	if (page == NULL)
		goto fail;
	tt->firstpage->page = page;
	i = 1;
	goto cpy;
	while (nkeys > 0) {
		left = page;
		i = (i + 1) % (tt->conf->page_sizes_n / 2 + 1);
		page_size = SMALLEST(tt) > (nkeys+1) ? LARGEST(tt) : tt->conf->page_sizes[i];
		page = twltree_new_page(tt, page_size);
		if (page == NULL)
			goto fail;
		page->left = left;
		left->right = page;
cpy:
		page_size = (page_size - 1) > nkeys ? nkeys : (page_size - 1);
		memcpy(TUPITH(tt, page, 0), tuple_key, tt->sizeof_tuple_key * page_size);
		page->n_tuple_keys = page_size;
		tuple_key += tt->sizeof_tuple_key * page_size;
		nkeys -= page_size;
		tt->n_tuple_keys += page_size;
	}
	tt->page_index = twltree_alloc_inner(tt);
	if (tt->page_index == NULL)
		goto fail;
	page = tt->firstpage->page;
	while (page != NULL) {
		if ((r = make_search_key(tt, TUPITH(tt, page, page->n_tuple_keys-1))) != TWL_OK)
			goto fail;
		tt->search_index_key->page = page;
		r = twltree_insert(tt->page_index, tt->search_index_key, false);
		if (r != TWL_OK) {
			free_search_key(tt);
			goto fail;
		}
		tt->n_index_keys++;
		page = page->right;
	}
	return TWL_OK;
fail:
	twltree_free(tt);
	return TWL_NOMEMORY;
}

static twlerrcode_t
merge_pages(twltree_t *tt, index_key_t *key) {
	twlpage_t	*page;
	twlerrcode_t	r;
	page = key->page;
	index_key_t *nxtkey;
	twlpage_t *nxtpage;
	u_int32_t sum, right_sum, left_sum;

	right_sum = page->right ? page->n_tuple_keys + page->right->n_tuple_keys : 0;
	if (right_sum && right_sum < LARGEST(tt) - SMALLEST(tt) / 4) {
		tt->pi_iterator.direction = twlscan_forward;
		nxtpage = page->right;
		sum = right_sum;
	} else {
		left_sum = page->left ? page->n_tuple_keys + page->left->n_tuple_keys : 0;
		if (left_sum && left_sum < LARGEST(tt) - SMALLEST(tt) / 4) {
			tt->pi_iterator.direction = twlscan_backward;
			nxtpage = page->left;
			sum = left_sum;
		} else {
			return TWL_OK;
		}
	}

	twltree_iterator_next(&tt->pi_iterator);
	nxtkey = iterator_fetch(&tt->pi_iterator);
	assert(nxtkey->page == nxtpage);

	if (nxtkey->page == page->left) {
		index_key_t *tkey = nxtkey;
		nxtkey = key;
		key = tkey;
		page = key->page;
		nxtpage = nxtkey->page;
	}

	if (nxtpage->page_n < sum + 1) {
		r = twltree_realloc_page(tt, nxtkey, next_size(tt, sum, 0));
		if (r != TWL_OK)
			return TWL_OK;
		nxtpage = nxtkey->page;
	}

	assert(nxtpage->page_n > sum);

	TUPMOVE(tt, nxtpage, page->n_tuple_keys, 0, nxtpage->n_tuple_keys);
	TUPCPY(tt, nxtpage, 0, page, 0, page->n_tuple_keys);
	nxtpage->n_tuple_keys += page->n_tuple_keys;
	twltree_free_page(tt, key);
	return TWL_OK;
}

twlerrcode_t
twltree_delete(twltree_t *tt, void *tuple_key) {
	search_result_t s;
	twlerrcode_t	r;

	if (tt->n_tuple_keys == 0) /* empty tree */
		return TWL_NOTFOUND;

	if ((r = make_search_key(tt, tuple_key)) != TWL_OK)
		return r;

	r = search_tuple(tt, &s, tuple_key, tt->search_index_key->key);

	free_search_key(tt);
	if (r != TWL_OK)
		return r;
	if (!s.isEq)
		return TWL_NOTFOUND;

	assert(tt->n_tuple_keys > 0);
	tt->n_tuple_keys--;

	if (s.page->n_tuple_keys == 1) {
		if (tt->firstpage->page != s.page || s.page->right != NULL)
			twltree_free_page(tt, s.key);
		else
			s.page->n_tuple_keys = 0;
		return TWL_OK;
	}

	if (s.pos != s.page->n_tuple_keys - 1) {
		TUPMOVE(tt, s.page, s.pos, s.pos+1, s.page->n_tuple_keys - s.pos - 1);
	} else if (tt->page_index != NULL) {
		/* we need to rewrite stored key in inner twltree */
		/* no need to delete-insert cause order is not disturbed */
		free_index_key(tt, s.key);
		make_search_key2(tt, TUPITH(tt, s.page, s.page->n_tuple_keys - 2), s.key);
		copy_current_index_key_to_upper(tt, s.key);
	}
	s.page->n_tuple_keys--;

	if (s.page->n_tuple_keys + 1 + SMALLEST(tt)/8 < s.page->page_n) {
		if (s.page->page_n > SMALLEST(tt) || tt->page_index == NULL) {
			u_int32_t nxt = next_size(tt, s.page->n_tuple_keys, 1);
			assert(nxt <= s.page->page_n);
			if (nxt != s.page->page_n) {
				twltree_realloc_page(tt, s.key, nxt);
			}

			return TWL_OK;
		}

		return merge_pages(tt, s.key);
	}

	return TWL_OK;
}

twlerrcode_t
twltree_find_by_index_key_rc(twltree_t *tt, void const *index_key, void **tuple_key) {
	search_result_t s;
	twlerrcode_t	r;

	if (tt->n_tuple_keys == 0) /* empty tree */
		return TWL_NOTFOUND;

	if (tt->conf->index_key_cmp == tt->conf->tuple_key_cmp)
		r = search_tuple(tt, &s, index_key, index_key);
	else
		r = search_tuple(tt, &s, NULL, index_key);
	if (r != TWL_OK)
		return r;
	if (!s.isEq)
		return TWL_NOTFOUND;

	if (tuple_key != NULL)
		*tuple_key = TUPITH(tt, s.page, s.pos);
	return TWL_OK;
}

void*
twltree_find_by_index_key(twltree_t *tt, void const *index_key, twlerrcode_t *r) {
	void *res = NULL;
	twlerrcode_t t = twltree_find_by_index_key_rc(tt, index_key, &res);
	if (r != NULL) *r = t;
	return res;
}

twlerrcode_t
twltree_find_by_index_key_and_copy(twltree_t *tt, void const *index_key, void *tuple_key) {
	void *res = NULL;
	twlerrcode_t r = twltree_find_by_index_key_rc(tt, index_key, &res);
	if (r == TWL_OK)
		memcpy(tuple_key, res, tt->sizeof_tuple_key);
	return r;
}

void
twltree_iterator_init(twltree_t *tt, twliterator_t *it, twlscan_direction_t direction) {
	index_key_t 	*key;

	it->page = NULL;
	it->ith = 0;
	it->sizeof_tuple_key = tt->sizeof_tuple_key;
	it->direction = direction;
	assert(direction == twlscan_forward || direction == twlscan_backward);
	if (tt->n_tuple_keys == 0)
		return;

	if (direction == twlscan_forward || tt->page_index == NULL) {
		it->page = tt->firstpage->page;
		it->ith = (direction == twlscan_forward) ? 0 : (it->page->n_tuple_keys - 1);
	} else {
		twltree_iterator_init(tt->page_index, &tt->pi_iterator, direction);
		key = iterator_fetch(&tt->pi_iterator);
		if (key != NULL) {
			it->page = key->page;
			it->ith = (direction == twlscan_forward) ? 0 : (it->page->n_tuple_keys - 1);
		}
	}

}

int
twltree_iterator_init_set(twltree_t *tt, twliterator_t *it, void* tuple_key, twlscan_direction_t direction) {
	search_result_t s;
	twlerrcode_t	r;

	it->page = NULL;
	it->sizeof_tuple_key = tt->sizeof_tuple_key;
	it->direction = direction;

	if (tt->n_tuple_keys == 0) /* empty tree */
		return TWL_OK;

	if ((r = make_search_key(tt, tuple_key)) != TWL_OK)
		return r;

	r = search_border(tt, &s, tuple_key, tt->search_index_key->key, direction);
	free_search_key(tt);
	if (r != TWL_OK)
		return r;
	if (s.key == NULL)
		return TWL_OK;

	it->page = s.page;
	it->ith = s.pos;
	fixup_iterator(it, direction);
	return TWL_OK;
}

int
twltree_iterator_init_set_index_key(twltree_t *tt, twliterator_t *it, void *index_key, twlscan_direction_t direction) {
	search_result_t s;
	twlerrcode_t	r;

	it->page = NULL;
	it->sizeof_tuple_key = tt->sizeof_tuple_key;
	it->direction = direction;

	if (tt->n_tuple_keys == 0) /* empty tree */
		return TWL_OK;

	if (tt->conf->index_key_cmp == tt->conf->tuple_key_cmp)
		r = search_border(tt, &s, index_key, index_key, direction);
	else
		r = search_border(tt, &s, NULL, index_key, direction);
	if (r != TWL_OK)
		return r;
	if (s.key == NULL)
		return TWL_OK;

	it->page = s.page;
	it->ith = s.pos;
	fixup_iterator(it, direction);
	return TWL_OK;
}

void*
twltree_iterator_next(twliterator_t *it) {
	void	*tuple_key = NULL;

	if (it->page) {
		tuple_key = TUPITH(it, it->page, it->ith);

		if (it->direction == twlscan_forward) {
			it->ith += it->direction;

			if (it->ith >= it->page->n_tuple_keys) {
				it->page = it->page->right;
				it->ith = 0;
			}
		} else if (it->direction == twlscan_backward) {
			if (it->ith == 0) {
				it->page = it->page->left;
				if (it->page)
					it->ith = it->page->n_tuple_keys - 1;
			} else {
				it->ith--;
			}
		} else {
			abort();
		}
	}

	return tuple_key;
}

size_t
twltree_bytes(twltree_t *tt)
{
	size_t res = 0;
	res += sizeof(*tt);
	res += (tt->sizeof_index_key + TWLIKTHDRSZ + 8) / 8 * 8 * 3; /* search_index_key */
	res += tt->sizeof_tuple_key * tt->n_tuple_keys;
	res += offsetof(twlpage_t, data) * (tt->n_index_keys + 1);
	if (tt->page_index)
		res += twltree_bytes(tt->page_index);
	return res;
}
