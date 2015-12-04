/*
 * Copyright (C) 2015 Mail.RU
 * Copyright (C) 2015 Sokolov Yuriy <funny.falcon@gmail.com>
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
#include <malloc.h>

#include "../bsearch.h"
#include "nihtree.h"

#if NIH_TREE_DEBUG
#define trace(format, ...) fprintf(stderr, "%s:%d " format "\n", __func__, __LINE__, ##__VA_ARGS__)
#else
#define trace(...)
#endif
#ifndef NIH_TREE_NO_SPREAD
#define NIH_TREE_NO_SPREAD 0
#endif

#define VOIDMOVE(ptr, cnt, at, oldlen, newlen, elsize) do {\
	void* _p = (ptr); \
	ssize_t _c = (cnt); \
	ssize_t _a = (at); \
	ssize_t _o = (oldlen); \
	ssize_t _n = (newlen); \
	size_t _sz = (elsize); \
	memmove(_p+(_a+_n)*_sz, _p+(_a+_o)*_sz, _sz*(_c-_a-_o)); \
} while(0)
#define ARRMOVE(ptr, cnt, at, oldlen, newlen) \
	VOIDMOVE((ptr), (cnt), (at), (oldlen), (newlen), sizeof((ptr)[0]))
#define VOIDCPY(aptr, aind, bptr, bind, cnt, elsize) do {\
	void* _ap = (aptr); \
	void* _bp = (bptr); \
	ssize_t _ai = (aind); \
	ssize_t _bi = (bind); \
	ssize_t _c = (cnt);   \
	size_t _sz = (elsize); \
	memcpy(_ap+_ai*_sz, _bp+_bi*_sz, _c*_sz); \
} while(0)
#define ARRCPY(aptr, aind, bptr, bind, cnt)  \
	VOIDCPY((aptr), (aind), (bptr), (bind), (cnt), sizeof((aptr)[0]))
static inline int
RUP(int v, int m) { return (v + (m-1)) / m; }
static inline int
UNRUP(int v, int m) { return v*m - (m-1); }

static int
nih_flexi_capa(int cnt, int maxcapa) {
	int capa = 1;
	while (capa <= cnt) {
		capa = RUP(capa*5, 4);
	}
	if (capa > maxcapa) {
		capa = maxcapa;
	}
	return capa;
}


typedef struct nihleaf {
	unsigned height : 4; /* height == 0 for leaf */
	unsigned rc : 28;
	uint16_t cnt;  /* tuple count */
	unsigned capa : 15; /* capacity (for root) */
	unsigned last_op_delete: 1; /* 0 - no or insertion, 1 - deletion */
	uint8_t  data[0];
} nihleaf_t;

typedef struct nihnode {
	unsigned height : 4; /* 0 < height <= 15 for inner node */
	unsigned rc : 28;
	uint16_t cnt;   /* children count */
	unsigned capa : 15;  /* capacity (for root) */
	unsigned last_op_join: 1;
	uint32_t total; /* total tuples under this node */
	nihpage_t* children[0]; /* pointers to children */
} nihnode_t;

union nihpage {
	struct nihpage_common common;
	nihleaf_t leaf;
	nihnode_t node;
};

static inline uint32_t nihpage_total(nihpage_t* page) {
	return page->common.height == 0 ? page->leaf.cnt : page->node.total;
}

static inline void*
nihleaf_tuple(nihtree_conf_t* conf, nihleaf_t* leaf, uint32_t i) {
	assert(leaf->cnt > i);
	return leaf->data + conf->sizeof_tuple * i;
}

static inline uint32_t*
nihnode_counts(nihnode_t *node) {
	return ((uint32_t*)(node->children + node->capa));
}

static inline void*
nihnode_keys(nihnode_t* node) {
	return ((uint8_t*)node->children) +
		node->capa*(sizeof(nihpage_t*)+sizeof(uint32_t));
}

/* counts: using well known counting approach */
static void
counts_optimize(uint32_t* cnt, int n) {
	int i, j;
	for (i = 1; i < n; i++) {
		j = i + (i&-i);
		if (j <= n) cnt[j-1] += cnt[i-1];
	}
}

static void
counts_deoptimize(uint32_t* cnt, int n) {
	int i, j;
	for (i = n-1; i >= 1; i--) {
		j = i + (i&-i);
		if (j <= n) cnt[j-1] -= cnt[i-1];
	}
}
/* returns sum first n elements */
static uint32_t
counts_sumfirst(uint32_t* cnt, int n) {
	if (n == 0) return 0;
	uint32_t v = cnt[n-1];
	while ((n ^= n&-n) > 0) {
		v += cnt[n-1];
	}
	return v;
}
static void
counts_add(uint32_t* cnt, int n, int i, int add) {
	i++;
	while (i <= n) {
		cnt[i-1] += add;
		i += i&-i;
	}
}
/* done counts */

static void*
nih_default_realloc(void *ptr, size_t size, void * a __attribute__((unused))) {
	if (size == 0 && ptr == NULL) {
		return NULL;
	}
	return realloc(ptr, size);
}

static void*
nih_alloc(nihtree_conf_t* conf, size_t size) {
	return conf->nhrealloc(NULL, size, conf->arg);
}

static void
nih_free(nihtree_conf_t* conf, void* p) {
	conf->nhrealloc(p, 0, conf->arg);
}

niherrcode_t
nihtree_conf_init(nihtree_conf_t* conf) {
	if (conf->sizeof_tuple == 0)
		return NIH_WRONG_CONFIG;
	if (conf->tuple_2_key == NULL) {
		if (conf->sizeof_key != 0 &&
				conf->sizeof_key != conf->sizeof_tuple) {
			return NIH_WRONG_CONFIG;
		}
		if (conf->key_tuple_cmp != NULL &&
				conf->key_tuple_cmp != conf->key_cmp) {
			return NIH_WRONG_CONFIG;
		}
	} else {
		if (conf->sizeof_key == 0) {
			return NIH_WRONG_CONFIG;
		}
		if (conf->key_cmp == NULL) {
			return NIH_WRONG_CONFIG;
		}
	}
	if (conf->leaf_max > 0 && (conf->leaf_max < 10 || conf->leaf_max >= (1<<13))) {
		return NIH_WRONG_CONFIG;
	}
	if (conf->inner_max > 0 &&
			(conf->inner_max < 10 || conf->inner_max >= (1<<13))) {
		return NIH_WRONG_CONFIG;
	}
	if (conf->tuple_2_key == NULL) {
		conf->sizeof_key = conf->sizeof_tuple;
		conf->key_tuple_cmp = conf->key_cmp;
	}
	if (conf->leaf_max == 0)
		conf->leaf_max = 50;
	if (conf->inner_max == 0)
		conf->leaf_max = 500;
	if (conf->nhrealloc == NULL)
		conf->nhrealloc = nih_default_realloc;

	return NIH_OK;
}

static nihleaf_t*
nihleaf_alloc(nihtree_conf_t* conf, uint32_t capa) {
	size_t sz = sizeof(nihleaf_t) + capa * conf->sizeof_tuple;
	nihleaf_t* a = nih_alloc(conf, sz);
	if (a != NULL) {
		memset(a, 0, sizeof(*a));
		a->capa = capa;
	}
	return a;
}

static void
nihleaf_moved(nihtree_conf_t* conf, nihleaf_t* leaf) {
	if (leaf->rc > 0) {
		if (conf->leaf_copied)
			conf->leaf_copied(leaf->data, leaf->cnt, conf->arg);
		leaf->rc--;
	} else {
		nih_free(conf, leaf);
	}
}

static nihleaf_t*
nihleaf_realloc(nihtree_conf_t* conf, nihleaf_t* l, int newcapa) {
	nihleaf_t *t = nihleaf_alloc(conf, newcapa);
	if (t == NULL) return NULL;
	t->cnt = l->cnt;
	memcpy(t->data, l->data, l->cnt * conf->sizeof_tuple);
	nihleaf_moved(conf, l);
	return t;
}

static nihleaf_t*
nihleaf_willmodify(nihtree_conf_t* conf, nihleaf_t* leaf) {
	if (leaf->rc == 0)
		return leaf;
	nihleaf_t* copy = nihleaf_alloc(conf, leaf->capa);
	if (copy == NULL) return NULL;
	memcpy(copy, leaf, sizeof(*leaf) + leaf->cnt * conf->sizeof_tuple);
	copy->rc = leaf->rc;
	if (conf->leaf_copied)
		conf->leaf_copied(copy->data, copy->cnt, conf->arg);
	leaf->rc--;
	return copy;
}

static void
nihleaf_release(nihtree_conf_t* conf, nihleaf_t* leaf) {
	if (leaf->rc > 0) {
		leaf->rc--;
		return;
	}
	if (conf->leaf_destroyed)
		conf->leaf_destroyed(leaf->data, leaf->cnt, conf->arg);
	nih_free(conf, leaf);
}

static nihnode_t*
nihnode_alloc(nihtree_conf_t* conf, uint32_t capa) {
	size_t elem_sz = conf->sizeof_key + sizeof(nihpage_t*) + sizeof(uint32_t);
	size_t sz = sizeof(nihnode_t) + capa * elem_sz;
	nihnode_t* a = nih_alloc(conf, sz);
	if (a != NULL) {
		memset(a, 0, sizeof(*a));
		a->capa = capa;
	}
	return a;
}

static nihnode_t*
nihnode_willmodify(nihtree_conf_t* conf, nihnode_t* node) {
	if (node->rc == 0)
		return node;
	nihnode_t* copy = nihnode_alloc(conf, node->capa);
	if (copy == NULL)
		return NULL;
	size_t elem_sz = conf->sizeof_key + sizeof(nihpage_t*) + sizeof(uint32_t);
	memcpy(copy, node, sizeof(node) + node->capa * elem_sz);
	copy->rc = 0;
	for (int i=0; i<copy->cnt; i++) {
		copy->children[i]->common.rc++;
	}
	node->rc--;
	return copy;
}

static nihnode_t*
nihnode_realloc(nihtree_conf_t* conf, nihnode_t* node, uint32_t capa) {
	nihnode_t *t = nihnode_alloc(conf, capa);
	if (t == NULL)
		return NULL;
	memcpy(t, node, sizeof(*node));
	t->capa = capa;
	t->rc = 0;
	memcpy(t->children, node->children, sizeof(nihpage_t*)*node->cnt);
	uint32_t* ncnt = nihnode_counts(node);
	uint32_t* tcnt = nihnode_counts(t);
	memcpy(tcnt, ncnt, sizeof(uint32_t*)*node->cnt);
	void* nkeys = nihnode_keys(node);
	void* tkeys = nihnode_keys(t);
	memcpy(tkeys, nkeys, conf->sizeof_key*node->cnt);
	if (node->rc > 0) {
		int i;
		for (i=0; i<t->cnt; i++) {
			t->children[i]->common.rc++;
		}
		node->rc--;
	} else {
		nih_free(conf, node);
	}
	return t;
}

static inline void
nihnode_counts_check(nihnode_t* node, uint32_t* cnts) {
#if NIH_TREE_DEBUG
	for (int i=0; i < node->cnt; i++) {
		assert(nihpage_total(node->children[i]) == cnts[i]);
	}
#else
	(void)node; (void)cnts;
#endif
}

static void
nihnode_counts_deoptimize(nihnode_t* node) {
	uint32_t* cnts = nihnode_counts(node);
	counts_deoptimize(cnts, node->cnt);
	nihnode_counts_check(node, cnts);
}

static void
nihnode_counts_optimize(nihnode_t* node) {
	uint32_t* cnts = nihnode_counts(node);
	nihnode_counts_check(node, cnts);
	counts_optimize(cnts, node->cnt);
	node->total = counts_sumfirst(cnts, node->cnt);
}

static void
nihnode_counts_add(nihnode_t* node, int pos, int diff) {
	uint32_t* cnts = nihnode_counts(node);
	counts_add(cnts, node->cnt, pos, diff);
	node->total += diff;
}

static void nihpage_release(nihtree_conf_t* conf, nihpage_t* page);
static void
nihnode_release(nihtree_conf_t* conf, nihnode_t* node) {
	if (node->rc > 0) {
		node->rc--;
		return;
	}
	for (int i=0; i<node->cnt; i++) {
		nihpage_release(conf, node->children[i]);
	}
	nih_free(conf, node);
}

static nihpage_t*
nihpage_willmodify(nihtree_conf_t* conf, nihpage_t* page) {
	if (page->common.height == 0) {
		return (nihpage_t*)nihleaf_willmodify(conf, &page->leaf);
	} else {
		return (nihpage_t*)nihnode_willmodify(conf, &page->node);
	}
}

static void
nihpage_release(nihtree_conf_t* conf, nihpage_t* page) {
	if (page->common.height == 0) {
		nihleaf_release(conf, &page->leaf);
	} else {
		nihnode_release(conf, &page->node);
	}
}

void
nihtree_retain(nihtree_t* tree) {
	if (tree->root) {
		tree->root->common.rc++;
	}
}

void
nihtree_release(nihtree_t* tree, nihtree_conf_t* conf) {
	if (tree->root) {
		nihpage_release(conf, tree->root);
		tree->root = NULL;
	}
}

static inline int
nihnode_pos(nihnode_t *node, nihtree_conf_t* conf, void const *key, nihscan_direction_t dir) {
	BSEARCH_STRUCT(int) bs;
	BSEARCH_INIT(&bs, node->cnt);
	void* keys = nihnode_keys(node);
	while (BSEARCH_NOT_FOUND(&bs)) {
		void *skey = keys + bs.mid*conf->sizeof_key;
		int diff = conf->key_cmp(key, skey, conf->arg);
		if (dir == nihscan_search)
			BSEARCH_STEP_TO_EQUAL_MAY_BREAK(&bs, diff);
		else if (dir == nihscan_forward)
			BSEARCH_STEP_TO_FIRST_GE(&bs, diff);
		else
			BSEARCH_STEP_TO_FIRST_GT(&bs, diff);
	}
	if (dir == nihscan_search) {
		if (!bs.equal) {
			bs.mid--;
		}
	} else if (bs.mid > 0) {
		bs.mid--;
	}
	return bs.mid;
}

typedef struct search_res {
	int pos;
	bool equal;
} search_res_t;

static inline search_res_t
nihleaf_pos(nihleaf_t *leaf, nihtree_conf_t* conf, void const *key, void* buf, nihscan_direction_t dir) {
	BSEARCH_STRUCT(int) bs;
	BSEARCH_INIT(&bs, leaf->cnt);
	while (BSEARCH_NOT_FOUND(&bs)) {
		void *tuple = leaf->data + bs.mid*conf->sizeof_tuple;
		int diff;
		if (conf->key_tuple_cmp != NULL) {
			diff = conf->key_tuple_cmp(key, tuple, conf->arg);
		} else {
			if (!conf->tuple_2_key(tuple, buf, conf->arg))
				abort();
			diff = conf->key_cmp(key, buf, conf->arg);
		}
		if (dir == nihscan_search) {
			BSEARCH_STEP_TO_EQUAL_MAY_BREAK(&bs, diff);
		} else if (dir == nihscan_forward) {
			BSEARCH_STEP_TO_FIRST_GE(&bs, diff);
		} else {
			BSEARCH_STEP_TO_FIRST_GT(&bs, diff);
		}
	}
	search_res_t res;
	res.pos = bs.mid;
	res.equal = bs.equal;
	return res;
}

static int
nihnode_pos_search(nihnode_t *node, nihtree_conf_t* conf, void const *key) {
	return nihnode_pos(node, conf, key, nihscan_search);
}

static int
nihnode_pos_forward(nihnode_t *node, nihtree_conf_t* conf, void const *key) {
	return nihnode_pos(node, conf, key, nihscan_forward);
}

static int
nihnode_pos_backward(nihnode_t *node, nihtree_conf_t* conf, void const *key) {
	return nihnode_pos(node, conf, key, nihscan_backward);
}

static search_res_t
nihleaf_pos_search(nihleaf_t *leaf, nihtree_conf_t* conf, void const *key, void* buf) {
	return nihleaf_pos(leaf, conf, key, buf, nihscan_search);
}

static search_res_t
nihleaf_pos_forward(nihleaf_t *leaf, nihtree_conf_t* conf, void const *key, void* buf) {
	return nihleaf_pos(leaf, conf, key, buf, nihscan_forward);
}

static search_res_t
nihleaf_pos_backward(nihleaf_t *leaf, nihtree_conf_t* conf, void const *key, void* buf) {
	return nihleaf_pos(leaf, conf, key, buf, nihscan_backward);
}

static void*
nihtree_find_by_key_i(nihpage_t *page, nihtree_conf_t* conf, void const *key, niherrcode_t *r, void* buf) {
	while (page->common.height > 0) {
		nihnode_t* node = &page->node;
		int pos = nihnode_pos_search(node, conf, key);
		if (pos == -1) {
			*r = NIH_NOTFOUND;
			return NULL;
		}
		page = node->children[pos];
	}
	search_res_t s = nihleaf_pos_search(&page->leaf, conf, key, buf);
	if (!s.equal) {
		*r = NIH_NOTFOUND;
		return NULL;
	}
	*r = NIH_OK;
	return page->leaf.data + s.pos*conf->sizeof_tuple;
}

void*
nihtree_find_by_key(nihtree_t *tt, nihtree_conf_t* conf, void const *key, niherrcode_t *r) {
	void *buf = NULL;
	niherrcode_t t;
	if (r == NULL) r = &t;
	if (tt->root == NULL) {
		*r = NIH_NOTFOUND;
		return NULL;
	}
	if (conf->key_tuple_cmp == NULL)
		buf = alloca(conf->sizeof_key);
	return nihtree_find_by_key_i(tt->root, conf, key, r, buf);
}

void*
nihtree_find_by_key_buf(nihtree_t *tt, nihtree_conf_t* conf, void const *key, niherrcode_t *r, void* buf) {
	niherrcode_t t;
	if (r == NULL) r = &t;
	if (tt->root == NULL) {
		*r = NIH_NOTFOUND;
		return NULL;
	}
	assert(conf->key_tuple_cmp == NULL || buf != NULL);
	return nihtree_find_by_key_i(tt->root, conf, key, r, buf);
}

static uint32_t
nihtree_key_position_i(nihpage_t *page, nihtree_conf_t* conf, void const *key, niherrcode_t *r, void* buf) {
	uint32_t res = 0;
	while (page->common.height > 0) {
		nihnode_t* node = &page->node;
		int pos = nihnode_pos_search(node, conf, key);
		if (pos == -1) {
			*r = NIH_NOTFOUND;
			return 0;
		}
		res += counts_sumfirst(nihnode_counts(node), pos);
		page = node->children[pos];
	}
	search_res_t s = nihleaf_pos_search(&page->leaf, conf, key, buf);
	res += s.pos;
	*r = s.equal ? NIH_OK : NIH_NOTFOUND;
	return res;
}

uint32_t
nihtree_key_position(nihtree_t *tt, nihtree_conf_t* conf, void const *key, niherrcode_t *r) {
	void *buf = NULL;
	niherrcode_t t;
	if (r == NULL) r = &t;
	if (tt->root == NULL) {
		*r = NIH_NOTFOUND;
		return 0;
	}
	if (conf->key_tuple_cmp == NULL)
		buf = alloca(conf->sizeof_key);
	return nihtree_key_position_i(tt->root, conf, key, r, buf);
}

uint32_t
nihtree_key_position_buf(nihtree_t *tt, nihtree_conf_t* conf, void const *key, niherrcode_t *r, void *buf) {
	niherrcode_t t;
	if (r == NULL) r = &t;
	if (tt->root == NULL) {
		*r = NIH_NOTFOUND;
		return 0;
	}
	assert(conf->key_tuple_cmp == NULL || buf != NULL);
	return nihtree_key_position_i(tt->root, conf, key, r, buf);
}

typedef struct modify_ctx {
	nihtree_conf_t* conf;
	void	*tuple, *key, *buf;
	bool	replace;
	bool    added;
	uint16_t child_pos;
} modify_ctx_t;

static void
nihnode_insert_at(nihtree_conf_t* conf, nihnode_t* n, int at, nihpage_t* child, void* key) {
	nihpage_t** ch = n->children;
	uint32_t* cnts = nihnode_counts(n);
	void* keys = nihnode_keys(n);
	assert(n->capa >= n->cnt + 1);
	ARRMOVE(ch, n->cnt, at, 0, 1);
	ARRMOVE(cnts, n->cnt, at, 0, 1);
	VOIDMOVE(keys, n->cnt, at, 0, 1, conf->sizeof_key);
	ch[at] = child;
	cnts[at] = nihpage_total(child);
	memcpy(keys + at*conf->sizeof_key, key, conf->sizeof_key);
	n->cnt++;
}

static void
nihnode_delete_at(nihtree_conf_t* conf, nihnode_t* n, int at, int k) {
	if (k == 0) return;
	nihpage_t** ch = n->children;
	uint32_t* cnts = nihnode_counts(n);
	void* keys = nihnode_keys(n);
	ARRMOVE(ch, n->cnt, at, k, 0);
	ARRMOVE(cnts, n->cnt, at, k, 0);
	VOIDMOVE(keys, n->cnt, at, k, 0, conf->sizeof_key);
	n->cnt -= k;
}

enum reorg_command {
	RA_NOOP,
	RA_SPLIT,
	RA_JOIN,
	RA_SPREAD,
};
struct reorg_action {
	uint16_t act, pos;
	uint16_t len, newlen;
};

static int
simulate(nihnode_t* node, int pos, int size_max, struct reorg_action acts[1]) {
	int offset = node->cnt-5 < pos-2 ? node->cnt-5 : pos-2;
	if (offset < 0) offset = 0;
	int n = node->cnt < 5 ? node->cnt : 5;
	nihpage_t* child = node->children[pos];
	int child_cnt = child->common.cnt;
	if (!child->common.last_op_delete && child_cnt >= size_max/2-1 && child_cnt < size_max)
		return 0;

	uint32_t sum = 0;
	uint32_t sizes[5];
	int opos = pos - offset;
	int j, l, r;
	for (j = 0; j < n; j++) {
		sizes[j] = node->children[offset+j]->common.cnt;
		sum += sizes[j];
	}
#if NIH_TREE_NO_SPREAD
	if (opos > 0 && sizes[opos-1] + child_cnt <= size_max - 1) {
		acts[0].act = RA_JOIN;
		acts[0].pos = pos-1;
		return 1;
	} else if (opos < n-1 && sizes[opos+1] + child_cnt <= size_max - 1) {
		acts[0].act = RA_JOIN;
		acts[0].pos = pos;
		return 1;
	}
	if (child_cnt >= size_max) {
		acts[0].act = RA_SPLIT;
		acts[0].pos = pos;
		return 1;
	} else {
		return 0;
	}
#endif
	if (opos > 0 && sizes[opos-1] + child_cnt <= size_max - 2) {
		acts[0].act = RA_JOIN;
		acts[0].pos = pos-1;
		return 1;
	} else if (opos < n-1 && sizes[opos+1] + child_cnt <= size_max - 2) {
		acts[0].act = RA_JOIN;
		acts[0].pos = pos;
		return 1;
	}
	if (child_cnt <= size_max / 4 || sum <= (size_max-2)*(n-1)) {
		acts[0].act = RA_SPREAD;
		acts[0].pos = offset;
		acts[0].len = n;
		acts[0].newlen = RUP(sum, size_max-2);
		return 1;
	}
	l = 0; r = n;
	while (l<opos && sizes[l] >= size_max-2) { sum -= sizes[l]; l++; }
	while (r>opos+1 && sizes[r-1] >= size_max-2) { sum -= sizes[r-1]; r--; }
	if (r - l <= 1 && child_cnt < size_max)
		return 0;

	if (child_cnt < size_max && sum > (size_max-2)*(r-l-1))
		/* children are full enough to not be compacted */
		return 0;
	if (child_cnt >= size_max && sum > (size_max*13/16)*(r-l)) {
		acts[0].act = RA_SPLIT;
		acts[0].pos = pos;
		return 1;
	}

	acts[0].act = RA_SPREAD;
	acts[0].pos = l+offset;
	acts[0].len = r-l;
	acts[0].newlen = RUP(sum, size_max-2);
	return 1;
}

static niherrcode_t nihnode_compact_first_level_spread(nihtree_conf_t* conf, nihnode_t* node, struct reorg_action* act);
static niherrcode_t nihnode_compact_first_level_split(nihtree_conf_t* conf, nihnode_t* node, void* buf, int pos);
static niherrcode_t nihnode_compact_first_level_join(nihtree_conf_t* conf, nihnode_t* node, int pos);
static niherrcode_t nihnode_compact_high_level_spread(nihtree_conf_t* conf, nihnode_t* node, struct reorg_action* act);
static niherrcode_t nihnode_compact_high_level_split(nihtree_conf_t* conf, nihnode_t* node, int pos);
static niherrcode_t nihnode_compact_high_level_join(nihtree_conf_t* conf, nihnode_t* node, int pos);

static niherrcode_t
nihnode_compact(nihtree_conf_t* conf, nihnode_t* node, int pos, void* buf) {
	struct reorg_action acts[1] = {{RA_NOOP}};
	if (!simulate(node, pos, node->height == 1 ? conf->leaf_max : conf->inner_max, acts))
		return NIH_OK;

	nihnode_counts_deoptimize(node);
	niherrcode_t r = NIH_OK;
	if (node->height == 1) {
		switch (acts[0].act) {
		case RA_NOOP: break;
		case RA_SPLIT:
			r = nihnode_compact_first_level_split(conf, node, buf, acts[0].pos);
			break;
		case RA_JOIN:
			r = nihnode_compact_first_level_join(conf, node, acts[0].pos);
			break;
		case RA_SPREAD:
			r = nihnode_compact_first_level_spread(conf, node, acts);
			break;
		default:
			abort();
		}
	} else {
		switch (acts[0].act) {
		case RA_NOOP: break;
		case RA_SPLIT:
			r = nihnode_compact_high_level_split(conf, node, acts[0].pos);
			break;
		case RA_JOIN:
			r = nihnode_compact_high_level_join(conf, node, acts[0].pos);
			break;
		case RA_SPREAD:
			r = nihnode_compact_high_level_spread(conf, node, acts);
			break;
		default:
			abort();
		}
	}
	nihnode_counts_optimize(node);
	return r;
}

static niherrcode_t
nihnode_compact_first_level_split(nihtree_conf_t* conf, nihnode_t* node, void* buf, int pos)
{
	uint32_t *cnts = nihnode_counts(node);
	nihleaf_t *chld, *rchld;

	chld = &node->children[pos]->leaf;
	chld = nihleaf_willmodify(conf, chld);
	if (chld == NULL) return NIH_NOMEMORY;
	node->children[pos] = (nihpage_t*)chld;

	rchld = nihleaf_alloc(conf, conf->leaf_max);
	if (rchld == NULL) return NIH_NOMEMORY;
	int lsize = chld->cnt / 2;
	int rsize = chld->cnt - lsize;
	VOIDCPY(rchld->data, 0, chld->data, lsize, rsize, conf->sizeof_tuple);
	chld->last_op_delete = 0;
	chld->cnt = lsize;
	rchld->cnt = rsize;
	void* key = rchld->data;
	if (conf->tuple_2_key) {
		if (!conf->tuple_2_key(rchld->data, buf, conf->arg))
			abort();
		key = buf;
	}
	cnts[pos] = chld->cnt;
	if (chld->capa > conf->leaf_max) {
		chld = nihleaf_realloc(conf, chld, conf->leaf_max);
		if (chld != NULL)
			node->children[pos] = (nihpage_t*)chld;
	}
	nihnode_insert_at(conf, node, pos+1, (nihpage_t*)rchld, key);
	node->last_op_join = 0;
	return NIH_MODIFIED;
}

static niherrcode_t
nihnode_compact_first_level_join(nihtree_conf_t* conf, nihnode_t* node, int pos) {
	assert(pos >= 0 && pos < node->cnt-1);
	uint32_t *cnts = nihnode_counts(node);
	nihleaf_t *chld, *rchld;

	chld = &node->children[pos]->leaf;
	chld = nihleaf_willmodify(conf, chld);
	if (chld == NULL) return NIH_NOMEMORY;
	node->children[pos] = (nihpage_t*)chld;
	chld->last_op_delete = 0;

	rchld = &node->children[pos+1]->leaf;
	VOIDCPY(chld->data, chld->cnt, rchld->data, 0,
			rchld->cnt, conf->sizeof_tuple);
	chld->cnt += rchld->cnt;
	assert(chld->cnt <= conf->leaf_max);
	cnts[pos] = chld->cnt;
	nihnode_delete_at(conf, node, pos+1, 1);
	nihleaf_moved(conf, rchld);
	node->last_op_join = 1;
	return NIH_MODIFIED;
}

static niherrcode_t
nihnode_compact_first_level_spread(nihtree_conf_t* conf, nihnode_t* node, struct reorg_action* act)
{
	nihleaf_t* ochlds[5];
	nihleaf_t* nchlds[5];
	int sum = 0, sz = 0;
	int j;
	assert(act->newlen <= act->len);
	for (j=0; j<act->len; j++) {
		ochlds[j] = (nihleaf_t*)node->children[act->pos+j];
		sum += ochlds[j]->cnt;
	}
	sz = RUP(sum, act->newlen);
	for (j=0; j<act->newlen; j++) {
		nchlds[j] = nihleaf_alloc(conf, conf->leaf_max);
		if (nchlds[j] == NULL) {
			int i;
			for (i = 0; i < j; i++) {
				nih_free(conf, nchlds[j]);
			}
			return NIH_NOMEMORY;
		}
	}
	int oi = 0, ni = 0, oti = 0, tn = sz;
	while (sum > 0) {
		assert(oi < act->len && ni < act->newlen);
		int now = tn <= (ochlds[oi]->cnt - oti) ? tn : ochlds[oi]->cnt - oti;
		VOIDCPY(nchlds[ni]->data, nchlds[ni]->cnt, ochlds[oi]->data, oti,
				now, conf->sizeof_tuple);
		oti += now;
		if (oti == ochlds[oi]->cnt) {
			oi++; oti = 0;
		}
		nchlds[ni]->cnt += now;
		tn -= now;
		if (tn == 0) {
			ni++; tn = sz;
		}
		sum -= now;
	}
	nihnode_delete_at(conf, node, act->pos + act->newlen, act->len - act->newlen);
	uint32_t *cnts = nihnode_counts(node);
	void* key = nihnode_keys(node) + act->pos*conf->sizeof_key;
	assert(conf->tuple_2_key != NULL || conf->sizeof_tuple == conf->sizeof_key);
	for (j=0; j<act->newlen; j++) {
		node->children[act->pos+j] = (nihpage_t*)nchlds[j];
		cnts[act->pos+j] = nchlds[j]->cnt;
		if (conf->tuple_2_key) {
			if (!conf->tuple_2_key(nchlds[j]->data, key, conf->arg))
				abort();
		} else {
			memcpy(key, nchlds[j]->data, conf->sizeof_tuple);
		}
		key += conf->sizeof_key;
		if (conf->leaf_copied)
			conf->leaf_copied(nchlds[j]->data, nchlds[j]->cnt, conf->arg);
	}
	for (j=0; j<act->len; j++) {
		nihleaf_moved(conf, ochlds[j]);
	}
	node->last_op_join = act->newlen < act->len;
#if NIH_TREE_DEBUG
	for (int j=0; j<node->cnt-1; j++) {
		void* akey = nihnode_keys(node) + conf->sizeof_key * j;
		void* bkey = nihnode_keys(node) + conf->sizeof_key * (j+1);
		assert(conf->key_cmp(akey, bkey, conf->arg) < 0);
	}
#endif
	return NIH_MODIFIED;
}

static niherrcode_t
nihnode_compact_high_level_split(nihtree_conf_t* conf, nihnode_t* node, int pos)
{
	uint32_t *cnts = nihnode_counts(node);
	nihnode_t *chld, *rchld;

	chld = &node->children[pos]->node;
	chld = nihnode_willmodify(conf, chld);
	if (chld == NULL) return NIH_NOMEMORY;
	node->children[pos] = (nihpage_t*)chld;

	rchld = nihnode_alloc(conf, conf->inner_max);
	if (rchld == NULL) return NIH_NOMEMORY;
	rchld->height = chld->height;

	uint32_t *lcnts = nihnode_counts(chld);
	uint32_t *rcnts = nihnode_counts(rchld);
	nihnode_counts_deoptimize(chld);

	int lsize = chld->cnt / 2;
	int rsize = chld->cnt - lsize;
	void *lkeys = nihnode_keys(chld);
	void *rkeys = nihnode_keys(rchld);
	VOIDCPY(rkeys, 0, lkeys, lsize, rsize, conf->sizeof_key);
	ARRCPY(rcnts, 0, lcnts, lsize, rsize);
	ARRCPY(rchld->children, 0, chld->children, lsize, rsize);

	chld->cnt = lsize;
	rchld->cnt = rsize;

	nihnode_counts_optimize(chld);
	nihnode_counts_optimize(rchld);
	cnts[pos] = chld->total;
	nihnode_insert_at(conf, node, pos+1, (nihpage_t*)rchld, rkeys);
	node->last_op_join = 0;
	return NIH_MODIFIED;
}

static niherrcode_t
nihnode_compact_high_level_join(nihtree_conf_t* conf, nihnode_t* node, int pos) {
	assert(pos >= 0 && pos < node->cnt-1);
	uint32_t *cnts = nihnode_counts(node);
	nihnode_t *chld, *rchld;

	chld = &node->children[pos]->node;
	chld = nihnode_willmodify(conf, chld);
	if (chld == NULL) return NIH_NOMEMORY;
	node->children[pos] = (nihpage_t*)chld;

	rchld = &node->children[pos+1]->node;
	uint32_t *lcnts = nihnode_counts(chld);
	uint32_t *rcnts = nihnode_counts(rchld);
	nihnode_counts_deoptimize(chld);
	void *lkeys = nihnode_keys(chld);
	void *rkeys = nihnode_keys(rchld);
	VOIDCPY(lkeys, chld->cnt, rkeys, 0, rchld->cnt, conf->sizeof_key);
	ARRCPY(lcnts, chld->cnt, rcnts, 0, rchld->cnt);
	ARRCPY(chld->children, chld->cnt, rchld->children, 0, rchld->cnt);
	counts_deoptimize(lcnts + chld->cnt, rchld->cnt);
	nihnode_counts_check(rchld, lcnts + chld->cnt);
	chld->cnt += rchld->cnt;
#if NIH_TREE_DEBUG
	uint32_t old_total = chld->total;
#endif
	nihnode_counts_optimize(chld);
#if NIH_TREE_DEBUG
	assert(old_total + rchld->total == chld->total);
#endif
	cnts[pos] = chld->total;
	nihnode_delete_at(conf, node, pos+1, 1);
	//nihnode_release(conf, rchld);
	nih_free(conf, rchld);
	node->last_op_join = 1;
	return NIH_MODIFIED;
}

static niherrcode_t
nihnode_compact_high_level_spread(nihtree_conf_t* conf, nihnode_t* node, struct reorg_action* act)
{
	nihnode_t* ochlds[5];
	nihnode_t* nchlds[5];
	int sum = 0, sz = 0;
	int j;
	assert(act->newlen <= act->len);
#if NIH_TREE_DEBUG
	for (int j=0; j<node->cnt-1; j++) {
		void* akey = nihnode_keys(node) + conf->sizeof_key * j;
		void* bkey = nihnode_keys(node) + conf->sizeof_key * (j+1);
		assert(conf->key_cmp(akey, bkey, conf->arg) < 0);
	}
#endif
	for (j=0; j<act->len; j++) {
		ochlds[j] = nihnode_willmodify(conf, (nihnode_t*)node->children[act->pos+j]);
		if (ochlds[j] == NULL)
			return NIH_NOMEMORY;
		node->children[act->pos+j] = (nihpage_t*)ochlds[j];
		sum += ochlds[j]->cnt;
	}
	sz = RUP(sum, act->newlen);
	unsigned height = node->height - 1;
	for (j=0; j<act->newlen; j++) {
		nchlds[j] = nihnode_alloc(conf, conf->inner_max);
		if (nchlds[j] == NULL) {
			int i;
			for (i = 0; i < j; i++) {
				nih_free(conf, nchlds[j]);
			}
			return NIH_NOMEMORY;
		}
		nchlds[j]->height = height;
	}
	for (j=0; j<act->len; j++) {
		nihnode_counts_deoptimize(ochlds[j]);
	}
	int oi = 0, ni = 0, oti = 0, tn = sz;
	while (sum > 0) {
		assert(oi < act->len && ni < act->newlen);
		int now = tn <= (ochlds[oi]->cnt - oti) ? tn : ochlds[oi]->cnt - oti;
		void* okey = nihnode_keys(ochlds[oi]);
		void* nkey = nihnode_keys(nchlds[ni]);
		uint32_t* ocnts = nihnode_counts(ochlds[oi]);
		uint32_t* ncnts = nihnode_counts(nchlds[ni]);
		VOIDCPY(nkey, nchlds[ni]->cnt, okey, oti, now, conf->sizeof_key);
		ARRCPY(ncnts, nchlds[ni]->cnt, ocnts, oti, now);
		ARRCPY(nchlds[ni]->children, nchlds[ni]->cnt, ochlds[oi]->children, oti, now);
		oti += now;
		if (oti == ochlds[oi]->cnt) {
			oi++; oti = 0;
		}
		nchlds[ni]->cnt += now;
		tn -= now;
		if (tn == 0) {
			ni++; tn = sz;
		}
		sum -= now;
	}
	nihnode_delete_at(conf, node, act->pos + act->newlen, act->len - act->newlen);
	for (j=0; j<act->newlen; j++) {
		nihnode_counts_optimize(nchlds[j]);
	}
	uint32_t *cnts = nihnode_counts(node);
	void* key = nihnode_keys(node) + act->pos*conf->sizeof_key;
	for (j=0; j<act->newlen; j++) {
		node->children[act->pos+j] = (nihpage_t*)nchlds[j];
		cnts[act->pos+j] = nchlds[j]->total;
		void* nkey = nihnode_keys(nchlds[j]);
		memcpy(key, nkey, conf->sizeof_key);
		key += conf->sizeof_key;
	}
	for (j=0; j<act->len; j++) {
		//nihnode_release(conf, ochlds[j]);
		nih_free(conf, ochlds[j]);
	}
#if NIH_TREE_DEBUG
	for (int j=0; j<node->cnt-1; j++) {
		void* akey = nihnode_keys(node) + conf->sizeof_key * j;
		void* bkey = nihnode_keys(node) + conf->sizeof_key * (j+1);
		assert(conf->key_cmp(akey, bkey, conf->arg) < 0);
	}
#endif
	node->last_op_join = act->newlen < act->len;
	return NIH_MODIFIED;
}

static nihpage_t*
nihtree_expand_root_leaf(nihtree_conf_t *conf, nihleaf_t* leaf, void* buf) {
	if (conf->flexi_size) {
		int maxcapa = conf->leaf_max * 3 / 2;
		if (leaf->capa < maxcapa) {
			int newcapa = nih_flexi_capa(leaf->capa, maxcapa);
			leaf = nihleaf_realloc(conf, leaf, newcapa);
			return (nihpage_t*)leaf;
		}
	}
	nihnode_t* newroot = conf->flexi_size ?
		nihnode_alloc(conf, 2) :
		nihnode_alloc(conf, conf->inner_max);
	if (newroot == NULL)
		return NULL;
	void* key = leaf->data;
	newroot->height = 1;
	if (conf->tuple_2_key) {
		if (!conf->tuple_2_key(key, buf, conf->arg))
			abort();
		key = buf;
	}
	nihnode_insert_at(conf, newroot, 0, (nihpage_t*)leaf, key);
	newroot->total = leaf->cnt;
	return (nihpage_t*)newroot;
}

static nihnode_t*
nihtree_expand_root_node(nihtree_conf_t *conf, nihnode_t* node) {
	if (node->cnt < node->capa)
		return node;
	if (node->cnt < conf->inner_max) {
		int ncapa = nih_flexi_capa(node->cnt, conf->inner_max);
		return nihnode_realloc(conf, node, ncapa);
	}
	nihnode_t* newroot = conf->flexi_size ?
		nihnode_alloc(conf, 2) :
		nihnode_alloc(conf, conf->inner_max);
	if (newroot == NULL)
		return NULL;
	newroot->height = node->height+1;
	void* key = nihnode_keys(node);
	nihnode_insert_at(conf, newroot, 0, (nihpage_t*)node, key);
	newroot->total = node->total;
	return newroot;
}

static nihleaf_t*
nihtree_compact_root_leaf(nihtree_conf_t* conf, nihleaf_t* leaf) {
	if (leaf->cnt == 0) {
		nihleaf_release(conf, leaf);
		return NULL;
	}
	if (!conf->flexi_size) {
		return leaf;
	}
	if (leaf->cnt >= leaf->capa*3/4) {
		return leaf;
	}
	int ncapa = nih_flexi_capa(leaf->cnt, leaf->capa);
	if (ncapa == leaf->capa)
		return leaf;
	nihleaf_t* t = nihleaf_realloc(conf, leaf, ncapa);
	return t ? t : leaf;
}

static nihpage_t*
nihtree_compact_root_node(nihtree_conf_t* conf, nihnode_t* node) {
	if (node->cnt == 1) {
		nihpage_t *res = node->children[0];
		res->common.rc++;
		nihnode_release(conf, node);
		return res;
	}
	if (!conf->flexi_size) {
		return (nihpage_t*)node;
	}
	if (node->cnt+2 >= node->capa || node->cnt >= node->capa*3/4) {
		return (nihpage_t*)node;
	}
	if (node->height == 1 && node->cnt == 2) {
		int maxcapa = conf->leaf_max * 3 / 2;
		if (node->total < maxcapa - 3) {
			int newcapa = nih_flexi_capa(node->total+2, maxcapa);
			nihleaf_t *newroot = nihleaf_alloc(conf, newcapa);
			if (newroot == NULL)
				return (nihpage_t*)node;
			nihleaf_t *lchld = &node->children[0]->leaf;
			nihleaf_t *rchld = &node->children[1]->leaf;
			VOIDCPY(newroot->data, 0, lchld->data, 0,
					lchld->cnt, conf->sizeof_tuple);
			VOIDCPY(newroot->data, lchld->cnt, rchld->data, 0,
					rchld->cnt, conf->sizeof_tuple);
			newroot->cnt = node->total;
			if (conf->leaf_copied) {
				conf->leaf_copied(newroot->data, newroot->cnt, conf->arg);
			}
			nihnode_release(conf, node);
			return (nihpage_t*)newroot;
		}
	}
	int ncapa = nih_flexi_capa(node->cnt, node->capa);
	if (ncapa == node->capa)
		return (nihpage_t*)node;
	nihnode_t* t = nihnode_realloc(conf, node, ncapa);
	return (nihpage_t*)(t ? t : node);
}

static niherrcode_t
nihtree_insert_leaf(modify_ctx_t *ctx, nihleaf_t *leaf, bool root) {
	nihtree_conf_t *conf = ctx->conf;
	struct search_res s = nihleaf_pos_search(leaf, conf, ctx->key, ctx->buf);
	ctx->child_pos = s.pos;
	if (s.equal) {
		if (ctx->replace) {
			memcpy(leaf->data + s.pos*conf->sizeof_tuple,
					ctx->tuple, conf->sizeof_tuple);
			return NIH_OK;
		}
		return NIH_DUPLICATE;
	}
	assert(root || leaf->cnt != leaf->capa);
	if (root && leaf->cnt == leaf->capa)
		return NIH_NOTFOUND;
	if (leaf->cnt != s.pos)
		VOIDMOVE(leaf->data, leaf->cnt, s.pos, 0, 1, conf->sizeof_tuple);
	memcpy(leaf->data + s.pos*conf->sizeof_tuple, ctx->tuple,conf->sizeof_tuple);
	leaf->cnt++;
	leaf->last_op_delete = 0;
	ctx->added = true;
	return NIH_OK;
}

static niherrcode_t
nihtree_insert_node(modify_ctx_t *ctx, nihnode_t *node) {
	niherrcode_t r;
	nihtree_conf_t *conf = ctx->conf;
	int pos = nihnode_pos_search(node, conf, ctx->key);
	if (pos == -1) pos = 0;
#if NIH_TREE_DEBUG
	nihnode_counts_deoptimize(node);
	nihnode_counts_optimize(node);
#endif
	r = nihnode_compact(conf, node, pos, ctx->buf);
	if (r == NIH_MODIFIED) {
		pos = nihnode_pos_search(node, conf, ctx->key);
		if (pos == -1) pos = 0;
	} else if (r != NIH_OK)
		return r;
#if NIH_TREE_DEBUG
	nihnode_counts_deoptimize(node);
	nihnode_counts_optimize(node);
#endif
	nihpage_t** pp = &node->children[pos];
	if (node->height == 1) {
		nihleaf_t* child = &(*pp)->leaf;
		child = nihleaf_willmodify(conf, child);
		if (child == NULL)
			return NIH_NOMEMORY;
		*pp = (nihpage_t*)child;
		r = nihtree_insert_leaf(ctx, child, false);
		if (r != NIH_OK)
			return r;
	} else {
		nihnode_t* child = &(*pp)->node;
		child = nihnode_willmodify(conf, child);
		if (child == NULL)
			return NIH_NOMEMORY;
		*pp = (nihpage_t*)child;
		r = nihtree_insert_node(ctx, child);
		if (r != NIH_OK)
			return r;
	}
	if (ctx->added)
		nihnode_counts_add(node, pos, 1);
	if (ctx->child_pos == 0) {
		void* ch_key = nihnode_keys(node) + conf->sizeof_key * pos;
		memcpy(ch_key, ctx->key, conf->sizeof_key);
		ctx->child_pos = pos;
	} else {
		ctx->child_pos = 1;
	}
#if NIH_TREE_DEBUG
	for (int j=0; j<node->cnt-1; j++) {
		void* akey = nihnode_keys(node) + conf->sizeof_key * j;
		void* bkey = nihnode_keys(node) + conf->sizeof_key * (j+1);
		assert(conf->key_cmp(akey, bkey, conf->arg) < 0);
	}
#endif
#if NIH_TREE_DEBUG
	nihnode_counts_deoptimize(node);
	nihnode_counts_optimize(node);
#endif
	return NIH_OK;
}

static niherrcode_t
nihtree_insert_root(modify_ctx_t *ctx, nihpage_t **pp) {
	niherrcode_t r;
	nihtree_conf_t *conf = ctx->conf;
	nihpage_t *p;
	p = nihpage_willmodify(ctx->conf, *pp);
	if (p == NULL) return NIH_NOMEMORY;
	*pp = p;
	if (p->common.height == 0) {
		/* root is leaf */
		nihleaf_t *leaf = &p->leaf;
		r = nihtree_insert_leaf(ctx, leaf, true);
		if (r != NIH_NOTFOUND) {
			return r;
		}
		p = nihtree_expand_root_leaf(conf, leaf, ctx->buf);
		if (p == NULL)
			return NIH_NOMEMORY;
		*pp = p;
		if (p->common.height == 0) {
			return nihtree_insert_leaf(ctx, &p->leaf, false);
		}
		/* else insert into node */
	}
	nihnode_t* node = nihtree_expand_root_node(conf, &p->node);
	if (node == NULL)
		return NIH_NOMEMORY;
	*pp = (nihpage_t*)node;
	r = nihtree_insert_node(ctx, node);
	if (r != NIH_OK)
		return r;
	return NIH_OK;
}

niherrcode_t
nihtree_insert_key_buf(nihtree_t *tt, nihtree_conf_t* conf, void *tuple, bool replace, void* key, void *buf) {
	if (tt->root == NULL) {
		nihleaf_t* leaf = conf->flexi_size ?
			nihleaf_alloc(conf, 1) :
			nihleaf_alloc(conf, conf->leaf_max);
		if (leaf == NULL) return NIH_NOMEMORY;
		memcpy(leaf->data, tuple, conf->sizeof_tuple);
		leaf->cnt = 1;
		tt->root = (nihpage_t*)leaf;
		return NIH_OK;
	}
	modify_ctx_t ctx;
	ctx.conf = conf;
	ctx.tuple = tuple;
	assert(key != NULL);
	assert(conf->tuple_2_key == NULL || buf != NULL);
	ctx.key = key;
	ctx.buf = buf;
	ctx.replace = replace;
	ctx.added = false;
	return nihtree_insert_root(&ctx, &tt->root);
}

niherrcode_t
nihtree_insert(nihtree_t *tt, nihtree_conf_t* conf, void *tuple, bool replace) {
	void *key = tuple, *buf = NULL;
	if (conf->tuple_2_key != NULL) {
		key = alloca(conf->sizeof_key);
		buf = alloca(conf->sizeof_key);
		if (!conf->tuple_2_key(tuple, key, conf->arg))
			return NIH_SUPPORT;
	}
	return nihtree_insert_key_buf(tt, conf, tuple, replace, key, buf);
}

niherrcode_t
nihtree_insert_buf(nihtree_t *tt, nihtree_conf_t* conf, void *tuple, bool replace, void* buf) {
	void *key = tuple;
	if (conf->tuple_2_key != NULL) {
		key = buf;
		buf += conf->sizeof_key;
		if (!conf->tuple_2_key(tuple, key, conf->arg))
			return NIH_SUPPORT;
	} else {
		buf = NULL;
	}
	return nihtree_insert_key_buf(tt, conf, tuple, replace, key, buf);
}

static niherrcode_t
nihtree_delete_leaf(modify_ctx_t* ctx, nihleaf_t* leaf) {
	nihtree_conf_t *conf = ctx->conf;
	struct search_res s = nihleaf_pos_search(leaf, conf, ctx->key, ctx->buf);
	if (!s.equal)
		return NIH_NOTFOUND;
	if (s.pos != leaf->cnt)
		VOIDMOVE(leaf->data, leaf->cnt, s.pos, 1, 0, conf->sizeof_tuple);
	ctx->child_pos = s.pos;
	leaf->cnt--;
	leaf->last_op_delete = 1;
	return NIH_OK;
}

static niherrcode_t
nihtree_delete_node(modify_ctx_t* ctx, nihnode_t *node) {
	niherrcode_t r;
	nihtree_conf_t *conf = ctx->conf;
	int pos = nihnode_pos_search(node, conf, ctx->key);
	if (pos == -1)
		return NIH_NOTFOUND;
#if NIH_TREE_DEBUG
	nihnode_counts_deoptimize(node);
	nihnode_counts_optimize(node);
#endif
	r = nihnode_compact(conf, node, pos, ctx->buf);
	if (r == NIH_MODIFIED) {
		pos = nihnode_pos_search(node, conf, ctx->key);
	} else if (r != NIH_OK)
		return r;
	nihpage_t** pp = &node->children[pos];
	void* ch_key = nihnode_keys(node) + conf->sizeof_key * pos;
#if NIH_TREE_DEBUG
	nihnode_counts_deoptimize(node);
	nihnode_counts_optimize(node);
#endif
	if (node->height == 1) {
		nihleaf_t* child = &(*pp)->leaf;
		child = nihleaf_willmodify(conf, child);
		if (child == NULL)
			return NIH_NOMEMORY;
		*pp = (nihpage_t*)child;
		r = nihtree_delete_leaf(ctx, child);
		if (r != NIH_OK)
			return r;
		if (ctx->child_pos == 0) {
			void* key = child->data;
			if (conf->tuple_2_key != NULL) {
				if (!conf->tuple_2_key(key, ctx->buf, conf->arg))
					abort();
				key = ctx->buf;
			}
			memcpy(ch_key, key, conf->sizeof_key);
		}
	} else {
		nihnode_t* child = &(*pp)->node;
		child = nihnode_willmodify(conf, child);
		if (child == NULL)
			return NIH_NOMEMORY;
		*pp = (nihpage_t*)child;
		r = nihtree_delete_node(ctx, child);
		if (r != NIH_OK)
			return r;
		if (ctx->child_pos == 0) {
			memcpy(ch_key, nihnode_keys(child), conf->sizeof_key);
		}
	}
	nihnode_counts_add(node, pos, -1);
#if NIH_TREE_DEBUG
	for (int j=0; j<node->cnt-1; j++) {
		void* akey = nihnode_keys(node) + conf->sizeof_key * j;
		void* bkey = nihnode_keys(node) + conf->sizeof_key * (j+1);
		assert(conf->key_cmp(akey, bkey, conf->arg) < 0);
	}
#endif
#if NIH_TREE_DEBUG
	nihnode_counts_deoptimize(node);
	nihnode_counts_optimize(node);
#endif
	ctx->child_pos = ctx->child_pos == 0 ? pos : 1;
	return NIH_OK;
}

static niherrcode_t
nihtree_delete_root(modify_ctx_t* ctx, nihpage_t** pp) {
	niherrcode_t r;
	nihtree_conf_t *conf = ctx->conf;
	nihpage_t *p;
	p = nihpage_willmodify(ctx->conf, *pp);
	if (p == NULL) return NIH_NOMEMORY;
	*pp = p;
	if (p->common.height == 0) {
		/* root is leaf */
		nihleaf_t *leaf = &p->leaf;
		r = nihtree_delete_leaf(ctx, leaf);
		if (r != NIH_OK) {
			return r;
		}
		*pp = (nihpage_t*)nihtree_compact_root_leaf(conf, leaf);
		return r;
	}
	nihnode_t* node = nihtree_expand_root_node(conf, &p->node);
	if (node == NULL)
		return NIH_NOMEMORY;
	*pp = (nihpage_t*)node;
	r = nihtree_delete_node(ctx, node);
	if (r != NIH_OK)
		return r;
	*pp = nihtree_compact_root_node(conf, node);
	return NIH_OK;
}

niherrcode_t
nihtree_delete_buf(nihtree_t *tt, nihtree_conf_t* conf, void *key, void* buf) {
	if (tt->root == NULL) return NIH_NOTFOUND;
	modify_ctx_t ctx;
	ctx.conf = conf;
	ctx.key = key;
	assert(conf->tuple_2_key == NULL || buf != NULL);
	ctx.buf = buf;
	return nihtree_delete_root(&ctx, &tt->root);
}

niherrcode_t
nihtree_delete(nihtree_t *tt, nihtree_conf_t* conf, void *key) {
	if (tt->root == NULL) return NIH_NOTFOUND;
	void* buf = NULL;
	if (conf->tuple_2_key != NULL) {
		buf = alloca(conf->sizeof_key);
	}
	return nihtree_delete_buf(tt, conf, key, buf);
}

void
nihtree_iter_init(nihtree_t *tt, nihtree_conf_t *conf,
		nihtree_iter_t *it, nihscan_direction_t direction)
{
	it->direction = direction;
	it->sizeof_tuple = conf->sizeof_tuple;
	if (tt->root == NULL) {
		it->ptr = it->end = NULL;
		it->cursor[0].page = NULL;
		it->cursor[0].end = NULL;
		return;
	}
	nihpage_t* page = tt->root;
	int height = page->common.height;
	assert(height <= it->max_height);
	it->cursor[height].page = NULL;
	it->cursor[height].end = NULL;
	if (direction == nihscan_forward) {
		while (page->common.height > 0) {
			nihnode_t* node = &page->node;
			it->cursor[node->height-1].page = node->children;
			it->cursor[node->height-1].end = node->children + node->cnt;
			page = node->children[0];
		}
		nihleaf_t* leaf = &page->leaf;
		it->ptr = leaf->data;
		it->end = leaf->data + leaf->cnt * conf->sizeof_tuple;
	} else if (direction == nihscan_backward) {
		while (page->common.height > 0) {
			nihnode_t* node = &page->node;
			it->cursor[node->height-1].page = node->children + node->cnt;
			it->cursor[node->height-1].end = node->children;
			page = node->children[node->cnt-1];
		}
		nihleaf_t* leaf = &page->leaf;
		it->ptr = leaf->data + leaf->cnt * conf->sizeof_tuple;
		it->end = leaf->data;
	} else {
		abort();
	}
}

static void nihtree_iter_fix_forward(nihtree_iter_t *it);
void
nihtree_iter_init_set_buf(nihtree_t *tt, nihtree_conf_t* conf,
		nihtree_iter_t* it, void *key, nihscan_direction_t direction, void* buf) {
	it->direction = direction;
	it->sizeof_tuple = conf->sizeof_tuple;
	if (tt->root == NULL) {
		it->ptr = it->end = NULL;
		it->cursor[0].page = NULL;
		return;
	}
	nihpage_t* page = tt->root;
	int height = page->common.height;
	assert(height <= it->max_height);
	it->cursor[height].page = NULL;
	it->cursor[height].end = NULL;
	if (direction == nihscan_forward) {
		while (page->common.height > 0) {
			nihnode_t* node = &page->node;
			int pos = nihnode_pos_forward(node, conf, key);
			it->cursor[node->height-1].page = node->children + pos;
			it->cursor[node->height-1].end = node->children + node->cnt;
			page = node->children[pos];
		}
		nihleaf_t* leaf = &page->leaf;
		search_res_t s = nihleaf_pos_forward(leaf, conf, key, buf);
		it->ptr = leaf->data + s.pos * conf->sizeof_tuple;
		it->end = leaf->data + leaf->cnt * conf->sizeof_tuple;
		if (!s.equal && it->ptr == it->end) {
			nihtree_iter_fix_forward(it);
		}
	} else if (direction == nihscan_backward) {
		while (page->common.height > 0) {
			nihnode_t* node = &page->node;
			int pos = nihnode_pos_backward(node, conf, key);
			it->cursor[node->height-1].page = node->children + (pos+1);
			it->cursor[node->height-1].end = node->children;
			page = node->children[pos];
		}
		nihleaf_t* leaf = &page->leaf;
		search_res_t s = nihleaf_pos_backward(leaf, conf, key, buf);
		it->ptr = leaf->data + s.pos * conf->sizeof_tuple;
		it->end = leaf->data;
	} else {
		abort();
	}
}

void
nihtree_iter_init_set(nihtree_t *tt, nihtree_conf_t* conf,
		nihtree_iter_t* it, void *key, nihscan_direction_t direction) {
	void *buf = NULL;
	if (conf->key_tuple_cmp == NULL) {
		buf = alloca(conf->sizeof_key);
	}
	nihtree_iter_init_set_buf(tt, conf, it, key, direction, buf);
}

static void
nihtree_iter_fix_forward(nihtree_iter_t *it) {
	int h = 0;
	for(;;h++) {
		assert(h <= it->max_height);
		if (it->cursor[h].page == NULL) {
			it->ptr = NULL;
			return;
		}
		assert(it->cursor[h].page < it->cursor[h].end);
		it->cursor[h].page++;
		if (it->cursor[h].page != it->cursor[h].end) {
			break;
		}
	}
	nihpage_t* page = *(nihpage_t**)it->cursor[h].page;
	for(h--;h>=0;h--) {
		nihnode_t* node = &page->node;
		it->cursor[h].page = node->children;
		it->cursor[h].end = node->children + node->cnt;
		page = node->children[0];
	}
	nihleaf_t* leaf = &page->leaf;
	it->ptr = leaf->data;
	it->end = leaf->data + leaf->cnt * it->sizeof_tuple;
}

void*
nihtree_iter_leaf_backward(nihtree_iter_t *it) {
	int h = 0;
	for(;;h++) {
		assert(h <= it->max_height+1);
		if (it->cursor[h].page == NULL) {
			it->ptr = NULL;
			return NULL;
		}
		it->cursor[h].page--;
		if (it->cursor[h].page != it->cursor[h].end) {
			break;
		}
	}
	nihpage_t* page = *(it->cursor[h].page-1);
	for(h--;h>=0;h--) {
		nihnode_t* node = &page->node;
		it->cursor[h].page = node->children + node->cnt;
		it->cursor[h].end = node->children;
		page = node->children[node->cnt-1];
	}
	nihleaf_t* leaf = &page->leaf;
	it->ptr = leaf->data + (leaf->cnt-1) * it->sizeof_tuple;
	it->end = leaf->data;
	return it->ptr;
}

void*
nihtree_iter_next(nihtree_iter_t *it) {
	if (it->ptr == NULL) {
		return NULL;
	}
	if (it->direction == nihscan_forward) {
		void *res = it->ptr;
		assert(it->ptr != it->end);
		it->ptr += it->sizeof_tuple;
		if (it->ptr == it->end) {
			nihtree_iter_fix_forward(it);
		}
		return res;
	} else {
		if (it->ptr > it->end) {
			it->ptr -= it->sizeof_tuple;
			return it->ptr;
		}
		return nihtree_iter_leaf_backward(it);
	}
}

static size_t
nihtree_node_bytes(nihnode_t* node, nihtree_conf_t* conf) {
	size_t sum = sizeof(*node);
	sum += (sizeof(void*)+sizeof(uint32_t)+conf->sizeof_key) * node->capa;
	if (node->height == 1) {
		sum += node->cnt * (sizeof(nihleaf_t) + conf->sizeof_tuple*conf->leaf_max);
	} else {
		int i;
		for (i=0; i<node->cnt; i++) {
			sum += nihtree_node_bytes(&node->children[i]->node, conf);
		}
	}
	return sum;
}

size_t
nihtree_bytes(nihtree_t *tt, nihtree_conf_t* conf) {
	if (tt->root == NULL)
		return 0;
	if (tt->root->common.height == 0) {
		return sizeof(nihleaf_t) + tt->root->leaf.capa * conf->sizeof_tuple;
	}
	return nihtree_node_bytes(&tt->root->node, conf);
}

uint32_t
nihtree_node_tuple_space(nihnode_t *node) {
	int i;
	size_t sum = 0;
	if (node->height > 1) {
		for (i=0; i<node->cnt; i++) {
			sum += nihtree_node_tuple_space(&node->children[i]->node);
		}
	} else {
		for (i=0; i<node->cnt; i++) {
			sum += node->children[i]->leaf.capa;
		}
	}
	return sum;
}

uint32_t
nihtree_tuple_space(nihtree_t *tt) {
	if (tt->root == NULL)
		return 0;
	if (tt->root->common.height == 0) {
		return tt->root->common.capa;
	}
	return nihtree_node_tuple_space(&tt->root->node);
}

size_t
nihtree_leaf_header_size() {
	return sizeof(nihleaf_t);
}
