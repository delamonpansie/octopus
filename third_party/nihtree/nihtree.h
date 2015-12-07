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

#include <sys/types.h>
#include <inttypes.h>
#include <stdbool.h>

typedef struct nihtree_conf {
	int sizeof_tuple;
	int sizeof_key; /* ATTENTION: alloca is used to allocate space for */

	/* 10 < leaf_max < (1<<13)
	 * 10 < inner_max < (1<<13) */
	int leaf_max; /* maximum tuple number in leaf page */
	int inner_max; /* maximum child/key number in inner page */
	bool flexi_size;

	void *arg;

	/*
         * signature of compare functions matches signature of support functions 
	 * for qsort_arg/sptree
	 */
	int	(*key_cmp)(void const* key_searched, void const* key_stored, void* arg);
	int	(*key_tuple_cmp)(void const* key_searched, void const* tuple_stored, void* arg);

	/*
	 * optional, if == NULL then sizeof_key should == sizeof_tuple.
	 * may return false on not inserted tuple indicating wrong format
	 * may not return false on already inserted tuple (will cause abort)
	 */
	bool	(*tuple_2_key)(const void *tuple, void* key, void *arg);

	void*   (*nhrealloc)(void *ptr, size_t size, void *arg);

	/* callback when leaf page is copied
	 * ptr - pointer to tuple array
	 * cnt - count of tuples
	 */
	void    (*leaf_copied)(void *ptr, uint32_t cnt, void *arg);
	/* callback when leaf page is destroyed
	 * ptr - pointer to tuple array
	 * cnt - count of tuples
	 */
	void    (*leaf_destroyed)(void *ptr, uint32_t cnt, void *arg);
} nihtree_conf_t;

struct nihpage_common {
	unsigned height : 4;
	unsigned rc : 28;
	uint16_t cnt;
	uint16_t capa : 15;
	unsigned last_op_delete: 1;
	uint32_t root_total;
};
typedef union nihpage nihpage_t;

typedef struct nihtree {
	nihpage_t *root;
} nihtree_t;

typedef enum niherrcode {
	NIH_OK = 0,
	NIH_WRONG_CONFIG,
	NIH_NOMEMORY,
	NIH_SUPPORT,
	NIH_DUPLICATE,
	NIH_NOTFOUND,
/* internal states */
	NIH_MODIFIED = 1
} niherrcode_t;

typedef enum nihscan_direction {
	nihscan_forward  = +1,
	nihscan_search   = 0,
	nihscan_backward = -1
} nihscan_direction_t;

/* this is variable sized struct, use nihtree_iter_need_size to discover proper size */
typedef struct nihtree_iter_t {
	nihscan_direction_t	direction;
	int8_t  max_height;
	int     sizeof_tuple;
	void   *ptr, *end;
	struct {
		nihpage_t **page, **end;
	} cursor[1];
} nihtree_iter_t;

niherrcode_t nihtree_conf_init(nihtree_conf_t* conf);

static inline void nihtree_init(nihtree_t* tree) {
	tree->root = NULL;
}

void nihtree_retain(nihtree_t* tree);
void nihtree_release(nihtree_t* tree, nihtree_conf_t* conf);

static inline uint32_t nihtree_count(nihtree_t* tree) {
	if (tree->root == NULL) return 0;
	struct nihpage_common *root = (struct nihpage_common*)tree->root;
	return root->height == 0 ? root->cnt : root->root_total;
}

static inline uint32_t nihtree_height(nihtree_t* tree) {
	if (tree->root == NULL) return 0;
	struct nihpage_common *root = (struct nihpage_common*)tree->root;
	return root->height+1;
}

/* nihtree_insert uses alloca if conf->tuple_2_key != NULL */
niherrcode_t nihtree_insert(nihtree_t *tt, nihtree_conf_t* conf, void *tuple, bool replace);
niherrcode_t nihtree_delete(nihtree_t *tt, nihtree_conf_t* conf, void *key);

/* returns pointer to tuple, copies result code */
/* returns result code could != NIH_OK if tuple_2_key fails for stored key */
void* nihtree_find_by_key(nihtree_t *tt, nihtree_conf_t* conf, void const *key,
		niherrcode_t *r);

/* returns positions of key next to or equal to key
 * if key is really found, then r == NIH_OK
 * if key is not found, then r == NIH_NOT_FOUND
 */
uint32_t nihtree_key_position(nihtree_t *tt, nihtree_conf_t* conf, void const *key,
		niherrcode_t *r);

static inline size_t
nihtree_iter_need_size(int height) {
	return sizeof(nihtree_iter_t) + sizeof(void*)*2*(height-1);
}

static inline int
nihtree_iter_max_height(size_t bytes) {
	return (bytes - sizeof(nihtree_iter_t)) / 2 / sizeof(void*);
}

/* exacmple proc for iterator allocation
 * nihtree_iter_t* iter = NULL;
 * nihtree_iter_realloc(&iter, nihtree_height(tree));
 */

static inline void
nihtree_iter_realloc(nihtree_iter_t** tt, int height, void* (*realloc)(void*, size_t)) {
	if (*tt == NULL) {
		*tt = (nihtree_iter_t*)realloc(NULL, nihtree_iter_need_size(height));
		(*tt)->max_height = height;
	} else if ((*tt)->max_height < height) {
		*tt = (nihtree_iter_t*)realloc((void*)*tt, nihtree_iter_need_size(height));
		assert(*tt != NULL);
		(*tt)->max_height = height;
	}
}

void nihtree_iter_init(nihtree_t *tt, nihtree_conf_t *conf,
		nihtree_iter_t *it, nihscan_direction_t direction);
/* sets iterator to point to tuple which match key value
 * (if it is not exist, then tuple which is first in iteration order after key value) */
void nihtree_iter_init_set(nihtree_t *tt, nihtree_conf_t* conf,
		nihtree_iter_t *it, void *key, nihscan_direction_t direction);
void* nihtree_iter_next(nihtree_iter_t *it);

size_t nihtree_bytes(nihtree_t *tt, nihtree_conf_t* conf);
uint32_t nihtree_tuple_space(nihtree_t *tt);
size_t nihtree_leaf_header_size();


/* FOLLOWING INTERFACE IF YOU DON'T WANT TO ALLOW ALLOCA OR WANT MORE PERFORMANCE
 * buf is for makeing keys for tuples
 * except nihtree_insert_buf, buf should be at least conf->sizeof_key
 * for nihtree_insert_buf, buf should be at least 2*conf->sizeof_key */
niherrcode_t nihtree_insert_buf(nihtree_t *tt, nihtree_conf_t* conf, void *tuple, bool replace, void *buf);
/* key should be already built using conf->tuple_2_key */
niherrcode_t nihtree_insert_key_buf(nihtree_t *tt, nihtree_conf_t* conf, void *tuple, bool replace, void *key, void *buf);
niherrcode_t nihtree_delete_buf(nihtree_t *tt, nihtree_conf_t* conf, void *key, void *buf);
void* nihtree_find_by_key_buf(nihtree_t *tt, nihtree_conf_t* conf, void const *key,
		niherrcode_t *r, void *buf);
uint32_t nihtree_key_position_buf(nihtree_t *tt, nihtree_conf_t* conf, void const *key,
		niherrcode_t *r, void *buf);
void nihtree_iter_init_set_buf(nihtree_t *tt, nihtree_conf_t* conf,
		nihtree_iter_t* it, void *key, nihscan_direction_t direction, void* buf);
