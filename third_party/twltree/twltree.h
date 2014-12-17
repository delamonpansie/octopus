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

#ifndef TWLTREE_H
#define TWLTREE_H

#include <sys/types.h>
#include <inttypes.h>
#include <stdbool.h>

/*
 * Declaration of sptree. We don't want to include sptree.h
 * here to prevent interference with apllication's usage of sptree
 */
struct sptree_t;
struct sptree_iterator;

/*
 * forward declarations
 */
struct twlpage_t;
struct index_key_t;


/*
 * Definitions
 * - index key - key on first level
 * - tuple key - key on second level
 * both key type should have a constant size
 */

typedef enum twlflag_t {
	TWL_CLEAN = 0x00,
	TWL_OVERLEFT = 0x01,
	TWL_OVERRIGHT = 0x02,
	TWL_INNER = 0x100,
	TWL_FLAGS_MASK = TWL_OVERLEFT | TWL_OVERRIGHT | TWL_INNER,
} twlflag_t;

typedef enum twlscan_direction_t {
	twlscan_forward  = +1,
	twlscan_search   = 0,
	twlscan_backward = -1
} twlscan_direction_t;

typedef struct twliterator_t {
	struct twlpage_t	*page;
	u_int32_t		ith;
	u_int32_t		sizeof_tuple_key;
	twlscan_direction_t	direction;
} twliterator_t;

typedef struct twltree_conf_t {
	void 	(*index_key_free)(void *index_key, void *arg);

	/*
	 * optional, if == NULL then sizeof_index_key == sizeof_tuple_key.
	 * returns true on success
	 */
	bool	(*tuple_key_2_index_key)(void *index_key, const void *tuple_key, void *arg);

	/*
         * signature of compare functions matches signature of support functions 
	 * for qsort_arg/sptree
	 */
	int	(*tuple_key_cmp)(const void*, const void*, void*);
	int	(*index_key_cmp)(const void*, const void*, void*);


	u_int32_t*	page_sizes; /* page sizes */
	u_int32_t	page_sizes_n; /* number of page sizes */

} twltree_conf_t;

typedef struct twltree_t {
	/*
	 * user-defined methods/constants etc
	 */
	struct twltree_conf_t	*conf;
	void	*arg; /* third arg for tuple_key_2_index_key/tuple_key_cmp/index_key_cmp,
		       * second for index_key_free */

	void*	(*tlrealloc)(void *ptr, size_t size);
	u_int32_t	sizeof_index_key;
	u_int32_t	sizeof_tuple_key;

	/*
	 * counters
	 */
	u_int32_t	n_tuple_keys;
	u_int32_t	n_index_keys;
	u_int32_t	n_tuple_space;

	/*
	 * Private members
	 */
	struct twltree_t	*page_index;
	struct twltree_t	*child;
	struct twliterator_t	pi_iterator;
	u_int32_t	flags; /* actually, bitwise OR of twlflag_t flags */

	struct index_key_t	*search_index_key;
	struct index_key_t	*stored_index_key;
	struct index_key_t	*firstpage;
} twltree_t;

typedef enum twlerrcode_t {
	TWL_OK = 0,
	TWL_WRONG_CONFIG,
	TWL_NOMEMORY,
	TWL_SUPPORT,
	TWL_DUPLICATE,
	TWL_NOTFOUND
} twlerrcode_t;

twlerrcode_t twltree_init(twltree_t *tt);
void twltree_free(twltree_t *tt);
twlerrcode_t twltree_bulk_load(twltree_t *tt, void *tuple_keys, u_int32_t keysn);

twlerrcode_t twltree_insert(twltree_t *tt, void *tuple_key, bool replace);
twlerrcode_t twltree_delete(twltree_t *tt, void *tuple_key);
/* returns pointer to tuple_key, copies result code */
/* returns result code could != TWL_OK if tuple_key_2_index_key fails for stored key */
void* twltree_find_by_index_key(twltree_t *tt, void const *index_key, twlerrcode_t *r);
/* returns result code, copies pointer to tuple_key  */
twlerrcode_t twltree_find_by_index_key_rc(twltree_t *tt, void const *index_key, void **tuple_key);
/* returns result code, copies tuple_key  */
twlerrcode_t twltree_find_by_index_key_and_copy(twltree_t *tt, void const *index_key, void *tuple_key);

void twltree_iterator_init(twltree_t *tt, twliterator_t *it, 
			   twlscan_direction_t direction);
/* could return error only if tuple_key_2_index_key returns error */
int  twltree_iterator_init_set(twltree_t *tt, twliterator_t *it, void *index_key,
			       twlscan_direction_t direction);
/* could return error only if tuple_key_2_index_key returns error for already stored tuple */
int  twltree_iterator_init_set_index_key(twltree_t *tt, twliterator_t *it, void *index_key,
			       twlscan_direction_t direction);
void* twltree_iterator_next(twliterator_t *it);

size_t twltree_bytes(twltree_t *tt);

size_t twltree_page_header_size();
#endif
