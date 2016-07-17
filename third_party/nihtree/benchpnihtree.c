#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>
#include "nihtree.c"
#include "../salloc/salloc.h"
#include "../qsort_arg/qsort_arg.h"


typedef struct threeInt {
	int		id;
	long		a;
	long		b;
} threeInt;

typedef struct threeIntInd {
	int		id;
	threeInt*	p;
} __attribute__((packed)) threeIntInd;


static int
compareKeyFn(const void *a, const void *b, void *arg __attribute__((unused))) {
	int ia = ((threeIntInd*)a)->id;
	int ib = ((threeIntInd*)b)->id;
	if ( ia == ib )
		return 0;
	return ( ia > ib ) ? 1 : -1; 
}

static int
compareTupleKeyFn(const void *a, const void *b, void *arg __attribute__((unused))) {
	if ( ((threeIntInd*)a)->id == (*(threeInt**)b)->id )
		return 0;
	return ( ((threeIntInd*)a)->id > (*(threeInt**)b)->id ) ? 1 : -1;
}

static int
compareFn(const void *a, const void *b, void *arg __attribute__((unused))) {
	if ( (*(threeInt**)a)->id == (*(threeInt**)b)->id )
		return 0;
	return ( (*(threeInt**)a)->id > (*(threeInt**)b)->id ) ? 1 : -1; 
}

static bool
mkIndexNodeFn(const void *tk, void *ik, void *arg __attribute__((unused))) {
	threeIntInd *a = ik;
	a->p = *(threeInt**)tk;
	a->id = a->p->id;
	return 1;
}

static inline double
timediff(struct timeval *begin, struct timeval *end) {
    return ((double)( end->tv_sec - begin->tv_sec )) + ( (double)( end->tv_usec - begin->tv_usec ) ) / 1.0e+6;
}

static inline double
elapsedtime(struct timeval *begin) {
    struct timeval end;
	gettimeofday(&end,NULL);
	return timediff(begin,&end);
}

struct slab_cache **caches = NULL;
int cachen = 0;


#define LEAF_MAX 32

static void*
srealloc(void* p, size_t size, void *arg __attribute__((unused)))
{
	if (size == 0) { sfree(p); return NULL; }
	void* r;
	//if (size == LEAF_MAX*sizeof(void*)+nihtree_leaf_header_size()) {
		struct slab_cache *cache = NULL;
		int i;
		for (i=0; i<cachen; i++) {
			if (caches[i]->item_size == size) {
				cache = caches[i];
				break;
			}
			if (caches[i]->item_size > size) {
				break;
			}
		}
		if (cache == NULL) {
			printf("size: %zd\n", size);
			caches = realloc(caches, (cachen+1)*sizeof(void*));
			memmove(caches+i+1, caches+i, sizeof(void*)*(cachen-i));
			cache = calloc(1, sizeof(struct slab_cache));
			slab_cache_init(cache, size, SLAB_GROW, "alloc");
			caches[i] = cache;
			cachen++;
		}
		r = slab_cache_alloc(cache);
	//} else {
		//r = salloc(size);
	//}
	if (p != NULL) {
		size_t oldsize = slab_cache_of_ptr(p)->item_size;
		memcpy(r, p, oldsize < size ? oldsize : size);
		sfree(p);
	}
	return r;
}

#define N	4000000
#define D	250
#define M	100000

int
main(int argc __attribute__((unused)), char *argv[] __attribute__((unused))) {
	int			i, NN, n, K, nFound = 0;
	threeIntInd		d;
	threeInt		*array;
	threeInt		**arrayInd;
	struct timeval		begin, begin2;
	double			elapsed, elapsed2;
	double			msum;
	threeInt		*I;
	threeInt	**P = NULL;
	nihtree_t	tt;
	nihtree_conf_t	tc;
	nihtree_iter_t		*it = alloca(nihtree_iter_need_size(16));
	uint64_t bytes_used, items_used;
	it->max_height = 16;

	srand(1);
	salloc_init(0, 160, 1.02);

	memset(&tt, 0, sizeof(tt));
	memset(&tc, 0, sizeof(tc));
	tc.sizeof_key = sizeof(threeIntInd);
	tc.sizeof_tuple = sizeof(threeInt*);
	tc.tuple_2_key = mkIndexNodeFn;
	tc.key_cmp = compareKeyFn;
#if 0
	tc.key_tuple_cmp = compareTupleKeyFn;
#else
	tc.key_tuple_cmp = NULL; (void)compareTupleKeyFn;
#endif
	tc.arg = (void*)0;
	(void)srealloc;
	//tt.tlrealloc = NULL;
	tc.nhrealloc = srealloc;
	tc.inner_max = 128;
	tc.leaf_max = LEAF_MAX;

	nihtree_conf_init(&tc);
	nihtree_init(&tt);

	array = malloc(N*2*sizeof(array[0]));
	arrayInd = malloc(N*2*sizeof(arrayInd[0]));
	for (i = 0; i < N*2; i++) {
		array[i].id = rand() % (2*N);
		//array[i].id = i;
		arrayInd[i] = &array[i];
	}
	gettimeofday(&begin2, NULL);
	//qsort_arg(arrayInd, N*3/2, sizeof(arrayInd[0]), compareFn, NULL);
	qsort_arg(arrayInd, N*2, sizeof(arrayInd[0]), compareFn, NULL);
#define unique 1
#if unique
	NN = 1;
	for (i = 1; i < N*2; i++) {
		//assert(arrayInd[NN-1]->id <= arrayInd[i]->id);
		if (arrayInd[NN-1]->id != arrayInd[i]->id) {
			NN++;
		}
		arrayInd[NN-1] = arrayInd[i];
	}
#else
	NN = 2*N;
#endif

	(void)compareFn;

	gettimeofday(&begin, NULL);
	threeIntInd keyb[2];
#define INSERT 0
#if INSERT
	for (i = 0; i < N; i++) {
		nihtree_insert_buf(&tt, &tc, &arrayInd[i], false, keyb);
		//nihtree_insert(&tt, &tc, &arrayInd[i], false);
	}
#else
	nihtree_append_buf(&tt, &tc, arrayInd, N, keyb);
#endif
	printf("tuples %d tuple_space %d bytes %zd\n",
			nihtree_count(&tt), nihtree_tuple_space(&tt), nihtree_bytes(&tt, &tc));

	//gettimeofday(&begin, NULL);
#if INSERT
	for (i = 0; i < NN-N; i++) {
		nihtree_insert_buf(&tt, &tc, &arrayInd[i+N], false, keyb);
		//nihtree_insert(&tt, &tc, &arrayInd[i+N], false);
	}
#else
	nihtree_append_buf(&tt, &tc, arrayInd+N, NN-N, keyb);
#endif
	elapsed = elapsedtime(&begin);
	elapsed2 = elapsedtime(&begin2);
	printf("tuples %d tuple_space %d bytes %zd\n",
			nihtree_count(&tt), nihtree_tuple_space(&tt), nihtree_bytes(&tt, &tc));
	printf("Insert %d rows: %.2f secs, %.2g records per second, %.2g secs per record\n",
				N,
				elapsed,
				((double)N)/elapsed,
				elapsed/((double)N)
			);
	printf("total time %.2f seconds\n", elapsed2);
	slab_total_stat(&bytes_used, &items_used);
	printf("Slab: %zd bytes %zd items\n", bytes_used, items_used);
	free(arrayInd);

	gettimeofday(&begin, NULL);
	K = N/4;
	for (i = 0; i < K; i++) {
		I = &array[rand() % (2*N - 1)];
		nihtree_delete(&tt, &tc, &I->id);
	}
	elapsed = elapsedtime(&begin);
	printf("Delete %d rows: %.2f secs, %.2g records per second, %.2g secs per record\n",
				K,
				elapsed,
				((double)K)/elapsed,
				elapsed/((double)K)
			);
	slab_total_stat(&bytes_used, &items_used);
	printf("tuples %d tuple_space %d bytes %zd\n",
			nihtree_count(&tt), nihtree_tuple_space(&tt), nihtree_bytes(&tt, &tc));
	printf("Slab: %zd bytes %zd items\n", bytes_used, items_used);

	gettimeofday(&begin, NULL);
	for (i = 0; i < N; i++) {
		d.id = rand() % (2*N);
		d.p = NULL;
		if ((P = nihtree_find_by_key(&tt, &tc, &d, NULL)) != NULL) {
			if ((*P)->id != d.id) {
				printf("Error %d != %d\n", (*P)->id, d.id);
				abort();
			}
			nFound++;
		}
	}
	elapsed = elapsedtime(&begin);
	printf("Search %d rows (%d found): %.2f secs, %.2g records per second, %.2g secs per record\n",
				N, nFound,
				elapsed,
				((double)N)/elapsed,
				elapsed/((double)N)
			);

	gettimeofday(&begin, NULL);
	msum = 0;
	for (n = 0; n < D; n++) {
		nihtree_iter_init(&tt, &tc, it, nihscan_forward);
		i = 0;
		while( i < 5*M && (P = nihtree_iter_next(it)) != NULL ) {
			if (i>=4*M)
				msum += (*P)->id;
			i++;
		}
	}
	elapsed = elapsedtime(&begin);
	printf("Iterate %d rows with %d offset: %.2f secs, %.2g requests per second, %.2g secs per request (sum %f)\n",
				M, 4*M,
				elapsed,
				((double)D)/elapsed,
				elapsed/((double)D), msum
			);
	gettimeofday(&begin, NULL);
	msum = 0;
	for (n = 0; n < D; n++) {
		nihtree_iter_init(&tt, &tc, it, nihscan_forward);
		nihtree_iter_skip(it, 4*M);
		i = 0;
		while( i < M && (P = nihtree_iter_next(it)) != NULL ) {
			msum += (*P)->id;
			i++;
		}
	}
	elapsed = elapsedtime(&begin);
	printf("Iterate %d rows with %d offset: %.2f secs, %.2g requests per second, %.2g secs per request (sum %f)\n",
				M, 4*M,
				elapsed,
				((double)D)/elapsed,
				elapsed/((double)D), msum
			);
	nihtree_release(&tt, &tc);
	slab_total_stat(&bytes_used, &items_used);
	printf("Slab: %zd bytes %zd items\n", bytes_used, items_used);

	return 0;
}
