#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>
#include "nihtree.c"


typedef struct threeInt {
	int		id;
	int		a;
	int		b;
} threeInt;


static int
compareIndexFn(const void *a, const void *b, void *arg __attribute__((unused))) {
	if ( *(int*)a == *(int*)b )
		return 0;
	return ( *(int*)a > *(int*)b ) ? 1 : -1;
}

static int
compareTupleKeyFn(const void *a, const void *b, void *arg __attribute__((unused))) {
	if ( ((threeInt*)a)->id == *(int*)b )
		return 0;
	return ( *(int*)a > ((threeInt*)b)->id ) ? 1 : -1;
}

static bool
mkIndexNodeFn(const void *tk, void *ik, void *arg __attribute__((unused))) {
	*(int*)ik = ((threeInt*)tk)->id;
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

#define N	4000000
#define D	250
#define M	100000

int
main(int argc __attribute__((unused)), char *argv[] __attribute__((unused))) {
	int			i, n, K, nFound = 0;
	threeInt		e;
	double			msum;
	struct timeval		begin;
	double			elapsed;
	threeInt		*I;
	nihtree_t	tt;
	nihtree_conf_t	tc;
	nihtree_iter_t		*it = alloca(nihtree_iter_need_size(16));
	it->max_height = 16;

	srand(1);

	memset(&tt, 0, sizeof(tt));
	memset(&tc, 0, sizeof(tc));
	tc.sizeof_key = sizeof(int);
	tc.sizeof_tuple = sizeof(threeInt);
	tc.tuple_2_key = mkIndexNodeFn;
	tc.key_cmp = compareIndexFn;
#if 0
	tc.key_tuple_cmp = compareTupleKeyFn;
#else
	tc.key_tuple_cmp = NULL; (void)compareTupleKeyFn;
#endif
	tc.arg = (void*)0;
	tc.nhrealloc = NULL;
	tc.inner_max = 128;
	tc.leaf_max = 32;

	nihtree_conf_init(&tc);
	nihtree_init(&tt);

	for (i = 0; i < N; i++) {
		e.id = rand() % (2*N);
		nihtree_insert(&tt, &tc, &e, false);
	}

	gettimeofday(&begin, NULL);
	for (i = 0; i < N; i++) {
		e.id = rand() % (2*N);
		nihtree_insert(&tt, &tc, &e, false);
	}
	elapsed = elapsedtime(&begin);
	printf("Insert %d rows: %.2f secs, %.2g records per second, %.2g secs per record\n",
				N,
				elapsed,
				((double)N)/elapsed,
				elapsed/((double)N)
			);


	gettimeofday(&begin, NULL);
	K = N/4;
	for (i = 0; i < K; i++) {
		e.id = rand() % (2*N);
		nihtree_delete(&tt, &tc, &e.id);
	}
	elapsed = elapsedtime(&begin);
	printf("Delete %d rows: %.2f secs, %.2g records per second, %.2g secs per record\n",
				K,
				elapsed,
				((double)K)/elapsed,
				elapsed/((double)K)
			);

	gettimeofday(&begin, NULL);
	for (i = 0; i < N; i++) {
		e.id = rand() % (2*N);
		if ((I = nihtree_find_by_key(&tt, &tc, &e.id, NULL)) != NULL) {
			if (I->id != e.id) {
				printf("Error %d != %d\n", I->id, e.id);
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
		while( i < 5*M && (I = nihtree_iter_next(it)) != NULL ) {
			if (i>=4*M)
				msum += (*I).id;
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
		while( i < M && (I = nihtree_iter_next(it)) != NULL ) {
			msum += (*I).id;
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
	return 0;
}
