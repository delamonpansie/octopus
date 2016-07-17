#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "nihtree.c"

static bool printCalls = false;
//static bool printCalls = true;
static nihtree_t	tt;
static nihtree_conf_t	tc;

#define	MAGICK	(0xBADC0DED)

typedef struct ikey_t {
	int32_t m;
	int32_t	k;
} ikey_t;

typedef struct tuple_t {
	int32_t	k;
	int32_t	payload;
} tuple_t;

static bool
tk2ik(const void *tuple_key, void *index_key, void *arg) {

	if (MAGICK != (uintptr_t)arg)
		puts("tk2ik: arg check fails");

	((ikey_t*)index_key)->m = MAGICK;
	((ikey_t*)index_key)->k = ((tuple_t*)tuple_key)->k;

	if (printCalls)
		printf("tk2ik: %d:%d => %d\t%p => %p\n", ((tuple_t*)tuple_key)->k, ((tuple_t*)tuple_key)->payload,
		       ((ikey_t*)index_key)->k, tuple_key, index_key);

	return true;
}

static int iterator_equal_many = 0;
static int
tk_cmp(const void* a, const void* b, void* arg) {
	tuple_t	*bt = (tuple_t*)b;
	ikey_t  *at = (ikey_t*)a;

	if (MAGICK != (uintptr_t)arg)
		puts("tk_cmp: arg check fails");
	if (MAGICK != at->m)
		puts("k_cmp: at check fails");

	if (printCalls)
		printf("tk_cmp: %d:%d <=> %d:%d\n",
		       at->k, at->m,
		       bt->k, bt->payload);

	if (iterator_equal_many == 0)
		return at->k - bt->k;
	else
		return at->k / 13 - bt->k / 13;
}

static int
k_cmp(const void* a, const void* b, void* arg) {
	ikey_t	*bt = (ikey_t*)b,
		*at = (ikey_t*)a;

	if (MAGICK != (uintptr_t)arg)
		puts("k_cmp: arg check fails");
	if (MAGICK != at->m)
		puts("k_cmp: at check fails");
	if (MAGICK != bt->m)
		puts("k_cmp: bt check fails");
	assert(at->m == MAGICK);
	assert(bt->m == MAGICK);

	if (printCalls)
		printf("k_cmp: %d <=> %d\t %p <=>%p\n", at->k, bt->k, at, bt);

	if (iterator_equal_many == 0)
		return at->k - bt->k;
	else
		return at->k / 13 - bt->k / 13;
}

static int __attribute__((unused))
tt_cmp(const void* a, const void* b) {
	tuple_t	*bt = (tuple_t*)b;
	tuple_t *at = (tuple_t*)a;

	return at->k - bt->k;
}

static size_t total_size = 0;
static size_t total_count = 0;
static void*
realloc_count(void* ptr, size_t size, void* arg)
{
	if (MAGICK != (uintptr_t)arg)
		puts("k_cmp: arg check fails");
	size_t tsize = size + sizeof(size_t);
	if (ptr == NULL) {
		size_t* nptr = malloc(tsize);
		*nptr = size;
		total_size += size;
		total_count++;
		if (printCalls)
			printf("realloc: %p => %p %zd\n", ptr, nptr+1, size);
		return (void*)(nptr + 1);
	}
	size_t *optr = (size_t*)ptr - 1;
	size_t *nptr = NULL;
	total_size -= *optr;
	total_count--;
	if (size != 0) {
		nptr = realloc_count(NULL, size, arg);
		memcpy(nptr, ptr, *optr < size ? *optr : size);
	}
	free(optr);
	if (printCalls)
		printf("realloc: %p => %p %zd\n", ptr, nptr, size);
	return nptr;
}

#define N (633)

void
check_order(nihtree_t *tt, nihtree_conf_t* conf, nihtree_iter_t *it) {
	niherrcode_t 	err = 0;
	int cnt = nihtree_count(tt);
	int i = 0;
	ikey_t ik = {.m = MAGICK};
	tuple_t *tp;
	nihtree_iter_init(tt, conf, it, nihscan_forward);
	while ((tp = nihtree_iter_next(it)) != NULL) {
		ik.k = tp->k;
		int pos = nihtree_key_position(tt, conf, &ik, &err);
		assert(err == NIH_OK);
		assert(pos == i);
		i++;
	}
	assert(cnt == i);
	for (i = 0; i < 1024; i++) {
		ik.k = i;
		int pos = nihtree_key_position(tt, conf, &ik, &err);
		int npos;
		nihtree_iter_init_set(tt, conf, it, &ik, nihscan_forward);
		tp = nihtree_iter_next(it);
		if (tp == NULL) {
			npos = cnt;
		} else {
			ik.k = tp->k;
			npos = nihtree_key_position(tt, conf, &ik, &err);
		}
		assert(pos == npos);
	}
}

static void
mix_check(uint32_t ch[3], uint32_t m, int i) {
	//printf("mix_check %u %u %u\n", m, ch[0], ch[1]);
	m *= 0xdeadbeef;
	ch[0] ^= m;
	m ^= m >> 16;
	m *= 0xdeadbeef;
	ch[1] ^= m;
	ch[2] += i;
}

static void
check_skip(nihtree_iter_t *it, unsigned skip, uint32_t ch[3], tuple_t *tpls) {
	tuple_t *tp;
	unsigned i = -1;
	printf("Checking skip %u\n", skip);
	for(;;) {
		nihtree_iter_skip(it, skip);
		i += skip;
		if ((tp = nihtree_iter_next(it)) == NULL)
			break;
		mix_check(ch, tp->k, -1);
		i++;
		if (tp->k != tpls[i].k || tp->payload != tpls[i].payload)
			printf("WRONG DATA %d->%d (wants %d->%d)\n",
					tp->k, tp->payload,
					tpls[i].k, tpls[i].payload);
	}
	printf("Check skip %u: %u %u %d\n", skip, ch[0], ch[1], ch[2]);
}

int
main(int argn, char *argv[]) {
	niherrcode_t 	err = 0;
	int		i, j, f, b, fe, be;
	size_t          itsize = nihtree_iter_need_size(31);
	nihtree_iter_t	*it = alloca(itsize);
	tuple_t		*tp = NULL, *ptp, t;
	tuple_t         tpls[N-1], trev[N-1];
	ikey_t		ik = {.m = MAGICK};
	uint32_t	ch[9];
	it->max_height = 16;

	memset(&tc, 0, sizeof(tc));
	tc.sizeof_key = sizeof(ikey_t);
	tc.sizeof_tuple = sizeof(tuple_t);
	tc.leaf_max = 10;
	tc.inner_max = 10;
	tc.tuple_2_key = tk2ik;
	tc.key_cmp = k_cmp;
	i = argn > 1 ? atoi(argv[1]) : 0;
	if (i & 1) {
		tc.key_tuple_cmp = tk_cmp;
	}
	if (i & 2) {
		tc.flexi_size = true;
	}
	tc.arg = (void*)MAGICK;
	tc.nhrealloc = realloc_count;

	nihtree_conf_init(&tc);
	if (err != NIH_OK)
		printf("nihtree_init returns %d\n", err);
	nihtree_init(&tt);

	puts("==========Inserts========");
	for(i=1; i<N; i++) {
		tpls[i-1].k = t.k = (541 * i) & (1023);
		tpls[i-1].payload =  t.payload = i;

		printf("INSERT %d:%d\n", t.k, t.payload);
		err = nihtree_insert(&tt, &tc, &t, false);
		if (err != NIH_OK)
			printf("nihtree_insert returns %d\n", err);
		assert(nihtree_count(&tt) == i);

		ik.k = t.k;
		tp = nihtree_find_by_key(&tt, &tc, &ik, &err);
		assert(err == NIH_OK && ik.k == tp->k && tp->payload == i);
		nihtree_iter_init(&tt, &tc, it, nihscan_forward);
		j=0;
		ptp = NULL;
		while((tp = nihtree_iter_next(it)) != NULL) {
			j ++;
			assert(!ptp || ptp->k < tp->k);
			ptp = tp;
		}
	}
	check_order(&tt, &tc, it);

	printf("Stats: %d tuples\n", nihtree_count(&tt));

	qsort(tpls, N-1, sizeof(tuple_t), tt_cmp);
	for (i=0; i<N-1; i++) {
		trev[N-2-i] = tpls[i];
	}
	puts("==========Appends========");
	{
		static nihtree_t	tt2;
		nihtree_init(&tt2);
		nihtree_iter_t	*it2 = alloca(itsize);
		tuple_t *tp2;
		char buf[sizeof(tuple_t)*2];
		int j = 0;
		it2->max_height = 16;
		while (j < N-1) {
			int k = ((j+1) * 13) & 127;
			k = k ?: 1;
			k = (j+k < N-1) ? k : N-1-j;
			err = nihtree_append_buf(&tt2, &tc, tpls+j, k, buf);
			if (err != NIH_OK)
				printf("nihtree_insert returns %d\n", err);
			j += k;
			assert(nihtree_count(&tt2) == j);
		}
		printf("Sizeof tt2=%d\n", nihtree_count(&tt2));
		nihtree_iter_init(&tt, &tc, it, nihscan_forward);
		nihtree_iter_init(&tt2, &tc, it2, nihscan_forward);
		while((tp = nihtree_iter_next(it)) != NULL) {
			tp2 = nihtree_iter_next(it2);
			assert(tp2 != NULL);
			assert(tp2->k == tp->k);
			assert(tp2->payload == tp->payload);
		}
		tp2 = nihtree_iter_next(it2);
		assert(tp2 == NULL);
		printf("tt2 is equivalent of tt\n");
		nihtree_release(&tt2, &tc);
	}

	puts("==========Searches========");
	f = b = 0;
	for(i=1; i<N; i++) {
		ik.k = (541 * i) & (1023);

		tp = nihtree_find_by_key(&tt, &tc, &ik, &err);
		if (err != NIH_OK)
			printf("nihtree_find returns %d for %d\n", err, ik.k);
		else if (ik.k != tp->k || tp->payload != i)
			printf("nihtree_find searches %d but found %d\n", ik.k, tp->k);
		else
			f++;
	}

	for(i=N; i<=1024; i++) {
		ik.k = (541 * i) & (1023);

		tp = nihtree_find_by_key(&tt, &tc, &ik, &err);
		if (err != NIH_NOTFOUND)
			printf("nihtree_find returns %d\n", err);
		else
			b++;
	}

	if (f + b == 1024)
		printf("found %d missed %d total 1024\n", f, b);
	else
		printf("ERROR: found %d missed %d total %d != 1024\n", f, b, f+b);

	puts("==========Scan Forward========");
	nihtree_iter_init(&tt, &tc, it, nihscan_forward);
	memset(ch, 0, sizeof(ch));
	i = 0;
	ptp = NULL;
	while((tp = nihtree_iter_next(it)) != NULL) {
		i ++;
		printf("SCAN %d:%d\n", tp->k, tp->payload);

		if (ptp && ptp->k >= tp->k)
			printf("WRONG ORDER\n");
		if (tp->k != tpls[i-1].k || tp->payload != tpls[i-1].payload)
			printf("WRONG DATA %d->%d (wants %d->%d)\n",
					tp->k, tp->payload,
					tpls[i-1].k, tpls[i-1].payload);
		ptp = tp;

		if ((i&1) == 0) mix_check(ch, tp->k, 1);
		if ((i%13) == 0) mix_check(ch+3, tp->k, 1);
		if ((i%100) == 0) mix_check(ch+6, tp->k, 1);
	}
	printf("Found: %d tuples\n", i);
	nihtree_iter_init(&tt, &tc, it, nihscan_forward);
	check_skip(it, 1, ch, tpls);
	nihtree_iter_init(&tt, &tc, it, nihscan_forward);
	check_skip(it, 12, ch+3, tpls);
	nihtree_iter_init(&tt, &tc, it, nihscan_forward);
	check_skip(it, 99, ch+6, tpls);

	puts("==========Scan Backward========");
	nihtree_iter_init(&tt, &tc, it, nihscan_backward);
	memset(ch, 0, sizeof(ch));
	i = 0;
	ptp = NULL;
	while((tp = nihtree_iter_next(it)) != NULL) {
		i ++;
		printf("SCAN %d:%d\n", tp->k, tp->payload);

		if (ptp && ptp->k <= tp->k)
			printf("WRONG ORDER\n");
		if (tp->k != tpls[N-1-i].k || tp->payload != tpls[N-1-i].payload)
			printf("WRONG DATA %d->%d (wants %d->%d)\n",
					tp->k, tp->payload,
					tpls[N-1-i].k, tpls[N-1-i].payload);
		ptp = tp;
		if ((i&1) == 0) mix_check(ch, tp->k, 1);
		if ((i%13) == 0) mix_check(ch+3, tp->k, 1);
		if ((i%100) == 0) mix_check(ch+6, tp->k, 1);
	}
	printf("Found: %d tuples\n", i);

	nihtree_iter_init(&tt, &tc, it, nihscan_backward);
	check_skip(it, 1, ch, trev);
	nihtree_iter_init(&tt, &tc, it, nihscan_backward);
	check_skip(it, 12, ch+3, trev);
	nihtree_iter_init(&tt, &tc, it, nihscan_backward);
	check_skip(it, 99, ch+6, trev);

	for(i=0; i<1024; i++) {
		ik.k = i;
		f = fe = b = be = 0;

		nihtree_iter_init_set(&tt, &tc, it, &ik, nihscan_forward);
		while((tp = nihtree_iter_next(it)) != NULL) {
			//printf("SCAN %d:%d\n", tp->k, tp->payload);
			if (tp->k == ik.k)
				fe++;
			if (tp->k < ik.k)
				printf("WRONG ORDER\n");
			f++;
		}
		printf("Scan forward  starting with key %d found %d+%d tuples\n", ik.k, fe, f-fe);

		nihtree_iter_init_set(&tt, &tc, it, &ik, nihscan_backward);
		while((tp = nihtree_iter_next(it)) != NULL) {
			//printf("SCAN %d:%d\n", tp->k, tp->payload);
			if (tp->k == ik.k)
				be++;
			if (tp->k > ik.k)
				printf("WRONG ORDER\n");
			b++;
		}
		printf("Scan backward starting with key %d found %d+%d tuples\n", ik.k, be, b-be);

		if (fe != be)
			printf("forward equal == %d != backward equal == %d\n", fe, be);
		if (f -fe + b != N - 1)
			printf("forward + backward != %d\n", N - 1);
	}

	iterator_equal_many = 1;
	for(i=0; i<1024; i++) {
		ik.k = i;
		f = fe = b = be = 0;

		nihtree_iter_init_set(&tt, &tc, it, &ik, nihscan_forward);
		while((tp = nihtree_iter_next(it)) != NULL) {
			if (tp->k / 13 == ik.k / 13)
				fe++;
			if (tp->k / 13 < ik.k / 13)
				printf("WRONG ORDER\n");
			f++;
		}
		printf("Scan forward  starting with key %d found %d+%d tuples\n", ik.k, fe, f-fe);

		nihtree_iter_init_set(&tt, &tc, it, &ik, nihscan_backward);
		while((tp = nihtree_iter_next(it)) != NULL) {
			if (tp->k / 13 == ik.k / 13)
				be++;
			if (tp->k / 13 > ik.k / 13)
				printf("WRONG ORDER\n");
			b++;
		}
		printf("Scan backward starting with key %d found %d+%d tuples\n", ik.k, be, b-be);

		if (fe != be)
			printf("forward equal == %d != backward equal == %d\n", fe, be);
		if (f - fe + b != N - 1)
			printf("forward - equal + backward != %d\n", N - 1);
	}
	iterator_equal_many = 0;

	puts("==========Insert Duplicates========");
	for(i=1; i<N; i+=2) {
		t.k = (541 * i) & (1023);
		t.payload = i+1024;

		err = nihtree_insert(&tt, &tc, &t, false);
		if (err != NIH_DUPLICATE)
			printf("nihtree_insert returns %d\n", err);
	}
	check_order(&tt, &tc, it);

	for(i=1; i<N; i+=2) {
		t.k = (541 * i) & (1023);
		t.payload = i+1024;

		printf("INSERT %d:%d\n", t.k, t.payload);
		err = nihtree_insert(&tt, &tc, &t, true);
		if (err != NIH_OK)
			printf("nihtree_insert returns %d\n", err);
	}
	check_order(&tt, &tc, it);

	printf("Stats: %d tuples\n", nihtree_count(&tt));

	ik.k = 0;
	err = nihtree_delete(&tt, &tc, &ik);
	if (err != NIH_NOTFOUND)
		printf("DELETS NON-EXISTED KEY\n");

	for(i=1; i<19*N/20; i++) {
		ik.k = (541 * i) & (1023);

		//printf("DELETE %d\n", ik.k);
		err = nihtree_delete(&tt, &tc, &ik);
		if (err != NIH_OK)
			printf("nihtree_delete returns %d\n", err);
	}
	check_order(&tt, &tc, it);
	printf("Stats: %d tuples\n", nihtree_count(&tt));

	puts("==========Scan Forward REDUCED========");
	nihtree_iter_init(&tt, &tc, it, nihscan_forward);
	i=0;
	ptp = NULL;
	while((tp = nihtree_iter_next(it)) != NULL) {
		i ++;
		printf("SCAN %d:%d\n", tp->k, tp->payload);

		if (ptp && ptp->k >= tp->k)
			printf("WRONG ORDER\n");
		ptp = tp;
	}
	printf("Found: %d tuples\n", i);

	nihtree_release(&tt, &tc);
	printf("Unfreed alloced memory: %zd bytes in %zd allocations\n",
			total_size, total_count);

	nihtree_init(&tt);

	puts("==========Inserts========");
	for(i=1; i<N; i++) {
		t.k = (541 * i) & (1023);
		t.payload = i;

		err = nihtree_insert(&tt, &tc, &t, false);
		if (err != NIH_OK)
			printf("nihtree_insert returns %d\n", err);
		//check_order(&tt, &tc, it);
	}

	printf("Stats: %d tuples\n", nihtree_count(&tt));
	puts("==========Deletes========");

	for(i=1; i<N; i++) {
		ik.k = (541 * i) & (1023);

		err = nihtree_delete(&tt, &tc, &ik);
		if (err != NIH_OK)
			printf("nihtree_delete returns %d\n", err);
		//check_order(&tt, &tc, it);
	}
	printf("Stats: %d tuples\n", nihtree_count(&tt));

	nihtree_release(&tt, &tc);
	printf("Unfreed alloced memory: %zd bytes in %zd allocations\n",
			total_size, total_count);

	return 0;
}
