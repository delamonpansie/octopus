/*
 * Copyright (C) 2010-2017 Mail.RU
 * Copyright (C) 2010-2012, 2014, 2016 Yury Vostrikov
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

#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>
#import <util.h>
#import <fiber.h>
#import <octopus_ev.h>
#import <tbuf.h>
#import <say.h>
#include <cfg/defs.h>
#import <stat.h>
#include <assoc.h>

#if CFG_lua_path
#import <src-lua/octopus_lua.h>
#endif

#if CFG_graphite_addr
#import <graphite.h>
static void stat_send_to_graphite();
#endif

#define SECS 5

typedef u64 au64 __attribute__((__aligned__(1)));
typedef u32 au32 __attribute__((__aligned__(1)));

enum stat_base_type { SBASE_STATIC = 1, SBASE_DYNAMIC = 2, SBASE_CALLBACK = 3 };
enum stat_accum_type { SACC_ACCUM = 1, SACC_DOUBLE = 2, SACC_GAUGE = 3 };
int stat_current_base = -1;

struct stat_name {
	u64 hsh;
	int len;
	char str[];
};

static u64 hash_name(const char* str, int len);

struct accum {
	u64 hsh;
	struct stat_name const * name;
	enum stat_accum_type type;
	i64 cnt;
	double sum;
	double min;
	double max;
	au32 *hist;
};
#define HIST_MIN (-10)
#define HIST_MAX (20)
#define HIST_STEP (0.5)
#define HIST_CNT (1+(int)((HIST_MAX - HIST_MIN)*2))

const u64 half_log2 = ((u64)0x15f61<<32)|(u64)0x9980c433ul; /* exact */
const u64 quot_log2 = (u64)0x518 << 40; /* approximate */
static inline int
hist_pos(double val) {
	int p = 0;
	if (val > 0) {
#if defined(__amd64__) || defined(__i386__)
		union {
			double fl;
			u64 i;
		} v = {.fl = val};
		/* approximate round(log2(v)*2) */
		p = (int)((v.i + quot_log2)>>51) - 2046;
#else
		p = (int)round(log(val)/M_LN2*2);
#endif
		p -= HIST_MIN * 2 - 1;
		if (p < 0) {
			p = 0;
		} else if (p >= HIST_CNT) {
			p = HIST_CNT-1;
		}
	}
	return p;
}

static inline double
hist_val(int p) {
	if (p == 0) return 0;
	p += HIST_MIN * 2 - 1;
#if defined(__amd64__) || defined(__i386__)
	union {
		double fl;
		u64 i;
	} v;
	v.i = ((u64)(p + 2046) << 51) - (u64)(p&1)*half_log2;
	return v.fl;
#else
	return pow((double)p/2, 2);
#endif
}

struct stat_percent {
	double p50, p90, p99;
};
static struct stat_percent
hist_percent(uint32_t *hist, uint32_t cnt)
{
	int i;
	uint32_t sum = 0, c1 = cnt/100, c10 = cnt/10, c50 = cnt/2;
	struct stat_percent p = {-1,-1,-1};
	for (i=HIST_CNT-1; i>=0; i--) {
		if (hist[i] == 0)
			continue;
		sum += hist[i];
		if (p.p99 < 0 && sum >= c1) {
			p.p99 = hist_val(i);
		}
		if (p.p90 < 0 && sum >= c10) {
			p.p90 = hist_val(i);
		}
		if (sum >= c50) {
			p.p50 = hist_val(i);
			break;
		}
	}
	return p;
}

#define MH_STATIC 1
#undef load_factor
#define load_factor 0.55

#define mh_name _accum
#define mh_neighbors 1
#define mh_byte_map 0
#define MH_QUADRATIC_PROBING 1
#define MH_INCREMENTAL_RESIZE 0
#define mh_slot_t struct accum
#define mh_slot_key(h, s) ((s)->hsh)
#define mh_hash(h, hsh) ( (hsh) )
#define mh_eq(h, a, b) ( (a) == (b) )
#include <mhash.h>

struct palloc_config stat_pool_cfg = {.name = "stat_names_pool"};
struct stat_accum {
	struct mh_accum_t values;
	struct palloc_pool* name_pool;
};

static void
stat_accum_init(struct stat_accum* sa) {
	mh_accum_initialize(&sa->values);
	sa->name_pool = palloc_create_pool(stat_pool_cfg);
}

static void
stat_accum_destruct(struct stat_accum* sa)
{
	palloc_destroy_pool(sa->name_pool);
	sa->name_pool = NULL;
	mh_accum_destruct(&sa->values);
	memset(&sa->values, 0, sizeof(sa->values));
}

static struct stat_name*
stat_accum_alloc_name(struct stat_accum* sa, u64 hsh, char const * name, int len)
{
	struct stat_name* nm = palloc(sa->name_pool, sizeof(struct stat_name) + len + 1);
	nm->hsh = hsh;
	nm->len = len;
	memcpy(nm->str, name, len);
	nm->str[len] = 0;
	return nm;
}

static struct accum*
stat_accum_get_accum(struct stat_accum* sa, u64 hsh, char const * name, int len)
{
	u32 k = mh_accum_get(&sa->values, hsh);
	if (k == mh_end(&sa->values)) {
		struct accum tmp = {.hsh = hsh};
		tmp.name = stat_accum_alloc_name(sa, tmp.hsh, name, len);
		tmp.type = 0;
		tmp.cnt = 0;
		tmp.sum = 0;
		tmp.min = 1e300;
		tmp.max = -1e300;
		mh_accum_sput(&sa->values, &tmp, NULL);
		k = mh_accum_get(&sa->values, hsh);
		assert(k != mh_end(&sa->values));
	}
	/* rely on MH_INCREMENTAL_RESIZE == 0 */
	return mh_accum_slot(&sa->values, k);
}

static void
stat_accum_dup_accum(struct stat_accum* sa, struct accum const *old)
{
	struct accum cpy = *old;
	cpy.name = stat_accum_alloc_name(sa, cpy.name->hsh, cpy.name->str, cpy.name->len);
	if (cpy.hist != NULL) {
		u32 *hist = palloc(sa->name_pool, HIST_CNT * sizeof(u32));
		memmove(hist, cpy.hist, HIST_CNT * sizeof(u32));
		cpy.hist = hist;
	}
	mh_accum_sput(&sa->values, &cpy, NULL);
}

struct stat_base {
	const char *name;
	enum stat_base_type type;
	stat_get_current_callback cb;
	struct {
		struct {
			struct accum *v;
			int n;
		} static_names;
		struct stat_accum dynamic_names;
	} current;
	struct stat_accum records[5];
	int recordsn;
	int next_free;
#ifdef CFG_graphite_addr
	struct stat_accum periodic;
	ev_tstamp period_start;
	ev_tstamp period_stop;
#endif
};

static struct stat_base *stat_bases = NULL;
static int stat_basen = 0;
static int stat_free = -1;

static int
stat_register_any(char const *base_name, enum stat_base_type type, 
		stat_get_current_callback cb, char const * const * opnames, size_t count)
{
	int i, bn;
	struct stat_base *bs;
	if (stat_free == -1) {
		int oldbasen = stat_basen;
		stat_basen = stat_basen*2 ?: 8;
		stat_bases = xrealloc(stat_bases, sizeof(*stat_bases)*stat_basen);
		memset(stat_bases + oldbasen, 0, sizeof(*stat_bases) * (stat_basen - oldbasen));
		for (i = oldbasen; i < stat_basen-1; i++) {
			stat_bases[i].next_free = i+1;
		}
		stat_bases[stat_basen-1].next_free = -1;
		stat_free = oldbasen;
	}
	bn = stat_free;
	bs = stat_bases + bn;
	stat_free = bs->next_free;
	bs->next_free = -2;
	bs->type = type;
	bs->cb = cb;
	bs->name = xstrdup(base_name);
	stat_accum_init(&bs->current.dynamic_names);
	if (type == SBASE_STATIC) {
		struct accum *v = xcalloc(count, sizeof(struct accum));
		bs->current.static_names.v = v;
		bs->current.static_names.n = count;
		for (i = 0; i < count; i++) {
			if (opnames[i] == NULL)
				continue;
			v[i].name = stat_malloc_name(opnames[i], strlen(opnames[i]));
			v[i].hsh = v[i].name->hsh;
			v[i].type = 0;
			v[i].cnt = 0;
			v[i].sum = 0;
			v[i].max = -1e300;
			v[i].min = 1e300;
		}
	} else if (type == SBASE_CALLBACK) {
		assert(cb != NULL);
	}
#ifdef CFG_graphite_addr
	stat_accum_init(&bs->periodic);
	bs->period_start = ev_now();
	bs->period_stop = ev_now();
#endif
	for (i = 0; i < nelem(bs->records); i++) {
		stat_accum_init(&bs->records[i]);
	}
	bs->recordsn = 0;
	return bn;
}

void
stat_unregister(int base)
{
	int i;
	struct stat_base *bs;
	assert(base < stat_basen);
	bs = stat_bases + base;
	assert(bs->name != NULL && bs->next_free == -2);
	bs->next_free = stat_free;
	stat_free = base;
	if (bs->type == SBASE_STATIC) {
		struct accum *v = bs->current.static_names.v;
		for (i = 0; i < bs->current.static_names.n; i++) {
			if (v[i].hist != NULL) {
				free(v[i].hist);
			}
			free((void*)v[i].name);
		}
		free(v);
	}
	stat_accum_destruct(&bs->current.dynamic_names);
	for (i = 0; i < nelem(bs->records); i++) {
		stat_accum_destruct(&bs->records[i]);
	}
}

static void
merge_stat(const char *basename, struct stat_accum *small, struct stat_accum *big) {
	mh_foreach(_accum, &small->values, k) {
		struct accum *ac = mh_accum_slot(&small->values, k);
		uint32_t ix = mh_accum_get(&big->values, ac->hsh);
		if (ix == mh_end(&big->values)) {
			stat_accum_dup_accum(big, ac);
		} else {
			/* we rely on MH_INCREMENTAL_RESIZE == 0 here */
			struct accum *bc = mh_accum_slot(&big->values, ix);
			if (bc->type != ac->type) {
				say_warn("stat types doesn't match for %s.%.*s : %d != %d",
						basename, ac->name->len, ac->name->str,
						ac->type, bc->type);
				continue;
			}
			if (ac->type == SACC_ACCUM) {
				bc->sum += ac->sum;
			} else if (ac->type == SACC_DOUBLE) {
				bc->cnt += ac->cnt;
				bc->sum += ac->sum;
				if (bc->max < ac->max)
					bc->max = ac->max;
				if (bc->min > ac->min)
					bc->min = ac->min;
				for (int i = 0; i < HIST_CNT; i++) {
					bc->hist[i] += ac->hist[i];
				}
			} else if (ac->type == SACC_GAUGE) {
				bc->sum = ac->sum;
			} else {
				say_warn("unknown stat type for %s.%.*s: %d",
						basename, ac->name->len, ac->name->str,
						ac->type);
			}
		}
	}
}

static void stat_cpu_usage();

/* fiber for shifting seconds */
static void
stat_shift_all(ev_periodic *w _unused_, int revents _unused_) {
	int i;
#ifdef CFG_graphite_addr
	/* send at the beginning, cause we should send for previous minute */
	stat_send_to_graphite();
#endif
	stat_cpu_usage();

	for (stat_current_base=0; stat_current_base<stat_basen; stat_current_base++) {
		struct stat_base *bs = stat_bases + stat_current_base;
		struct stat_accum cur;
		if (bs->name == NULL) {
			continue;
		}
		if (bs->type == SBASE_CALLBACK) {
			bs->cb(stat_current_base);
		}
		if (bs->type == SBASE_CALLBACK || bs->type == SBASE_DYNAMIC) {
			cur = bs->current.dynamic_names;
			memset(&bs->current.dynamic_names, 0,
					sizeof(bs->current.dynamic_names));
			stat_accum_init(&bs->current.dynamic_names);
		} else if (bs->type == SBASE_STATIC) {
			struct accum *v = bs->current.static_names.v;
			memset(&cur, 0, sizeof(cur));
			stat_accum_init(&cur);
			for (i = 0; i < bs->current.static_names.n; i++) {
				if (v[i].name == NULL || v[i].type == 0) continue;
				stat_accum_dup_accum(&cur, &v[i]);
				v[i].type = 0;
				v[i].cnt = 0;
				v[i].sum = 0;
				v[i].max = -1e300;
				v[i].min = 1e300;
				if (v[i].hist != NULL) {
					for (int j = 0; j < HIST_CNT; j++)
						v[i].hist[j] = 0;
				}
			}
		} else {
			panic("unknown stat type");
		}

		stat_accum_destruct(&bs->records[nelem(bs->records)-1]);
		for (i = nelem(bs->records) - 1; i > 0; i--) {
			bs->records[i] = bs->records[i-1];
		}
		bs->records[0] = cur;
		bs->recordsn = MIN(bs->recordsn+1, nelem(bs->records));
#ifdef CFG_graphite_addr
		merge_stat(bs->name, &cur, &bs->periodic);
		bs->period_stop = ev_now();
#endif
	}
	stat_current_base = -1;
}

static int
stat_compare_accums(void const* a, void const* b)
{
	struct accum const *aa = (struct accum const* )a;
	struct accum const *ba = (struct accum const* )b;
	return strcmp(aa->name->str, ba->name->str);
}

static void
stat_admin_out(struct stat_accum *cur, struct tbuf *b)
{
	int i;
	struct tbuf key = TBUF(NULL, 0, fiber->pool);
	struct accum *accums = p0alloc(fiber->pool, mh_size(&cur->values) * sizeof(struct accum));
	struct accum *p = accums;
	mh_foreach(_accum, &cur->values, k) {
		*p = *mh_accum_slot(&cur->values, k);
		p++;
	}
	qsort(accums, mh_size(&cur->values), sizeof(*accums), stat_compare_accums);
	p = accums;
	for (i = 0; i<mh_size(&cur->values); i++, p++) {
		tbuf_printf(&key, "%.*s:", p->name->len, p->name->str);
		if (p->type == SACC_ACCUM) {
			double rps = p->sum / nelem(stat_bases[0].records);
			tbuf_printf(b, "  %-25.*s { rps: %-8.3f }\r\n",
					  (int)tbuf_len(&key), (char*)key.ptr, rps);
		} else if (p->type == SACC_DOUBLE) {
			double avg, sum_rps;
			i64 cnt_rps;
			bool first = true;
#define COMMA (({bool f = first; first = false; f;}) ? " " : ", ")
			tbuf_printf(b, "  %-25.*s {", (int)tbuf_len(&key), (char*)key.ptr);
			if (p->cnt != 0) {
				avg = p->sum / p->cnt;
				tbuf_printf(b, "%savg: %-8.3f", COMMA, avg);
			}
			if (p->min != 1e300) {
				tbuf_printf(b, "%smin: %-8.3f", COMMA, p->min);
				tbuf_printf(b, "%smax: %-8.3f", COMMA, p->max);
				struct stat_percent pcnt = hist_percent(p->hist, p->cnt);
				tbuf_printf(b, "%sp50: %-8.3f", COMMA, pcnt.p50);
				tbuf_printf(b, "%sp90: %-8.3f", COMMA, pcnt.p90);
				tbuf_printf(b, "%sp99: %-8.3f", COMMA, pcnt.p99);
			}
			sum_rps = p->sum / nelem(stat_bases[0].records);
			if (p->cnt != 0)
				tbuf_printf(b, "%scnt: %-8"PRIi64, COMMA, p->cnt);
			if (p->cnt != 0 || p->sum != 0)
				tbuf_printf(b, "%ssum: %-8.3f", COMMA, p->sum);
			if (p->cnt != 0) {
				cnt_rps = p->cnt / nelem(stat_bases[0].records);
				tbuf_printf(b, "%scnt_rps: %-8"PRIi64, COMMA, cnt_rps);
			}
			if (p->sum != 0)
				tbuf_printf(b, "%ssum_rps: %-8.3f", COMMA, sum_rps);
			tbuf_printf(b, " }\r\n");
		} else if (p->type == SACC_GAUGE) {
			tbuf_printf(b, "  %-25.*s %-8.3f\r\n",
					  (int)tbuf_len(&key), (char*)key.ptr, p->sum);
		}
		tbuf_reset(&key);
	}
}

static void*
tmp_realloc(void* ptr, size_t newsz)
{
	size_t oldsz = 0;
	void* newptr;
	if (newsz == 0)
		return NULL;
	newptr = palloc(fiber->pool, newsz+sizeof(size_t));
	*(size_t*)newptr = newsz;
	newptr += sizeof(size_t);
	if (ptr != NULL) {
		oldsz = *(((size_t*)ptr)-1);
		memcpy(newptr, ptr, MIN(oldsz, newsz));
	}
	return newptr;
}

void
stat_print(struct tbuf *out)
{
	int i, j;
	struct mh_cstr_t stats = {.realloc = tmp_realloc};
	mh_cstr_initialize(&stats);

	for (i=0; i<stat_basen; i++) {
		struct stat_base *bs = stat_bases + i;
		struct stat_accum *cur;
		if (bs->name == NULL || bs->recordsn == 0) continue;
		u32 k = mh_cstr_get(&stats, bs->name);
		if (k == mh_end(&stats)) {
			cur = p0alloc(fiber->pool, sizeof(struct stat_accum));
			cur->values.realloc = tmp_realloc;
			mh_accum_initialize(&cur->values);
			cur->name_pool = fiber->pool;
			mh_cstr_put(&stats, bs->name, cur, NULL);
		} else {
			cur = (struct stat_accum*)mh_cstr_value(&stats, k);
		}
		for (j = bs->recordsn-1; j >= 0; j--) {
			merge_stat(bs->name, &bs->records[j], cur);
		}
	}

	mh_foreach(_cstr, &stats, k) {
		char const *name = mh_cstr_key(&stats, k);
		struct stat_accum* cur = (struct stat_accum*)mh_cstr_value(&stats, k);
		if (strcmp(name, "stat") == 0) {
			tbuf_printf(out, "statistics:\r\n");
		} else {
			tbuf_printf(out, "statistics@%s:\r\n", name);
		}
		stat_admin_out(cur, out);
	}
}

int
stat_register_callback(char const *base_name, stat_get_current_callback cb)
{
	return stat_register_any(base_name, SBASE_CALLBACK, cb, NULL, 0);
}

int
stat_register_named(char const *base_name)
{
	return stat_register_any(base_name, SBASE_DYNAMIC, NULL, NULL, 0);
}

int
stat_register_static(char const *base_name, char const * const * opnames, size_t count)
{
	return stat_register_any(base_name, SBASE_STATIC, 0, opnames, count);
}

int
stat_register(char const * const * opnames, size_t count)
{
	return stat_register_static("stat", opnames, count);
}

static void
acc_sum(struct accum *acc, double value)
{
	acc->type = SACC_ACCUM;
	acc->sum += value;
}

static void
acc_gauge(struct accum *acc, double value)
{
	acc->type = SACC_GAUGE;
	acc->sum = value;
}

static void
acc_aggregate(struct accum *acc, double value)
{
	acc->type = SACC_DOUBLE;
	acc->cnt++;
	acc->sum += value;
	if ( acc->max < value )
		acc->max = value;
	if ( acc->min > value )
		acc->min = value;
	acc->hist[hist_pos(value)]++;
}

void
stat_sum_named(int base, char const * name, int len, double value)
{
	assert(base < stat_basen);
	struct stat_base *bs = &stat_bases[base];
	assert(bs->name != NULL);
	u64 hsh = hash_name(name, len);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names, hsh, name, len);
	acc_sum(acc, value);
}

void
stat_gauge_named(int base, char const * name, int len, double value)
{
	assert(base < stat_basen);
	struct stat_base *bs = &stat_bases[base];
	assert(bs->name != NULL);
	u64 hsh = hash_name(name, len);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names, hsh, name, len);
	acc_gauge(acc, value);
}

void
stat_aggregate_named(int base, char const * name, int len, double value)
{
	assert(base < stat_basen);
	struct stat_base *bs = &stat_bases[base];
	assert(bs->name != NULL);
	u64 hsh = hash_name(name, len);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names, hsh, name, len);
	if (acc->hist == NULL) {
		acc->hist = p0alloc(bs->current.dynamic_names.name_pool, HIST_CNT * sizeof(uint32_t));
	}
	acc_aggregate(acc, value);
}

void
stat_sum_fastnamed(int base, struct stat_name const * name, double value)
{
	assert(base < stat_basen);
	struct stat_base *bs = &stat_bases[base];
	assert(bs->name != NULL);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names,
			name->hsh, name->str, name->len);
	acc_sum(acc, value);
}

void
stat_gauge_fastnamed(int base, struct stat_name const * name, double value)
{
	assert(base < stat_basen);
	struct stat_base *bs = &stat_bases[base];
	assert(bs->name != NULL);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names,
			name->hsh, name->str, name->len);
	acc_gauge(acc, value);
}

void
stat_aggregate_fastnamed(int base, struct stat_name const * name, double value)
{
	assert(base < stat_basen);
	struct stat_base *bs = &stat_bases[base];
	assert(bs->name != NULL);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names,
			name->hsh, name->str, name->len);
	if (acc->hist == NULL) {
		acc->hist = p0alloc(bs->current.dynamic_names.name_pool, HIST_CNT * sizeof(uint32_t));
	}
	acc_aggregate(acc, value);
}

void
stat_sum_static(int base, int name, double value)
{
	acc_sum(&stat_bases[base].current.static_names.v[name], value);
}

void
stat_gauge_static(int base, int name, double value)
{
	acc_gauge(&stat_bases[base].current.static_names.v[name], value);
}

void
stat_aggregate_static(int base, int name, double value)
{
	struct accum *acc = &stat_bases[base].current.static_names.v[name];
	if (acc->hist == NULL) {
		acc->hist = xcalloc(HIST_CNT, sizeof(uint32_t));
	}
	acc_aggregate(acc, value);
}

void
stat_collect(int base, int name, i64 value)
{
	stat_sum_static(base, name, value);
}

void
stat_collect_double(int base, int name, double value)
{
	stat_aggregate_static(base, name, value);
}

void
stat_report_sum(char const * name, int len, double value)
{
	assert(stat_current_base != -1);
	struct stat_base *bs = &stat_bases[stat_current_base];
	assert(bs->name != NULL);
	u64 hsh = hash_name(name, len);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names, hsh, name, len);
	acc->type = SACC_ACCUM;
	acc->sum = value;
}

void
stat_report_gauge(char const * name, int len, double value)
{
	assert(stat_current_base != -1);
	struct stat_base *bs = &stat_bases[stat_current_base];
	assert(bs->name != NULL);
	u64 hsh = hash_name(name, len);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names, hsh, name, len);
	acc->type = SACC_GAUGE;
	acc->sum = value;
}

void
stat_report_double(char const * name, int len, double sum, i64 cnt, double min, double max)
{
	assert(stat_current_base != -1);
	struct stat_base *bs = &stat_bases[stat_current_base];
	assert(bs->name != NULL);
	u64 hsh = hash_name(name, len);
	struct accum *acc = stat_accum_get_accum(&bs->current.dynamic_names, hsh, name, len);
	acc->type = SACC_DOUBLE;
	acc->sum = sum;
	acc->cnt = cnt;
	acc->min = min;
	acc->max = max;
}

#ifdef CFG_graphite_addr
static void
stat_accum_reset(struct stat_accum* sa) {
	mh_accum_clear(&sa->values);
	prelease(sa->name_pool);
}

static void
stat_base_print_to_graphite(struct stat_base *bs)
{
	double diff_time = bs->period_stop - bs->period_start;
	if (diff_time < 0.8)
		return;
	if (graphite_sock == -1)
		goto end;
	mh_foreach(_accum, &bs->periodic.values, k) {
		struct accum *acc = mh_accum_slot(&bs->periodic.values, k);
		if (acc->type == SACC_ACCUM) {
			double rps = acc->sum / diff_time;
			graphite_send2(bs->name, acc->name->str, rps);
		} else if (acc->type == SACC_DOUBLE) {
			if (acc->cnt != 0) {
				double avg = acc->sum / acc->cnt;
				double cnt_rps = (double)acc->cnt / diff_time;
				graphite_send3(bs->name, acc->name->str, "avg", avg);
				graphite_send3(bs->name, acc->name->str, "cnt_rps", cnt_rps);
				graphite_send3(bs->name, acc->name->str, "cnt", acc->cnt);
			}
			if (acc->min != 1e300) {
				graphite_send3(bs->name, acc->name->str, "min", acc->min);
				graphite_send3(bs->name, acc->name->str, "max", acc->max);
				struct stat_percent pcnt = hist_percent(acc->hist, acc->cnt);
				graphite_send3(bs->name, acc->name->str, "p50", pcnt.p50);
				graphite_send3(bs->name, acc->name->str, "p90", pcnt.p90);
				graphite_send3(bs->name, acc->name->str, "p99", pcnt.p99);
			}
			if (acc->sum != 0 || acc->cnt != 0) {
				double sum_rps = (double)acc->sum / diff_time;
				graphite_send3(bs->name, acc->name->str, "sum", acc->sum);
				graphite_send3(bs->name, acc->name->str, "sum_rps", sum_rps);
			}
		} else if (acc->type == SACC_GAUGE) {
			graphite_send2(bs->name, acc->name->str, acc->sum);
		}
	}
end:
	stat_accum_reset(&bs->periodic);
	bs->period_start = bs->period_stop;
}

static void
stat_send_to_graphite()
{
	static ev_tstamp last = 0;
	ev_tstamp now = floor(ev_now()/60);
	int i;
	if (now == last)
		return;
	if (last == 0) {
		last = now;
		return;
	}
	last = now;
	for (i = 0; i < stat_basen; i++) {
		stat_base_print_to_graphite(&stat_bases[i]);
	}
	graphite_flush_now();
}
#endif

static double
timeval2double(struct timeval *tv)
{
	return (double)tv->tv_sec + (double)tv->tv_usec * 1e-6;
}

#if HAVE_CLOCK_GETTIME
#if defined(CLOCK_MONOTONIC_RAW)
#define USE_CLOCK CLOCK_MONOTONIC_RAW
#elif defined(CLOCK_MONOTONIC)
#define USE_CLOCK CLOCK_MONOTONIC
#else
#define USE_CLOCK CLOCK_REALTIME
#endif
static int use_clock = USE_CLOCK;

static void __attribute__((constructor))
init_use_clock()
{
	struct timespec ts;
	if (clock_gettime(use_clock, &ts))
		use_clock = CLOCK_REALTIME;
}

static double
get_current_time()
{
	struct timespec ts;
	if (clock_gettime(use_clock, &ts))
		return 0;
	return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}
#else
#define get_current_time() ev_time()
#endif

#define CPUSTAT(_) \
        _(CPU_USER, 1)		\
	_(CPU_SYS, 2)		\
	_(CPU, 3)		\
	_(MINOR_FAULT, 4)	\
	_(MAJOR_FAULT, 5)	\
	_(CTXSW_VOLUNT, 6)	\
	_(CTXSW_INVOLUNT, 7)    \
	_(VM_SIZE, 8)		\
	_(VM_RSS, 9)		\
	_(VM_SWAP, 10)

enum cpu_stat ENUM_INITIALIZER(CPUSTAT);
static char const * const cpu_stat_names[] = ENUM_STR_INITIALIZER(CPUSTAT);
static int sys_stat_base;
static double prevtime = 0, prevuser = 0, prevsys = 0;
static struct rusage prevusage;
static int sysbase;
static ssize_t vm_size = 0, vm_rss = 0, vm_swap = 0;

static void
stat_cpu_usage_init()
{
	sys_stat_base = stat_register_static("sys", cpu_stat_names, nelem(cpu_stat_names));
	double curtime = get_current_time();
	if (curtime == 0) {
		return;
	}
	if (getrusage(RUSAGE_SELF, &prevusage) != 0) {
		return;
	}
	prevtime = curtime;
	prevuser = timeval2double(&prevusage.ru_utime);
	prevsys = timeval2double(&prevusage.ru_stime);
}

static int
stat_read_rss() {
	static ev_tstamp last = 0;
	ev_tstamp cur = floor(ev_now() / 60);
	if (cur == last) {
		return vm_size != 0;
	}
	cur = last;
	FILE* f = fopen("/proc/self/status", "r");
	if (f == NULL) {
		return vm_size != 0;
	}
	char *line = NULL;
	size_t cap = 0, sz = 0;
	int cnt = 3;
	while (cnt > 0 && (sz = getline(&line, &cap, f)) > 0) {
		if (strncmp(line, "VmSize:", 7) == 0) {
			char *l = line + 7;
			while (*l > 0 && (*l < '0' || *l > '9')) l++;
			if (*l > 0)
				vm_size = (size_t)atol(l) * 1024;
			cnt--;
		} else if (strncmp(line, "VmRSS:", 6) == 0) {
			char *l = line + 6;
			while (*l > 0 && (*l < '0' || *l > '9')) l++;
			if (*l > 0)
				vm_rss = (size_t)atol(l) * 1024;
			cnt--;
		} else if (strncmp(line, "VmSwap:", 7) == 0) {
			char *l = line + 7;
			while (*l > 0 && (*l < '0' || *l > '9')) l++;
			if (*l > 0)
				vm_swap = (size_t)atol(l) * 1024;
			cnt--;
		} else if (strncmp(line, "volunt", 6) == 0) {
			break;
		}
	}
	fclose(f);
	if (line != NULL)
		free(line);
	return vm_size != 0;
}

static void
stat_cpu_usage()
{
	double curtime = 0, curuser = 0, cursys = 0;
	struct rusage curusage;
	if (stat_read_rss()) {
		stat_gauge_static(sysbase, VM_SIZE, vm_size);
		stat_gauge_static(sysbase, VM_RSS, vm_rss);
		stat_gauge_static(sysbase, VM_SWAP, vm_swap);
	}
	curtime = get_current_time();
	if (curtime == 0) {
		return;
	}
	if (getrusage(RUSAGE_SELF, &curusage) != 0) {
		return;
	}
	curuser = timeval2double(&curusage.ru_utime);
	cursys = timeval2double(&curusage.ru_stime);
	if (prevtime != 0) {
		double dlttime = curtime - prevtime;
		double userusage = 100 * (curuser - prevuser) / dlttime;
		double sysusage = 100 * (cursys - prevsys) / dlttime;
		double cpuusage = userusage + sysusage;
		stat_aggregate_static(sysbase, CPU_USER, userusage);
		stat_aggregate_static(sysbase, CPU_SYS, sysusage);
		stat_aggregate_static(sysbase, CPU, cpuusage);
		stat_aggregate_static(sysbase, MINOR_FAULT,
				curusage.ru_minflt - prevusage.ru_minflt);
		stat_aggregate_static(sysbase, MAJOR_FAULT,
				curusage.ru_majflt - prevusage.ru_majflt);
		stat_aggregate_static(sysbase, CTXSW_VOLUNT,
				curusage.ru_nvcsw - prevusage.ru_nvcsw);
		stat_aggregate_static(sysbase, CTXSW_INVOLUNT,
				curusage.ru_nivcsw - prevusage.ru_nivcsw);
	}
	prevtime = curtime;
	prevuser = curuser;
	prevsys = cursys;
	prevusage = curusage;
}

#if CFG_lua_path
void
stat_lua_callback(int base)
{
	lua_State *L = fiber->L;
	int top = lua_gettop(L);
	luaO_pushtraceback(L);
	lua_getglobal(L, "stat");
	lua_getfield(L, -1, "_report");
	lua_pushinteger(L, base);
	/* try to call callback to make a report */
	if (lua_pcall(L, 1, 0, top+1)) {
		const char *reason = lua_tostring(L, -1);
		say_error("lua stat callback error ([%d:%s]): %s",
				base, stat_bases[base].name, reason);
	}
	lua_settop(L, top);
}
#endif

static ev_periodic stat_shift_all_periodic;
void
stat_init()
{
	ev_periodic_init(&stat_shift_all_periodic, stat_shift_all, 0.999, 1, 0);
	ev_periodic_start(&stat_shift_all_periodic);
	stat_cpu_usage_init();
#if CFG_lua_path
	luaO_require_or_panic("stat", true, NULL);
#endif
}

static u64 hash_name(const char* str, int len) {
#define MULT1 0x6956abd6ed268a3bULL
#define MULT2 0xacd5ad43274593b1ULL
	u64 a = 0;
	u64 b = 0;
	if (len == 0) {
	} else if (len < 4) {
		u8 const * bs = (u8 const*)str;
		u64 v = bs[0] | (bs[len/2]<<8) | (bs[len-1]<<24);
		a = v;
		b = v;
	} else if (len <= 8) {
		u64 v1 = *(au32*)str;
		u64 v2 = *(au32*)(str+len-4);
		a = (v1<<32)|v2;
		b = (v2<<32)|v1;
	} else {
		u64 v;
		while (len > 8) {
			v = *(au64*)str;
			a ^= v; a=(a<<32)|(a>>32); a *= MULT1;
			b = (b<<32)|(b>>32); b^=v; b *= MULT2;
			str += 8;
			len -= 8;
		}
		v = *(au64*)(str+len-8);
		a ^= v; a=(a<<32)|(a>>32);
		b = (b<<32)|(b>>32); b ^= v;
	}
	a ^= (a >> 33) | ((u64)len << 32);
	b ^= b >> 33;
	a *= MULT1;
	b *= MULT2;
	a ^= a >> 32;
	b ^= b >> 32;
	a *= MULT1;
	b *= MULT2;
	return a ^ b ^ (a >> 32) ^ (b >> 33);
}

struct stat_name const * stat_malloc_name(const char* str, int len) {
	struct stat_name* nm = malloc(sizeof(struct stat_name)+len+1);
	nm->hsh = hash_name(str, len);
	nm->len = len;
	memcpy(nm->str, str, len);
	nm->str[len] = 0;
	return nm;
}

register_source();
