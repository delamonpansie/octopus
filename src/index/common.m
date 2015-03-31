/*
 * Copyright (C) 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2012, 2013, 2014 Yuriy Vostrikov
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

#import <config.h>
#import <index.h>
#import <assoc.h>
#import <say.h>

/*
  Note about *_addr() compare functions:
  it's ok to return 0: sptree_iterator_init_set() will select
  leftmost node in case of equality.
  it is guaranteed that pattern is a first arg.
*/

int
u32_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;
	else
		return 0;
}
int
i32_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.i32, b = nb->key.i32;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;
	else
		return 0;
}
int
u64_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;
	else
		return 0;
}
static inline int
lstr_field_compare(const union index_field *fa, const union index_field *fb)
{
	if (fa->str.prefix1 < fb->str.prefix1) return -1;
	if (fa->str.prefix1 > fb->str.prefix1) return 1;
	if (fa->str.prefix2 < fb->str.prefix2) return -1;
	if (fa->str.prefix2 > fb->str.prefix2) return 1;
	if (fa->str.len <= 6) {
		return (int)fa->str.len - (int)fb->str.len;
	}
	if (fb->str.len <= 6) return 1;
	const char *d1 = fa->str.len <= 14 ? fa->str.data.bytes : fa->str.data.ptr;
	const char *d2 = fb->str.len <= 14 ? fb->str.data.bytes : fb->str.data.ptr;
	int r = memcmp(d1, d2, MIN(fa->str.len, fb->str.len) - 6);
	if (r != 0)
		return r;

	return (int)fa->str.len - (int)fb->str.len;
}
int
i64_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.i64, b = nb->key.i64;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;
	else
		return 0;
}
int
lstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return lstr_field_compare(&na->key, &nb->key);
}
int
cstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return strcmp(na->key.ptr, nb->key.ptr);
}


int
u32_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u32_compare(nb, na, x);
}
int
i32_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i32_compare(nb, na, x);
}
int
u64_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u64_compare(nb, na, x);
}
int
i64_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u64_compare(nb, na, x);
}
int
lstr_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return lstr_compare(nb, na, x);
}
int
cstr_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return cstr_compare(nb, na, x);
}

static inline int addr_compare(const struct index_node *na, const struct index_node *nb)
{
	if ((uintptr_t)na->obj <= 1)
		return 0;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

int
u32_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;

	return addr_compare(na, nb);
}
int
i32_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.i32, b = nb->key.i32;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;

	return addr_compare(na, nb);
}
int
u64_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;

	return addr_compare(na, nb);
}
int
i64_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.i64, b = nb->key.i64;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;

	return addr_compare(na, nb);
}
int
lstr_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{

	int r = lstr_field_compare(&nb->key, &na->key);
	if (r != 0)
		return r;

	return addr_compare(na, nb);
}
int
cstr_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        int r = strcmp(na->key.ptr, nb->key.ptr);
	if (r != 0)
		return r;

	return addr_compare(na, nb);
}


int
u32_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u32_compare_with_addr(nb, na, x);
}
int
i32_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i32_compare_with_addr(nb, na, x);
}
int
u64_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u64_compare_with_addr(nb, na, x);
}
int
i64_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i64_compare_with_addr(nb, na, x);
}
int
lstr_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return lstr_compare_with_addr(nb, na, x);
}
int
cstr_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return cstr_compare_with_addr(nb, na, x);
}


int
u32_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	return a == b;
}
int
u64_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	return a == b;
}
static inline int
lstr_field_eq(const union index_field *fa, const union index_field *fb)
{
	if (fa->str.len != fb->str.len) return 0;
	if (fa->str.prefix1 != fb->str.prefix1) return 0;
	if (fa->str.prefix2 != fb->str.prefix2) return 0;
	if (fa->str.len <= 6) return 1;
	if (fa->str.len <= 14) {
		return fa->str.data.u64 == fb->str.data.u64;
	} else {
		const u8 *d1 = fa->str.data.ptr;
		const u8 *d2 = fb->str.data.ptr;
		return memcmp(d1, d2, fa->str.len - 6) == 0;
	}
}
int
lstr_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return lstr_field_eq(&na->key, &nb->key);
}
int
cstr_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return strcmp(na->key.ptr, nb->key.ptr) == 0;
}

int
u32_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	return a == b && na->obj == nb->obj;
}
int
u64_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	return a == b && na->obj == nb->obj;
}
int
lstr_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return  lstr_eq(na, nb, x) && na->obj == nb->obj;
}
int
cstr_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return cstr_eq(na, nb, x) && na->obj == nb->obj;
}

void
u32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->key.u32 = 0;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u32))
			index_raise("key is not u32");
		pattern->key.u32 = read_u32(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

void
i32_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->key.u32 = INT32_MIN;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u32))
			index_raise("key is not i32");
		pattern->key.u32 = read_u32(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}
void
u64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->key.u64 = 0;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u64))
			index_raise("key is not i64");
		pattern->key.u64 = read_u64(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

void
i64_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;

	u32 len;
	switch (cardinality) {
	case 0: pattern->key.u64 = INT64_MIN;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u64))
			index_raise("key is not i64");
		pattern->key.u64 = read_u64(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

void
set_lstr_field_noninline(union index_field *f, u32 len, const u8* s)
{
	set_lstr_field(f, len, s);
}

void
lstr_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;
	if (cardinality == 1) {
		u32 size = read_varint32(key);
		set_lstr_field(&pattern->key, size, read_bytes(key, size));
	} else if (cardinality == 0) {
		set_lstr_field(&pattern->key, 0, NULL);
	}
	else
		index_raise("cardinality too big");
}



static int
field_compare(union index_field *f1, union index_field *f2, enum index_field_type type)
{
	switch (type) {
	case SNUM16:
		return f1->i16 > f2->i16 ? 1 : f1->i16 == f2->i16 ? 0 : -1;
	case UNUM16:
		return f1->u16 > f2->u16 ? 1 : f1->u16 == f2->u16 ? 0 : -1;
	case SNUM32:
		return f1->i32 > f2->i32 ? 1 : f1->i32 == f2->i32 ? 0 : -1;
	case UNUM32:
		return f1->u32 > f2->u32 ? 1 : f1->u32 == f2->u32 ? 0 : -1;
	case SNUM64:
		return f1->i64 > f2->i64 ? 1 : f1->i64 == f2->i64 ? 0 : -1;
	case UNUM64:
		return f1->u64 > f2->u64 ? 1 : f1->u64 == f2->u64 ? 0 : -1;
	case STRING:
		return lstr_field_compare(f1, f2);
	case UNDEF:
		abort();
	}
	abort();
}

int
tree_node_compare(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	/* if pattern is partialy specified compare only significant fields.
	   it's ok to return 0 here: sptree_iterator_init_set() will select
	   leftmost node in case of equality.
	   it is guaranteed that pattern is a first arg.
	*/

	int n = (uintptr_t)na->obj < nelem(ic->field) ? (uintptr_t)na->obj : ic->cardinality;

	if (n > 0) {
		int r = field_compare(&na->key, &nb->key, ic->field[0].type) * ic->field[0].sort_order;
		if (n == 1 || r != 0)
			return r;
	}

	for (int i = 1; i < n; ++i) {
		union index_field *akey = (void *)&na->key + ic->field[i].offset;
		union index_field *bkey = (void *)&nb->key + ic->field[i].offset;
		int r = field_compare(akey, bkey, ic->field[i].type);
		if (r != 0)
			return r * ic->field[i].sort_order;
	}
	return 0;
}

int
tree_node_compare_with_addr(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	int r = tree_node_compare(na, nb, ic);
	if (r != 0)
		return r;

	if ((uintptr_t)na->obj < nelem(ic->field)) /* `na' is a pattern */
		return r;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

static int
field_eq(union index_field *f1, union index_field *f2, enum index_field_type type)
{
	switch (type) {
	case SNUM16:
	case UNUM16:
		return f1->u16 == f2->u16;
	case SNUM32:
	case UNUM32:
		return f1->u32 == f2->u32;
	case SNUM64:
	case UNUM64:
		return f1->u64 == f2->u64;
	case STRING:
		return lstr_field_eq(f1, f2);
	case UNDEF:
		abort();
	}
	abort();
}

int
tree_node_eq(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	/* if pattern is partialy specified compare only significant fields.
	   it's ok to return 0 here: sptree_iterator_init_set() will select
	   leftmost node in case of equality.
	   it is guaranteed that pattern is a first arg.
	*/

	int n = (uintptr_t)na->obj < nelem(ic->field) ? (uintptr_t)na->obj : ic->cardinality;

	if (n > 0) {
		if (field_eq(&na->key, &nb->key, ic->field[0].type) == 0)
			return 0;
	}

	for (int i = 1; i < n; ++i) {
		union index_field *akey = (void *)&na->key + ic->field[i].offset;
		union index_field *bkey = (void *)&nb->key + ic->field[i].offset;
		int r = field_eq(akey, bkey, ic->field[i].type);
		if (r == 0)
			return 0;
	}
	return 1;
}

int
tree_node_eq_with_addr(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	return na->obj == nb->obj && tree_node_eq(na, nb, ic);
}

#define KNUTH_MULT 0x5851f42d4c957f2dULL

static inline u64
lstr_hash(const union index_field *f, u64 h) {
	h = (h ^ f->str.len) * KNUTH_MULT;
	h = (h << 13) | (h >> 51);
	if (f->str.len > 6) {
		if (f->str.len > 14) {
			const u8 *d = f->str.data.ptr;
			u32 l = f->str.len - 6;
			while (l >= 4) {
				u32 k = *(u32*)d;
				h = (h ^ k) * KNUTH_MULT;
				h = (h << 13) | (h >> 51);
				d += 4;
				l -= 4;
			}
			if (l & 1) h ^= d[0];
			if (l & 2) h ^= *(u16*)(d+(l&1)) << ((l&1) * 8);
			h *= KNUTH_MULT;
			h = (h << 13) | (h >> 51);
		} else {
			h = (h ^ f->str.data.u64) * KNUTH_MULT;
			h = (h << 13) | (h >> 51);
		}
	}
	h = (h ^ f->str.prefix2) * KNUTH_MULT;
	h = (h << 13) | (h >> 51);
	h = (h ^ f->str.prefix1) * KNUTH_MULT;
	return h;
}

u32
gen_hash_node(const struct index_node *n, struct index_conf *ic)
{
	u64 h = 0xbada5515bad;
	int c = ic->cardinality;

	if (c == 1) {
		switch (ic->field[0].type) {
		case SNUM32:
		case UNUM32:
			return n->key.u32;
		case SNUM64:
		case UNUM64:
			return (u32)(((u64)n->key.u64 * KNUTH_MULT) >> 32);
		case STRING:
			return lstr_hash(&n->key, h) >> 32;
		default:
			abort();
		}
	}

	for (int i = 0; i < c; ++i) {
		union index_field *key = (void *)&n->key + ic->field[i].offset;
		switch(ic->field[i].type) {
		case SNUM16:
		case UNUM16:
			h = (h ^ key->u16) * KNUTH_MULT;
			break;
		case SNUM32:
		case UNUM32:
			h = (h ^ key->u32) * KNUTH_MULT;
			break;
		case SNUM64:
		case UNUM64:
			h = (h ^ key->u64) * KNUTH_MULT;
			break;
		case STRING:
			h = lstr_hash(&n->key, h);
			break;
		case UNDEF:
			abort();
		}
		h = (h << 13) | (h >> 51);
	}
	h *= KNUTH_MULT;
	return (u32)(h >> 32);
}

void
gen_set_field(union index_field *f, enum index_field_type type, int len, const void *data)
{
	switch (type) {
	case UNUM16:
	case SNUM16:
		if (len != sizeof(u16))
			index_raise("key size mismatch, expected u16");
		f->u16 = *(u16 *)data;
		return;
	case UNUM32:
	case SNUM32:
		if (len != sizeof(u32))
			index_raise("key size mismatch, expected u32");
		f->u32 = *(u32 *)data;
		return;
	case UNUM64:
	case SNUM64:
		if (len != sizeof(u64))
			index_raise("key size mismatch, expected u64");
		f->u64 = *(u64 *)data;
		return;
	case STRING:
		if (len > 0xffff)
			index_raise("string key too long");
		set_lstr_field(f, len, data);
		return;
	case UNDEF:
		abort();
	}
	abort();
}

void
gen_init_pattern(struct tbuf *key_data, int cardinality, struct index_node *pattern_, void *arg)
{
	struct index_node *pattern = (void *)pattern_;
	struct index_conf *ic = arg;

	if (cardinality > ic->cardinality || cardinality > nelem(ic->field))
                index_raise("cardinality too big");

	for (int i = 0; i < cardinality; i++) {
		u32 len = read_varint32(key_data);
		void *key = read_bytes(key_data, len);

		union index_field *f = (void *)&pattern->key + ic->field[i].offset;
		gen_set_field(f, ic->field[i].type, len, key);
	}

	pattern->obj = (void *)(uintptr_t)cardinality;
}

void
index_conf_validate(struct index_conf *d)
{
	if (d->n > 8)
		index_raise("index_conf.n is too big");
	if (d->cardinality == 0)
		index_raise("index_conf.cardinality is 0");
	if (d->cardinality > nelem(d->field))
		index_raise("index_conf.cardinality is too big");
	if (d->type < 0 || d->type > COMPACTTREE)
		index_raise("index_conf.type is invalid");
	if (d->unique > 1)
		index_raise("index_conf.unique is not bool");

	for (int k = 0; k < d->cardinality; k++) {
		d->fill_order[k] = k;
		if (d->field[k].sort_order != ASC && d->field[k].sort_order != DESC)
			index_raise("index_conf.field[_].sort_order is invalid");
		if (d->field[k].type < UNUM16 || d->field[k].type > STRING)
			index_raise("index_conf.field[_].type is invalid");
		if (d->field[k].index + 1 > d->min_tuple_cardinality)
			d->min_tuple_cardinality = d->field[k].index + 1;
	}

	for (int i = d->cardinality-1; i > 0; i--)
		for (int j = 0; j < i; j++) {
			int inda = d->fill_order[j];
			int indb = d->fill_order[j+1];

			if (d->field[inda].index > d->field[indb].index) {
				d->fill_order[j+1] = inda;
				d->fill_order[j] = indb;
			} else if (d->field[inda].index == d->field[indb].index) {
				index_raise("index_conf.field[_].index is duplicate");
			}
		}
}

void
index_conf_read(struct tbuf *data, struct index_conf *c)
{
	char version = read_i8(data);
	if (version != 1)
		index_raise("index_conf bad version");

	c->min_tuple_cardinality = read_u8(data);
	c->cardinality = read_u8(data);
	c->type = read_i8(data);
	c->unique = read_u8(data);

	if (c->cardinality > nelem(c->field))
		index_raise("index_conf.cardinality is too big");

	for (int i = 0; i < c->cardinality; i++) {
		c->field[i].index = read_u8(data);
		c->field[i].sort_order = read_i8(data);
		c->field[i].type = read_i8(data);
	}
}

void
index_conf_print(struct tbuf *out, const struct index_conf *c)
{
	tbuf_printf(out, "min_tuple_cardinality:%i cardinality:%i type:%i unique:%i",
		    c->min_tuple_cardinality, c->cardinality, c->type, c->unique);
	for (int i = 0; i < c->cardinality; i++)
		tbuf_printf(out, " field%i:{index:%i type:%i sort:%i}", i,
			    c->field[i].index, c->field[i].type, c->field[i].sort_order);
}

void
index_conf_write(struct tbuf *data, struct index_conf *c)
{
	char version = 1;
	write_i8(data, version);

	write_i8(data, c->min_tuple_cardinality);
	write_i8(data, c->cardinality);
	write_i8(data, c->type);
	write_i8(data, c->unique);

	for (int i = 0; i < c->cardinality; i++) {
		write_i8(data, c->field[i].index);
		write_i8(data, c->field[i].sort_order);
		write_i8(data, c->field[i].type);
	}
}


register_source();
