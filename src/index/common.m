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
u32_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u32_compare(nb, na, x);
}

int
u32_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;

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
u32_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u32_compare_with_addr(nb, na, x);
}

int
u32_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	return a == b;
}

int
u32_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u32 a = na->key.u32, b = nb->key.u32;
	return a == b && na->obj == nb->obj;
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
	case 0: pattern->key.u32 = 0;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u32))
			index_raise("key is not i32");
		pattern->key.u32 = read_u32(key) - INT32_MIN;
		break;
	default:
		index_raise("cardinality too big");
	}
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

int
u64_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u64_compare(nb, na, x);
}

int
u64_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;

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
u64_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return u64_compare_with_addr(nb, na, x);
}

int
u64_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	return a == b;
}

int
u64_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	u64 a = na->key.u64, b = nb->key.u64;
	return a == b && na->obj == nb->obj;
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
	case 0: pattern->key.u64 = 0;
		break;
	case 1: len = read_varint32(key);
		if (len != sizeof(u64))
			index_raise("key is not i64");
		pattern->key.u64 = read_u64(key) - INT64_MIN;
		break;
	default:
		index_raise("cardinality too big");
	}
}

int
lstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return llexstrcmp(na->key.ptr, nb->key.ptr);
}

int
lstr_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return lstr_compare(nb, na, x);
}

int
lstr_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{

	int r = llexstrcmp(na->key.ptr, nb->key.ptr);
	if (r != 0)
		return r;

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
lstr_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return lstr_compare_with_addr(nb, na, x);
}

int
lstr_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	const u8 *a = na->key.ptr, *b = nb->key.ptr;
	int al, bl;

	al = LOAD_VARINT32(a);
	bl = LOAD_VARINT32(b);

	return al == bl && memcmp(a, b, al) == 0;
}

int
lstr_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return na->obj == nb->obj && lstr_eq(na, nb, x);
}

void
lstr_init_pattern(struct tbuf *key, int cardinality,
		 struct index_node *pattern, void *x __attribute__((unused)))
{
	pattern->obj = NULL;
	static u8 empty[] = {0};
	void *f;

	if (cardinality == 1)
		f = read_field(key);
	else if (cardinality == 0)
		f = &empty;
	else
		index_raise("cardinality too big");

	pattern->key.ptr = f;
}

int
cstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return strcmp(na->key.ptr, nb->key.ptr);
}

int
cstr_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return cstr_compare(nb, na, x);
}

int
cstr_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        int r = strcmp(na->key.ptr, nb->key.ptr);
	if (r != 0)
		return r;

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
cstr_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return cstr_compare_with_addr(nb, na, x);
}

int
cstr_eq(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return strcmp(na->key.ptr, nb->key.ptr) == 0;
}

int
cstr_eq_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return strcmp(na->key.ptr, nb->key.ptr) == 0 && na->obj == nb->obj;
}

static int
field_compare(union index_field *f1, union index_field *f2, enum index_field_type type)
{
	const void *d1, *d2;
	int r;

	switch (type & ~SIGNFLAG) {
	case UNUM16:
		return f1->u16 > f2->u16 ? 1 : f1->u16 == f2->u16 ? 0 : -1;
	case UNUM32:
		return f1->u32 > f2->u32 ? 1 : f1->u32 == f2->u32 ? 0 : -1;
	case UNUM64:
		return f1->u64 > f2->u64 ? 1 : f1->u64 == f2->u64 ? 0 : -1;
	case STRING:
		d1 = f1->str.len <= sizeof(f1->str.data) ? f1->str.data.bytes : f1->str.data.ptr;
		d2 = f2->str.len <= sizeof(f2->str.data) ? f2->str.data.bytes : f2->str.data.ptr;
		r = memcmp(d1, d2, MIN(f1->str.len, f2->str.len));
		if (r != 0)
			return r;

		return f1->str.len > f2->str.len ? 1 : f1->str.len == f2->str.len ? 0 : -1;
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

	int n = (uintptr_t)na->obj < nelem(ic->field_index) ? (uintptr_t)na->obj : ic->cardinality;

	for (int i = 0; i < n; ++i) {
		union index_field *akey = (void *)&na->key + ic->offset[i];
		union index_field *bkey = (void *)&nb->key + ic->offset[i];
		int r = field_compare(akey, bkey, ic->field_type[i]);
		if (r != 0)
			return r * ic->sort_order[i];
	}
	return 0;
}

int
tree_node_compare_with_addr(struct index_node *na, struct index_node *nb, struct index_conf *ic)
{
	int r = tree_node_compare(na, nb, ic);
	if (r != 0)
		return r;

	if ((uintptr_t)na->obj < nelem(ic->field_index)) /* `na' is a pattern */
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
	const void *d1, *d2;

	switch (type & ~SIGNFLAG) {
	case UNUM16:
		return f1->u16 == f2->u16;
	case UNUM32:
		return f1->u32 == f2->u32;
	case UNUM64:
		return f1->u64 == f2->u64;
	case STRING:
		d1 = f1->str.len <= sizeof(f1->str.data) ? f1->str.data.bytes : f1->str.data.ptr;
		d2 = f2->str.len <= sizeof(f2->str.data) ? f2->str.data.bytes : f2->str.data.ptr;
		return d1 == d2 && memcmp(d1, d2, MIN(f1->str.len, f2->str.len)) == 0;
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

	int n = (uintptr_t)na->obj < nelem(ic->field_index) ? (uintptr_t)na->obj : ic->cardinality;

	for (int i = 0; i < n; ++i) {
		union index_field *akey = (void *)&na->key + ic->offset[i];
		union index_field *bkey = (void *)&nb->key + ic->offset[i];
		int r = field_eq(akey, bkey, ic->field_type[i]);
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
static u32
dumb_hash(const char *d, u32 l, u64 h)
{
	while (l >= 4) {
		u32 k = *(u32*)d;
		h = (h ^ k) * KNUTH_MULT;
		h = (h << 13) | (h >> 51);
		d += 4;
		l -= 4;
	}
	if (l & 1) h ^= d[0];
	if (l & 2) h ^= *(u16*)(d+(l&1)) << ((l&1) * 8);
	h ^= l << 24;
	h *= KNUTH_MULT;
	return h;
}

u32
gen_hash_node(const struct index_node *n, struct index_conf *ic)
{
	u64 h = 0x33;
	int c = ic->cardinality;

	if (c == 1) {
		switch (ic->field_type[0] & ~SIGNFLAG) {
		case UNUM32:
			return n->key.u32;
		case UNUM64:
			return (u32)(((u64)n->key.u64 * KNUTH_MULT) >> 32);
		case STRING: {
			const void *k = n->key.ptr;
			u32 l = LOAD_VARINT32(k);
			h = dumb_hash(k, l, h);
			h ^= h >> 33;
			h *= KNUTH_MULT;
			return (u32)(h >> 32);
		     }
		}
	}

	for (int i = 0; i < c; ++i) {
		char const *d;
		u32 len;
		union index_field *key = (void *)&n->key + ic->offset[i];
		switch(ic->field_type[i] & ~SIGNFLAG) {
		case UNUM16:
			h = (h ^ key->u16) * KNUTH_MULT;
			break;
		case UNUM32:
			h = (h ^ key->u32) * KNUTH_MULT;
			break;
		case UNUM64:
			h = (h ^ key->u64) * KNUTH_MULT;
			break;
		case STRING:
			d = key->str.len <= sizeof(key->str.data) ? key->str.data.bytes : key->str.data.ptr;
			len = key->str.len;
			h = dumb_hash(d, len, h);
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
		if (len != sizeof(u16))
			index_raise("key size mismatch, expected u16");
		f->u16 = *(u16 *)data;
		return;
	case SNUM16:
		if (len != sizeof(i16))
			index_raise("key size mismatch, expected i16");
		f->u16 = *(u16 *)data - INT16_MIN;
		return;
	case UNUM32:
		if (len != sizeof(u32))
			index_raise("key size mismatch, expected u32");
		f->u32 = *(u32 *)data;
		return;
	case SNUM32:
		if (len != sizeof(i32))
			index_raise("key size mismatch, expected i32");
		f->u32 = *(u32 *)data - INT32_MIN;
		return;
	case UNUM64:
		if (len != sizeof(u64))
			index_raise("key size mismatch, expected u64");
		f->u64 = *(u64 *)data;
		return;
	case SNUM64:
		if (len != sizeof(i64))
			index_raise("key size mismatch, expected i64");
		f->u64 = *(u64 *)data - INT64_MIN;
		return;
	case STRING:
		if (len > 0xffff)
			index_raise("string key too long");
		f->str.len = len;
		if (len <= sizeof(f->str.data))
			memcpy(f->str.data.bytes, data, len);
		else
			f->str.data.ptr = data;
		return;
	case SIGNFLAG:
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

	if (cardinality > ic->cardinality || cardinality > nelem(ic->field_index))
                index_raise("cardinality too big");

	for (int i = 0; i < cardinality; i++) {
		u32 len = read_varint32(key_data);
		void *key = read_bytes(key_data, len);

		union index_field *f = (void *)&pattern->key + ic->offset[i];
		gen_set_field(f, ic->field_type[i], len, key);
		key += len;
	}

	pattern->obj = (void *)(uintptr_t)cardinality;
}
