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
i32_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	i32 a = na->key.u32, b = nb->key.u32;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;
	else
		return 0;
}

int
i32_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i32_compare(nb, na, x);
}

int
i32_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	i32 a = na->key.u32, b = nb->key.u32;
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
i32_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i32_compare_with_addr(nb, na, x);
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
			index_raise("key is not u32");
		pattern->key.u32 = read_u32(key);
		break;
	default:
		index_raise("cardinality too big");
	}
}

int
i64_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	i64 a = na->key.u64, b = nb->key.u64;
	if (a > b)
		return 1;
	else if (a < b)
		return -1;
	else
		return 0;
}

int
i64_compare_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i64_compare(nb, na, x);
}

int
i64_compare_with_addr(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	i64 a = na->key.u64, b = nb->key.u64;
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
i64_compare_with_addr_desc(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return i64_compare_with_addr(nb, na, x);
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
			index_raise("key is not u64");
		pattern->key.u64 = read_u64(key);
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

static int
field_compare(union index_field *f1, union index_field *f2, enum index_field_type type)
{
	const void *d1, *d2;
	int r;

	switch (type) {
	case NUM16:
		return f1->u16 > f2->u16 ? 1 : f1->u16 == f2->u16 ? 0 : -1;
	case NUM32:
		return f1->u32 > f2->u32 ? 1 : f1->u32 == f2->u32 ? 0 : -1;
	case NUM64:
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
		int j = ic->cmp_order[i];
		union index_field *akey = (void *)&na->key + ic->offset[j];
		union index_field *bkey = (void *)&nb->key + ic->offset[j];
		int r = field_compare(akey, bkey, ic->field_type[j]);
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

void
gen_set_field(union index_field *f, enum index_field_type type, int len, const void *data)
{
	switch (type) {
	case NUM16:
		if (len != sizeof(u16))
			index_raise("key size mismatch, expected u16");
		f->u16 = *(u16 *)data;
		return;
	case NUM32:
		if (len != sizeof(u32))
			index_raise("key size mismatch, expected u32");
		f->u32 = *(u32 *)data;
		return;
	case NUM64:
		if (len != sizeof(u64))
			index_raise("key size mismatch, expected u64");
		f->u64 = *(u64 *)data;
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
		int j = ic->cmp_order[i];

		union index_field *f = (void *)&pattern->key + ic->offset[j];
		gen_set_field(f, ic->field_type[j], len, key);
		key += len;
	}

	pattern->obj = (void *)(uintptr_t)cardinality;
}
