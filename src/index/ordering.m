/*
 * Copyright (C) 2012, 2013 Mail.RU
 * Copyright (C) 2012, 2013 Yuriy Vostrikov
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
lstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
	return llexstrcmp(na->key.ptr, nb->key.ptr);
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
cstr_compare(const struct index_node *na, const struct index_node *nb, void *x __attribute__((unused)))
{
        return strcmp(na->key.ptr, nb->key.ptr);
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
