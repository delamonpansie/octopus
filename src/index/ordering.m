/*
 * Copyright (C) 2012 Mail.RU
 * Copyright (C) 2012 Yuriy Vostrikov
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

int
i32_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i32 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;
	else
		return 0;
}

int
i32_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i32 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

int
i64_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i64 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;
	else
		return 0;
}

int
i64_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	i64 *a = (void *)na->key, *b = (void *)nb->key;
	if (*a > *b)
		return 1;
	else if (*a < *b)
		return -1;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}

int
lstr_compare(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{
	return lstrcmp(*(void **)na->key, *(void **)nb->key);
}

int
lstr_compare_with_addr(struct index_node *na, struct index_node *nb, void *x __attribute__((unused)))
{

	int r = lstrcmp(*(void **)na->key, *(void **)nb->key);
	if (r != 0)
		return r;

	if (na->obj > nb->obj)
		return 1;
	else if (na->obj < nb->obj)
		return -1;
	else
		return 0;
}
