/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

#import <util.h>
#include <stdbool.h>

#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>
#import <index.h>

@implementation DummyIndex
- (id)
init_with_index:(Index *)_index
{
	[super init];
	index = _index;
	memcpy(&conf, &_index->conf, sizeof(conf));
	dtor = index->dtor;
	dtor_arg = index->dtor_arg;
	return self;
}

- (bool)
is_wrapper_of:(Class)some_class
{
	return [index isKindOf:some_class];
}

- (id)
unwrap
{
	Index *i = index;
	[self free];
	return i;
}

- (int)
eq:(struct tnt_object *)a :(struct tnt_object *)b
{
	(void)a; (void)b;
	return 0;
}

- (void)
iterator_init
{
}

- (void)
iterator_init_with_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	(void)key_data; (void)cardinality;
}

- (void)
iterator_init_with_object:(struct tnt_object *)obj
{
	(void)obj;
}

- (void)
iterator_init_with_node:(const struct index_node *)node
{
	(void)node;
}

- (struct tnt_object *)
iterator_next
{
	return NULL;
}

- (struct tnt_object *)
iterator_next_verify_pattern
{
	return NULL;
}

- (u32)
size
{
	return 0;
}

- (u32)
slots
{
	return 0;
}

- (size_t)
bytes
{
	return 0;
}

- (void)
replace:(struct tnt_object *)obj
{
	(void)obj;
}

- (int)
remove:(struct tnt_object *)obj
{
	(void)obj;
	/* During initial loading all Tree indexes are DummyIndex.
	   This trick speedup initial loading.
	   Because of assertion check in box.m:obj_remove()
	   We have to pretend that we're deleted one row */
	return 1;
}

- (struct tnt_object *)
find:(const char *)key
{
	(void)key;
	return NULL;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data cardinalty:(u32)cardinality
{
	(void)key_data;
	(void)cardinality;
	return NULL;
}

- (struct tnt_object *)
find_obj:(struct tnt_object *)obj
{
	(void)obj;
	return NULL;
}

- (struct tnt_object *)
find_node:(const struct index_node *)node
{
	(void)node;
	return NULL;
}
- (void)
clear
{
}
@end
