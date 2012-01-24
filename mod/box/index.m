/*
 * Copyright (C) 2010, 2011 Mail.RU
 * Copyright (C) 2010, 2011 Yuriy Vostrikov
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

#include <config.h>
#include <stdbool.h>

#import <say.h>
#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>

#import <mod/box/box.h>
#import <mod/box/index.h>
#import <cfg/tarantool_cfg.h>


@implementation Index
- (void)
valid_object:(struct tnt_object*)obj
{
	/* FIXME: caching */
	dtor(obj, &node, dtor_arg);
}
@end

@implementation DummyIndex
- (id)
init_with_index:(Index *)_index
{
	[super init];
	index = _index;
	n = index->n;
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

- (void)
iterator_init
{
}

- (void)
iterator_init:(struct tbuf *)key_data with_cardinalty:(u32)cardinality
{
        (void)key_data;
	(void)cardinality;
}

- (struct tnt_object *)
iterator_next
{
	return NULL;
}

- (void)
replace:(struct tnt_object *)obj
{
	(void)obj;
}

- (void)
remove:(struct tnt_object *)obj
{
	(void)obj;
}

- (struct tnt_object *)
find_key:(struct tbuf *)key_data with_cardinalty:(u32)cardinality
{
	(void)key_data;
	(void)cardinality;
	return NULL;
}

- (struct tnt_object *)
find_by_obj:(struct tnt_object *)obj
{
	(void)obj;
	return NULL;
}
@end


void
validate_indexes(struct box_txn *txn)
{
	foreach_index(index, txn->object_space) {
                [index valid_object:txn->obj];

		if (index->unique) {
                        struct tnt_object *obj = [index find_by_obj:txn->obj];

                        if (obj != NULL && obj != txn->old_obj)
                                box_raise(ERR_CODE_INDEX_VIOLATION, "unique index violation");
                }
	}
}
