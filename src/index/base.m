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

#import <say.h>
#import <tbuf.h>
#import <fiber.h>
#import <pickle.h>
#import <index.h>

@implementation IndexError
@end

@implementation Index

+ (Index *)
new_conf:(struct index_conf *)ic dtor:(const struct dtor_conf *)dc
{
	Index *i;
	if (ic->cardinality == 1) {
		if (ic->type == HASH && ic->unique == false)
			return nil;

		switch (ic->field_type[0]) {
		case NUM32:
			i = ic->type == HASH ? [Int32Hash alloc] : [Int32Tree alloc];
			i->dtor = dc->u32;
			break;
		case NUM64:
			i = ic->type == HASH ? [Int64Hash alloc] : [Int64Tree alloc];
			i->dtor = dc->u64;
			break;
		case STRING:
			i = ic->type == HASH ? [LStringHash alloc] : [StringTree alloc];
			i->dtor = dc->lstr;
			break;
		case NUM16:
			panic("NUM16 single column indexes unupported");
		default:
			abort();
		}

		i->dtor_arg = (void *)(uintptr_t)ic->field_index[0];
	} else {
		assert(ic->type == TREE);
		i = [GenTree alloc];
		i->dtor = dc->generic;
		i->dtor_arg = (void *)&i->conf;
	}

	return [i init:ic];
}

- (Index *)
init:(struct index_conf *)ic
{
	[super init];
	if (ic != NULL)
		memcpy(&conf, ic, sizeof(*ic));
	return self;
}

- (void)
valid_object:(struct tnt_object*)obj
{
	dtor(obj, &node_a, dtor_arg);
}

- (u32)
cardinality
{
	return 1;
}

- (int)
eq:(struct tnt_object *)obj_a :(struct tnt_object *)obj_b
{
	dtor(obj_a, &node_a, dtor_arg);
	dtor(obj_b, &node_b, dtor_arg);
	return memcmp(&node_a.key, &node_b.key, node_size - sizeof(struct tnt_object *)) == 0;
}

@end

void __attribute__((noreturn)) oct_cold
index_raise_(const char *file, int line, const char *msg)
{
	@throw [[IndexError palloc] init_line:line
					 file:file
				    backtrace:NULL
				       reason:msg];
}
