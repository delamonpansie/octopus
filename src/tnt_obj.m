/*
 * Copyright (C) 2011 Mail.RU
 * Copyright (C) 2011 Yuriy Vostrikov
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

#import <salloc.h>
#import <tarantool.h>
#import <say.h>
#import <net_io.h>
#import <iproto.h>
#import <util.h>

#include <stdint.h>



struct tnt_object *
object_alloc(u8 type, size_t size)
{
	struct tnt_object *obj = salloc(sizeof(struct tnt_object) + size);

	if (obj == NULL)
		iproto_raise(ERR_CODE_MEMORY_ISSUE, "can't allocate object");

	obj->type = type;
	obj->flags = obj->refs = 0;

	say_debug("object_alloc(%zu) = %p", size, obj);
	return obj;
}


void
object_ref(struct tnt_object *obj, int count)
{
	assert(obj->refs + count >= 0);
	obj->refs += count;

	if (obj->refs == 0)
		sfree(obj);
}
