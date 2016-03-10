/*
 * Copyright (C) 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2011, 2012, 2013, 2014 Yuriy Vostrikov
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

#define OBJECT_FUN_INLINE
#import <salloc.h>
#import <octopus.h>
#import <say.h>
#import <net_io.h>
#import <iproto.h>
#import <util.h>
#import <fiber.h>

#include <stdint.h>

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>


struct tnt_object *
object_alloc(u8 type, int gc, size_t size)
{
	struct tnt_object *obj;
	if (gc) {
		struct gc_oct_object *gcobj = salloc(sizeof(struct gc_oct_object) + size);
		if (gcobj == NULL)
			goto err;
		gcobj->refs = 0;
		obj = &gcobj->obj;
	} else {
		obj = salloc(sizeof(struct tnt_object) + size);
		if (obj == NULL)
			goto err;
	}
	obj->type = type;
	obj->flags = 0;

	say_debug3("object_alloc(%zu) = %p", size, obj);
	return obj;
err:
	iproto_raise(ERR_CODE_MEMORY_ISSUE,
		     salloc_error == ESALLOC_NOCACHE ?
		     "bad object size": "can't allocate object");
}


void
object_ref(struct tnt_object *obj, int count)
{
	struct gc_oct_object *gcobj = container_of(obj, struct gc_oct_object, obj);
	assert(gcobj->refs + count >= 0);
	gcobj->refs += count;

	if (gcobj->refs == 0)
		sfree(gcobj);
}

void
object_incr_ref(struct tnt_object *obj)
{
	assert(obj->type == 1);
	struct gc_oct_object *gcobj = container_of(obj, struct gc_oct_object, obj);
	assert(gcobj->refs + 1 > 0);
	gcobj->refs++;
}

void
object_incr_ref_autorelease(struct tnt_object *obj)
{
	struct gc_oct_object *gcobj = container_of(obj, struct gc_oct_object, obj);
	assert(gcobj->refs + 1 > 0);
	gcobj->refs++;
	autorelease((id)((uintptr_t)obj | 1));
}

void
object_decr_ref(struct tnt_object *obj)
{
	struct gc_oct_object *gcobj = container_of(obj, struct gc_oct_object, obj);
	assert(gcobj->refs - 1 >= 0);
	gcobj->refs--;

	if (gcobj->refs == 0) {
		say_debug3("object_decr_ref(%p) free", gcobj);
		sfree(gcobj);
	}
}

static struct { struct tnt_object *obj; struct Fiber *waiter; } *ow;
static int ows, ows_used;

void
object_yield(struct tnt_object *obj)
{
	if (ows_used == ows) {
		int ows2 = ows ? ows * 2 : 128;
		ow = xrealloc(ow, ows2 * sizeof(*ow));
		memset(ow + ows, 0, sizeof(*ow) * (ows2 - ows));
		ows = ows2;
	}

	ow[ows_used].obj = obj;
	ow[ows_used].waiter = fiber;
	obj->flags |= YIELD;
	ows_used++;
	yield();
	return;
}


void
object_lock(struct tnt_object *obj)
{
	if (obj->flags & WAL_WAIT)
		iproto_raise(ERR_CODE_NODE_IS_RO, "object is locked");

	say_debug2("object_lock(%p)", obj);
	obj->flags |= WAL_WAIT;
}

void
object_unlock(struct tnt_object *obj)
{
	assert(obj->flags & WAL_WAIT);

	say_debug2("object_unlock(%p)", obj);
	obj->flags &= ~WAL_WAIT;

	if (obj->flags & YIELD) {
		int i, j = 0;
		for (i = 0; i < ows_used; i++) {
			if (ow[i].obj != obj) {
				if (j < i) ow[j] = ow[i];
				j++;
				continue;
			}
			obj->flags &= ~YIELD;
			fiber_wake(ow[i].waiter, NULL);
		}
		if (j < ows_used) {
			memset(ow+j, 0, sizeof(*ow) * (ows_used - j));
			ows_used = j;
		}
	}
}


register_source();
