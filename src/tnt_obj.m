/*
 * Copyright (C) 2011, 2012 Mail.RU
 * Copyright (C) 2011, 2012 Yuriy Vostrikov
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
#import <octopus.h>
#import <say.h>
#import <net_io.h>
#import <iproto.h>
#import <util.h>

#include <stdint.h>

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>


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

void
object_incr_ref(struct tnt_object *obj)
{
	assert(obj->refs + 1 > 0);
	obj->refs++;
}

void
object_decr_ref(struct tnt_object *obj)
{
	assert(obj->refs - 1 >= 0);
	obj->refs--;

	if (obj->refs == 0)
		sfree(obj);
}

void
object_lock(struct tnt_object *obj)
{
	if (obj->flags & WAL_WAIT)
		iproto_raise(ERR_CODE_NODE_IS_RO, "object is locked");

	say_debug("object_lock(%p)", obj);
	obj->flags |= WAL_WAIT;
}

void
object_unlock(struct tnt_object *obj)
{
	assert(obj->flags & WAL_WAIT);

	say_debug("object_unlock(%p)", obj);
	obj->flags &= ~WAL_WAIT;
}

const char *objectlib_name = "Octopus.object"; /* there is a copy of this string
						  in src/net2.lua */

void
luaT_pushobject(struct lua_State *L, struct tnt_object *obj)
{
	void **ptr = lua_newuserdata(L, sizeof(void *));
	luaL_getmetatable(L, objectlib_name);
	lua_setmetatable(L, -2);
	*ptr = obj;
	object_incr_ref(obj);
}

static int
object_gc_(struct lua_State *L)
{
	struct tnt_object *obj = *(void **)luaL_checkudata(L, 1, objectlib_name);
	object_decr_ref(obj);
	return 0;
}

static const struct luaL_reg object_mt [] = {
	{"__gc", object_gc_},
	{NULL, NULL}
};

int
luaT_objinit(struct lua_State *L)
{
	luaL_newmetatable(L, objectlib_name);
	luaL_register(L, NULL, object_mt);
	lua_pop(L, 1);
	return 0;
}

register_source();
