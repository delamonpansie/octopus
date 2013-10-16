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

const char *indexlib_name = "Tarantool.index";

#import <util.h>
#import <say.h>
#import <fiber.h>
#import <index.h>
#import <pickle.h>
#import <octopus.h>

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>


struct tbuf *
luaT_i32_ctor(struct lua_State *L, int i)
{
	struct tbuf *key = tbuf_alloc(fiber->pool);
	if (lua_isnumber(L, i) || lua_isstring(L, i)) {
		char len = 4;
		tbuf_append(key, &len, 1); /* varint repr of 4 */
		i32 val = lua_tointeger(L, i);
		tbuf_append(key, &val, len);
	} else {
		lua_pushliteral(L, "can't convert to u32");
		lua_error(L);
	}
	return key;
}

struct tbuf *
luaT_i64_ctor(struct lua_State *L, int i)
{
	struct tbuf *key = tbuf_alloc(fiber->pool);
	const  u64 *ptr;

	char len = 8;
	tbuf_append(key, &len, 1); /* varint repr of 8 */

	if (lua_isnumber(L, i) || lua_isstring(L, i)) {
		i64 val = lua_tointeger(L, i); /* may overflow */
		tbuf_append(key, &val, len);
	} else if ((ptr = lua_topointer(L, i))) { /* cdata */
		tbuf_append(key, ptr, 8);
	} else {
		lua_pushliteral(L, "can't convert to u64");
		lua_error(L);
	}
	return key;
}


struct tbuf *
luaT_lstr_ctor(struct lua_State *L, int i)
{
	struct tbuf *key = tbuf_alloc(fiber->pool);
	size_t len;
	const char *str = lua_tolstring(L, i, &len);
	if (str == NULL) {
		lua_pushliteral(L, "can't convert to string");
		lua_error(L);
	}
	write_varint32(key, len);
	tbuf_append(key, str, len);
	return key;
}

struct tbuf *
luaT_cstr_ctor(struct lua_State *L, int i)
{
	struct tbuf *key = tbuf_alloc(fiber->pool);
	size_t len;
	const char *str = lua_tolstring(L, i, &len);
	if (str == NULL) {
		lua_pushliteral(L, "can't convert to string");
		lua_error(L);
	}
	tbuf_append(key, str, len);
	return key;
}


void
luaT_pushindex(struct lua_State *L, Index *index)
{
	void **ptr = lua_newuserdata(L, sizeof(void *));
	luaL_getmetatable(L, indexlib_name);
	lua_setmetatable(L, -2);
	*ptr = index;
}

static Index<BasicIndex> *
luaT_checkindex(struct lua_State *L, int i)
{
	return *(Index<BasicIndex> **)luaL_checkudata(L, i, indexlib_name);
}

static Index<HashIndex> *
luaT_checkindex_hash(struct lua_State *L, int i)
{
	Index *index = luaT_checkindex(L, i);
	if (index->conf.type != HASH) {
		lua_pushliteral(L, "index type must be hash");
		lua_error(L);
	}

	return (Index<HashIndex> *)index;
}

void
luaT_pushobject(struct lua_State *L, struct tnt_object *obj);
static int
luaT_index_hashget(struct lua_State *L)
{
	id<HashIndex> index = luaT_checkindex_hash(L, 1);
	u32 i = luaL_checkinteger(L, 2);
	struct tnt_object *obj = [index get:i];
	if (obj != NULL && !ghost(obj)) {
		luaT_pushobject(L, obj);
		return 1;
	}
	return 0;
}

static int
luaT_index_hashsize(struct lua_State *L)
{
	id<HashIndex> index = luaT_checkindex_hash(L, 1);
	lua_pushinteger(L, [index slots]);
	return 1;
}

static int
luaT_index_index(struct lua_State *L)
{
	Index<BasicIndex> *index = luaT_checkindex(L, 1);
	struct tnt_object *obj;
	struct tbuf *key;

	if (index->lua_ctor == NULL) {
		lua_pushliteral(L, "not implemented");
		lua_error(L);
	}

	key = index->lua_ctor(L, 2);
	obj = [index find_key:key with_cardinalty:1];
	if (obj != NULL && !ghost(obj)) {
		luaT_pushobject(L, obj);
		return 1;
	}
	return 0;
}

static int
luaT_iter_next(struct lua_State *L)
{
	Index<BasicIndex> *index = luaT_checkindex(L, 1);

	struct tnt_object *obj = [index iterator_next];
	if (obj != NULL && !ghost(obj)) {
		luaT_pushobject(L, obj);
		return 1;
	}
	return 0;
}

static int
luaT_index_iter(struct lua_State *L)
{
	Index<BasicIndex> *index = luaT_checkindex(L, 1);

	if ([index cardinality] != 1) {
		lua_pushliteral(L, "multi column indexes unsupported");
		lua_error(L);
	}

	if (lua_isnumber(L, 2) || lua_isstring(L, 2)) {
		struct tbuf *key = index->lua_ctor(L, 2);
		[index iterator_init:key with_cardinalty:1];
	} else if (lua_isnil(L, 2) || lua_isnone(L, 2)) {
		[index iterator_init];
	} else if (lua_isuserdata(L, 2)) {
		struct tnt_object *obj = *(void **)luaL_checkudata(L, 2, objectlib_name);
		[index iterator_init_with_object:obj];
	} else {
		lua_pushliteral(L, "wrong key type");
		lua_error(L);
	}

	lua_pushcfunction(L, luaT_iter_next);
	lua_pushvalue(L, 1);
	return 2;
}


static const struct luaL_reg indexlib_m [] = {
	{"__index", luaT_index_index},
	{NULL, NULL}
};

static const struct luaL_reg indexlib [] = {
	{"hashget", luaT_index_hashget},
	{"hashsize", luaT_index_hashsize},
	{"iter", luaT_index_iter},
	{NULL, NULL}
};


int
luaT_indexinit(struct lua_State *L)
{
	luaL_newmetatable(L, indexlib_name);
	luaL_register(L, NULL, indexlib_m);
	lua_pop(L, 1);

	luaL_findtable(L, LUA_GLOBALSINDEX, "index", 0);
	luaL_register(L, NULL, indexlib);
	lua_pop(L, 1);

	lua_getglobal(L, "require");
	lua_pushliteral(L, "index");
	if (lua_pcall(L, 1, 0, 0) != 0)
		panic("luaT_index_index: %s", lua_tostring(L, -1));

	return 0;
}
