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
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>

#import <fiber.h>
#import <say.h>
#import <pickle.h>
#import <assoc.h>
#import <net_io.h>
#import <index.h>

#import <mod/box/box.h>
#import <mod/box/moonbox.h>

const char *objectlib_name = "Tarantool.box.object";


static void
luaT_dumpstack(const char *prefix, lua_State *L)
{
	say_crit("%s", prefix);
	for (int i = 1; i <= lua_gettop(L); i++)
		say_crit("  [%i] = %s", i, luaL_typename(L, i));
}

void
luaL_checktable(lua_State *L, int i, const char *metatable)
{
	luaL_checktype(L, i, LUA_TTABLE);
	if (!lua_getmetatable(L, i))
		luaL_error(L, "no metatable #%i", i);
	luaL_getmetatable(L, metatable);
	if (!lua_equal(L, -1, -2)) {
		luaT_dumpstack("checktable", L);
		luaL_argerror(L, i, metatable);
	}
	lua_pop(L, 2);
}

void
luaT_pushobject(struct lua_State *L, struct tnt_object *obj)
{
	void **ptr = lua_newuserdata(L, sizeof(void *));
	luaL_getmetatable(L, objectlib_name);
	lua_setmetatable(L, -2);
	*ptr = obj;
	object_ref(obj, 1);
}

static int
object_gc_(struct lua_State *L)
{
	struct tnt_object *obj = *(void **)luaL_checkudata(L, 1, objectlib_name);
	object_ref(obj, -1);
	return 0;
}

static int
tuple_len_(struct lua_State *L)
{
	struct tnt_object *obj = *(void **)luaL_checkudata(L, 1, objectlib_name);
	struct box_tuple *tuple = box_tuple(obj);
	lua_pushnumber(L, tuple->cardinality);
	return 1;
}

static int
tuple_index_(struct lua_State *L)
{
	struct tnt_object *obj = *(void **)luaL_checkudata(L, 1, objectlib_name);
	struct box_tuple *tuple = box_tuple(obj);

	int i = luaL_checkint(L, 2);
	if (i >= tuple->cardinality) {
		lua_pushliteral(L, "index too small");
		lua_error(L);
	}

	void *field = tuple_field(tuple, i);
	u32 len = load_varint32(&field);
	lua_pushlstring(L, field, len);
	return 1;
}

#if 0
struct box_tuple *
luaT_toboxtuple(struct lua_State *L, int table)
{
	luaL_checktype(L, table, LUA_TTABLE);

	u32 bsize = 0, cardinality = lua_objlen(L, table);

	for (int i = 0; i < cardinality; i++) {
		lua_rawgeti(L, table, i + 1);
		u32 len = lua_objlen(L, -1);
		lua_pop(L, 1);
		bsize += varint32_sizeof(len) + len;
	}

	struct box_tuple *tuple = tuple_alloc(bsize);
	tuple->cardinality = cardinality;

	u8 *p = tuple->data;
	for (int i = 0; i < cardinality; i++) {
		lua_rawgeti(L, table, i + 1);
		size_t len;
		const char *str = lua_tolstring(L, -1, &len);
		lua_pop(L, 1);

		p = save_varint32(p, len);
		memcpy(p, str, len);
		p += len;
	}

	return tuple;
}
#endif

static const struct luaL_reg tuple_mt [] = {
	{"__len", tuple_len_},
	{"__index", tuple_index_},
	{"__gc", object_gc_},
	{NULL, NULL}
};


const char *netmsg_metaname = "Tarantool.netmsg";
const char *netmsgpromise_metaname = "Tarantool.netmsg.promise";

static int
luaT_pushnetmsg(struct lua_State *L)
{
	struct netmsg_tailq *q = lua_newuserdata(L, sizeof(struct netmsg_tailq));
	luaL_getmetatable(L, netmsg_metaname);
	lua_setmetatable(L, -2);

	TAILQ_INIT(q);
	say_debug("create lua netmsg q:%p", q);
	return 1;
}

static struct netmsg_tailq *
luaT_checknetmsg(struct lua_State *L, int i)
{
	return luaL_checkudata(L, i, netmsg_metaname);
}

static int
netmsg_gc(struct lua_State *L)
{
	struct netmsg_tailq *q = luaT_checknetmsg(L, 1);
	struct netmsg *m, *tmp;

	TAILQ_FOREACH_SAFE(m, q, link, tmp)
		netmsg_release(m);
	return 0;
}

static int
netmsg_add_iov(struct lua_State *L)
{
	struct netmsg_tailq *q = luaT_checknetmsg(L, 1);
	struct netmsg *m = netmsg_tail(q, NULL);

	switch (lua_type (L, 2)) {
	case LUA_TNIL:
		lua_createtable(L, 2, 0);
		luaL_getmetatable(L, netmsgpromise_metaname);
		lua_setmetatable(L, -2);

		struct iovec *v = net_reserve_iov(&m);
		lua_pushlightuserdata(L, v);
		lua_rawseti(L, -2, 1);

		lua_pushvalue(L, 1);
		lua_rawseti(L, -2, 2);
		return 1;

	case LUA_TSTRING:
		net_add_lua_iov(&m, L, 2);
		return 0;

	case LUA_TUSERDATA: {
		struct tnt_object *obj = *(void **)luaL_checkudata(L, 2, objectlib_name);
		tuple_add_iov(&m, obj);
		return 0;
	}
	default:
		return luaL_argerror(L, 2, "expected nil, string or tuple");
	}
}


static int
netmsg_fixup_promise(struct lua_State *L)
{
	luaL_checktable(L, 1, netmsgpromise_metaname);
	lua_rawgeti(L, 1, 1);
	struct iovec *v = lua_touserdata(L, -1);
	v->iov_base = (char *) luaL_checklstring(L, 2, &v->iov_len);
	return 0;
}

static const struct luaL_reg netmsg_lib [] = {
	{"alloc", luaT_pushnetmsg},
	{"add_iov", netmsg_add_iov},
	{"fixup_promise", netmsg_fixup_promise},
	{NULL, NULL}
};

static const struct luaL_reg netmsg_mt [] = {
	{"__gc", netmsg_gc},
	{NULL, NULL}
};


const char *indexlib_name = "Tarantool.box.index";

static void
luaT_pushindex(struct lua_State *L, int obj_space, int idx)
{
	int *ptr = lua_newuserdata(L, sizeof(int) * 2);
	luaL_getmetatable(L, indexlib_name);
	lua_setmetatable(L, -2);
	ptr[0] = obj_space;
	ptr[1] = idx;
}

static Index<BasicIndex> *
luaT_checkindex(struct lua_State *L, int i)
{
	int *d = luaL_checkudata(L, i, indexlib_name);
	Index<BasicIndex> *index  = object_space_registry[d[0]].index[d[1]];
	if (index == nil) {
		lua_pushliteral(L, "nonexistant index");
		lua_error(L);
	}
	return index;
}

static Index<HashIndex> *
luaT_checkindex_hash(struct lua_State *L, int i)
{
	Index *index = luaT_checkindex(L, i);
	if (index->type != HASH) {
		lua_pushliteral(L, "index type must be hash");
		lua_error(L);
	}

	return (Index<HashIndex> *)index;
}

static int
luaT_index_index(struct lua_State *L)
{
	id<BasicIndex> index = luaT_checkindex(L, 1);

	struct tnt_object *obj = NULL;
	struct tbuf *key = tbuf_alloc(fiber->pool);

	if (lua_isstring(L, 2)) {
		size_t len;
		const char *str = lua_tolstring(L, 2, &len);
		write_varint32(key, len);
		tbuf_append(key, str, len);
		obj = [index find_key:key with_cardinalty:1];
	} else if (lua_istable(L, 2)) {
		int cardinality = 0;
		lua_pushnil(L);  /* first key */
		while (lua_next(L, 2) != 0) {
			size_t len;
			const char *str = lua_tolstring(L, -1, &len);
			write_varint32(key, len);
			tbuf_append(key, str, len);
			cardinality++;
			lua_pop(L, 1);
		}
		obj = [index find_key:key with_cardinalty:cardinality];
	} else {
		key = luaT_checktbuf(L, 2);
		obj = [index find_key:key with_cardinalty:1];
	}

	if (obj != NULL) {
		luaT_pushobject(L, obj);
		return 1;
	}
	return 0;
}

static int
luaT_index_hashget(struct lua_State *L)
{
	id<HashIndex> index = luaT_checkindex_hash(L, 1);
	u32 i = luaL_checkinteger(L, 2);
	luaT_pushobject(L, [index get:i]);
	return 1;
}

static int
luaT_index_hashsize(struct lua_State *L)
{
	id<HashIndex> index = luaT_checkindex_hash(L, 1);
	lua_pushinteger(L, [index buckets]);
	return 1;
}

static int
luaT_index_hashnext(struct lua_State *L)
{
	id<HashIndex> index = luaT_checkindex_hash(L, 1);
	u32 i;

	if (lua_isnil(L, 2))
		i = 0;
	else
		i = luaL_checkinteger(L, 2) + 1;

	u32 buckets = [index buckets];
	do {
		struct tnt_object *obj = [index get:i];
		if (obj != NULL) {
			lua_pushinteger(L, i);
			luaT_pushobject(L, obj);
			return 2;
		}
	} while (++i < buckets);

	return 0;
}

static int
luaT_index_treenext(struct lua_State *L)
{
	Tree *index = (Tree *)luaT_checkindex(L, 1);
	if (index->type != TREE) {
		lua_pushliteral(L, "index type must be tree");
		lua_error(L);
	}

	struct tnt_object *obj = [index iterator_next];
	if (obj != NULL) {
		luaT_pushobject(L, obj);
		return 1;
	}
	return 0;
}

static int
luaT_index_treeiter(struct lua_State *L)
{
	Tree *index = (Tree *)luaT_checkindex(L, 1);
	if (index->type != TREE) {
		lua_pushliteral(L, "index type must be tree");
		lua_error(L);
	}

	struct tbuf *key = tbuf_alloc(fiber->pool);
	if (lua_isstring(L, 2)) {
		size_t len;
		const char *str = lua_tolstring(L, 2, &len);
		write_varint32(key, len);
		tbuf_append(key, str, len);
		[index iterator_init:key with_cardinalty:1];
	} else if (lua_istable(L, 2)) {
		int cardinality = 0;
		lua_pushnil(L);  /* first key */
		while (lua_next(L, 2) != 0) {
			size_t len;
			const char *str = lua_tolstring(L, -1, &len);
			write_varint32(key, len);
			tbuf_append(key, str, len);
			cardinality++;
			lua_pop(L, 1);
		}
		[index iterator_init:key with_cardinalty:cardinality];
	} else if (lua_isnil(L, 2)) {
		[index iterator_init:NULL with_cardinalty:0];
	} else if (lua_isuserdata(L, 2)) {
		struct tnt_object *obj = *(void **)luaL_checkudata(L, 2, objectlib_name);
		[index iterator_init_with_object:obj];
	} else {
		lua_pushliteral(L, "wrong key type");
		lua_error(L);
	}
	lua_pushcfunction(L, luaT_index_treenext);
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
	{"hashnext", luaT_index_hashnext},
	{"treeiter", luaT_index_treeiter},
	{NULL, NULL}
};


static int
luaT_box_dispatch(struct lua_State *L)
{
#if 1
	lua_pushliteral(L, "not implemented");
	lua_error(L);
	return 0;
#else
	u32 op = luaL_checkinteger(L, 1);
	size_t len;
	const char *req = luaL_checklstring(L, 2, &len);
	struct tbuf request_data = { .data = (char *)req,
				     .len = len,
				     .size = len,
				     .pool = NULL };

	struct palloc_pool *pool = palloc_create_pool("lua");
	struct netmsg_tailq q = TAILQ_HEAD_INITIALIZER(q);
	struct box_txn txn;

	txn_init(&txn, netmsg_tail(&q, pool), BOX_QUIET);
	iproto_reply(&txn.m, op, 0);
	u32 ret = box_dispach(&txn, RW, op, &request_data);
	palloc_destroy_pool(pool);

	lua_pushinteger(L, ret);
	return 1;
#endif
}


static const struct luaL_reg boxlib [] = {
	{"dispatch", luaT_box_dispatch},
	{NULL, NULL}
};



static int
luaT_pushfield(struct lua_State *L)
{
	size_t len, flen;
	const char *str = luaL_checklstring(L, 1, &len);
	flen = len + varint32_sizeof(len);
	u8 *dst = alloca(flen); /* FIXME: this will crash, given str is large enougth */
	u8 *tail = save_varint32(dst, len);
	memcpy(tail, str, len);
	lua_pushlstring(L, (char *)dst, flen);
	return 1;
}

static int
luaT_pushu32(struct lua_State *L)
{
	u32 i = luaL_checkinteger(L, 1);
	u8 *dst = alloca(sizeof(i));
	memcpy(dst, &i, sizeof(i));
	lua_pushlstring(L, (char *)dst, sizeof(i));
	return 1;
}

void
luaT_openbox(struct lua_State *L)
{
        lua_getglobal(L, "package");
        lua_getfield(L, -1, "path");
        lua_pushliteral(L, ";mod/box/src-lua/?.lua");
        lua_concat(L, 2);
        lua_setfield(L, -2, "path");
        lua_pop(L, 1);

	luaL_newmetatable(L, netmsgpromise_metaname);
	luaL_newmetatable(L, netmsg_metaname);
	luaL_register(L, NULL, netmsg_mt);
	luaL_register(L, "netmsg", netmsg_lib);
	lua_pop(L, 3);

        lua_getglobal(L, "require");
        lua_pushliteral(L, "box_prelude");
	if (lua_pcall(L, 1, 0, 0) != 0)
		panic("moonbox: %s", lua_tostring(L, -1));

	luaL_newmetatable(L, indexlib_name);
	luaL_register(L, NULL, indexlib_m);
	luaL_register(L, "box.index", indexlib);

	luaL_newmetatable(L, objectlib_name);
	luaL_register(L, NULL, tuple_mt);

	luaL_register(L, "box", boxlib);

	lua_getglobal(L, "string");
	lua_pushcfunction(L, luaT_pushfield);
	lua_setfield(L, -2, "tofield");
	lua_pushcfunction(L, luaT_pushu32);
	lua_setfield(L, -2, "tou32");
	lua_pop(L, 1);

	lua_createtable(L, 0, 0); /* namespace_registry */
	for (uint32_t n = 0; n < object_space_count; ++n) {
		if (!object_space_registry[n].enabled)
			continue;

		lua_createtable(L, 0, 0); /* namespace */
		lua_pushliteral(L, "index");
		lua_createtable(L, 0, 0); /* index */
		for (int i = 0; i < MAX_IDX; i++) {
			Index *index = object_space_registry[n].index[i];
			if (!index)
				break;
			luaT_pushindex(L, n, i);
			lua_rawseti(L, -2, i); /* index[i] = index_uvalue */
		}
		lua_rawset(L, -3); /* namespace.index = index */

		lua_pushliteral(L, "cardinality");
		lua_pushinteger(L, object_space_registry[n].cardinality);
		lua_rawset(L, -3); /* namespace.cardinality = cardinality */

		lua_pushliteral(L, "n");
		lua_pushinteger(L, n);
		lua_rawset(L, -3); /* namespace.n = n */

		lua_rawseti(L, -2, n); /* namespace_registry[n] = namespace */
	}
	lua_setglobal(L, "object_space_registry");
}


u32
box_dispach_lua(struct netmsg *dst, struct tbuf *data)
{
	lua_State *L = fiber->L;

	i32 n = read_u32(data);

	luaT_pushnetmsg(L);

	lua_getglobal(L, "box");
	lua_pushliteral(L, "user_proc");
	lua_rawget(L, -2); /* user_proc table */
	lua_remove(L, -2);
	read_push_field(L, data); /* proc_name */
	lua_rawget(L, -2); /* stack top is the proc */
	lua_remove(L, -2);

	if (lua_isnil(L, 1)) {
		lua_settop(L, 0);
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "no such proc");
	}

	lua_pushvalue(L, 1);

	lua_getglobal(L, "object_space_registry");
	lua_rawgeti(L, -1, n);
	lua_remove(L, -2); /* remove object_space_registry table */

	u32 nargs = read_u32(data);
	for (int i = 0; i < nargs; i++)
		read_push_field(L, data);

	/* FIXME: switch to native exceptions */
	if (lua_pcall(L, 2 + nargs, 1, 0)) {
		say_error("lua_pcall() failed: %s", lua_tostring(L, -1));
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "lua_pcall() failed");
	}

	u32 ret = luaL_checkinteger(L, -1);
	lua_pop(L, 1);

	struct netmsg_tailq *q = luaT_checknetmsg(L, 1);
	netmsg_concat(dst, q);
	lua_pop(L, 1);
	return ret;
}
