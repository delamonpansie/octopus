/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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
#import <fiber.h>
#import <say.h>
#import <pickle.h>
#import <assoc.h>
#import <net_io.h>
#import <index.h>

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>

#import <mod/box/box.h>
#import <mod/box/moonbox.h>


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
	u32 len = LOAD_VARINT32(field);
	lua_pushlstring(L, field, len);
	return 1;
}

static const struct luaL_reg object_mt [] = {
	{"__len", tuple_len_},
	{"__index", tuple_index_},
	{NULL, NULL}
};

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



const char *netmsg_metaname = "Tarantool.netmsg";
const char *netmsgpromise_metaname = "Tarantool.netmsg.promise";

static int
luaT_pushnetmsg(struct lua_State *L)
{
	struct netmsg_tailq *q = lua_newuserdata(L, sizeof(struct netmsg_tailq));
	luaL_getmetatable(L, netmsg_metaname);
	lua_setmetatable(L, -2);

	TAILQ_INIT(q);
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



static int
luaT_box_dispatch(struct lua_State *L)
{
	u32 op = luaL_checkinteger(L, 1);
	size_t len;
	const char *req = luaL_checklstring(L, 2, &len);
	struct tbuf request_data = { .data = (char *)req,
				     .len = len,
				     .size = len,
				     .pool = NULL };

	struct box_txn txn;
	memset(&txn, 0, sizeof(txn));
	txn.op = op;
	@try {
		box_prepare_update(&txn, &request_data);
		txn_submit_to_storage(&txn);
		txn_commit(&txn);
		if (txn.obj != NULL) {
			luaT_pushobject(L, txn.obj);
			return 1;
		}
	}
	@catch (Error *e) {
		txn_abort(&txn);
		lua_pushstring(L, e->reason);
		lua_error(L);
	}
	@finally {
		txn_cleanup(&txn);
	}
	return 0;
}

static int
luaT_box_index(struct lua_State *L)
{
	int n = luaL_checkinteger(L, 1);
	int i = luaL_checkinteger(L, 2);
	if (n < 0 || n >= object_space_count) {
		lua_pushliteral(L, "bad object_space num");
		lua_error(L);
	}
	if (i < 0 || i >= MAX_IDX) {
		lua_pushliteral(L, "bad index num");
		lua_error(L);
	}

	Index *index = object_space_registry[n].index[i];
	if (!index)
		return 0;
	luaT_pushindex(L, index);
	return 1;
}

static const struct luaL_reg boxlib [] = {
	{"index", luaT_box_index},
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

static int
luaT_pushu16(struct lua_State *L)
{
	u16 i = luaL_checkinteger(L, 1);
	u8 *dst = alloca(sizeof(i));
	memcpy(dst, &i, sizeof(i));
	lua_pushlstring(L, (char *)dst, sizeof(i));
	return 1;
}

static int
luaT_pushu8(struct lua_State *L)
{
	u8 i = luaL_checkinteger(L, 1);
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

	lua_getglobal(L, "string");
	lua_pushcfunction(L, luaT_pushfield);
	lua_setfield(L, -2, "tofield");
	lua_pushcfunction(L, luaT_pushu32);
	lua_setfield(L, -2, "tou32");
	lua_pushcfunction(L, luaT_pushu16);
	lua_setfield(L, -2, "tou16");
	lua_pushcfunction(L, luaT_pushu8);
	lua_setfield(L, -2, "tou8");
	lua_pop(L, 1);

	luaL_newmetatable(L, objectlib_name);
	luaL_register(L, NULL, object_mt);
	lua_pop(L, 1);

	luaL_findtable(L, LUA_GLOBALSINDEX, "box", 0);
	luaL_register(L, NULL, boxlib);

	lua_createtable(L, 0, 0); /* namespace_registry */
	for (uint32_t n = 0; n < object_space_count; ++n) {
		if (!object_space_registry[n].enabled)
			continue;

		lua_createtable(L, 0, 0); /* namespace */

		lua_pushliteral(L, "cardinality");
		lua_pushinteger(L, object_space_registry[n].cardinality);
		lua_rawset(L, -3); /* namespace.cardinality = cardinality */

		lua_pushliteral(L, "n");
		lua_pushinteger(L, n);
		lua_rawset(L, -3); /* namespace.n = n */

		lua_rawseti(L, -2, n); /* namespace_registry[n] = namespace */
	}
	lua_setfield(L, -2, "object_space");
	lua_pop(L, 1);

	lua_getglobal(L, "require");
        lua_pushliteral(L, "box_prelude");
	if (lua_pcall(L, 1, 0, 0) != 0)
		panic("moonbox: %s", lua_tostring(L, -1));

}


static int
luaT_find_proc(lua_State *L, char *fname, i32 len)
{
	lua_pushvalue(L, LUA_GLOBALSINDEX);
	do {
		char *e = memchr(fname, '.', len);
		if (e == NULL)
			e = fname + len;

		lua_pushlstring(L, fname, e - fname);
		lua_gettable(L, -2);
		lua_remove(L, -2);

		len -= e - fname + 1;
		fname = e + 1;
	} while (len > 0);
	return 1;
}

u32
box_dispach_lua(struct box_txn *txn, struct tbuf *data)
{
	lua_State *L = fiber->L;

	u32 flags = read_u32(data); (void)flags; /* compat, ignored */
	u32 flen = read_varint32(data);
	void *fname = read_bytes(data, flen);
	u32 nargs = read_u32(data);

	luaT_pushnetmsg(L);

	luaT_find_proc(L, fname, flen);
	lua_pushvalue(L, 1);
	for (int i = 0; i < nargs; i++)
		read_push_field(L, data);

	/* FIXME: switch to native exceptions ? */
	if (lua_pcall(L, 1 + nargs, 1, 0)) {
		IProtoError *err = [IProtoError palloc];
		[err init_code:ERR_CODE_ILLEGAL_PARAMS
			  line:__LINE__
			  file:__FILE__
		     backtrace:NULL
			format:"%s", lua_tostring(L, -1)];
		lua_settop(L, 0);
		@throw err;
	}

	struct netmsg_tailq *q = luaT_checknetmsg(L, 1);
	txn->m = netmsg_concat(txn->m->tailq, q, txn->m->pool);

	u32 ret = luaL_checkinteger(L, 2);
	lua_pop(L, 2);
	return ret;
}
