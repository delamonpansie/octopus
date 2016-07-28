/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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

#import <src-lua/octopus_lua.h>
#include <third_party/luajit/src/lj_obj.h> /* for LJ_TCDATA */
#import <mod/box/box.h>
#import <mod/box/src-lua/moonbox.h>

#import <shard.h>

const int object_space_max_idx = MAX_IDX;

Box *
shard_box()
{
	return ((struct box_txn *)fiber->txn)->box;
}

int
shard_box_id()
{
	return shard_box()->shard->id;
}

int
box_version()
{
	return shard_box()->version;
}

struct object_space *
object_space_l(Box *box, int n)
{
	@try {
		return object_space(box, n);
	}
	@catch (Error *e) {
		return NULL;
	}
}

int
shard_box_next_primary_n(int n)
{
	int i = MAX_SHARD;
	n++;
	if (n < 0) n = 0;
	for (;i>0;n++,i--) {
		if (n >= MAX_SHARD)
			n = 0;
		if (shard_rt[n].shard &&
		    shard_rt[n].shard->executor &&
		    ![shard_rt[n].shard is_replica])
			return n;
	}
	return -1;
}

bool
box_is_primary(Box* box)
{
	return ![box->shard is_replica];
}

u32 *
box_tuple_cache_update(int cardinality, const unsigned char *data)
{
	u32 *cache = palloc(fiber->pool, 2 * cardinality * sizeof(u32));
	const unsigned char *field = data;
	for (int i = 0; i < cardinality; i++) {
		u32 len = LOAD_VARINT32(field);
		cache[i * 2] = len;
		cache[i * 2 + 1] = field - data;
		field = field + len;
	}
	return cache;
}

struct tnt_object *
box_small_tuple_palloc_clone(struct tnt_object *obj)
{
	assert(obj->type == BOX_SMALL_TUPLE);
	int obj_size = sizeof(struct tnt_object) + sizeof(struct box_small_tuple) + tuple_bsize(obj);
	struct tnt_object *copy = palloc(fiber->pool, obj_size);
	return memcpy(copy, obj, obj_size);
}

static int
push_obj(struct lua_State *L, struct tnt_object *obj)
{
	switch (obj->type) {
	case BOX_TUPLE:
		object_incr_ref_autorelease(obj);
		lua_pushlightuserdata(L, obj);
		return 1;
	case BOX_SMALL_TUPLE:
		obj = box_small_tuple_palloc_clone(obj);
		lua_pushlightuserdata(L, obj);
		return 1;
	}
	abort();
}

static int
luaT_box_dispatch(struct lua_State *L)
{
	size_t len;
	const char *req;
	int op = luaL_checkinteger(L, 1);

	if (lua_type(L, 2) == ~LJ_TCDATA) {
		char * const *p = lua_topointer(L, 2);
		req = *p;
		len = luaL_checkinteger(L, 3);
	} else {
		req = luaL_checklstring(L, 2, &len);
	}
	@try {
		struct box_op *bop = box_prepare(fiber->txn, op, req, len);
		if ((bop->flags & BOX_RETURN_TUPLE) == 0)
			return 0;

		if (bop->ret_obj != NULL)
			return push_obj(L, bop->ret_obj);
	}
	@catch (Error *e) {
		if ([e respondsTo:@selector(code)])
			lua_pushfstring(L, "code:%d reason:%s", [(id)e code], e->reason);
		else
			lua_pushstring(L, e->reason);
		[e release];
		lua_error(L);
	}
	return 0;
}

static const struct luaL_reg boxlib [] = {
	{"_dispatch", luaT_box_dispatch},
	{NULL, NULL}
};


void
luaT_openbox(struct lua_State *L)
{
        lua_getglobal(L, "package");
        lua_getfield(L, -1, "path");
        lua_pushliteral(L, ";mod/box/src-lua/?.lua");
        lua_concat(L, 2);
        lua_setfield(L, -2, "path");
        lua_pop(L, 1);

	luaL_findtable(L, LUA_GLOBALSINDEX, "box", 0);
	luaL_register(L, NULL, boxlib);
	lua_pop(L, 1);

	luaO_pushtraceback(L);
	lua_getglobal(L, "require");
        lua_pushliteral(L, "box_prelude");
	if (lua_pcall(L, 1, 0, -3) != 0)
		panic("moonbox: %s", lua_tostring(L, -1));
	lua_pop(L, 1);
}

static void restore_top(int *top) {
	lua_settop(fiber->L, *top-1);
}

static void
read_push_field(lua_State *L, struct tbuf *buf)
{
	void *p = buf->ptr;
	u32 data_len = read_varint32(buf);

	if (unlikely(buf->ptr + data_len > buf->end)) {
		buf->ptr = p;
		tbuf_too_short();
	} else {
		lua_pushlstring(L, buf->ptr, data_len);
		buf->ptr += data_len;
	}
}

static int box_entry_i = 0;
void
box_dispach_lua(struct netmsg_head *wbuf, struct iproto *request)
{
	WITH_AUTORELEASE;

	lua_State *L = fiber->L;
	struct tbuf data = TBUF(request->data, request->data_len, fiber->pool);

	u32 flags = read_u32(&data); (void)flags; /* compat, ignored */
	u32 flen = read_varint32(&data);
	void *fname = read_bytes(&data, flen);
	u32 nargs = read_u32(&data);

	luaO_pushtraceback(L);
	int __attribute__((cleanup(restore_top))) top = lua_gettop(L);

	if (box_entry_i == 0) {
		lua_getglobal(L, "box");
		lua_getfield(L, -1, "entry");
		lua_remove(L, -2);
		box_entry_i = lua_ref(L, LUA_REGISTRYINDEX);
	}
	lua_rawgeti(L, LUA_REGISTRYINDEX, box_entry_i);

	lua_pushlstring(L, fname, flen);
	lua_pushlightuserdata(L, wbuf);
	lua_pushlightuserdata(L, request);

	if (!lua_checkstack(L, nargs)) {
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "too many args to exec_lua");
	}

	for (int i = 0; i < nargs; i++)
		read_push_field(L, &data);

	/* FIXME: switch to native exceptions ? */
	if (lua_pcall(L, 3 + nargs, LUA_MULTRET, top)) {
		const char *reason = lua_tostring(L, -1);
		int code = ERR_CODE_ILLEGAL_PARAMS;

		if (strncmp(reason, "code:", 5) == 0) {
			char *r = strchr(reason, 'r');
			if (r && strncmp(r, "reason:", 7) == 0) {
				code = atoi(reason + 5);
				reason = r + 7;
			}
		}

		iproto_raise(code, reason);
	}

	if (box_submit(fiber->txn) == -1)
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

	int newtop = lua_gettop(L);
	if (newtop != top) {
		if (newtop != top + 3 && newtop != top + 2) {
			iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS,
				"illegal return from wrapped function %s", fname);
		}
		struct netmsg_mark mark;
		netmsg_getmark(wbuf, &mark);
		struct iproto_retcode *reply = iproto_reply(wbuf, request,
							    lua_tointeger(L, top + 2));
		if (newtop == top + 3 && !lua_isnil(L, top + 3)) {
			lua_remove(L, top + 2);
			lua_pushlightuserdata(L, wbuf);

			if (lua_pcall(L, 2, 0, top)) {
				netmsg_rewind(wbuf, &mark);
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, lua_tostring(L, -1));
			}
		}
		iproto_reply_fixup(wbuf, reply);
	}
}

register_source();

