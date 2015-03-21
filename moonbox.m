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
#include <third_party/luajit/src/lj_obj.h> /* for LJ_TCDATA */
#import <mod/box/box.h>
#import <mod/box/moonbox.h>

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

static int
luaT_box_dispatch(struct lua_State *L)
{
	size_t len;
	const char *req;
	struct box_txn txn = { .op = luaL_checkinteger(L, 1) };

	if (lua_type(L, 2) == ~LJ_TCDATA) {
		char * const *p = lua_topointer(L, 2);
		req = *p;
		len = luaL_checkinteger(L, 3);
	} else {
		req = luaL_checklstring(L, 2, &len);
	}
	@try {
		[recovery check_replica];

		box_prepare(&txn, &TBUF(req, len, NULL));
		if (txn->obj_affected > 0 && txn->object_space->wal) {
			if ([recovery submit:req len:len tag:txn.op<<5|TAG_WAL] != 1)
				iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write row");
		}
		box_commit(&txn);

		if (txn.obj != NULL) {
			object_incr_ref(txn.obj);
			lua_pushlightuserdata(L, txn.obj);
			return 1;
		} else if (txn.op == DELETE && txn.old_obj != NULL) {
			/* object_incr_ref is require because box_cleanup() is called before
			   lua has chance to increase ref counter on passed pointer */
			object_incr_ref(txn.old_obj);
			lua_pushlightuserdata(L, txn.old_obj);
			return 1;
		}
	}
	@catch (Error *e) {
		box_rollback(&txn);
		if ([e respondsTo:@selector(code)])
			lua_pushfstring(L, "code:%d reason:%s", [(id)e code], e->reason);
		else
			lua_pushstring(L, e->reason);
		lua_error(L);
	}
	@finally {
		box_cleanup(&txn);
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

	luaT_pushtraceback(L);
	lua_getglobal(L, "require");
        lua_pushliteral(L, "box_prelude");
	if (lua_pcall(L, 1, 0, -3) != 0)
		panic("moonbox: %s", lua_tostring(L, -1));
	lua_pop(L, 1);
}


static int box_entry_i = 0;
void
box_dispach_lua(struct conn *c, struct iproto *request)
{
	lua_State *L = fiber->L;
	struct tbuf data = TBUF(request->data, request->data_len, fiber->pool);

	u32 flags = read_u32(&data); (void)flags; /* compat, ignored */
	u32 flen = read_varint32(&data);
	void *fname = read_bytes(&data, flen);
	u32 nargs = read_u32(&data);

	luaT_pushtraceback(L);
	int top = lua_gettop(L);

	if (box_entry_i == 0) {
		lua_getglobal(L, "box");
		lua_getfield(L, -1, "entry");
		lua_remove(L, -2);
		box_entry_i = lua_ref(L, LUA_REGISTRYINDEX);
	}
	lua_rawgeti(L, LUA_REGISTRYINDEX, box_entry_i);

	lua_pushlstring(L, fname, flen);
	lua_pushlightuserdata(L, c);
	lua_pushlightuserdata(L, request);

	if (!lua_checkstack(L, nargs)) {
		lua_settop(L, top-1);
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "too many args to exec_lua");
	}

	for (int i = 0; i < nargs; i++)
		read_push_field(L, &data);

	/* FIXME: switch to native exceptions ? */
	if (lua_pcall(L, 3 + nargs, LUA_MULTRET, top)) {
		IProtoError *err = [IProtoError palloc];
		const char *reason = lua_tostring(L, -1);
		int code = ERR_CODE_ILLEGAL_PARAMS;

		if (strncmp(reason, "code:", 5) == 0) {
			char *r = strchr(reason, 'r');
			if (r && strncmp(r, "reason:", 7) == 0) {
				code = atoi(reason + 5);
				reason = r + 7;
			}
		}

		[err init_code:code line:__LINE__ file:__FILE__
		     backtrace:NULL format:"%s", reason];
		lua_settop(L, top-1);
		@throw err;
	}

	int newtop = lua_gettop(L);
	if (newtop != top) {
		if (newtop != top + 3 && newtop != top + 2) {
			IProtoError *err = [IProtoError palloc];
			int code = ERR_CODE_ILLEGAL_PARAMS;
			[err init_code:code line:__LINE__ file:__FILE__
			     backtrace:NULL
			     format:"illegal return from wrapped function %s", fname];
			lua_settop(L, top-1);
			@throw err;
		}
		struct netmsg_mark mark;
		netmsg_getmark(&c->out_messages, &mark);
		struct iproto_retcode *reply = iproto_reply(&c->out_messages, request,
							    lua_tointeger(L, top + 2));
		if (newtop == top + 3 && !lua_isnil(L, top + 3)) {
			lua_remove(L, top + 2);
			lua_pushlightuserdata(L, c);
			if (lua_pcall(L, 2, 0, top)) {
				IProtoError *err = [IProtoError palloc];
				const char *reason = lua_tostring(L, -1);
				int code = ERR_CODE_ILLEGAL_PARAMS;
				netmsg_rewind(&c->out_messages, &mark);

				[err init_code:code line:__LINE__ file:__FILE__
				     backtrace:NULL format:"%s", reason];
				lua_settop(L, top-1);
				@throw err;
			}
		}
		iproto_reply_fixup(&c->out_messages, reply);
	}
	lua_settop(L, top-1);
}

register_source();

