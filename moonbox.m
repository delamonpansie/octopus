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
#include <third_party/luajit/src/lj_obj.h> /* for LJ_TCDATA */
#import <mod/box/box.h>
#import <mod/box/moonbox.h>

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
		if ([recovery submit:req len:len tag:txn.op<<5|TAG_WAL] != 1)
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write row");
		box_commit(&txn);

		if (txn.obj != NULL) {
			lua_pushlightuserdata(L, txn.obj);
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

	lua_getglobal(L, "require");
        lua_pushliteral(L, "box_prelude");
	if (lua_pcall(L, 1, 0, 0) != 0)
		panic("moonbox: %s", lua_tostring(L, -1));
}


void
box_dispach_lua(struct conn *c, struct iproto *request)
{
	lua_State *L = fiber->L;
	struct tbuf data = TBUF(request->data, request->data_len, fiber->pool);

	u32 flags = read_u32(&data); (void)flags; /* compat, ignored */
	u32 flen = read_varint32(&data);
	void *fname = read_bytes(&data, flen);
	u32 nargs = read_u32(&data);

	if (luaT_find_proc(L, fname, flen) == 0) {
		lua_pop(L, 1);
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "no such proc: %.*s", flen, fname);
	}

	luaT_pushptr(L, c);
	luaT_pushptr(L, request);

	for (int i = 0; i < nargs; i++)
		read_push_field(L, &data);

	/* FIXME: switch to native exceptions ? */
	if (lua_pcall(L, 2 + nargs, 0, 0)) {
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
		lua_settop(L, 0);
		@throw err;
	}
}

register_source();

