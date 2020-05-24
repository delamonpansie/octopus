/*
 * Copyright (C) 2016 Mail.RU
 * Copyright (C) 2016 Yuriy Vostrikov
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

#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>

#import <say.h>
#import <objc.h>
#import <fiber.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

lua_State *root_L = NULL;

static int
luaO_print(struct lua_State *L)
{
	int n = lua_gettop(L);
	for (int i = 1; i <= n; i++)
		say_info("%s", lua_tostring(L, i));
	return 0;
}

static int
luaO_panic(struct lua_State *L)
{
	const char *err = "unknown error";
	if (lua_isstring(L, -1))
		err = lua_tostring(L, -1);
	panic("lua failed with: %s", err);
}


static int
luaO_error(struct lua_State *L)
{
	const char *err = "unknown lua error";
	if (lua_isstring(L, -1))
		err = lua_tostring(L, -1);

	/* FIXME: use native exceptions ? */
	@throw [Error with_reason:err];
}


static int /* FIXME: FFFI! */
luaO_os_ctime(lua_State *L)
{
	const char *filename = luaL_checkstring(L, 1);
	struct stat buf;

	if (stat(filename, &buf) < 0)
		luaL_error(L, "stat(`%s'): %s", filename, strerror_o(errno));
	lua_pushnumber(L, buf.st_ctime + (lua_Number)buf.st_ctim.tv_nsec / 1.0e9);
	return 1;
}

int
luaO_traceback(lua_State *L)
{
	if (!lua_isstring(L, 1)) { /* Non-string error object? Try metamethod. */
		if (lua_isnoneornil(L, 1) ||
				!luaL_callmeta(L, 1, "__tostring") ||
				!lua_isstring(L, -1)) {
			lua_settop(L, 1);
			lua_getglobal(L, "tostring");
			lua_pushvalue(L, -2);
			lua_call(L, 1, 0);
		}
		lua_remove(L, 1);  /* Replace object by result of __tostring metamethod. */
	}
	luaL_traceback(L, L, lua_tostring(L, 1), 1);
	return 1;
}

static int luaO_traceback_i = 0;
void
luaO_pushtraceback(lua_State *L)
{
	lua_rawgeti(L, LUA_REGISTRYINDEX, luaO_traceback_i);
}


static void
luaO_fiber_trampoline(va_list ap)
{
	struct lua_State *pL = va_arg(ap, struct lua_State *),
			  *L = fiber->L;

	lua_pushcfunction(L, luaO_traceback);
	lua_xmove(pL, L, 1);
	if (lua_pcall(L, 0, 0, -2) != 0) {
		say_error("lua_pcall(): %s", lua_tostring(L, -1));
		lua_pop(L, 1);
	}
	lua_pop(L, 1);
}

static int
luaO_fiber_create(struct lua_State *L)
{
	if (!lua_isfunction(L, 1)) {
		lua_pushliteral(L, "fiber.create: arg is not a function");
		lua_error(L);
	}

	fiber_create("lua", luaO_fiber_trampoline, L);
	return 0;
}

static int
luaO_fiber_sleep(struct lua_State *L)
{
	lua_Number delay = luaL_checknumber(L, 1);
	fiber_sleep(delay);
	return 0;
}

static int
luaO_fiber_gc(struct lua_State *L _unused_)
{
	fiber_gc();
	return 0;
}

static int
luaO_fiber_yield(struct lua_State *L _unused_)
{
	yield();
	return 0;
}


static const struct luaL_Reg fiberlib [] = {
	{"create", luaO_fiber_create},
	{"sleep", luaO_fiber_sleep},
	{"gc", luaO_fiber_gc},
	{"yield", luaO_fiber_yield},
	{NULL, NULL}
};

int
luaO_openfiber(struct lua_State *L)
{
	luaL_register(L, "fiber", fiberlib);
	lua_pop(L, 1);
	return 0;
}

void
lua_fiber_init(struct Fiber *fiber)
{
	if (root_L == NULL) {
		root_L = luaL_newstate();
		lua_atpanic(root_L, luaO_error);
		assert(root_L != NULL);
	}

	static int lua_reg_cnt;
	char lua_reg_name[16];
	sprintf(lua_reg_name, "_fiber:%i", lua_reg_cnt++);
	fiber->L = lua_newthread(root_L);
	lua_setfield(root_L, LUA_REGISTRYINDEX, lua_reg_name);
}

void
luaO_init()
{
	struct lua_State *L = root_L;

	/* any lua error during initial load is fatal */
	lua_atpanic(L, luaO_panic);

	luaL_openlibs(L);
	lua_register(L, "print", luaO_print);

	luaO_openfiber(L);

	if (cfg.lua_path != NULL) {
		lua_getglobal(L, "package");
		lua_pushstring(L, cfg.lua_path);
		lua_setfield(L, -2, "path");
		lua_pop(L, 1);
	}

	lua_getglobal(L, "os");
	lua_pushcfunction(L, luaO_os_ctime);
	lua_setfield(L, -2, "ctime");
	lua_pop(L, 1);

	lua_pushcfunction(L, luaO_traceback);
	lua_getglobal(L, "require");
	lua_pushliteral(L, "prelude");
	if (lua_pcall(L, 1, 0, -3))
		panic("lua_pcall() failed: %s", lua_tostring(L, -1));

	luaO_traceback_i = luaL_ref(L, LUA_REGISTRYINDEX);

	lua_atpanic(L, luaO_error);
}

int
luaO_find_proc(lua_State *L, const char *fname, i32 len)
{
	lua_pushvalue(L, LUA_GLOBALSINDEX);
	do {
		const char *e = memchr(fname, '.', len);
		if (e == NULL)
			e = fname + len;

		if (lua_isnil(L, -1))
			return 0;
		lua_pushlstring(L, fname, e - fname);
		lua_gettable(L, -2);
		lua_remove(L, -2);

		len -= e - fname + 1;
		fname = e + 1;
	} while (len > 0);
	if (lua_isnil(L, -1))
		return 0;
	return 1;
}

int
luaO_require(const char *modname)
{
	struct lua_State *L = fiber->L;
	luaO_pushtraceback(L);
	lua_getglobal(L, "require");
	lua_pushstring(L, modname);
	if (!lua_pcall(L, 1, 0, -3)) {
		say_info("Lua module '%s' loaded", modname);
		lua_pop(L, 1);
		return 1;
	} else {
		const char *err = lua_tostring(L, -1);
		char buf[64];
		int ret = 0;
		snprintf(buf, sizeof(buf), "module '%s' not found", modname);
		if (strstr(err, buf) == NULL) {
			say_debug("luaO_require(%s): failed with `%s'", modname, err);
			ret = -1;
		}
		lua_remove(L, -2);
		return ret;
	}
}

void
luaO_require_or_panic(const char *modname, bool panic_on_missing, const char *error_format)
{
	int ret = luaO_require(modname);
	if (ret == 1)
		return;
	if (ret == 0 && !panic_on_missing) {
		lua_pop(fiber->L, 1);
		return;
	}
	if (error_format == NULL) {
		error_format = "unable to load `%s' lua module: %s";
	}
	panic(error_format, modname, lua_tostring(fiber->L, -1));
}
