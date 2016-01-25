/*
 * Copyright (C) 2015 Mail.RU
 * Copyright (C) 2015 Yuriy Sokolov
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

#import <fiber.h>
#import <util.h>
#import <say.h>
#import <objc.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>
#include <string.h>

#include <constant_kv.h>
#include <cfg/defs.h>
#include <cfg/octopus.h>
#include <assoc.h>

#import <octopus_ev.h>

static constant_kv_cb_f *ckv_callbacks = NULL;
static int ckv_callbacks_n = 0;

struct constant_kv {
	char const *name;
	char const *path;
	uint32_t name_hash;
	uint8_t kind;
	bool force_reload;
	struct ckv* ckv;
} *constant_kvs = NULL;
static int constant_kvs_n = 0;

static uint32_t
constant_kv_name_hash(const char *name) {
	uint32_t h = 5381;
	for (;*name; name++) {
		h = h*33 ^ *name;
	}
	return h;
}

static struct constant_kv*
constant_kv_by_name(const char *name) {
	uint32_t hash = constant_kv_name_hash(name);
	int i;
	for (i = 0; i < constant_kvs_n; i++) {
		if (constant_kvs[i].name_hash == hash &&
				strcmp(constant_kvs[i].name, name) == 0) {
			return constant_kvs + i;
		}
	}
	return NULL;
}

static void
ckv_errcb(void* arg _unused_, const char* path, const char* call, int num)
{
	if (strcmp(call, "format") == 0) {
		say_error("constant_kv format %s:%d", path, num);
	} else {
		say_error("constant_kv %s(%s): %s", call, path, strerror_o(num));
	}
}

static void
ckv_callback(va_list arg)
{
	const char* name = va_arg(arg, const char*);
	int i;
	for(i=0; i<ckv_callbacks_n; i++) {
		ckv_callbacks[i](name);
	}
}

static void
constant_kv_watch(ev_timer *w _unused_, int event _unused_)
{
	int i;
	struct constant_kv *kv = constant_kvs;
	assert(kv != NULL);
	for (i=0; i < constant_kvs_n; i++, kv++) {
		struct stat newstat, *oldstat;
		struct ckv *new = NULL;
		if (stat(kv->path, &newstat) == -1) {
			say_syserror("stat(%s)", kv->path);
			continue;
		}
		if (!kv->force_reload && kv->ckv != NULL) {
			oldstat = ckv_fstat(kv->ckv);
			if (oldstat->st_dev      == newstat.st_dev
			    && oldstat->st_ino   == newstat.st_ino
			    && oldstat->st_size  == newstat.st_size
			    && oldstat->st_ctime == newstat.st_ctime) {
				continue;
			}
		}
		say_debug("reloading %s", kv->name);
		for (;;) {
			new = ckv_open(kv->path, kv->kind, 0, ckv_errcb, NULL);
			if (!new)
				break;
			if (stat(kv->path, &newstat) == -1) {
				say_syserror("stat(%s)", kv->path);
				ckv_close(new);
				new = NULL;
				break;
			}
			oldstat = ckv_fstat(new);
			if (oldstat->st_dev      == newstat.st_dev
			    && oldstat->st_ino   == newstat.st_ino
			    && oldstat->st_size  == newstat.st_size
			    && oldstat->st_ctime == newstat.st_ctime) {
				break;
			}
			ckv_close(new);
		}
		if (new) {
			if (kv->ckv != NULL)
				ckv_close(kv->ckv);
			kv->ckv = new;
			kv->force_reload = false;
			fiber_create("ckv_callback", ckv_callback, kv->name);
		}
	}
}

void
register_constant_kv_or_fail(const char* _name, const char* _path, enum ckv_kind kind)
{
	assert(_name != NULL && _path != NULL);
	const char* name = strdup(_name);
	const char* path = strdup(_path);
	assert(name != NULL && path != NULL);
	struct constant_kv *kv = constant_kv_by_name(name);
	if (kv != NULL) {
		panic("already registered constant_kv '%s'", name);
	}
	constant_kvs = xrealloc(constant_kvs, (constant_kvs_n+1)*sizeof(*constant_kvs));
	kv = constant_kvs + constant_kvs_n;
	constant_kvs_n++;
	kv->name = name;
	kv->path = path;
	kv->name_hash = constant_kv_name_hash(name);
	kv->kind = kind;
	kv->ckv = ckv_open(path, kind, 0, ckv_errcb, NULL);
	if (kv->ckv == NULL) {
		panic("registraction constant_kv '%s' failed", name);
	}
}

int
constant_kv_get(const char* name, const char* key, int key_len,
		struct ckv_str* result, struct ckv_str* format)
{
	struct constant_kv *kv = constant_kv_by_name(name);
	if (kv == NULL) return 2;
	if (key_len <= 0) key_len = strlen(key);
	return !ckv_key_get(kv->ckv, key, key_len, result, format);
}

int
constant_kv_geti(const char* name, const char* key, int key_len, int _default)
{
	struct constant_kv *kv = constant_kv_by_name(name);
	if (kv == NULL) return _default;
	if (key_len <= 0) key_len = strlen(key);
	return ckv_key_get_atoi(kv->ckv, key, key_len, _default);
}

bool
constant_kv_registered(const char* name)
{
	struct constant_kv *kv = constant_kv_by_name(name);
	return kv != NULL;
}

void
register_constant_kv_callback(constant_kv_cb_f cb)
{
	ckv_callbacks = xrealloc(ckv_callbacks, (ckv_callbacks_n + 1)*sizeof(cb));
	ckv_callbacks[ckv_callbacks_n] = cb;
	ckv_callbacks_n++;
}

static void
lua_constant_kv_callback(const char* name)
{
	lua_State *L = fiber->L;
	lua_getglobal(L, "ckv");
	lua_getfield(L, -1, "__call_callbacks");
	lua_remove(L, -2);
	lua_pushstring(L, name);
	lua_call(L, 1, 0);
}

#if CFG_constant_kv
static enum ckv_kind
ckv_kind_by_name(const char *kinds, const char *path)
{
	if (strcmp(kinds, "text") == 0)
		return CKV_TEXT_NOFORMAT;
	if (strcmp(kinds, "text_fmt") == 0)
		return CKV_TEXT;
	if (strcmp(kinds, "cdb") == 0)
		return CKV_CDB_NOFORMAT;
	if (strcmp(kinds, "ckb_1f") == 0)
		return CKV_CDB_BYTEFORMAT;
	if (strcmp(kinds, "onlineconf") == 0) {
		const char* dotp = rindex(path, '.');
		if (dotp != NULL && strcmp(dotp, ".cdb") == 0)
			return CKV_CDB_BYTEFORMAT;
		else
			return CKV_TEXT;
	}
	return -1;
}

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
static int
check_config(struct octopus_cfg *new)
{
	extern void out_warning(int v, char *format, ...);
	int i, fd, res = 0;
	struct stat pstat;
	const char *name, *path;
	enum ckv_kind kind;
	struct mh_cstr_t *names;
	if (new->constant_kv == NULL && constant_kvs_n > 0) {
		out_warning(0, "new constant_kv section is empty, but old is not");
		return -1;
	}
	if (new->constant_kv == NULL) {
		return 0;
	}
	names = mh_cstr_init(xrealloc);
	for (i = 0; new->constant_kv[i] != NULL; i++) {
		if (!CNF_STRUCT_DEFINED(new->constant_kv[i]))
			continue;
		name = new->constant_kv[i]->name;
		path = new->constant_kv[i]->file;
		if (!mh_cstr_put(names, name, &new->constant_kv[i], NULL)) {
			out_warning(0, "constant_kv name '%s' duplicates", name);
			res = -1;
		}
		if (stat(path, &pstat) == -1) {
			out_warning(0, "constant_kv stat(%s): %s", path, strerror_o(errno));
			res = -1;
		}
		fd = open(path, O_RDONLY|O_CLOEXEC);
		if (fd == -1) {
			out_warning(0, "constant_kv read(%s): %s", path, strerror_o(errno));
			res = -1;
		} else {
			close(fd);
		}
		kind = ckv_kind_by_name(new->constant_kv[i]->format, new->constant_kv[i]->file);
		if (kind == -1) {
			out_warning(0, "costant_kv format '%s' unknown", new->constant_kv[i]->format);
			res = -1;
		}

	}
	for (i = 0; i < constant_kvs_n; i++) {
		struct constant_kv *kv = constant_kvs + i;
		uint32_t pos = mh_cstr_get(names, kv->name);
		if (pos == mh_end(names)) {
			out_warning(0, "constant_kv old name '%s' is not defined in new config",
					kv->name);
			res = -1;
		}
	}
	mh_cstr_destroy(names);
	return res;
}

static void
load_config(struct octopus_cfg *new)
{
	const char *name, *path;
	enum ckv_kind kind;
	struct constant_kv *kv;
	int i;
	if (new->constant_kv == NULL) {
		return;
	}
	for (i = 0; new->constant_kv[i] != NULL; i++) {
		if (!CNF_STRUCT_DEFINED(new->constant_kv[i]))
			continue;
		name = new->constant_kv[i]->name;
		path = new->constant_kv[i]->file;
		kind = ckv_kind_by_name(new->constant_kv[i]->format, path);
		kv = constant_kv_by_name(name);
		if (kv == NULL) {
			register_constant_kv_or_fail(name, path, kind);
		} else if (strcmp(kv->path, path) != 0 || kv->kind != kind) {
			free((void*)kv->path);
			kv->path = strdup(path);
			kv->kind = kind;
			kv->force_reload = true;
			/* will be reloaded at next timer hit */
		}
	}
}

static void
reload_config(struct octopus_cfg *old _unused_, struct octopus_cfg *new)
{
	load_config(new);
}
#endif //CFG_constant_kv

static ev_timer constant_kv_timer;
static void
init(void)
{
#if CFG_constant_kv
	load_config(&cfg);
#endif
	ev_timer_init(&constant_kv_timer, constant_kv_watch, 2, 2);
	ev_timer_start(&constant_kv_timer);
	switch (luaT_require("ckv")) {
	case 1: register_constant_kv_callback(lua_constant_kv_callback);
	case 0: break;
	case -1:
		panic("unable to load `ckv' lua module: %s", lua_tostring(fiber->L, -1));
	}
}

static struct tnt_module constant_kv_mod = {
	.name = "constant_kv",
	.init = init,
#if CFG_constant_kv
	.check_config = check_config,
	.reload_config = reload_config,
#endif
};
register_module(constant_kv_mod);

register_source();
