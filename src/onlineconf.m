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

#include <onlineconf.h>
#include <cfg/defs.h>
#include <cfg/octopus.h>
#include <assoc.h>

#import <octopus_ev.h>

static ev_timer onlineconf_timer;
static onlineconf_cb_f *onlineconf_callbacks = NULL;
static int onlineconf_callbacks_n = 0;

static char const * const nullname = "";

struct onlineconf {
	char const *name;
	char const *path;
	uint32_t name_hash;
        enum ckv_kind kind;
	bool force_reload;
	struct ckv* ckv;
} *onlineconfs = NULL;

static int onlineconfs_n = 0;
static int onlineconf_default = 0;

static uint32_t onlineconf_name_hash(const char *name);
static struct onlineconf* onlineconf_by_name(const char *name);

static int
onlineconf_get_generic(const char* name, const char* key, struct ckv_str* result, struct ckv_str* format)
{
	struct onlineconf *kv;
	if (name == NULL) name = nullname;
	kv = onlineconf_by_name(name);
	if (kv == NULL) {
                result->str = NULL;
                result->len = 0;
                return 0;
        }
	int key_len = strlen(key);
	return ckv_key_get(kv->ckv, key, key_len, result, format);
}

int
onlineconf_get(const char* name, const char* key, struct ckv_str* result)
{
	return onlineconf_get_generic(name, key, result, NULL);
}

int
onlineconf_get_json(const char* name, const char* key, struct ckv_str* result)
{
	struct ckv_str format = {NULL, 0};
	if (onlineconf_get_generic(name, key, result, &format)) {
		if ((format.len == 1 && format.str[0] == 'j') ||
			(format.len == 4 && memcmp(format.str, "json", 4) == 0)) {
			return 1;
		}
		result->str = NULL;
		result->len = 0;
	}
	return 0;
}

int
onlineconf_geti(const char* name, const char* key, int _default)
{
	struct ckv_str r = {NULL, 0};
	if (onlineconf_get_generic(name, key, &r, NULL)) {
		int sum = 0;
		int neg = 1;
		if (r.str[0] == '-') {
			neg = -1;
			r.str++;
			r.len--;
		} else if (r.str[0] == '+') {
			r.str++;
			r.len--;
		}
		static const int8_t dig[256] = {
			['0']=1, ['1']=2, ['2']=3, ['3']=4, ['4']=5,
			['5']=6, ['6']=7, ['7']=9, ['8']=9, ['9']=10,
		};
		for (; r.len && dig[(uint8_t)r.str[0]] != 0; r.str++, r.len--) {
			sum = (sum * 10) + (dig[(uint8_t)r.str[0]] - 1);
		}
		if (r.len == 0)
			return sum*neg;
	}
	return _default;
}

bool
onlineconf_registered(const char* name)
{
	struct onlineconf *kv;
        if (name == NULL) name = nullname;
	kv = onlineconf_by_name(name);
	return kv != NULL;
}

void
register_onlineconf_callback(onlineconf_cb_f cb)
{
	onlineconf_callbacks = xrealloc(onlineconf_callbacks, (onlineconf_callbacks_n + 1)*sizeof(cb));
	onlineconf_callbacks[onlineconf_callbacks_n] = cb;
	onlineconf_callbacks_n++;
}

static uint32_t
onlineconf_name_hash(const char *name) {
	uint32_t h = 5381;
	for (;*name; name++) {
		h = h*33 ^ *name;
	}
	return h;
}

static struct onlineconf*
onlineconf_by_name(const char *name) {
	uint32_t hash = onlineconf_name_hash(name);
	int i;
	for (i = 0; i < onlineconfs_n; i++) {
		if (onlineconfs[i].name_hash == hash &&
				(onlineconfs[i].name == name || // handle default namespace case
				strcmp(onlineconfs[i].name, name) == 0)) {
			return onlineconfs + i;
		}
	}
	return NULL;
}

static void
ckv_errcb(void* arg _unused_, const char* path, const char* call, int num)
{
	if (strcmp(call, "format") == 0) {
		say_error("onlineconf format %s:%d", path, num);
	} else {
		say_error("onlineconf %s(%s): %s", call, path, strerror_o(num));
	}
}

static void
onlineconf_callback(va_list arg)
{
	const char* name = va_arg(arg, const char*);
	int i;
	for(i=0; i<onlineconf_callbacks_n; i++) {
		onlineconf_callbacks[i](name);
	}
}

static bool
stat_same(const struct stat *oldstat, const struct stat *newstat)
{
	return oldstat->st_dev   == newstat->st_dev
	    && oldstat->st_ino   == newstat->st_ino
	    && oldstat->st_mode  == newstat->st_mode
	    && oldstat->st_size  == newstat->st_size
	    && oldstat->st_ctime == newstat->st_ctime;
}

static void
onlineconf_watch(ev_timer *w _unused_, int event _unused_)
{
	int i;
	struct onlineconf *kv = onlineconfs;
	assert(kv != NULL);
	for (i=0; i < onlineconfs_n; i++, kv++) {
		struct stat newstat, *oldstat;
		struct ckv *new = NULL;
		if (stat(kv->path, &newstat) == -1) {
			say_syserror("stat(%s)", kv->path);
			continue;
		}
		if (!kv->force_reload && kv->ckv != NULL) {
			oldstat = ckv_fstat(kv->ckv);
			if (stat_same(oldstat, &newstat)) {
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
			if (stat_same(oldstat, &newstat)) {
				break;
			}
			ckv_close(new);
		}
		if (new) {
			if (kv->ckv != NULL)
				ckv_close(kv->ckv);
			kv->ckv = new;
			kv->force_reload = false;
			fiber_create("onlineconf_callback", onlineconf_callback, kv->name);
		}
	}
}

static void
lua_onlineconf_callback(const char* name)
{
	lua_State *L = fiber->L;
	lua_getglobal(L, "onlineconf");
	lua_getfield(L, -1, "__call_callbacks");
	lua_remove(L, -2);
	lua_pushstring(L, name);
	lua_call(L, 1, 0);
}

static enum ckv_kind
ckv_kind_by_path(const char *path)
{
	const char* dotp = rindex(path, '.');
	if (dotp != NULL && strcmp(dotp, ".cdb") == 0)
		return CKV_CDB_BYTEFORMAT;
	else
		return CKV_TEXT_WITH_FORMAT;
}

static void
register_onlineconf_or_fail(const char* _name, const char* _path)
{
	assert(_name != NULL && _path != NULL);
	const char* name = _name ? strdup(_name) : nullname;
	const char* path = strdup(_path);
	assert(name != NULL && path != NULL);
	struct onlineconf *kv = onlineconf_by_name(name);
	if (kv != NULL) {
		panic("already registered onlineconf namespace '%s'", name);
	}
	onlineconfs = xrealloc(onlineconfs, (onlineconfs_n+1)*sizeof(*onlineconfs));
	kv = onlineconfs + onlineconfs_n;
	onlineconfs_n++;
	kv->name = name;
	kv->path = path;
	kv->name_hash = onlineconf_name_hash(name);
	kv->kind = ckv_kind_by_path(path);
	kv->ckv = ckv_open(path, kv->kind, 0, ckv_errcb, NULL);
	if (kv->ckv == NULL) {
		panic("registraction onlineconf namespace '%s' failed", name);
	}
	say_info("successfully registered onlineconf namespace '%s' at path '%s'",
			name, path);
	if (!ev_is_active(&onlineconf_timer)) {
		ev_timer_start(&onlineconf_timer);
	}
}

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
static int
check_name_path(const char* name, const char* path, struct mh_cstr_t *names, int res)
{
	extern void out_warning(int v, char *format, ...);
	struct stat pstat;
        int fd;

        if (!mh_cstr_put(names, name, NULL, NULL)) {
                out_warning(0, "onlineconf name '%s' duplicates", name);
                res = -1;
        }
        if (stat(path, &pstat) == -1) {
                out_warning(0, "onlineconf stat(%s): %s", path, strerror_o(errno));
                res = -1;
        }
        fd = open(path, O_RDONLY|O_CLOEXEC);
        if (fd == -1) {
                out_warning(0, "onlineconf read(%s): %s", path, strerror_o(errno));
                res = -1;
        } else {
                close(fd);
        }
        return res;
}

static bool
equal_any(const char *name, ...) {
        va_list va;
        bool any = false;
        va_start(va, name);
        while (!any) {
                const char *match = va_arg(va, const char*);
                if (match == NULL) break;
                any = strcmp(name, match) == 0;
        }
        va_end(va);
        return any;
}

static int
check_config(struct octopus_cfg *new)
{
	extern void out_warning(int v, char *format, ...);
	int i, res = 0;
	const char *name, *path;
	struct mh_cstr_t *names;
	if (new->onlineconf == NULL && onlineconf_default) {
		out_warning(0, "new onlineconf is empty, but old is not");
		return -1;
	}
	if (new->onlineconf_additional == NULL && (onlineconfs_n - onlineconf_default)) {
		out_warning(0, "new onlineconf_additional section is empty, but old is not");
		return -1;
	}
	names = mh_cstr_init(xrealloc);
	if (new->onlineconf != NULL) {
                res = check_name_path("", new->onlineconf, names, res);
	}
	for (i = 0; new->onlineconf_additional[i] != NULL; i++) {
		if (!CNF_STRUCT_DEFINED(new->onlineconf_additional[i]))
			continue;
		name = new->onlineconf_additional[i]->namespace;
		path = new->onlineconf_additional[i]->file;
                if (equal_any(name, "", "additional", "get", "json", "json_raw",
                                        "geti", "register_callback", "__call_callbacks",
                                        NULL)) {
                        out_warning(0, "onlineconf additional namespace "
                                        "could not be named '%s'", name);
                } else {
                        res = check_name_path(name, path, names, res);
                }
	}
	for (i = 0; i < onlineconfs_n; i++) {
		struct onlineconf *kv = onlineconfs + i;
		uint32_t pos = mh_cstr_get(names, kv->name);
		if (pos == mh_end(names)) {
			out_warning(0, "onlineconf old name '%s' is not defined in new config",
					kv->name);
			res = -1;
		}
	}
	mh_cstr_destroy(names);
	return res;
}

static void
load_name_path(const char* name, const char *path)
{
	struct onlineconf *kv;
        enum ckv_kind kind;
        kv = onlineconf_by_name(name);
        if (kv == NULL) {
                register_onlineconf_or_fail(name, path);
                return;
        }
        kind = ckv_kind_by_path(path);
        if (strcmp(kv->path, path) != 0 || kv->kind != kind) {
                free((void*)kv->path);
                kv->path = strdup(path);
                kv->kind = ckv_kind_by_path(path);
                kv->force_reload = true;
                /* will be reloaded at next timer hit */
        }
}

static void
load_config(struct octopus_cfg *new)
{
	int i;
	if (new->onlineconf != NULL) {
                load_name_path(nullname, new->onlineconf);
	}
	for (i = 0; new->onlineconf_additional[i] != NULL; i++) {
                const char *name, *path;
		if (!CNF_STRUCT_DEFINED(new->onlineconf_additional[i]))
			continue;
		name = new->onlineconf_additional[i]->namespace;
		path = new->onlineconf_additional[i]->file;
                load_name_path(name, path);
	}
}

static void
reload_config(struct octopus_cfg *old _unused_, struct octopus_cfg *new)
{
	load_config(new);
}

static void
init(void)
{
	ev_timer_init(&onlineconf_timer, onlineconf_watch, 2, 2);
	load_config(&cfg);
	switch (luaT_require("onlineconf")) {
	case 1: register_onlineconf_callback(lua_onlineconf_callback);
	case 0: break;
	case -1:
		panic("unable to load `ckv' lua module: %s", lua_tostring(fiber->L, -1));
	}
}

static struct tnt_module onlineconf_mod = {
	.name = "onlineconf",
	.init = init,
	.check_config = check_config,
	.reload_config = reload_config,
};
register_module(onlineconf_mod);

register_source();
