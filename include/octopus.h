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

#ifndef OCTOPUS_H
#define OCTOPUS_H

#include <util.h>
#include <tbuf.h>
#include <cfg/octopus.h>

#include <stdbool.h>

struct tnt_object {
	int32_t refs;
	uint8_t type;
	uint8_t flags;
	uint8_t data[0];
} __attribute__((packed));

enum {
	TNT_MODULE_WAITING = 0,
	TNT_MODULE_INPROGRESS = 1,
	TNT_MODULE_INITED = 2,
};

struct tnt_module {
	struct tnt_module *next;
	const char *name, *version, **init_before, **depend_on;
	int _state;
	void (*init)(void);
	i32  (*check_config)(struct octopus_cfg *conf);
	void (*reload_config)(struct octopus_cfg *old_conf, struct octopus_cfg *new_conf);
	int  (*cat)(const char *filename);
	int  (*cat_scn)(i64 scn);
	void (*info)(struct tbuf *out, const char *what);
	void (*exec)(char *str, int len, struct tbuf *out);
};
struct tnt_module *modules_head, *current_module;
struct tnt_module *module(const char *);
void module_init(struct tnt_module *);
void register_module_(struct tnt_module *);
#define register_module(name)				\
	__attribute__((constructor)) static void	\
	register_module##name(void) {			\
		register_module_(&name);		\
	}
#define foreach_module(m) for (struct tnt_module *m = modules_head; m != NULL; m = m->next)

extern struct octopus_cfg cfg;
extern struct tbuf *cfg_out;
extern const char *cfg_filename;
extern bool init_storage, booting;
extern char *binary_filename;

extern char *cfg_err;
extern int cfg_err_len;
int reload_cfg(void);

void octopus_ev_init(void);
const char *octopus_version(void);
void octopus_info(struct tbuf *out);
unsigned tnt_uptime(void);

char **init_set_proc_title(int argc, char **argv);
void set_proc_title(const char *format, ...);

struct tnt_object *object_alloc(u8 type, size_t size);
void object_ref(struct tnt_object *obj, int count);
void object_incr_ref(struct tnt_object *obj);
void object_decr_ref(struct tnt_object *obj);
void object_lock(struct tnt_object *obj);
void object_yield(struct tnt_object *obj);
void object_unlock(struct tnt_object *obj);

enum tnt_object_flags {
	WAL_WAIT = 0x1,
	GHOST = 0x2,
	YIELD = 0x4
};

static inline bool ghost(struct tnt_object *obj)
{
	return obj->flags & GHOST;
}

void zero_io_collect_interval();
void unzero_io_collect_interval();

extern lua_State *root_L;
struct lua_src {
	const char *name;
	const char *start;
	size_t size;
};
extern struct lua_src *lua_src;

void luaT_init(void);
void luaT_pushobject(struct lua_State *L, struct tnt_object *obj);
int luaT_objinit(struct lua_State *L);
int luaT_require(const char *filename);
void luaT_require_or_panic(const char *filename, bool panic_on_missing, const char *error_format);
int luaT_find_proc(lua_State *L, const char *fname, i32 len);
int luaT_traceback(lua_State* L);
void luaT_pushtraceback(lua_State* L);

#endif
