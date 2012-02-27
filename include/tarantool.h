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

#import <config.h>
#import <tbuf.h>
#import <util.h>
#import <cfg/tarantool_cfg.h>

#include <stdbool.h>

struct tnt_object {
	i32 refs;
	u8 type;
	u8 flags;
	u8 data[0];
} __attribute__((packed));


struct log_io_iter;

struct tnt_module {
	struct tnt_module *next;
	char *name;
	void (*init)(void);
	i32  (*check_config)(struct tarantool_cfg *conf);
	void (*reload_config)(struct tarantool_cfg *old_conf, struct tarantool_cfg *new_conf);
	int  (*cat)(const char *filename);
	void (*snapshot)(bool);
	void (*info)(struct tbuf *out);
	void (*exec)(char *str, int len, struct tbuf *out);
};
struct tnt_module *modules_head;
struct tnt_module *module(const char *);
void register_module_(struct tnt_module *);
#define register_module(name)				\
	__attribute__((constructor)) static void	\
	register_module##name(void) {			\
		register_module_(&name);		\
	}

extern lua_State *root_L;
struct lua_src {
	const char *name;
	void *start;
	size_t size;
};
extern struct lua_src *lua_src;

extern struct tarantool_cfg cfg;
extern struct tbuf *cfg_out;
extern const char *cfg_filename;
extern char *custom_proc_title;
extern bool init_storage, booting;
extern char *binary_filename;
i32 reload_cfg();
int save_snapshot(void *ev __attribute__((unused)), int events __attribute__((unused)));
const char *tarantool_version(void);
void tarantool_info(struct tbuf *out);
unsigned tnt_uptime(void);

char **init_set_proc_title(int argc, char **argv);
void set_proc_title(const char *format, ...);



struct tnt_object *object_alloc(u8 type, size_t size);
void object_ref(struct tnt_object *obj, int count);
void object_incr_ref(struct tnt_object *obj);
void object_decr_ref(struct tnt_object *obj);

enum tnt_object_flags {
	WAL_WAIT = 0x1,
	GHOST = 0x2
};
static inline bool ghost(struct tnt_object *obj)
{
	return obj->flags & GHOST;
}
