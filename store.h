/*
 * Copyright (C) 2010, 2011, 2012, 2013 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013 Yuriy Vostrikov
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

#ifndef OCTOPUS_MEMCACHED_H
#define OCTOPUS_MEMCACHED_H

#include <util.h>
#include <index.h>

extern CStringHash *mc_index;

enum object_type {
	MC_OBJ = 1
};

struct mc_obj {
	u32 exptime;
	u32 flags;
	u64 cas;
	u16 key_len; /* including \0 */
	u16 suffix_len;
	u32 value_len;
	char data[0]; /* key + '\0' + suffix + '\n' +  data + '\n' */
} __attribute__((packed));

static inline struct mc_obj * __attribute__((always_inline))
mc_obj(struct tnt_object *obj)
{
	if (unlikely(obj->type != MC_OBJ))
		abort();
	return (struct mc_obj *)obj->data;
}

static inline int
mc_len(const struct mc_obj *m) { return sizeof(*m) + m->key_len + m->suffix_len + m->value_len; }

static inline const char *
mc_value(const struct mc_obj *m) { return m->data + m->key_len + m->suffix_len; }

static inline bool
expired(struct tnt_object *obj)
{
#ifdef MEMCACHE_NO_EXPIRE
	(void)obj;
	return 0;
#else
	struct mc_obj *m = mc_obj(obj);
 	return m->exptime == 0 ? 0 : m->exptime < ev_now();
#endif
}

int store(char *key, u32 exptime, u32 flags, u32 value_len, char *value);
int delete(char **keys, int n);
void flush_all(va_list ap);

extern struct mc_stats {
	u64 total_items;
	u32 curr_connections;
	u32 total_connections;
	u64 cmd_get;
	u64 cmd_set;
	u64 get_hits;
	u64 get_misses;
	u64 evictions;
	u64 bytes_read;
	u64 bytes_written;
} mc_stats;
void print_stats(struct conn *c);

int __attribute__((noinline)) memcached_dispatch(struct conn *c);
#endif
