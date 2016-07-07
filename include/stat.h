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

#ifndef STAT_H
#define STAT_H

#include <util.h>
#include <tbuf.h>

void stat_init(void);

typedef void (*stat_get_current_callback)(int base);

/* registers callback based stat, and returns it's position for fast access */
int stat_register_callback(char const *base_name, stat_get_current_callback cb);

/* registers name based stat, returns position for fast access */
int stat_register_named(char const * base_name);

/* registers static-offset based stat, returns position for fast access */
int stat_register_static(char const * base_name, char const * const * opnames, size_t count);

/* api for named stat */
void stat_sum_named(int base, char const * name, int len, double value);
void stat_gauge_named(int base, char const * name, int len, double value);
void stat_aggregate_named(int base, char const * name, int len, double value);

/* api for static-offset based stat */
void stat_sum_static(int base, int name, double value);
void stat_gauge_static(int base, int name, double value);
void stat_aggregate_static(int base, int name, double value);

/* api for callback based stat */
/* stat_report_sum should be called by callback, registered with stat_register_callback.
 * base is then current base.
 * value should be accumulated value, and it will be divided by length of period */
void stat_report_sum(char const * name, int len, double value);
/* stat_report_gauge should be called by callback, registered with stat_register_callback.
 * base is then current base.
 * value should be exact value that doesn't depend on period length */
void stat_report_gauge(char const * name, int len, double value);
/* stat_report_aggregate should be called by callback, registered with stat_register_callback.
 * base is then current base.
 * it should report sum, count, min and max */
void stat_report_aggregate(char const * name, int len, double sum, i64 cnt, double min, double max);
/* stat_current_base is a base for stat_report_* functions */
extern int stat_current_base;
char const* stat_name_of_base(int base);

/* you can use STAT_STR like: stat_collect_named(base, STAT_STR("myparam"), value) */
#define STAT_STR(str) (str), strlen(str)

/* backward compatible api */
/* mostly equivalent to stat_register_static("stat", opnames, count) */
int stat_register(char const * const *opnames, size_t count);
void stat_collect(int base, int name, i64 value);
/* should be separate name from stat_collect */
void stat_collect_double(int base, int name, double value);
void stat_print(struct tbuf *buf);

#if CFG_lua_path
void stat_lua_callback(int base);
#endif

#endif
