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

#ifndef ONLINECONF_H
#define ONLINECONF_H

#include <third_party/ckv/ckv.h>

// callback will accept name of registered onlineconf file.
// it should recheck that name matches.
typedef void (*onlineconf_cb_f)(const char* name);
// register function which will be called on onlineconf file change
void register_onlineconf_callback(onlineconf_cb_f cb);

bool onlineconf_registered(const char* name);

// get key value.
// returns 1 if key exists, 0 if key doesn't exists
int onlineconf_get(const char* name, const char* key, struct ckv_str* result);
// get key value if format it json
int onlineconf_get_json(const char* name, const char* key, struct ckv_str* result);
// get integer value
// if no key exists, then _default returned
int onlineconf_geti(const char* name, const char* key, int _default);
#endif
