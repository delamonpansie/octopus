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

#ifndef CONSTANT_KV_H
#define CONSTANT_KV_H

#include <third_party/ckv/ckv.h>

void register_constant_kv_or_fail(const char* _name, const char* _path, enum ckv_kind kind);

// callback will accept name of registered constant_kv file.
// it should recheck that name matches.
typedef void (*constant_kv_cb_f)(const char* name);
// register function which will be called on onlineconf file change
void register_constant_kv_callback(constant_kv_cb_f cb);

bool constant_kv_registered(const char* name);

// get key value.
// if key_len <= 0 then strlen(name) is called
// returns 0 if key exists, 1 if key doesn't exists, 2 if kv is not registered
int constant_kv_get(const char* name, const char* key, int key_len, struct ckv_str* result, struct ckv_str* format);
// get integer value
// if no key exists, then _default returned
int constant_kv_geti(const char* name, const char* key, int key_len, int _default);
#endif
