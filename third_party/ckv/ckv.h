/*
 * Copyright (C) 2015 Mail.RU
 * Copyright (C) 2015 Sokolov Yuriy aka funny_falcon
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

/* Utility to access text key-value files or cdb files
 * https://cr.yp.to/cdb.html
 * it always mmap file into memory, and returns interior pointers.
 */

enum ckv_kind {
	/* TEXT_WITH_FORMAT - text file with lines like
	 *     key value
	 *     key:format value
	 * where 'format' is custom hint for value parsing
	 */
	CKV_TEXT,
	CKV_TEXT_NOFORMAT,
	/* CDB - just cdb file */
	CKV_CDB_NOFORMAT,
	/* CDB_BYTEFORMAT - also cdb file, but first value byte counts as a format string */
	CKV_CDB_BYTEFORMAT
};

enum ckv_try_mmap {
	CKV_MMAP_MALLOC_ON_FAIL = 0,
	CKV_MMAP_OR_FAIL = 1,
	CKV_MALLOC_ONLY = 2,
};

struct ckv;
struct ckv_str {
	char const * str;
	int len;
};

/* callback to print error,
 * call = syscall name or "format"
 * num - line number if call is "format" or errno
 *       for cdb if call is "format" then num is always 0
 */
typedef void (*ckv_error_cb)(void *arg, const char* path, const char* call, int num);

/* opens file at `path`, reads content, prepares internal structs
 * returns pointer to ckv if all is ok
 * returns NULL otherwise, and errcb is called */
struct ckv* ckv_open(char const * path, enum ckv_kind kind, enum ckv_try_mmap try_mmap, ckv_error_cb errcb, void* errcbarg);
/* closes file, frees all internal structs */
void ckv_close(struct ckv* ckv);
/* returns pointer to struct stat - result of fstat */
struct stat* ckv_fstat(struct ckv* ckv);
/* returns number of keys */
int ckv_size(struct ckv* ckv);

/* reads key value, sets val and format to appropriate values
 * returns 1 if key exists, 0 otherwise */
int ckv_key_get(struct ckv *ckv, char const* key, int key_len, struct ckv_str* val, struct ckv_str* format);
/* reads key value and convert it to int, returns `_default` if no key exists */
int ckv_key_get_atoi(struct ckv *ckv, char const* key, int key_len, int _default);
