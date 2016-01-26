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

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <endian.h>
#if _POSIX_MAPPED_FILES > 0
#include <sys/mman.h>
#endif
#include "ckv.h"

struct ckv {
	void const * mem;
	uint32_t bsize;
	enum ckv_kind kind;
	int mmap;
	int size;
	struct ckv_text_item *items;
	uint32_t* hash;
	struct stat stat;
};

struct ckv_errcb {
	ckv_error_cb cb;
	const char* path;
	void* arg;
};

struct ckv_text_item {
	uint32_t hash;
	uint32_t next;
	uint32_t offset;
	uint16_t key_size;
	uint16_t fmt_offset, fmt_size;
	uint16_t val_offset;
	uint32_t val_size;
};

static uint32_t
cdb_hash(uint8_t const* p, int len) {
	uint8_t const *end = p + len;
	uint32_t hash = 5381;
	for (;p < end; p++) {
		hash = (hash * 33) ^ *p;
	}
	return hash;
}

struct cdb_pair {
	uint32_t a;
	uint32_t b;
};

static void
read_cdb_pair(void const* p, struct cdb_pair *pair) {
	pair->a = *(uint32_t __attribute__((packed))*)p;
	pair->b = *(uint32_t __attribute__((packed))*)(p + 4);
#if __BYTE_ORDER == __BIG_ENDIAN
	pair->a = hton(pair->a);
	pair->b = hton(pair->b);
#endif
}

/* check cdb format, count entries */
static int
ckv_cdb_check(struct ckv* ckv, struct ckv_errcb* errcb) {
	int i;
	struct cdb_pair p;
#if CKV_CDB_CHECK > 0
	struct cdb_pair h;
	void const *hp;
	int j;
#if CKV_CDB_CHECK > 1
	struct cdb_pair kv;
#endif
#endif
	void const *pp = ckv->mem;
	assert(ckv->size == 0);
	for (i=0; i < 256; i++, pp += 8) {
		read_cdb_pair(pp, &p);
		if (p.a > ckv->bsize) {
			errcb->cb(errcb->arg, errcb->path, "format", 0);
			return 0;
		}
		if ((ckv->bsize - p.a) / 8 < p.b) {
			errcb->cb(errcb->arg, errcb->path, "format", 0);
			return 0;
		}
#if CKV_CDB_CHECK > 0
		hp = ckv->mem + p.a;
		for (j = 0; j < p.b; j++, hp += 8) {
			read_cdb_pair(hp, &h);
			if (h.b == 0)
				continue;
			if (h.b > ckv->bsize - 8) {
				errcb->cb(errcb->arg, errcb->path, "format", 0);
				return 0;
			}
#if CKV_CDB_CHECK > 1
			read_cdb_pair(ckv->mem + h.b, &kv);
			if (ckv->bsize - h.b < kv.a ||
					ckv->bsize - h.b < kv.a + kv.b) {
				errcb->cb(errcb->arg, errcb->path, "format", 0);
				return 0;
			}
#endif
			ckv->size++;
		}
#else
		ckv->size += p.b/2;
#endif
	}
	return 1;
}

static int
ckv_text_parse(struct ckv* ckv, struct ckv_errcb* errcb) {
	assert(ckv->items == 0);
	assert(ckv->size == 0);
	unsigned char const *mem = ckv->mem;
	unsigned char const *line = mem, *en=NULL, *er=NULL, *endline;
	unsigned char const *cur, *format, *keyend, *sep;
	unsigned char const *end = line + ckv->bsize;
	int lines = 0;

	endline = line;
	while (endline < end && (endline = memchr(endline, '\n', end-endline))) {
		lines++;
		if (lines == 0x7fffffff) {
			errcb->cb(errcb->arg, errcb->path, "format", lines);
			return 0;
		}
		endline++;
	}
	if (endline < end) lines++;

	ckv->items = calloc(lines, sizeof(*ckv->items));
	if (ckv->items == NULL) {
		errcb->cb(errcb->arg, errcb->path, "calloc", errno);
		return 0;
	}
	lines |= 1;
	ckv->hash = calloc(lines+1, sizeof(uint32_t));
	if (ckv->hash == NULL) {
		errcb->cb(errcb->arg, errcb->path, "calloc", errno);
		return 0;
	}
	ckv->hash[0] = lines; /* size */
	lines = 0;
	for (;line < end; line = endline + 1) {
		if (en < line) {
			en = memchr(line, '\n', end-line);
			if (en == NULL) en = end;
		}
		if (er < line) {
			er = memchr(line, '\r', end-line);
			if (er == NULL) er = end;
		}
		while (line[0] == ' ' || line[0] == '\t') line++;
		endline = en < er ? en : er;
		if (endline == line) {
			if (line == en) lines++;
			continue;
		}
		lines++;
		if (line[0] == '#') {
			continue;
		}
		static char const jmp1[256] = { [' '] = 1, ['\t'] = 1, [':'] = 2 };
		cur = line;
		format = NULL;
		keyend = NULL;
		uint32_t hash = 5381;
		if (ckv->kind == CKV_TEXT_WITH_FORMAT)
			while (cur != endline && jmp1[*cur] == 0) {
				hash = (hash * 33) ^ *cur;
				cur++;
			}
		else /* CKV_TEXT_NOFORMAT */
			while (cur != endline && jmp1[*cur] != 1) {
				hash = (hash * 33) ^ *cur;
				cur++;
			}
		if (cur != endline && jmp1[*cur] == 2) {
			keyend = cur;
			format = cur+1;
			cur++;
			while (cur != endline && jmp1[*cur] != 1) cur++;
		}
		if (cur == endline || jmp1[*cur] != 1) {
			errcb->cb(errcb->arg, errcb->path, "format", lines);
			return 0;
		}
		sep = cur;
		if (sep-line > 0xffff) {
			errcb->cb(errcb->arg, errcb->path, "format", lines);
			return 0;
		}
		if (keyend == NULL) keyend = cur;
		if (format == NULL) format = cur;
		while (cur != endline && jmp1[*cur] == 1) cur++;
		struct ckv_text_item* item = ckv->items + ckv->size;
		item->offset = line - mem;
		item->key_size = keyend - line;
		item->fmt_offset = format - line;
		item->fmt_size = sep - format;
		item->val_offset = cur - line;
		item->val_size = endline - cur;
		item->hash = hash; //equal to cdb_hash((uint8_t const*)line, item->key_size);
		uint32_t pos = item->hash % ckv->hash[0] + 1;
		item->next = ckv->hash[pos];
		ckv->hash[pos] = ckv->size+1;
		ckv->size++;
	}
	return 1;
}

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

struct ckv*
ckv_open(char const *path, enum ckv_kind kind, enum ckv_try_mmap try_mmap, ckv_error_cb ecb, void* ecbarg) {
	struct ckv_errcb errcb = {ecb, path, ecbarg};
	struct stat fdstat;
	int fd = open(path, O_RDONLY|O_CLOEXEC);
	if (fd == -1) {
		ecb(ecbarg, path, "open", errno);
		return NULL;
	}
	if (fstat(fd, &fdstat) == -1) {
		ecb(ecbarg, path, "fstat", errno);
		close(fd);
		return NULL;
	}
	if (fdstat.st_size > 0x7fffffff) {
		ecb(ecbarg, path, "format", 0);
		close(fd);
		return NULL;
	}
	if ((kind == CKV_CDB_NOFORMAT || kind == CKV_CDB_BYTEFORMAT) && fdstat.st_size < 2048) {
		ecb(ecbarg, path, "format", 0);
		close(fd);
		return NULL;
	}
	struct ckv *ckv = calloc(1, sizeof(struct ckv));
	if (ckv == NULL) {
		ecb(ecbarg, path, "calloc", errno);
		close(fd);
		return NULL;
	}
	if (try_mmap == CKV_MMAP_MALLOC_ON_FAIL &&
			(kind == CKV_TEXT_WITH_FORMAT || kind == CKV_TEXT_NOFORMAT) &&
			(fdstat.st_mode & ~0444) != 0) {
		try_mmap = CKV_MALLOC_ONLY;
	}
	ckv->bsize = fdstat.st_size;
	ckv->kind = kind;
	memcpy(&ckv->stat, &fdstat, sizeof(fdstat));
#if _POSIX_MAPPED_FILES > 0
	if (try_mmap == CKV_MMAP_MALLOC_ON_FAIL || try_mmap == CKV_MMAP_OR_FAIL) {
		ckv->mem = mmap(NULL, fdstat.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
		if (ckv->mem != NULL) {
			ckv->mmap = 1;
		} else if (ckv->mem == NULL && try_mmap == CKV_MMAP_OR_FAIL) {
			ecb(ecbarg, path, "mmap", errno);
			close(fd);
			free(ckv);
			return NULL;
		}
	}
#else
	if (try_mmap == CKV_MMAP_OR_FAIL) {
		ecb(ecbarg, path, "nommap", EINVAL);
		return NULL;
	}
#endif
	if (ckv->mem == NULL) {
		void* mem = calloc(1, ckv->bsize);
		if (mem == NULL) {
			ecb(ecbarg, path, "calloc", errno);
			close(fd);
			free(ckv);
			return NULL;
		}
		ssize_t rd = read(fd, mem, ckv->bsize);
		close(fd);
		if (rd < ckv->bsize) {
			ecb(ecbarg, path, "read", rd < 0 ? errno : 0);
			close(fd);
			free(mem);
			free(ckv);
			return NULL;
		}
		ckv->mem = mem;
	}
	close(fd);
	int parsed = (kind == CKV_TEXT_WITH_FORMAT || kind == CKV_TEXT_NOFORMAT) ?
		ckv_text_parse(ckv, &errcb) :
		ckv_cdb_check(ckv, &errcb);
	if (!parsed)
		goto error;
	return ckv;
error:
	if (ckv->items) {
		free(ckv->items);
		free(ckv->hash);
	}
#if _POSIX_MAPPED_FILES > 0
	if (ckv->mmap)
		munmap((void*)ckv->mem, ckv->bsize);
	else
#endif
		free((void*)ckv->mem);
	free(ckv);
	return NULL;
}

void
ckv_close(struct ckv* ckv) {
	if (ckv->items) {
		free(ckv->items);
		free(ckv->hash);
	}
#if _POSIX_MAPPED_FILES > 0
	if (ckv->mmap)
		munmap((void*)ckv->mem, ckv->bsize);
	else
#endif
		free((void*)ckv->mem);
	free(ckv);
}

struct stat*
ckv_fstat(struct ckv* ckv) {
	return &ckv->stat;
}

int
ckv_size(struct ckv* ckv) {
	return ckv->size;
}

static int
ckv_text_get(struct ckv *ckv, char const* key, int key_len, struct ckv_str* val, struct ckv_str* fmt) {
	assert(ckv->hash != NULL);
	assert(ckv->items != NULL);
	uint32_t hash = cdb_hash((uint8_t const*)key, key_len);
	uint32_t pos = ckv->hash[hash % ckv->hash[0] + 1];
	while (pos != 0) {
		struct ckv_text_item *item = ckv->items + (pos-1);
		if (item->hash == hash && item->key_size == key_len) {
			char const * stkey = ckv->mem + item->offset;
			if (memcmp(stkey, key, key_len) == 0) {
				if (val != NULL) {
					val->str = stkey + item->val_offset;
					val->len = item->val_size;
				}
				if (fmt != NULL) {
					fmt->str = stkey + item->fmt_offset;
					fmt->len = item->fmt_size;
				}
				return 1;
			}
		}
		pos = item->next;
	}
	if (val != NULL) {
		val->str = NULL;
		val->len = 0;
	}
	if (fmt != NULL) {
		fmt->str = NULL;
		fmt->len = 0;
	}
	return 0;
}

static int
ckv_cdb_get(struct ckv *ckv, char const* key, int key_len, struct ckv_str* val, struct ckv_str* fmt) {
	uint32_t hash = cdb_hash((uint8_t const*)key, key_len);
	struct cdb_pair p, h, kv;
	read_cdb_pair(ckv->mem + (hash&0xff)*8, &p);
	if (p.b == 0) {
		goto notfound;
	}
	uint32_t pos = (hash >> 8) % p.b;
	for (;;) {
		read_cdb_pair(ckv->mem + p.a + pos*8, &h);
		if (h.b == 0)
			goto notfound;
		if (h.a == hash) {
			char const * stkey = ckv->mem + h.b + 8;
			read_cdb_pair(ckv->mem + h.b, &kv);
			if (key_len == kv.a && memcmp(stkey, key, key_len) == 0) {
				if (val != NULL) {
					val->str = stkey + kv.a;
					val->len = kv.b;
					if (ckv->kind == CKV_CDB_BYTEFORMAT) {
						val->str++;
						val->len--;
					}
				}
				if (fmt != NULL) {
					if (ckv->kind == CKV_CDB_BYTEFORMAT) {
						fmt->str = stkey + kv.a;
						fmt->len = 1;
					} else {
						fmt->str = NULL;
						fmt->len = 0;
					}
				}
				return 1;
			}
		}
		pos++;
		if (pos == p.b)
			pos = 0;
	}
notfound:
	if (val != NULL) {
		val->str = NULL;
		val->len = 0;
	}
	if (fmt != NULL) {
		fmt->str = NULL;
		fmt->len = 0;
	}
	return 0;
}

int
ckv_key_get(struct ckv *ckv, char const* key, int key_len, struct ckv_str* val, struct ckv_str* fmt) {
	switch (ckv->kind) {
	case CKV_TEXT_NOFORMAT:
	case CKV_TEXT_WITH_FORMAT:
		return ckv_text_get(ckv, key, key_len, val, fmt);
	case CKV_CDB_NOFORMAT:
	case CKV_CDB_BYTEFORMAT:
		return ckv_cdb_get(ckv, key, key_len, val, fmt);
	default:
		assert(0);
	}
}

