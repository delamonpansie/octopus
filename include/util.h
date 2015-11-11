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

#ifndef UTIL_H
#define UTIL_H

#include <config.h>

#ifndef HAVE_ALLOCA
#error Working alloca() required
#endif

#ifdef HAVE_ALLOCA_H
# include <alloca.h>
#elif defined __GNUC__
# undef alloca
# define alloca __builtin_alloca
#elif defined _AIX
# define alloca __alloca
#elif defined _MSC_VER
# include <malloc.h>
# define alloca _alloca
#else
# include <stddef.h>
void *alloca (size_t);
#endif

#include <unistd.h>
#include <stddef.h>

void *xcalloc(size_t nmemb, size_t size);
void *xmalloc(size_t size);
void *xrealloc(void *ptr, size_t size);

#ifndef MAX
# define MAX(_a, _b) ({ __typeof__(_a) a_ = (_a), b_ = (_b); a_ >= b_ ? a_ : b_; })
# define MIN(_a, _b) ({ __typeof__(_a) a_ = (_a), b_ = (_b); a_ <= b_ ? a_ : b_; })
# define CMP(_a, _b) ({ __typeof__(_a) a_ = (_a), b_ = (_b); a_ < b_ ? -1 : (a_ == b_ ? 0 : 1); })
#endif

/* Macros to define enum and corresponding strings. */
#ifndef ENUM_INITIALIZER
#  define ENUM_DEF(s, v, d...) s = v,
#  define ENUM_INITIALIZER(define) { define(ENUM_DEF) }
#endif
#ifndef ENUM_STR_INITIALIZER
#  define ENUM_STR_DEF(s, v, d...) [s] = #s,
#  define ENUM_STR_INITIALIZER(define) { define(ENUM_STR_DEF) }
#endif
#ifndef ENUM_DESCR_INITIALIZER
#  define ENUM_DESCR_DEF(s, v, d...) [s] = d,
#  define ENUM_DESCR_INITIALIZER(define) { define(ENUM_DESCR_DEF) }
#endif

/* Macros for printf functions */
#include <inttypes.h>
#if SIZEOF_OFF_T == 8
#  define PRIofft PRIi64
#else
#  define PRIofft PRIi32
#endif

/* Lua wants to allocate in lower 2GB, so our allocation
   should prefer addreses above */
#if SIZEOF_VOID_P == 8
#define MMAP_HINT_ADDR	((void*)((uintptr_t)0x100000000ULL))
#else
#define	MMAP_HINT_ADDR	NULL
#endif

#if HAVE__ATTRIBUTE_COLD
#  define oct_cold __attribute__((cold))
#else
#  define oct_cold
#endif

#define _unused_ __attribute__((__unused__))

#if HAVE__BUILTIN_EXPECT
#  define likely(x)    __builtin_expect((x),1)
#  define unlikely(x)  __builtin_expect((x),0)
#else
#  define likely(x)    (x)
#  define unlikely(x)  (x)
#endif

#define field_sizeof(compound_type, field) sizeof(((compound_type *)NULL)->field)

#ifndef offsetof
#define offsetof(type, member) ((size_t) &((type *)0)->member)
#endif

#ifndef __offsetof
#define __offsetof offsetof
#endif

#define nelem(x)     (sizeof((x))/sizeof((x)[0]))

#ifndef container_of
#define container_of(ptr, type, member) ({			\
	const typeof( ((type *)0)->member ) *__mptr = (ptr);	\
	(type *)( (char *)__mptr - offsetof(type, member) );	\
})
#endif

#ifndef TYPEALIGN
#define TYPEALIGN(ALIGNVAL,LEN)  \
        (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN(LEN)                 TYPEALIGN(sizeof(int16_t), (LEN))
#define INTALIGN(LEN)                   TYPEALIGN(sizeof(int32_t), (LEN))
#define MAXALIGN(LEN)                   TYPEALIGN(sizeof(int64_t), (LEN))
#define PTRALIGN(LEN)                   TYPEALIGN(sizeof(void*), (LEN))
#define CACHEALIGN(LEN)			TYPEALIGN(32, (LEN))
#endif

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

#ifndef HAVE_STRDUPA
# define strdupa(s)							\
({									\
	    const char *__orig = (s);					\
	    size_t __len = strlen(__orig) + 1;				\
	    char *__new = (char *)alloca(__len);			\
	    (char *)memcpy(__new, __orig, __len);			\
})
#endif

#define CRLF "\r\n"

#ifdef GCC
# define FORMAT_PRINTF gnu_printf
#else
# define FORMAT_PRINTF printf
#endif

void close_all_xcpt(int fdc, ...);
void maximize_core_rlimit();
int coredump(int dump_interval);

void __gcov_flush();

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif


extern pid_t master_pid;
pid_t oct_fork();
#ifdef OCT_CHILDREN
void keepalive(void);
void keepalive_read(void);
#endif

extern volatile int gdb_wait_lock;
void wait_gdb(void);

double drand(double top);

const char *tnt_backtrace(void);

#ifdef HAVE_LIBELF
struct symbol {
	void *addr;
	const char *name;
	void *end;
};
struct symbol *addr2symbol(void *addr);
void load_symbols(const char *name);
#endif

#ifdef NDEBUG
#  define assert(pred) (void)(0)
#else
#  define assert(pred) ((pred) ? (void)(0) :				\
			assert_fail(#pred, __FILE__, __LINE__, __FUNCTION__))
void assert_fail(const char *assertion, const char *file,
		 unsigned int line, const char *function)
	__attribute__ ((noreturn));
#endif

#define atoh(str, len) ({				\
	char *dst = alloca(len * 3 + 1);		\
	for (int i = 0; i < len; i++)			\
		sprintf(dst + i * 3, "%02x ",		\
			*((char *)str + i));		\
	dst[len * 3] = 0;				\
	dst;						\
})

void title(const char *fmt, ...);

#ifdef THREADS
const char* strerror_o(int eno);
#else
#define strerror_o(eno) strerror(eno)
#endif

#endif
