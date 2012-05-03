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

#ifndef HAVE_ALLOCA
#error Working alloca() required
#endif

#ifdef HAVE_ALLOCA_H
# include <alloca.h>
#elif defined __GNUC__
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

#ifndef MAX
# define MAX(a, b) ((a) > (b) ? (a) : (b))
# define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Macros to define enum and corresponding strings. */
#define ENUM_MEMBER(s, v, d...) s = v,
#define ENUM_STRS_MEMBER(s, v, d...) [s] = #s,
#define ENUM_DESC_STRS_MEMBER(s, v, d...) [s] = d,
#define ENUM(enum_name, enum_members) enum enum_name {enum_members(ENUM_MEMBER) enum_name##_MAX}
#define STRS(enum_name, enum_members) \
	char *enum_name##_strs[enum_name##_MAX + 1] = {enum_members(ENUM_STRS_MEMBER) '\0'}
#define DESC_STRS(enum_name, enum_members) \
	char *enum_name##_desc_strs[enum_name##_MAX + 1] = {enum_members(ENUM_DESC_STRS_MEMBER) '\0'}

/* Macros for printf functions */
#include <inttypes.h>
#if SIZEOF_OFF_T == 8
#  define PRIofft PRIi64
#else
#  define PRIofft PRIi32
#endif

#define nelem(x)     (sizeof((x))/sizeof((x)[0]))
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

#ifndef lengthof
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
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
void coredump(int dump_interval);

void __gcov_flush();

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif


extern pid_t master_pid;
pid_t tnt_fork();
void keepalive(void);

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

void _panic(int status, const char *filename, unsigned line,
	    const char *error, const char *backtrace, const char *format, ...)
	__attribute__((format(FORMAT_PRINTF, 6, 7), noreturn));

#define panic(...)					\
	_panic(EXIT_FAILURE, __FILE__, __LINE__,	\
	       NULL, tnt_backtrace(), __VA_ARGS__)
#define panic_exc(exc)							\
	_panic(EXIT_FAILURE, (exc)->file, (exc)->line,			\
	       NULL, (exc)->backtrace, "exception: %s", (exc)->reason)
#define panic_status(status, ...)			\
	_panic(status, __FILE__, __LINE__,		\
	       NULL, tnt_backtrace(), __VA_ARGS__)
#define panic_syserror(...)						\
	_panic(EXIT_FAILURE, __FILE__, __LINE__,			\
	       strerror(errno), tnt_backtrace(), __VA_ARGS__)

#ifdef NDEBUG
#  define assert(pred) (void)(0)
#else
#  define assert(pred) ((pred) ? (void)(0) :				\
			assert_fail(#pred, __FILE__, __LINE__, tnt_backtrace(), __FUNCTION__))
void assert_fail(const char *assertion, const char *file,
		 unsigned int line, const char *backtrace, const char *function)
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
