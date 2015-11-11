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

#ifndef SAY_H
#define SAY_H

#include <util.h>
#include <octopus.h>

#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>


enum say_level {
	TRACE = 0,		/* hardcore debugging */
	FATAL = 1,		/* do not this value use directly */
	ERROR,
	WARN,
	INFO,
	DEBUG,
	DEBUG2,
	DEBUG3
};

extern int stderrfd, sayfd, max_level, default_level, dup_to_stderr;
static int local_level = INFO;

void say_logger_init(int nonblock);
void vsay(int level, const char *filename, unsigned line, const char *error,
	  const char *format, va_list ap)
	__attribute__ ((format(FORMAT_PRINTF, 5, 0)));
void _say(int level, const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 4, 5)));
void _say_err(int level, const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 4, 5)));

void say_TRACE(const char *filename, unsigned line, const char *format, ...)
	__attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_ERROR(const char *filename, unsigned line, const char *format, ...)
	__attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_ERRORno(const char *filename, unsigned line, const char *format, ...) /* appends strerror(errno) */
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_WARN(const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_WARNno(const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_DEBUG(const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_DEBUG2(const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_DEBUG3(const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));
void say_INFO(const char *filename, unsigned line, const char *format, ...)
    __attribute__ ((format(FORMAT_PRINTF, 3, 4)));


int say_level_source(const char *file, int diff);
void say_list_sources(void);
void say_register_source(const char *file, int *level);
#define register_source()				\
	__attribute__((constructor)) static void	\
	register_source_(void) {			\
		say_register_source(__FILE__, &local_level); \
	}


#define will_say(level)         (local_level >= level || level <= FATAL)
#define say(suffix, level, ...) ({ if(unlikely(will_say(level)))	\
				say_##level##suffix(__FILE__, __LINE__, __VA_ARGS__); })
#define say_syserror(...)	say(no, ERROR, __VA_ARGS__)
#define say_syswarn(...)	say(no, WARN, __VA_ARGS__)
#define say_error(...)		say(, ERROR, __VA_ARGS__)
#define say_warn(...)		say(, WARN, __VA_ARGS__)
#define say_info(...)		say(, INFO, __VA_ARGS__)
#define say_debug(...)		say(, DEBUG, __VA_ARGS__)
#define say_debug2(...)		say(, DEBUG2, __VA_ARGS__)
#define say_debug3(...)		say(, DEBUG3, __VA_ARGS__)
#define say_trace(...)		say_TRACE(__FILE__, __LINE__, __VA_ARGS__)

#define ftrace()		say_trace("%s", __func__)
#define ftracef(format, ...)	say_trace("%s:"format, __func__, ##__VA_ARGS__)
#define ctrace()		say_trace("%s@%s", __func__, [self name])
#define ctracef(format, ...)	say_trace("%s@%s:"format, __func__, [self name], ##__VA_ARGS__)
#define otrace()		say_trace("%s@%s", __func__, [[self class] name])
#define otracef(format, ...)	say_trace("%s@%s:"format, __func__, [[self class] name], ##__VA_ARGS__)


void
vpanic(int status, const char *file, unsigned line,
       const char *error, const char *backtrace, const char *format, va_list ap)
	__attribute__((format(FORMAT_PRINTF, 6, 0), noreturn));

void _panic(const char *file, unsigned line, const char *format, ...)
	__attribute__((format(FORMAT_PRINTF, 3, 4), noreturn));

void _panic_syserror(const char *file, unsigned line, const char *format, ...)
	__attribute__((format(FORMAT_PRINTF, 3, 4), noreturn));

@class Error;
void panic_exc(Error *exc) __attribute__((noreturn));
void panic_exc_fmt(Error *exc, const char *format, ...) __attribute__((noreturn));
#define panic(...) _panic(__FILE__, __LINE__, __VA_ARGS__)
#define panic_syserror(...) _panic_syserror(__FILE__, __LINE__, __VA_ARGS__)


/* fg: 30 + color, bg: 40 + color */
#define RED(string) "\e[31m" string "\e[39m"
#define GREEN(string) "\e[32m" string "\e[39m"
#define YELLOW(string) "\e[33m" string "\e[39m"
#define BLUE(string) "\e[34m" string "\e[39m"
#define PINK(string) "\e[35m" string "\e[39m"
#define LIGHTBLUE(string) "\e[36m" string "\e[39m"
#endif
