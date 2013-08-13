/*
 * Copyright (C) 2010, 2011 Mail.RU
 * Copyright (C) 2010, 2011 Yuriy Vostrikov
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

#include <config.h>
#import <util.h>
#import <fiber.h>
#import <say.h>
#import <objc.h>

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <unistd.h>
#ifndef PIPE_BUF
# include <sys/param.h>
#endif

typedef const char* cstr;
struct node {
	int value;
	cstr key;
} __attribute__((packed));
#define mh_name _cstr
#define mh_key_t cstr
#define mh_val_t int
#if SIZEOF_VOID_P == 8
# define mh_hash(a) ({ (uint32_t)(((uintptr_t)a)>>33^((uintptr_t)a)^((uintptr_t)a)<<11); })
#else
# define mh_hash(a) ({ (uintptr_t)a; })
#endif

#define mh_eq(a, b) ({ strcmp(*(mh_key_t *)((a) + sizeof(mh_val_t)), (b)) == 0; })
#define MH_STATIC
#include <mhash.h>

static struct mhash_t *filter;

int stderrfd, sayfd = STDERR_FILENO;
int dup_to_stderr = 0;
int max_level, default_level = INFO;
int nonblocking;

static char
level_to_char(int level)
{
	switch (level) {
	case FATAL:
		return 'F';
	case ERROR:
		return 'E';
	case WARN:
		return 'W';
	case INFO:
		return 'I';
	case DEBUG:
		return 'D';
	default:
		return '_';
	}
}

void
say_register_source(const char *file)
{
	if (unlikely(filter == NULL)) {
		filter = mh_cstr_init(xrealloc);
		max_level = default_level;
	}

	mh_cstr_put(filter, file, default_level, NULL);
}

int
say_level_source(const char *file, int diff)
{
	int max = 0;
	int found = 0;
	for (int k = 0; k < mh_end(filter); k++) {
		if (!mh_exist(filter, k))
		    continue;
		struct node *n = mh_slot(filter, k);
		if (strcmp(file, "ALL") == 0 || strcmp(file, n->key) == 0) {
			n->value += diff;
			found = 1;
		}
		if (n->value > max)
			max = n->value;
	}
	max_level = max;
	return found;
}

void
say_list_sources(void)
{
	puts("ALL");
	for (int k = 0; k < mh_end(filter); k++) {
		if (!mh_exist(filter, k))
		    continue;
		struct node *n = mh_slot(filter, k);
		puts(n->key);
	}
}

int
say_filter(int level, const char *file)
{
	int k = file ? mh_cstr_get(filter, file) : mh_end(filter);
	return mh_end(filter) == k || mh_cstr_value(filter, k) >= level;
}


void
say_logger_init(int nonblock)
{
	int pipefd[2];
	pid_t pid;
	char *argv[] = { "/bin/sh", "-c", cfg.logger, NULL };
	char *envp[] = { NULL };

	if (cfg.logger != NULL) {
		if (pipe(pipefd) == -1) {
			say_syserror("pipe");
			goto out;
		}

		pid = tnt_fork();
		if (pid == -1) {
			say_syserror("pipe");
			goto out;
		}

		if (pid == 0) {
			signal(SIGINT, SIG_IGN);
			close(pipefd[1]);
			dup2(pipefd[0], STDIN_FILENO);
			execve(argv[0], argv, envp);
		} else {
			close(pipefd[0]);
			stderrfd = dup(STDERR_FILENO);
			dup2(pipefd[1], STDERR_FILENO);
			dup2(pipefd[1], STDOUT_FILENO);
			sayfd = pipefd[1];
		}
	} else {
		sayfd = STDERR_FILENO;
	}
out:
	if (nonblock) {
		say_info("setting nonblocking log output");
		int one = 1;
		ioctl(sayfd, FIONBIO, &one);
		nonblocking = 1;
	}

	setvbuf(stderr, NULL, _IONBF, 0);
}

void
vsay(int level, const char *filename, unsigned line,
     const char *error, const char *format, va_list ap)
{
	size_t p = 0, len = PIPE_BUF;
	const char *f;
	static char buf[PIPE_BUF];

	if (level > FATAL && !say_filter(level, filename))
		return;

	if (booting) {
		fprintf(stderr, "%s: ", binary_filename);
		vfprintf(stderr, format, ap);
		if (error)
			fprintf(stderr, ": %s", error);
		fprintf(stderr, "\n");
		return;
	}

	ev_now_update();

	p += snprintf(buf + p, len - p, "%.3f %i %i/%s",
		      ev_now(), getpid(), fiber->fid, fiber->name);

	if (filename != NULL)
		for (f = filename; *f; f++)
			if (*f == '/' && *(f + 1) != '\0')
				filename = f + 1;

	if (level <= ERROR && filename != NULL)
		p += snprintf(buf + p, len - p, " %s:%i", filename, line);

	p += snprintf(buf + p, len - p, " %c> ", level_to_char(level));
	/* until here it is guaranteed that p < len */

	p += vsnprintf(buf + p, len - p, format, ap);
	if (error && p < len - 1)
		p += snprintf(buf + p, len - p, ": %s", error);
	if (p >= len - 1)
		p = len - 1;
	*(buf + p) = '\n';

	int r, one = 1, zero = 0;
	if (level <= ERROR)
		ioctl(sayfd, FIONBIO, &zero);
	r = write(sayfd, buf, p + 1);
	(void)r;
	if (nonblocking && level <= ERROR)
		ioctl(sayfd, FIONBIO, &one);

	if (sayfd != STDERR_FILENO && (level <= dup_to_stderr || level == FATAL)) {
		r = write(stderrfd, buf, p + 1);
	}
}

void
_say(int level, const char *filename, unsigned line, const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	vsay(level, filename, line, NULL, format, ap);
	va_end(ap);
}

#define say_f(level, suffix, err)		\
void \
say_##level##suffix(const char *filename, unsigned line, const char *format, ...) \
{ \
	va_list ap; \
	int errno_saved = errno; \
	va_start(ap, format); \
	vsay(level, filename, line, err, format, ap); \
	va_end(ap); \
	errno = errno_saved; \
}

say_f(DEBUG, , NULL)
say_f(DEBUG2, , NULL)
say_f(DEBUG3, , NULL)
say_f(WARN, , NULL)
say_f(WARN, no, strerror(errno))
say_f(INFO, , NULL)
say_f(ERROR, , NULL)
say_f(ERROR, no, strerror(errno))

void __attribute__((format(FORMAT_PRINTF, 6, 0), noreturn))
vpanic(int status, const char *file, unsigned line,
       const char *error, const char *backtrace, const char *format, va_list ap)
{
	vsay(FATAL, file, line, error, format, ap);
	va_end(ap);
	if (backtrace)
		_say(FATAL, NULL, 0, "backtrace:\n%s", backtrace);

	fflush(NULL); /* fflush all stream, in order not to lose logs prior failure */
	_exit(status);
}

void __attribute__((format(FORMAT_PRINTF, 3, 4), noreturn))
_panic(const char *file, unsigned line, const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	vpanic(EXIT_FAILURE, file, line, NULL, tnt_backtrace(), format, ap);
}

void __attribute__((format(FORMAT_PRINTF, 3, 4), noreturn))
_panic_syserror(const char *file, unsigned line, const char *format, ...)
{
	va_list ap;
	const char *err = strerror(errno);
	va_start(ap, format);
	vpanic(EXIT_FAILURE, file, line, err, tnt_backtrace(), format, ap);
}

void __attribute__((noreturn))
panic_exc_aux(Error *exc, const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	vpanic(EXIT_FAILURE, exc->file, exc->line, NULL, exc->backtrace, format, ap);
}

void __attribute__((noreturn))
panic_exc(Error *exc)
{
	panic_exc_aux(exc, "exception: %s", exc->reason);
}

