/*
 * Copyright (C) 2010-2015 Mail.RU
 * Copyright (C) 2010-2015, 2020 Yury Vostrikov
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
#import <octopus_ev.h>
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

struct node {
	int* value;
	const char *key;
} __attribute__((packed));
#define mh_name _cstr
#define mh_slot_t struct node
#define mh_slot_key(h, node) (node)->key
#define mh_slot_val(node) (node)->value
#if SIZEOF_VOID_P == 8
# define mh_hash(h, a) ({ (uint32_t)(((uintptr_t)a)>>33^((uintptr_t)a)^((uintptr_t)a)<<11); })
#else
# define mh_hash(h, a) ({ (uintptr_t)a; })
#endif

#define mh_eq(h, a, b) ({ strcmp((a), (b)) == 0; })
#define MH_STATIC
#include <mhash.h>

static struct mh_cstr_t *filter;

int stderrfd, sayfd = STDERR_FILENO;
int dup_to_stderr = 0;
int max_level = 0;
int nonblocking;

#define HIST_SIZE (128 * 1024)
static char buf1[HIST_SIZE], buf2[HIST_SIZE];
struct {
	int count, free;
	char *tail, *lines[];
} *say_hist_a = (void *)buf1, *say_hist_b = (void *)buf2, *say_hist;

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
	case TRACE:
		return 'T';
	default:
		return '_';
	}
}

static void
set_max_level(int level)
{
	max_level = level;
	extern void rs_say_set_max_level(int level);
	rs_say_set_max_level(level);
}

void
say_register_source(const char *file, int *level)
{
	if (unlikely(filter == NULL)) {
		filter = mh_cstr_init(xrealloc);
		set_max_level(0);
	}

	mh_cstr_put(filter, file, level, NULL);
}

int
say_level_source(const char *file, int diff)
{
	int max = 0;
	int found = 0;
	for (int k = 0; k < mh_end(filter); k++) {
		if (!mh_cstr_slot_occupied(filter, k))
		    continue;
		struct node *n = mh_cstr_slot(filter, k);
		if (strcmp(file, "ALL") == 0 || strncmp(file, n->key, strlen(file)) == 0) {
			*(n->value) += diff;
			found = 1;
		}
		if (*(n->value) > max)
			max = *(n->value);
	}
	set_max_level(max);
	return found;
}

void
say_list_sources(void)
{
	puts("ALL");
	for (int k = 0; k < mh_end(filter); k++) {
		if (!mh_cstr_slot_occupied(filter, k))
		    continue;
		puts(mh_cstr_slot(filter, k)->key);
	}
}

void
say_logger_init(int nonblock)
{
	if (cfg.logger) {
#if OCT_CHILDREN
		int pipefd[2];
		pid_t pid;
		char *argv[] = { "/bin/sh", "-c", cfg.logger, NULL };
		char *envp[] = { NULL };

		if (pipe(pipefd) == -1) {
			say_syserror("pipe");
			goto out;
		}

		pid = oct_fork();
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
#else
		say_warn("logger disabled: no forking support");
		sayfd = STDERR_FILENO;
#endif
	} else {
		sayfd = STDERR_FILENO;
	}
#if OCT_CHILDREN
        out:
#endif
	if (nonblock) {
		say_info("setting nonblocking log output");
		int one = 1;
		ioctl(sayfd, FIONBIO, &one);
		nonblocking = 1;
	}

	setvbuf(stderr, NULL, _IONBF, 0);

	extern void rs_say_init();
	rs_say_init();
}

static void
say_hist_append(const char *line, ssize_t len)
{
	if (!say_hist || say_hist->free < len) {
		if (say_hist == say_hist_a)
			say_hist = say_hist_b;
		else
			say_hist = say_hist_a;
		say_hist->count = 0;
		say_hist->tail = (char *)say_hist + HIST_SIZE;
		say_hist->free = HIST_SIZE - sizeof(*say_hist) - sizeof(void *);
	}

	char *ptr = say_hist->tail - len - 1;
	memcpy(ptr, line, len);
	ptr[len] = 0;
	say_hist->lines[say_hist->count] = ptr;
	say_hist->count++;
	say_hist->tail -= len + 1;
	say_hist->free -= len + 1 + sizeof(void *);
}

void
vsay(int level, const char *filename, unsigned line,
     const char *error, const char *format, va_list ap)
{
	size_t p = 0, len = PIPE_BUF;
	const char *f, *cur;
	static __thread char buf[PIPE_BUF];

	if (booting) {
		fprintf(stderr, "%s: ", binary_filename);
		vfprintf(stderr, format, ap);
		if (error)
			fprintf(stderr, ": %s", error);
		fprintf(stderr, "\n");
		return;
	}

	ev_now_update();

	if (fiber != nil) {
		p += snprintf(buf + p, len - p, "%.3f %i %i/%s", ev_now(), getpid(), fiber->fid, fiber->name);
		if (fiber->ushard != -1)
			p += snprintf(buf + p, len - p, " {%i}", fiber->ushard);
	} else {
		p += snprintf(buf + p, len - p, "%.3f %i", ev_now(), getpid());
	}

	if ((level <= ERROR || level >= DEBUG) && filename != NULL) {
		for (f = filename, cur = __FILE__; *f && *f == *cur; f++, cur++)
			if (*f == '/' && *(f + 1) != '\0')
				filename = f + 1;

		p += snprintf(buf + p, len - p, " %s:%i", filename, line);
	}

	p += snprintf(buf + p, len - p, " %c> ", level_to_char(level));
	/* until here it is guaranteed that p < len */

	p += vsnprintf(buf + p, len - p, format, ap);
	if (error && p < len - 1)
		p += snprintf(buf + p, len - p, ": %s", error);
	if (p >= len - 1)
		p = len - 1;
	*(buf + p) = '\n';
	p++;

	int r, one = 1, zero = 0;
	if (level <= ERROR)
		ioctl(sayfd, FIONBIO, &zero);
	r = write(sayfd, buf, p);
	(void)r;
	if (nonblocking && level <= ERROR)
		ioctl(sayfd, FIONBIO, &one);

	if (sayfd != STDERR_FILENO && (level <= dup_to_stderr || level <= FATAL)) {
		r = write(stderrfd, buf, p);
	}

	say_hist_append(buf, p);
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
say_f(WARN, , NULL)
say_f(WARN, no, strerror_o(errno))
say_f(INFO, , NULL)
say_f(ERROR, , NULL)
say_f(ERROR, no, strerror_o(errno))
say_f(TRACE, , NULL)

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
	const char *err = strerror_o(errno);
	va_start(ap, format);
	vpanic(EXIT_FAILURE, file, line, err, tnt_backtrace(), format, ap);
}

void __attribute__((noreturn))
panic_exc_fmt(Error *exc, const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	vpanic(EXIT_FAILURE, exc->file, exc->line, NULL, exc->backtrace, format, ap);
}

void __attribute__((noreturn))
panic_exc(Error *exc)
{
	panic_exc_fmt(exc, "exception: %s", exc->reason);
}

