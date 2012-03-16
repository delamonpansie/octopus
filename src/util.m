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
#import <fiber.h>
#import <util.h>

#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef HAVE_LIBELF
# include <libelf.h>
# include <gelf.h>
#endif

#ifndef HAVE_STACK_END_ADDRESS
void *STACK_END_ADDRESS;
#endif

void
close_all_xcpt(int fdc, ...)
{
	int keep[fdc];
	va_list ap;
	struct rlimit nofile;

	va_start(ap, fdc);
	for (int j = 0; j < fdc; j++) {
		keep[j] = va_arg(ap, int);
	}
	va_end(ap);

	if (getrlimit(RLIMIT_NOFILE, &nofile) != 0)
		nofile.rlim_cur = 10000;

	for (int i = 3; i < nofile.rlim_cur; i++) {
		bool found = false;
		for (int j = 0; j < fdc; j++) {
			if (keep[j] == i) {
				found = true;
				break;
			}
		}
		if (!found)
			close(i);
	}
}

void
maximize_core_rlimit()
{
	struct rlimit c = { 0, 0 };
	if (getrlimit(RLIMIT_CORE, &c) < 0) {
		say_syserror("getrlimit");
		return;
	}
	c.rlim_cur = c.rlim_max;
	if (setrlimit(RLIMIT_CORE, &c) < 0)
		say_syserror("setrlimit");
}

void
coredump(int dump_interval)
{
	static time_t last_coredump = 0;
	time_t now = time(NULL);

	if (now - last_coredump < dump_interval)
		return;

	last_coredump = now;

	if (tnt_fork() == 0) {
		close_all_xcpt(0);
#ifdef COVERAGE
		__gcov_flush();
#endif
		maximize_core_rlimit();
		abort();
	}
}

pid_t master_pid;

pid_t
tnt_fork()
{
	pid_t pid = fork();
	if (pid == 0) {
		sigset_t set;
		sigfillset(&set);
		sigprocmask(SIG_UNBLOCK, &set, NULL);
		signal(SIGPIPE, SIG_DFL);
		signal(SIGCHLD, SIG_DFL);
		/* Ignore SIGINT coming from a TTY
		   our parent will send SIGTERM to us when he catches SIGINT */
		signal(SIGINT, SIG_IGN);
		ev_loop_fork();
	}
	return pid;
}

void *
xrealloc(void *ptr, size_t size)
{
	void *ret = realloc(ptr, size);
	if (size > 0 && ret == NULL)
		abort();
	return ret;
}

volatile int gdb_wait_lock = 1;
void
wait_gdb(void)
{
	while(gdb_wait_lock);
}

double
drand(double top)
{
	return (top * (double)rand()) / RAND_MAX;
}

#ifdef BACKTRACE

/*
 * we use global static buffer because it is too late to do
 * any allocation when we are printing bactrace and fiber stack is small
 */

static char backtrace_buf[4096 * 4];

/*
 * note, stack unwinding code assumes that binary is compiled with frame pointers
 */

struct frame {
	struct frame *rbp;
	void *ret;
};

char *
backtrace(void *frame_, void *stack, size_t stack_size)
{
	struct frame *frame = frame_;
	void *stack_top = stack + stack_size;
	void *stack_bottom = stack;

	char *p = backtrace_buf;
	size_t r, len = sizeof(backtrace_buf);
	while (stack_bottom <= (void *)frame && (void *)frame < stack_top) {
		r = snprintf(p, len, "        - { frame: %p, caller: %p",
			     (void *)frame + 2 * sizeof(void *), frame->ret);

		if (r >= len)
			goto out;
		p += r;
		len -= r;

#ifdef HAVE_LIBELF
		struct symbol *s = addr2symbol(frame->ret);
		if (s != NULL) {
			r = snprintf(p, len, " <%s+%zu> ", s->name, frame->ret - s->addr);
			if (r >= len)
				goto out;
			p += r;
			len -= r;

		}
#endif
		r = snprintf(p, len, " }\r\n");
		if (r >= len)
			goto out;
		p += r;
		len -= r;

#ifdef HAVE_LIBELF
		if (s != NULL) {
			if (strcmp(s->name, "main") == 0)
				break;
			if (strcmp(s->name, "coro_init") == 0)
				break;
		}
#endif
		frame = frame->rbp;
	}
	r = 0;
out:
	p += MIN(len - 1, r);
	*p = 0;
        return backtrace_buf;
}
#endif

#ifdef BACKTRACE
const char *
tnt_backtrace(void)
{
	void *frame = frame_addess();
	void *stack_top;
	size_t stack_size;

	if (fiber == NULL || fiber->name == NULL || strcmp(fiber->name, "sched") == 0) {
		stack_top = frame; /* we don't know where the system stack top is */
		stack_size = STACK_END_ADDRESS - frame;
	} else {
		stack_top = fiber->coro.stack;
		stack_size = fiber->coro.stack_size;
	}

	return backtrace(frame, stack_top, stack_size);
}
#else
const char *
tnt_backtrace(void)
{
	return NULL;
}
#endif

void __attribute__((format(FORMAT_PRINTF, 6, 7), noreturn))
_panic(int status, const char *filename, unsigned line,
       const char *error, const char *backtrace, const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	vsay(S_FATAL, filename, line, error, format, ap);
	va_end(ap);
	if (backtrace)
		_say(S_FATAL, NULL, 0, NULL, "backtrace:\n%s", backtrace);

	exit(status);
}

void __attribute__ ((noreturn))
assert_fail(const char *assertion, const char *file, unsigned line, const char *backtrace, const char *function)
{
	_say(S_FATAL, file, line, NULL, "%s: assertion %s failed.\n%s", function, assertion, backtrace);
	close_all_xcpt(0);
	abort();
}

#ifdef HAVE_LIBELF
static struct symbol *symbols;
static size_t symbol_count;

int
compare_symbol(const void *_a, const void *_b)
{
	const struct symbol *a = _a, *b = _b;
	if (a->addr > b->addr)
		return 1;
	if (a->addr == b->addr)
		return 0;
	return -1;
}

void
load_symbols(const char *name)
{
	Elf *elf;
	GElf_Shdr shdr;
	GElf_Sym sym;
	Elf_Scn *scn;
	Elf_Data *data;
	int fd, j = 0;

	elf_version(EV_CURRENT);

	if ((fd = open(name, O_RDONLY)) < 0) {
		say_syserror("load_symbols, open: %s", name);
		return;
	}

	if ((elf = elf_begin(fd, ELF_C_READ, NULL)) == NULL) {
		say_error("elf_begin: %s", elf_errmsg(-1));
		goto cleanup;
	}

	scn = NULL;
	while ((scn = elf_nextscn(elf, scn)) != NULL) {
		gelf_getshdr(scn, &shdr);
		if (shdr.sh_type != SHT_SYMTAB)
			continue;

		data = NULL;
		while ((data = elf_getdata(scn, data)) != NULL) {
			int count = shdr.sh_size / shdr.sh_entsize;
			for (int i = 0; i < count; i++) {
				gelf_getsym(data, i, &sym);
				if (GELF_ST_TYPE(sym.st_info) != STT_FUNC ||
				    sym.st_value == 0)
					continue;

				symbol_count++;
			}
		}
	}

	symbols = malloc(symbol_count * sizeof(struct symbol));

	scn = NULL;
	while ((scn = elf_nextscn(elf, scn)) != NULL) {
		gelf_getshdr(scn, &shdr);
		if (shdr.sh_type != SHT_SYMTAB)
			continue;

		data = NULL;
		while ((data = elf_getdata(scn, data)) != NULL) {
			int count = shdr.sh_size / shdr.sh_entsize;
			for (int i = 0; i < count; i++) {
				gelf_getsym(data, i, &sym);
				if (GELF_ST_TYPE(sym.st_info) != STT_FUNC ||
				    sym.st_value == 0)
					continue;

				char *name = elf_strptr(elf, shdr.sh_link, sym.st_name);
				symbols[j].name = strdup(name);
				symbols[j].addr = (void *)(uintptr_t)sym.st_value;
				symbols[j].end = (void *)(uintptr_t)sym.st_value + sym.st_size;
				j++;
			}
		}
	}

	qsort(symbols, symbol_count, sizeof(struct symbol), compare_symbol);

	if (symbol_count == 0)
		say_warn("no symbols were loaded");

cleanup:
	if (elf)
		elf_end(elf);
	close(fd);
}

struct symbol *
addr2symbol(void *addr)
{
	int low = 0, high = symbol_count, middle = -1;
	struct symbol *ret, key = {.addr = addr};

	while(low < high) {
		middle = low + (high - low) / 2;
		int diff = compare_symbol(symbols + middle, &key);

		if (diff < 0) {
			low = middle + 1;
		} else if (diff > 0) {
			high = middle;
		} else {
			ret = symbols + middle;
			goto out;
		}
	}
	ret = symbols + high - 1;

out:
	if (middle != -1 && ret->addr <= addr && addr <= ret->end)
		return ret;
	return NULL;
}

#endif
