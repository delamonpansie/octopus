/*
 * Copyright (C) 2016 Mail.RU
 * Copyright (C) 2016 Yury Vostrikov
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

#include <caml/memory.h>
#include <caml/callback.h>
#include <caml/custom.h>
#include <caml/alloc.h>
#include <caml/threads.h>

#include <fiber.h>
#include <third_party/queue.h>

typedef void (*scanning_action) (value, value *);

extern char * caml_top_of_stack;
extern char * caml_bottom_of_stack;
extern uintnat caml_last_return_address;
extern value * caml_gc_regs;
extern char * caml_exception_pointer;
extern int caml_backtrace_pos;
extern void * caml_backtrace_buffer;
extern value caml_backtrace_last_exn;

extern void (*caml_scan_roots_hook) (scanning_action);
extern void (*caml_enter_blocking_section_hook)(void);
extern void (*caml_leave_blocking_section_hook)(void);
extern int (*caml_try_leave_blocking_section_hook)(void);
extern uintnat (*caml_stack_usage_hook)(void);

extern void caml_do_local_roots(scanning_action f, char * bottom_of_stack,
				uintnat last_retaddr, value * gc_regs,
				struct caml__roots_block * local_roots);

void (*prev_scan_roots_hook) (scanning_action);
static void fiber_scan_roots(scanning_action action)
{
	Fiber *f;
	SLIST_FOREACH(f, &fibers, link) {
		struct caml_state *th = &f->ML;

		(*action)(th->backtrace_last_exn, &th->backtrace_last_exn);
		/* Don't rescan the stack of the current thread, it was done already */
		if (f == fiber)
			continue;

		if (th->bottom_of_stack != NULL)
			caml_do_local_roots(action, th->bottom_of_stack, th->last_retaddr,
					    th->gc_regs, th->local_roots);
	}

	if (prev_scan_roots_hook != NULL)
		(*prev_scan_roots_hook)(action);
}

static void
fiber_enter_blocking_section(void)
{
	/* Save the stack-related global variables in the thread descriptor
	   of the current thread */
	struct caml_state *curr_thread = &fiber->ML;
	curr_thread->top_of_stack = caml_top_of_stack;
	curr_thread->bottom_of_stack = caml_bottom_of_stack;
	curr_thread->last_retaddr = caml_last_return_address;
	curr_thread->gc_regs = caml_gc_regs;
	curr_thread->exception_pointer = caml_exception_pointer;
	curr_thread->local_roots = caml_local_roots;
	caml_local_roots = (void *)0xdead;
	caml_last_return_address = 0xbeef;

	curr_thread->backtrace_pos = caml_backtrace_pos;
	curr_thread->backtrace_buffer = caml_backtrace_buffer;
	curr_thread->backtrace_last_exn = caml_backtrace_last_exn;
}

static void
fiber_leave_blocking_section(void)
{
	struct caml_state *curr_thread = &fiber->ML;

	/* Restore the stack-related global variables */
	caml_top_of_stack = curr_thread->top_of_stack;
	caml_bottom_of_stack= curr_thread->bottom_of_stack;
	caml_last_return_address = curr_thread->last_retaddr;
	caml_gc_regs = curr_thread->gc_regs;
	caml_exception_pointer = curr_thread->exception_pointer;
	caml_local_roots = curr_thread->local_roots;

	caml_backtrace_pos = curr_thread->backtrace_pos;
	caml_backtrace_buffer = curr_thread->backtrace_buffer;
	caml_backtrace_last_exn = curr_thread->backtrace_last_exn;
}

static int
fiber_try_leave_blocking_section(void)
{
	return 0;
}

static uintnat (*prev_stack_usage_hook)(void);
static uintnat
fiber_stack_usage(void)
{
  uintnat sz = 0;
  Fiber *f;

  SLIST_FOREACH(f, &fibers, link) {
	  struct caml_state *th = &f->ML;

	  /* Don't add stack for current thread, this is done elsewhere */
	  if (f == fiber)
		  continue;
	  sz += (value *) th->top_of_stack - (value *) th->bottom_of_stack;
  }
  if (prev_stack_usage_hook != NULL)
	  sz += prev_stack_usage_hook();
  return sz;
}

void
fiber_caml_init(void)
{
	prev_scan_roots_hook = caml_scan_roots_hook;
	caml_scan_roots_hook = fiber_scan_roots;
	caml_enter_blocking_section_hook = fiber_enter_blocking_section;
	caml_leave_blocking_section_hook = fiber_leave_blocking_section;
	caml_try_leave_blocking_section_hook = fiber_try_leave_blocking_section;

	prev_stack_usage_hook = caml_stack_usage_hook;
	caml_stack_usage_hook = fiber_stack_usage;

	extern char **octopus_argv;
	caml_startup(octopus_argv);
	caml_enter_blocking_section();
}

value
stub_fiber_sleep(value tm)
{
	CAMLparam1(tm);
	caml_enter_blocking_section();
	fiber_sleep(Double_val(tm));
	caml_leave_blocking_section();
	CAMLreturn(Val_unit);
}

static void
caml_fiber_trampoline(va_list ap)
{
	value cb = va_arg(ap, value);
	value arg = va_arg(ap, value);
	caml_leave_blocking_section();
	caml_callback(cb, arg);
	caml_enter_blocking_section();
}

value
stub_fiber_create(value cb, value arg)
{
	CAMLparam2(cb, arg);
	caml_enter_blocking_section();
	fiber_create("caml", caml_fiber_trampoline, cb, arg);
	caml_leave_blocking_section();
	CAMLreturn(Val_unit);
}
