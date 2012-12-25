/*
 * Copyright (C) 2011 Mail.RU
 * Copyright (C) 2011 Yuriy Vostrikov
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


#import <util.h>
#import <fiber.h>
#import <object.h>
#import <palloc.h>
#import <say.h>

#if OBJC_GNU_RUNTIME
#include <objc/objc-api.h>
#elif OBJC_APPLE_RUNTIME
#include <objc/runtime.h>
#endif

@implementation Object (Palloc)

+ (id)
palloc
{
#if OBJC_GNU_RUNTIME
	Class class = (Class)self;
	id obj = p0alloc(fiber->pool, class_get_instance_size(class));
	obj->class_pointer = class;
#elif OBJC_APPLE_RUNTIME
	Class class = (Class)self;
	id obj = p0alloc(fiber->pool, class_getInstanceSize(class));
	obj->isa = class;
#else
# error Unknown runtime
#endif
	return obj;
}

+ (id)
palloc_from:(struct palloc_pool *)pool
{
#if OBJC_GNU_RUNTIME
	Class class = (Class)self;
	id obj = p0alloc(pool, class_get_instance_size(class));
	obj->class_pointer = class;
#elif OBJC_APPLE_RUNTIME
	Class class = (Class)self;
	id obj = p0alloc(pool, class_getInstanceSize(class));
	obj->isa = class;
#else
# error Unknown runtime
#endif
	return obj;
}
@end

@implementation Error
+ (Error *)
alloc
{
	abort(); /* + palloc should be used */
}

- (Error *)
init:(char *)reason_
{
	reason = reason_;
	return self;
}

- (Error *)
init_line:(unsigned)line_
     file:(const char *)file_
backtrace:(const char *)backtrace_
   reason:(const char *)reason_
{
	line = line_;
	file = file_;
	reason = (char *)reason_;
	if (backtrace_) {
		backtrace = palloc(fiber->pool, strlen(backtrace_) + 1);
		strcpy(backtrace, backtrace_);
	}
	return self;
}

- (Error *)
init_line:(unsigned)line_
     file:(const char *)file_
backtrace:(const char *)backtrace_
   format:(const char *)format, ...
{
	va_list ap;
	va_start(ap, format);
	vsnprintf(buf, sizeof(buf), format, ap);
	va_end(ap);

	return [self init_line:line_ file:file_ backtrace:backtrace_ reason:buf];
}
@end

register_source();
