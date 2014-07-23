/*
 * Copyright (C) 2011, 2013 Mail.RU
 * Copyright (C) 2011, 2013 Yuriy Vostrikov
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
#import <objc.h>
#import <palloc.h>
#import <say.h>
#import <config.h>

#if HAVE_OBJC_RUNTIME_H
#include <objc/runtime.h>
#elif HAVE_OBJC_OBJC_API_H
#include <objc/objc-api.h>
size_t
class_getInstanceSize(Class class)
{
	return class_get_instance_size(class);
}

Class
object_setClass(id o, Class class)
{
	if (!obj) {
		return Nil;
	}
	Class old = obj->class_pointer;
	obj->class_pointer = class;
	return old;
}
#else
# error Unknown runtime
#endif

@implementation Object (Octopus)

+ (id)
palloc
{
	Class class = (Class)self;
	id obj = p0alloc(fiber->pool, class_getInstanceSize(class));
	object_setClass(obj, class);
	return obj;
}

+ (id)
palloc_from:(struct palloc_pool *)pool
{
	Class class = (Class)self;
	id obj = p0alloc(pool, class_getInstanceSize(class));
	object_setClass(obj, class);
	return obj;
}

#if !HAVE_OBJC_OBJC_API_H
+ (id)
alloc
{
	Class class = (Class)self;
	id obj = calloc(1, class_getInstanceSize(class));
	object_setClass(obj, class);
	return obj;
}

- (id)
free
{
	free(self);
	return nil;
}

- (id)
init
{
	return self;
}

- (BOOL)
isMemberOf:(Class)class
{
	Class obj_class = object_getClass(self);
	return obj_class == class;
}

- (BOOL)
isKindOf:(Class)kind
{
	for (Class class = object_getClass(self);
	     class != Nil;
	     class = class_getSuperclass(class)) {
		if (class == kind)
			return 1;
	}
	return 0;
}

- (BOOL)
respondsTo:(SEL)selector
{
	Class class = object_getClass(self);
	return class_respondsToSelector(class, selector);
}

+ (Class)
class
{
	return self;
}

+ (const char *)
name
{
	return class_getName(self);
}

- (id)
perform:(SEL)selector
{
#if OBJC_GNU_RUNTIME
	return objc_msg_lookup(self, selector)(self, selector);
#elif OBJC_APPLE_RUNTIME
	return objc_msgSend(self, selector);
#else
# error Unknown runtime
#endif
}
#endif
@end


@implementation Error
+ (Error *)
alloc
{
	abort(); /* + palloc should be used */
}

-
init:(const char *)reason_
{
	reason = reason_;
	return self;
}

-
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

-
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
