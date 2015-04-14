/*
 * Copyright (C) 2011, 2013, 2014 Mail.RU
 * Copyright (C) 2011, 2013, 2014 Yuriy Vostrikov
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
object_setClass(id obj, Class class)
{
	if (!obj)
		return Nil;

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

- (id)
retain
{
	return self;
}

- (void)
release
{
}

- (id) autorelease
{
	return self;
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

void
scoped_release(id *obj)
{
	[*obj release];
}

@implementation Error
+ (Error *)
alloc
{
	abort(); /* + alloc should be called directly */
}

+ (Error *)
palloc
{
	abort(); /* + palloc should be not be called directly */
}

- (id)
retain
{
	rc++;
	return self;
}

- (void)
release
{
	if (--rc==0) {
		free(self);
	}
}

- (id)
autorelease
{
	autorelease(self);
	return self;
}

+ (id)
alloc: (size_t)add
{
	Class class = (Class)self;
	Error* obj = calloc(1, class_getInstanceSize(class) + add);
	object_setClass(obj, class);
	obj->rc = 1;
	return obj;
}

+ (id)
with_reason: (const char*) reason
{
	Class class = (Class)self;
	size_t len = strlen(reason);
	Error *err = [class alloc: len+1];
	err->reason = (char*)err + class_getInstanceSize(class);
	memcpy(err->reason, reason, len);
	err->reason[len] = 0;
	return err;
}

+ (id)
with_format: (const char*) format, ...
{
	Class class = (Class)self;
	int len;
	va_list ap;
	va_start(ap, format);
	len = vsnprintf(NULL, 0, format, ap);
	assert(len >= 0);
	va_end(ap);

	Error *err = [class alloc: len+1];
	err->reason = (char*)err + class_getInstanceSize(class);

	va_start(ap, format);
	vsnprintf(err->reason, len+1, format, ap);
	va_end(ap);
	return err;
}

+ (id)
with_backtrace: (const char *)backtrace
	format: (const char*) format, ...
{
	Class class = (Class)self;
	size_t blen = strlen(backtrace);
	int rlen;
	va_list ap;
	va_start(ap, format);
	rlen = vsnprintf(NULL, 0, format, ap);
	assert(rlen >= 0);
	va_end(ap);

	Error *err = [class alloc: blen+rlen+2];
	err->reason = (char*)err + class_getInstanceSize(class);
	err->backtrace = err->reason + rlen + 1;

	va_start(ap, format);
	vsnprintf(err->reason, rlen+1, format, ap);
	va_end(ap);
	memcpy(err->backtrace, backtrace, blen);
	err->backtrace[blen] = 0;
	return err;
}

+ (id)
with_backtrace: (const char *)backtrace
	reason: (const char*)reason;
{
	Class class = (Class)self;
	size_t blen = strlen(backtrace);
	size_t rlen = strlen(reason);
	Error *err = [class alloc: blen+rlen+2];
	err->reason = (char*)err + class_getInstanceSize(class);
	err->backtrace = err->reason + rlen + 1;
	memcpy(err->reason, reason, rlen);
	err->reason[rlen] = 0;
	memcpy(err->backtrace, backtrace, blen);
	err->backtrace[blen] = 0;
	return err;
}

- (id)
init_line:(unsigned)line_
     file:(const char *)file_
{
	line = line_;
	file = file_;
	return self;
}

- (const char*) reason
{
	return reason;
}

- (const char*) backtrace
{
	return backtrace;
}

- (const char*) file
{
	return file;
}

- (unsigned) line
{
	return line;
}
@end

register_source();
