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

#ifndef OBJECT_H
#define OBJECT_H

#import <objc/Object.h>
#include <palloc.h>
#include <util.h>

@interface Object (Palloc)
+ (id)palloc;
+ (id)palloc_from:(struct palloc_pool *)pool;
@end

@interface Error : Object {
@public
	char *reason;
	char buf[1024];
	unsigned line;
	const char *file;
	char *backtrace;
}
- (Error *)init:(char *)reason;
- (Error *)init_line:(unsigned)line_
		file:(const char *)file_
	   backtrace:(const char *)backtrace_
	      reason:(const char *)reason_;
- (Error *)init_line:(unsigned)line
		file:(const char *)file
	   backtrace:(const char *)backtrace
	      format:(const char *)format, ...;
@end


#define raise(fmt, ...)							\
	({								\
		say_debug("raise at %s:%i " fmt,			\
			  __FILE__, __LINE__, ##__VA_ARGS__);		\
		@throw [[Error palloc] init_line: __LINE__		\
					    file: __FILE__		\
				       backtrace: tnt_backtrace()	\
					  format:(fmt), ##__VA_ARGS__]; \
	})

#endif
