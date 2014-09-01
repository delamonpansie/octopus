/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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
#import <coro.h>
#import <palloc.h>

#include <third_party/libcoro/coro.h>

#if HAVE_VALGRIND_VALGRIND_H && !defined(NVALGRIND)
# include <valgrind/valgrind.h>
# include <valgrind/memcheck.h>
#else
# define VALGRIND_MAKE_MEM_DEFINED(_qzz_addr, _qzz_len) (void)0
# define VALGRIND_MAKE_MEM_UNDEFINED(_qzz_addr, _qzz_len) (void)0
# define VALGRIND_MALLOCLIKE_BLOCK(addr, sizeB, rzB, is_zeroed) (void)0
# define VALGRIND_FREELIKE_BLOCK(addr, rzB) (void)0
# define VALGRIND_MAKE_MEM_NOACCESS(_qzz_addr,_qzz_len) (void)0
# define VALGRIND_STACK_REGISTER(_qzz_addr,_qzz_len) (void)0
#endif

#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

void
octopus_coro_destroy(struct octopus_coro *coro)
{
	if (coro && coro->mmap != MAP_FAILED)
		munmap(coro->mmap, coro->mmap_size);
	free(coro);
}

struct octopus_coro *
octopus_coro_create(struct octopus_coro *coro, void (*f) (void *), void *data)
{
	const int page = sysconf(_SC_PAGESIZE);

	if (coro == NULL)
		coro = malloc(sizeof(*coro));

	if (coro == NULL)
		return NULL;

	memset(coro, 0, sizeof(*coro));

	coro->mmap_size = page * 16;
	coro->mmap = mmap(MMAP_HINT_ADDR, coro->mmap_size, PROT_READ | PROT_WRITE | PROT_EXEC,
			  MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

	if (coro->mmap == MAP_FAILED)
		goto fail;

	if (mprotect(coro->mmap, page, PROT_NONE) < 0)
		goto fail;

	(void)VALGRIND_MAKE_MEM_NOACCESS(coro->mmap, page);

	const int red_zone_size = sizeof(void *) * 4;
	coro->stack = coro->mmap + page;
	coro->stack_size = coro->mmap_size - page - red_zone_size;
	void **red_zone = coro->stack + coro->stack_size;
	red_zone[0] = red_zone[1] = NULL;
	red_zone[2] = red_zone[3] = (void *)(uintptr_t)0xDEADDEADDEADDEADULL;

	(void)VALGRIND_STACK_REGISTER(coro->stack, coro->stack + coro->stack_size);
	coro_create(&coro->ctx, f, data, coro->stack, coro->stack_size);
	return coro;

	int saved_errno;
fail:
	saved_errno = errno;
	octopus_coro_destroy(coro);
	errno = saved_errno;
	return NULL;
}
