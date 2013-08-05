/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2012 Teodor Sigaev
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

#ifdef LIBIPROTO_OCTOPUS
#include <config.h>
#endif
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

#ifdef LIBIPROTO_OCTOPUS
#include <client/libiproto/libiproto.h>
#else
#include <iproto_def.h>
#endif

static struct storage {
	u_int32_t	errcode;
	char		*desc;
} *errStorage = NULL;

static int	nErr = 0;

static int
errStorageCmp(const void *a, const void *b) {
	const struct storage 	*as = a,
	      			*bs = b;

	if (as->errcode == bs->errcode)
		return 0;

	return (as->errcode > bs->errcode) ? 1 : -1;
}

static void
_errcode_add_desc(u_int32_t errcode, const char *desc) {
	errStorage = realloc(errStorage, sizeof(*errStorage) * (nErr + 1));

	errStorage[nErr].errcode = errcode;
	errStorage[nErr].desc = strdup(desc);

	nErr++;

	if (nErr > 1)
		qsort(errStorage, nErr, sizeof(*errStorage), errStorageCmp);
}

static inline struct storage *
_errcode_desc(u_int32_t errcode) {
	struct storage	*StopLow = errStorage,
			*StopHigh = errStorage + nErr,
			*StopMiddle;

	/* Loop invariant: StopLow <= val < StopHigh */
	while (StopLow < StopHigh) {
		StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

		if (errcode < StopMiddle->errcode)
			StopHigh = StopMiddle;
		else if (errcode > StopMiddle->errcode)
			StopLow = StopMiddle + 1;
		else
			return StopMiddle;
	}

	return NULL;
}

static inline void
errcode_init() {
	if (errStorage == NULL) {
#define	errcode_add_desc	_errcode_add_desc
#ifdef LIBIPROTO_OCTOPUS
		ERRCODE_ADD(ERRCODE_DESCRIPTION, LIBIPROTO_ERROR_CODES);
#else
		ERRCODE_ADD(ERRCODE_DESCRIPTION, ERROR_CODES);
#endif
#undef errcode_add_desc
	}
}

void
errcode_add_desc(u_int32_t errcode, const char *desc) {
	struct storage	*e;

	errcode_init();

	if ((e = _errcode_desc(errcode)) != NULL) {
		free(e->desc);
		e->desc = strdup(desc);
	} else {
		_errcode_add_desc(errcode, desc);
	}
}

const char *
errcode_desc(u_int32_t errcode) {
	static const char *unknown = "Unknown error";
	struct storage  *e;

	errcode_init();

	if ((e = _errcode_desc(errcode)) != NULL)
		return e->desc;
	else
		return unknown;
}
