/*
 * Copyright (C) 2014 Mail.RU
 * Copyright (C) 2014 Yuriy Vostrikov
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
#import <pickle.h>
#import <log_io.h>
#import <say.h>

#include <third_party/crc32.h>

void
run_crc_calc(u32 *crc, u16 row_tag, const void *data, int len)
{
	int tag_type = row_tag & ~TAG_MASK;
	int tag = row_tag & TAG_MASK;

	if (tag_type == TAG_WAL && (tag == wal_data || tag >= user_tag))
		*crc = crc32c(*crc, data, len);
}

void
run_crc_record(struct run_crc *run_crc, u16 tag, i64 scn, u32 crc)
{
	if (scn_changer(tag)) {
		say_debug("save crc_hist SCN:%"PRIi64" CRC:0x%08x", scn, crc);

		struct run_crc_hist entry = { scn, crc };
		run_crc->hist[++(run_crc->i) % nelem(run_crc->hist)] = entry;
	}
}

void
run_crc_verify(struct run_crc *run_crc, struct tbuf *buf)
{
	i64 scn_of_crc = read_u64(buf);
	u32 log = read_u32(buf);
	read_u32(buf); /* ignore run_crc_mod */

	struct run_crc_hist *h = NULL;
	for (unsigned i = run_crc->i, j = 0; j < nelem(run_crc->hist); j++, i--) {
		struct run_crc_hist *p = run_crc->hist + (i % nelem(run_crc->hist));
		if (p->scn == scn_of_crc) {
			h = p;
			break;
		}
	}

	if (!h) {
		say_warn("unable to track run_crc: crc history too short"
			 " CRC_SCN:%"PRIi64, scn_of_crc);
		return;
	}

	if (h->value != log) {
		run_crc->mismatch |= 1;
		say_error("run_crc_log mismatch: SCN:%"PRIi64" saved:0x%08x computed:0x%08x",
			  scn_of_crc, log, h->value);
	} else {
		say_info("run_crc verified SCN:%"PRIi64, h->scn);
	}
	run_crc->verify_tstamp = ev_now();
}

ev_tstamp
run_crc_lag(struct run_crc *run_crc)
{
	return ev_now() - run_crc->verify_tstamp;
}

const char *
run_crc_status(struct run_crc *run_crc)
{
	if (run_crc->mismatch)
		return "LOG_CRC_MISMATCH";
	return "ok";
}

register_source();
