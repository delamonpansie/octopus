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
#import <tbuf.h>
#import <log_io.h>
#import <pickle.h>
#import <fiber.h>
#import <say.h>
#import <mod/box/box.h>

#include <sysexits.h>

static int
quote_all(int c __attribute__((unused)))
{
	return 1;
}
static int
quote_non_printable(int c)
{
	return !(0x20 <= c && c < 0x7f && !(c == '"' || c == '\\'));
}

static int (*quote)(int c) = quote_non_printable;

static void
field_print(struct tbuf *buf, void *f)
{
	uint32_t size;

	size = LOAD_VARINT32(f);

	if (quote != quote_all) {
		if (size == 2)
			tbuf_printf(buf, "%i:", *(u16 *)f);

		if (size == 4)
			tbuf_printf(buf, "%i:", *(u32 *)f);

		tbuf_printf(buf, "\"");
		while (size-- > 0) {
			if (quote(*(u8 *)f))
				tbuf_printf(buf, "\\x%02X", *(u8 *)f++);
			else
				tbuf_printf(buf, "%c", *(u8 *)f++);
		}
		tbuf_printf(buf, "\"");
	} else {
		tbuf_printf(buf, "\"");
		while (size-- > 0)
			tbuf_printf(buf, "\\x%02X", *(u8 *)f++);
		tbuf_printf(buf, "\"");
	}

}

static void
tuple_print(struct tbuf *buf, u32 cardinality, void *f)
{
	tbuf_printf(buf, "<");

	for (size_t i = 0; i < cardinality; i++, f = next_field(f)) {
		field_print(buf, f);

		if (likely(i + 1 < cardinality))
			tbuf_printf(buf, ", ");

	}

	tbuf_printf(buf, ">");
}

static void
xlog_print(struct tbuf *out, u16 op, struct tbuf *b)
{
	u32 n, key_cardinality, key_bsize;
	void *key;
	u32 cardinality, field_no;
	u32 flags = 0;
	u32 op_cnt;

	n = read_u32(b);

	switch (op) {
	case INSERT:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		cardinality = read_u32(b);
		u32 data_len = tbuf_len(b);
		void *data = read_bytes(b, data_len);

		if (tuple_bsize(cardinality, data, data_len) != data_len)
			abort();
		tuple_print(out, cardinality, data);
		break;

	case DELETE:
		flags = read_u32(b);
	case DELETE_1_3:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		key_cardinality = read_u32(b);
		key_bsize = tbuf_len(b);
		key = read_bytes(b, key_bsize);

		if (tuple_bsize(key_cardinality, key, key_bsize) != key_bsize)
			abort();

		if (op == DELETE)
			tbuf_printf(out, "flags:%08X ", flags);
		tuple_print(out, key_cardinality, key);
		break;

	case UPDATE_FIELDS:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		key_cardinality = read_u32(b);
		key_bsize = tuple_bsize(key_cardinality, b->ptr, tbuf_len(b));
		key = read_bytes(b, key_bsize);

		op_cnt = read_u32(b);

		tbuf_printf(out, "flags:%08X ", flags);
		tuple_print(out, key_cardinality, key);

		while (op_cnt-- > 0) {
			field_no = read_u32(b);
			u8 op = read_u8(b);
			void *arg = read_field(b);

			tbuf_printf(out, " [field_no:%i op:", field_no);
			switch (op) {
			case 0:
				tbuf_printf(out, "set ");
				break;
			case 1:
				tbuf_printf(out, "add ");
				break;
			case 2:
				tbuf_printf(out, "and ");
				break;
			case 3:
				tbuf_printf(out, "xor ");
				break;
			case 4:
				tbuf_printf(out, "or ");
				break;
			case 5:
				tbuf_printf(out, "splice ");
				break;
			case 6:
				tbuf_printf(out, "delete ");
				break;
			case 7:
				tbuf_printf(out, "insert ");
				break;
			}
			tuple_print(out, 1, arg);
			tbuf_printf(out, "] ");
		}
		break;

	case NOP:
		tbuf_printf(out, "NOP");
		break;
	default:
		tbuf_printf(out, "unknown wal op %" PRIi32, op);
	}

	if (tbuf_len(b) > 0)
		abort();
}

static void
snap_print(struct tbuf *out, struct tbuf *row)
{
	struct box_snap_row *snap = box_snap_row(row);
	tbuf_printf(out, "n:%i ", snap->object_space);
	tuple_print(out, snap->tuple_size, snap->data);
}


static void
print_row(struct tbuf *out, u16 tag, struct tbuf *r)
{
	int tag_type = tag & ~TAG_MASK;
	tag &= TAG_MASK;
	if (tag == wal_data) {
		u16 op = read_u16(r);
		xlog_print(out, op, r);
		return;
	}
	if (tag_type == TAG_WAL) {
		u16 op = tag >> 5;
		xlog_print(out, op, r);
		return;
	}
	if (tag == snap_data) {
		snap_print(out, r);
		return;
	}
}

@interface CatRecovery: FoldRecovery {
	i64 stop_scn;
}
@end

@implementation CatRecovery
- (id)
init_snap_dir:(const char *)snap_dirname
      wal_dir:(const char *)wal_dirname
     stop_scn:(i64)scn_
{
	[super init_snap_dir:snap_dirname
		     wal_dir:wal_dirname];
	stop_scn = scn_;
	return self;
}
- (i64)
snap_lsn
{
	return [snap_dir containg_scn:stop_scn];
}

- (void)
recover_row:(struct row_v12 *)r
{
	[super recover_row:r];
	struct tbuf *out = tbuf_alloc(fiber->pool);
	print_gen_row(out, r, print_row);
	puts(out->ptr);
	if (r->scn >= stop_scn && (r->tag & ~TAG_MASK) == TAG_WAL)
		exit(0);
}

- (void)
apply:(struct tbuf *)op tag:(u16)tag
{
	(void)op;
	(void)tag;
}

- (void)
wal_final_row
{
	say_error("unable to find record with SCN:%"PRIi64, stop_scn);
	exit(EX_OSFILE);
}

@end


int
box_cat_scn(i64 stop_scn)
{
	[[[CatRecovery alloc] init_snap_dir:cfg.snap_dir
				    wal_dir:cfg.wal_dir
				   stop_scn:stop_scn] recover_start];
	return 0;
}

int
box_cat(const char *filename)
{
	const char *q = getenv("BOX_CAT_QUOTE");
	if (q && !strcmp(q, "ALL")) {
		quote = quote_all;
	} else {
		quote = quote_non_printable;
	}
	read_log(filename, print_row);
	return 0; /* ignore return status of read_log */
}
