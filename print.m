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

void
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
	struct index_conf ic = { .n = 0 };

	n = read_u32(b);

	switch (op) {
	case INSERT:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		cardinality = read_u32(b);
		u32 data_len = tbuf_len(b);
		void *data = read_bytes(b, data_len);

		tbuf_printf(out, "flags:%08X ", flags);
		if (fields_bsize(cardinality, data, data_len) == data_len)
			tuple_print(out, cardinality, data);
		else
			tbuf_printf(out, "<CORRUPT TUPLE>");
		break;

	case DELETE:
		flags = read_u32(b);
	case DELETE_1_3:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		key_cardinality = read_u32(b);
		key_bsize = tbuf_len(b);
		key = read_bytes(b, key_bsize);

		if (fields_bsize(key_cardinality, key, key_bsize) != key_bsize) {
			tbuf_printf(out, "<CORRUPT KEY>");
			break;
		}

		if (op == DELETE)
			tbuf_printf(out, "flags:%08X ", flags);
		tuple_print(out, key_cardinality, key);
		break;

	case UPDATE_FIELDS:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		key_cardinality = read_u32(b);
		key_bsize = fields_bsize(key_cardinality, b->ptr, tbuf_len(b));
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
			default:
				tbuf_printf(out, "CORRUPT_OP:%i", op);
			}
			tuple_print(out, 1, arg);
			tbuf_printf(out, "] ");
		}
		break;

	case NOP:
		tbuf_printf(out, "NOP");
		break;

	case CREATE_OBJECT_SPACE:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		tbuf_printf(out, "flags:%08X ", flags);
		tbuf_printf(out, "cardinalty:%i ", read_i8(b));
		index_conf_read(b, &ic);
		tbuf_printf(out, "PK: ");
		index_conf_print(out, &ic);
		break;
	case CREATE_INDEX:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		tbuf_printf(out, "flags:%08X ", flags);
		ic.n = read_i8(b);
		index_conf_read(b, &ic);
		index_conf_print(out, &ic);
		break;
	case DROP_OBJECT_SPACE:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		tbuf_printf(out, "flags:%08X ", flags);
		break;
	case DROP_INDEX:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		tbuf_printf(out, "flags:%08X ", flags);
		tbuf_printf(out, "index:%i", read_i8(b));
		break;
	case TRUNCATE:
		tbuf_printf(out, "%s n:%i ", box_ops[op], n);
		flags = read_u32(b);
		break;
	default:
		tbuf_printf(out, "unknown wal op %" PRIi32, op);
	}

	if (tbuf_len(b) > 0)
		tbuf_printf(out, ", %i bytes unparsed %s", tbuf_len(b), tbuf_to_hex(b));
}

static void
snap_print(struct tbuf *out, struct tbuf *row)
{
	struct box_snap_row *snap = box_snap_row(row);
	tbuf_printf(out, "n:%i ", snap->object_space);
	tuple_print(out, snap->tuple_size, snap->data);
}


void
box_print_row(struct tbuf *out, u16 tag, struct tbuf *r)
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
	if (tag_type == TAG_SNAP) {
		if (tag == snap_data) {
			snap_print(out, r);
		} else {
			u16 op = tag >> 5;
			xlog_print(out, op, r);
		}
	}
}

const char *
box_row_to_a(u16 tag, struct tbuf *data)
{
	@try {
		struct tbuf *buf = tbuf_alloc(fiber->pool);
		struct tbuf tmp = *data;
		box_print_row(buf, tag, &tmp);
		return buf->ptr;
	}
	@catch (id e) {
		return tbuf_to_hex(data);
	}
}


@interface BoxPrint: Box <RecoverRow> {
	i64 stop_scn;
}
@end

@implementation BoxPrint
- (id)
init_stop_scn:(i64)stop_scn_
{
	[super init];
	stop_scn = stop_scn_;
	return self;
}

- (void)
recover_row:(struct row_v12 *)r
{
	struct tbuf *buf = tbuf_alloc(fiber->pool);
	[self print:r into:buf];
	puts(buf->ptr);
	if (r->scn >= stop_scn && (r->tag & ~TAG_MASK) == TAG_WAL)
		exit(0);
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
	BoxPrint *printer = [[BoxPrint alloc] init_stop_scn:stop_scn];
	XLogReader *reader = [[XLogReader alloc] init_recovery:(id)printer];
	snap_lsn = [snap_dir containg_scn:stop_scn shard:0];
	[reader load_from_local:0];
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
	read_log(filename, box_print_row);
	return 0; /* ignore return status of read_log */
}
