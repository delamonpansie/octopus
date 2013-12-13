/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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
#import <iproto.h>
#import <log_io.h>
#import <net_io.h>
#import <pickle.h>
#import <salloc.h>
#import <say.h>
#import <stat.h>
#import <octopus.h>
#import <tbuf.h>
#import <util.h>
#import <objc.h>
#import <assoc.h>
#import <index.h>
#import <paxos.h>

#import <mod/box/box.h>
#import <mod/box/moonbox.h>
#import <mod/box/box_version.h>

#include <third_party/crc32.h>

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sysexits.h>

static struct service box_primary, box_secondary;

static int stat_base;
static char * const ops[] = ENUM_STR_INITIALIZER(MESSAGES);

char *primary_addr;
struct object_space *object_space_registry;
const int object_space_count = 256, object_space_max_idx = MAX_IDX;

struct box_snap_row {
	u32 object_space;
	u32 tuple_size;
	u32 data_size;
	u8 data[];
} __attribute((packed));

void __attribute__((noreturn))
bad_object_type()
{
	raise("bad object type");
}

static inline struct box_snap_row *
box_snap_row(const struct tbuf *t)
{
	return (struct box_snap_row *)t->ptr;
}


void *
next_field(void *f)
{
	u32 size = LOAD_VARINT32(f);
	return (u8 *)f + size;
}

void *
tuple_field(struct box_tuple *tuple, size_t i)
{
	void *field = tuple->data;

	if (i >= tuple->cardinality)
		return NULL;

	while (i-- > 0)
		field = next_field(field);

	return field;
}

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

static struct tnt_object *
tuple_alloc(unsigned cardinality, unsigned size)
{
	struct tnt_object *obj = object_alloc(BOX_TUPLE, sizeof(struct box_tuple) + size);
	struct box_tuple *tuple = box_tuple(obj);

	tuple->bsize = size;
	tuple->cardinality = cardinality;
	say_debug("tuple_alloc(%u, %u) = %p", cardinality, size, tuple);
	return obj;
}

static bool
valid_tuple(u32 cardinality, const void *data, u32 data_len)
{
	struct tbuf tmp = TBUF(data, data_len, NULL);
	for (int i = 0; i < cardinality; i++)
		read_field(&tmp);

	return tbuf_len(&tmp) == 0;
}

static void
tuple_add(struct netmsg_head *h, struct iproto_retcode *reply, struct tnt_object *obj)
{
	struct box_tuple *tuple = box_tuple(obj);
	size_t size = tuple->bsize +
		      sizeof(tuple->bsize) +
		      sizeof(tuple->cardinality);

	reply->data_len += size;

	/* it's faster to copy & join small tuples into single large
	   iov entry. join is done by net_add_iov() */
	if (tuple->bsize > 512)
		net_add_obj_iov(h, obj, &tuple->bsize, size);
	else
		net_add_iov_dup(h, &tuple->bsize, size);
}

static void
validate_indexes(BoxTxn *txn)
{
	foreach_index(index, txn->object_space) {
                [index valid_object:txn->obj];

		if (index->conf.unique) {
                        struct tnt_object *obj = [index find_by_obj:txn->obj];

                        if (obj != NULL && obj != txn->old_obj)
				iproto_raise_fmt(ERR_CODE_INDEX_VIOLATION,
						 "duplicate key value violates unique index %i:%s",
						 index->conf.n, [[index class] name]);
                }
	}
}

static struct tnt_object *
txn_acquire(BoxTxn *txn, struct tnt_object *obj)
{
	if (unlikely(obj == NULL))
		return NULL;

	int i;
	for (i = 0; i < nelem(txn->ref); i++)
		if (txn->ref[i] == NULL) {
			object_lock(obj); /* throws exception on lock failure */
			txn->ref[i] = obj;
			object_incr_ref(obj);
			return obj;
		}
	panic("txn->ref[] to small i:%i", i);
}


static void __attribute__((noinline))
prepare_replace(BoxTxn *txn, size_t cardinality, const void *data, u32 data_len)
{
	if (cardinality == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality can't be equal to 0");
	if (data_len == 0 || !valid_tuple(cardinality, data, data_len))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "tuple encoding error");

	txn->obj = txn_acquire(txn, tuple_alloc(cardinality, data_len));
	struct box_tuple *tuple = box_tuple(txn->obj);
	memcpy(tuple->data, data, data_len);

	txn->old_obj = txn_acquire(txn, [txn->index find_by_obj:txn->obj]);
	txn->obj_affected = txn->old_obj != NULL ? 2 : 1;

	if (txn->flags & BOX_ADD && txn->old_obj != NULL)
		iproto_raise(ERR_CODE_NODE_FOUND, "tuple found");

	if (txn->flags & BOX_REPLACE && txn->old_obj == NULL)
		iproto_raise(ERR_CODE_NODE_NOT_FOUND, "tuple not found");

	validate_indexes(txn);

	say_debug("%s: old_obj:%p obj:%p", __func__, txn->old_obj, txn->obj);

	if (txn->old_obj == NULL) {
		/*
		 * if tuple doesn't exist insert GHOST tuple in indeces
		 * in order to avoid race condition
		 */
		foreach_index(index, txn->object_space)
			[index replace: txn->obj];
		object_incr_ref(txn->obj);

		txn->obj->flags |= GHOST;
	}
}

static void
obj_remove(struct object_space *object_space, struct tnt_object *obj)
{
	foreach_index(index, object_space) {
		int deleted = [index remove: obj];
		(void)deleted;
		assert(deleted == 1);
	}
	object_decr_ref(obj);
}

static void
commit_replace(BoxTxn *txn)
{
	say_debug("%s: old_obj:%p obj:%p", __func__, txn->old_obj, txn->obj);
	if (txn->old_obj != NULL)
		obj_remove(txn->object_space, txn->old_obj);

	if (txn->obj != NULL) {
		if (txn->obj->flags & GHOST) {
			txn->obj->flags &= ~GHOST;
		} else {
			foreach_index(index, txn->object_space)
				[index replace: txn->obj];
			object_incr_ref(txn->obj);
		}
	}

}

static void
rollback_replace(BoxTxn *txn)
{
	say_debug("rollback_replace: txn->obj:%p", txn->obj);

	if (txn->obj && txn->obj->flags & GHOST)
		obj_remove(txn->object_space, txn->obj);
}

static void
do_field_arith(u8 op, struct tbuf *field, const void *arg, u32 arg_size)
{
	if (tbuf_len(field) != arg_size)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "num op arg size not equal to field size");

	switch (arg_size) {
	case 2:
		switch (op) {
		case 1: *(u16 *)field->ptr += *(u16 *)arg; break;
		case 2: *(u16 *)field->ptr &= *(u16 *)arg; break;
		case 3: *(u16 *)field->ptr ^= *(u16 *)arg; break;
		case 4: *(u16 *)field->ptr |= *(u16 *)arg; break;
		default: iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unknown num op");
		}
		break;
	case 4:
		switch (op) {
		case 1: *(u32 *)field->ptr += *(u32 *)arg; break;
		case 2: *(u32 *)field->ptr &= *(u32 *)arg; break;
		case 3: *(u32 *)field->ptr ^= *(u32 *)arg; break;
		case 4: *(u32 *)field->ptr |= *(u32 *)arg; break;
		default: iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unknown num op");
		}
		break;
	case 8:
		switch (op) {
		case 1: *(u64 *)field->ptr += *(u64 *)arg; break;
		case 2: *(u64 *)field->ptr &= *(u64 *)arg; break;
		case 3: *(u64 *)field->ptr ^= *(u64 *)arg; break;
		case 4: *(u64 *)field->ptr |= *(u64 *)arg; break;
		default: iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unknown num op");
		}
		break;
	default: iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad num op size");
	}
}

static inline size_t __attribute__((pure))
field_len(const struct tbuf *b)
{
	return varint32_sizeof(tbuf_len(b)) + tbuf_len(b);
}

static size_t
do_field_splice(struct tbuf *field, const void *args_data, u32 args_data_size)
{
	struct tbuf args = TBUF(args_data, args_data_size, NULL);
	struct tbuf *new_field = NULL;
	const u8 *offset_field, *length_field, *list_field;
	u32 offset_size, length_size, list_size;
	i32 offset, length;
	u32 noffset, nlength;	/* normalized values */

	new_field = tbuf_alloc(fiber->pool);

	offset_field = read_field(&args);
	length_field = read_field(&args);
	list_field = read_field(&args);
	if (tbuf_len(&args)!= 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: bad args");

	offset_size = LOAD_VARINT32(offset_field);
	if (offset_size == 0)
		noffset = 0;
	else if (offset_size == sizeof(offset)) {
		offset = *(u32 *)offset_field;
		if (offset < 0) {
			if (tbuf_len(field) < -offset)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
					  "do_field_splice: noffset is negative");
			noffset = offset + tbuf_len(field);
		} else
			noffset = offset;
	} else
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: bad size of offset field");
	if (noffset > tbuf_len(field))
		noffset = tbuf_len(field);

	length_size = LOAD_VARINT32(length_field);
	if (length_size == 0)
		nlength = tbuf_len(field) - noffset;
	else if (length_size == sizeof(length)) {
		if (offset_size == 0)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
				  "do_field_splice: offset field is empty but length is not");

		length = *(u32 *)length_field;
		if (length < 0) {
			if ((tbuf_len(field) - noffset) < -length)
				nlength = 0;
			else
				nlength = length + tbuf_len(field) - noffset;
		} else
			nlength = length;
	} else
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: bad size of length field");
	if (nlength > (tbuf_len(field) - noffset))
		nlength = tbuf_len(field) - noffset;

	list_size = LOAD_VARINT32(list_field);
	if (list_size > 0 && length_size == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
			  "do_field_splice: length field is empty but list is not");
	if (list_size > (UINT32_MAX - (tbuf_len(field) - nlength)))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: list_size is too long");

	say_debug("do_field_splice: noffset = %i, nlength = %i, list_size = %u",
		  noffset, nlength, list_size);

	tbuf_reset(new_field);
	tbuf_append(new_field, field->ptr, noffset);
	tbuf_append(new_field, list_field, list_size);
	tbuf_append(new_field, field->ptr + noffset + nlength, tbuf_len(field) - (noffset + nlength));

	size_t diff = field_len(new_field) - field_len(field);

	*field = *new_field;
	return diff;
}

static void __attribute__((noinline))
prepare_update_fields(BoxTxn *txn, struct tbuf *data)
{
	struct tbuf *fields;
	const u8 *field;
	int i;
	u32 op_cnt;

	u32 key_cardinality = read_u32(data);
	txn->old_obj = txn_acquire(txn, [txn->index find_key:data with_cardinalty:key_cardinality]);

	op_cnt = read_u32(data);
	if (op_cnt > 128)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "too many ops");
	if (op_cnt == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "no ops");

	if (txn->old_obj == NULL) {
		/* pretend we parsed all data */
		tbuf_ltrim(data, tbuf_len(data));
		return;
	}
	txn->obj_affected = 1;

	struct box_tuple *old_tuple = box_tuple(txn->old_obj);
	size_t bsize = old_tuple->bsize;
	int cardinality = old_tuple->cardinality;
	int field_count = cardinality * 1.2;
	fields = palloc(fiber->pool, field_count * sizeof(struct tbuf));

	for (i = 0, field = old_tuple->data; i < cardinality; i++) {
		const void *src = field;
		int len = LOAD_VARINT32(field);
		/* .ptr  - start of varint
		   .end  - start of data
		   .free - len(data) */
		fields[i] = (struct tbuf){ .ptr = (void *)src, .end = (void *)field,
					   .free = len, .pool = NULL };
		field += len;
	}

	while (op_cnt-- > 0) {
		u8 op;
		u32 field_no, arg_size;
		const u8 *arg;
		struct tbuf *field = NULL;

		field_no = read_u32(data);
		op = read_u8(data);
		arg = read_field(data);
		arg_size = LOAD_VARINT32(arg);

		if (op <= 6) {
			if (field_no >= cardinality)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
					     "update of field beyond tuple cardinality");
			field = &fields[field_no];
		}
		if (op < 6) {
			if (field->pool == NULL) {
				void *field_data = field->end;
				int field_len = field->free;
				int expected_size = MAX(arg_size, field_len);
				field->ptr = palloc(fiber->pool, expected_size ? : 8);
				memcpy(field->ptr, field_data, field_len);
				field->end = field->ptr + field_len;
				field->free = expected_size - field_len;
				field->pool = fiber->pool;
			}
		}

		switch (op) {
		case 0:
			bsize -= field_len(field);
			bsize += varint32_sizeof(arg_size) + arg_size;
			tbuf_reset(field);
			tbuf_append(field, arg, arg_size);
			break;
		case 1 ... 4:
			do_field_arith(op, field, arg, arg_size);
			break;
		case 5:
			bsize += do_field_splice(field, arg, arg_size);
			break;
		case 6:
			if (arg_size != 0)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "delete must have empty arg");
			if (field_no == 0)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unabled to delete PK");

			if (field->pool == NULL) {
				bsize -= tbuf_len(field) + tbuf_free(field);
			} else {
				bsize -= field_len(field);
			}
			for (int i = field_no; i < cardinality - 1; i++)
				fields[i] = fields[i + 1];
			cardinality--;
			break;
		case 7:
			if (field_no == 0)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unabled to insert PK");
			if (field_no > cardinality)
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
					     "update of field beyond tuple cardinality");
			if (unlikely(field_count == cardinality)) {
				struct tbuf *tmp = fields;
				fields = p0alloc(fiber->pool,
						 (old_tuple->cardinality + 128) * sizeof(struct tbuf));
				memcpy(fields, tmp, field_count * sizeof(struct tbuf));
			}
			for (int i = cardinality - 1; i >= field_no; i--)
				fields[i + 1] = fields[i];
			void *ptr = palloc(fiber->pool, arg_size);
			fields[field_no] = TBUF(ptr, arg_size, fiber->pool);
			memcpy(fields[field_no].ptr, arg, arg_size);
			bsize += varint32_sizeof(arg_size) + arg_size;
			cardinality++;
			break;
		default:
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "invalid op");
		}
	}

	if (tbuf_len(data) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	txn->obj = txn_acquire(txn, tuple_alloc(cardinality, bsize));

	u8 *p = box_tuple(txn->obj)->data;
	i = 0;
	do {
		if (fields[i].pool == NULL) {
			void *ptr = fields[i].ptr;
			void *end = fields[i].end + fields[i].free;
			for (i++; i < cardinality; i++) {
				if (end != fields[i].ptr)
					break;
				else
					end = fields[i].end + fields[i].free;
			}
			memcpy(p, ptr, end - ptr);
			p += end - ptr;
		} else {
			int len = tbuf_len(&fields[i]);
			p = save_varint32(p, len);
			memcpy(p, fields[i].ptr, len);
			p += len;
			i++;
		}
	} while (i < cardinality);

	validate_indexes(txn);

	Index<BasicIndex> *pk = txn->object_space->index[0];
	if (![pk eq:txn->old_obj :txn->obj]) {
		foreach_index(index, txn->object_space)
			[index replace: txn->obj];
		object_ref(txn->obj, +1);

		txn->obj->flags |= GHOST;
		txn->obj_affected++;
	}
}


static void __attribute__((noinline))
process_select(struct netmsg_head *h, struct iproto_retcode *reply, Index<BasicIndex> *index,
	       u32 limit, u32 offset, struct tbuf *data)
{
	struct tnt_object *obj;
	uint32_t *found;
	u32 count = read_u32(data);

	say_debug("SELECT");
	found = palloc(h->pool, sizeof(*found));
	reply->data_len += sizeof(*found);
	net_add_iov(h, found, sizeof(*found));
	*found = 0;

	if (index->conf.type == HASH || (index->conf.unique && index->conf.cardinality == 1)) {
		for (u32 i = 0; i < count; i++) {
			u32 c = read_u32(data);
			obj = [index find_key:data with_cardinalty:c];
			if (obj == NULL)
				continue;
			if (unlikely(ghost(obj)))
				continue;
			if (unlikely(limit == 0))
				continue;
			if (unlikely(offset > 0)) {
				offset--;
				continue;
			}

			(*found)++;
			tuple_add(h, reply, obj);
			limit--;
		}
	} else {
		/* The only non unique index type is Tree */
		Tree *tree = (Tree *)index;
		for (u32 i = 0; i < count; i++) {
			u32 c = read_u32(data);
			[tree iterator_init:data with_cardinalty:c];

			if (unlikely(limit == 0))
				continue;

			while ((obj = [tree iterator_next_verify_pattern]) != NULL) {
				if (unlikely(ghost(obj)))
					continue;
				if (unlikely(limit == 0))
					continue;
				if (unlikely(offset > 0)) {
					offset--;
					continue;
				}

				(*found)++;
				tuple_add(h, reply, obj);
				--limit;
			}
		}
	}

	if (tbuf_len(data) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	stat_collect(stat_base, SELECT_KEYS, count);
}

static void __attribute__((noinline))
prepare_delete(BoxTxn *txn, struct tbuf *key_data)
{
	u32 c = read_u32(key_data);
	txn->old_obj = txn_acquire(txn, [txn->index find_key:key_data with_cardinalty:c]);
	txn->obj_affected = txn->old_obj != NULL;
}

static void
commit_delete(BoxTxn *txn)
{
	if (txn->old_obj)
		obj_remove(txn->object_space, txn->old_obj);
}


void
box_prepare_update(BoxTxn *txn)
{
	struct tbuf data = TBUF(txn->body, txn->body_len, NULL);
	say_debug("box_prepare_update(%i)", txn->op);

	i32 n = read_u32(&data);
	if (n < 0 || n > object_space_count - 1)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad namespace number");

	if (!object_space_registry[n].enabled)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "object_space %i is not enabled", n);

	if (object_space_registry[n].ignored)
		/* txn->object_space == NULL means this txn will be ignored */
		return;

	txn->object_space = &object_space_registry[n];
	txn->index = txn->object_space->index[0];

	switch (txn->op) {
	case INSERT:
		txn->flags = read_u32(&data);
		u32 cardinality = read_u32(&data);
		u32 data_len = tbuf_len(&data);
		void *tuple_bytes = read_bytes(&data, data_len);
		prepare_replace(txn, cardinality, tuple_bytes, data_len);
		break;

	case DELETE:
		txn->flags = read_u32(&data); /* RETURN_TUPLE */
	case DELETE_1_3:
		prepare_delete(txn, &data);
		break;

	case UPDATE_FIELDS:
		txn->flags = read_u32(&data);
		prepare_update_fields(txn, &data);
		break;

	case NOP:
		break;

	default:
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown opcode:%"PRIi32, txn->op);
	}

	if (txn->obj) {
		struct box_tuple *tuple = box_tuple(txn->obj);
		if (txn->object_space->cardinality > 0 &&
		    txn->object_space->cardinality != tuple->cardinality)
		{
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
				     "tuple cardinality must match object_space cardinality");
		}

		if (!valid_tuple(tuple->cardinality, tuple->data, tuple->bsize))
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "internal error");
	}
	if (tbuf_len(&data) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");
}

static void
box_lua_cb(struct iproto *request, struct conn *c)
{
	say_debug("%s: c:%p op:0x%02x sync:%u", __func__, c,
		  request->msg_code, request->sync);

	@try {
		ev_tstamp start = ev_now(), stop;

		if (unlikely(c->service != &box_primary))
			iproto_raise(ERR_CODE_NONMASTER, "updates forbiden on secondary port");

		box_dispach_lua(c, request);
		stat_collect(stat_base, EXEC_LUA, 1);

		stop = ev_now();
		if (stop - start > cfg.too_long_threshold)
			say_warn("too long %s: %.3f sec", ops[request->msg_code], stop - start);
	}
	@catch (Error *e) {
		say_warn("aborting lua request, [%s reason:\"%s\"] at %s:%d peer:%s",
			 [[e class] name], e->reason, e->file, e->line, conn_peer_name(c));
		if (e->backtrace)
			say_debug("backtrace:\n%s", e->backtrace);
		@throw;
	}
}

static void
box_paxos_cb(struct iproto *request __attribute__((unused)),
	     struct conn *c __attribute__((unused)))
{
	if ([recovery respondsTo:@selector(leader_redirect_raise)])
		[recovery perform:@selector(leader_redirect_raise)];
	else
		iproto_raise(ERR_CODE_UNSUPPORTED_COMMAND,
			     "PAXOS_LEADER unsupported in non cluster configuration");
}

static void
box_cb(struct iproto *request, struct conn *c)
{
	say_debug("%s: c:%p op:0x%02x sync:%u", __func__, c, request->msg_code, request->sync);

	struct BoxTxn *txn = [BoxTxn palloc];
	@try {
		ev_tstamp start = ev_now(), stop;

		if (unlikely(c->service != &box_primary))
			iproto_raise(ERR_CODE_NONMASTER, "updates forbiden on secondary port");

		[recovery check_replica];
		[txn prepare:request->msg_code
			data:request->data
			 len:request->data_len];

		if (!txn->object_space)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "ignored object space");

		if (txn->obj_affected > 0) {
			if ([recovery submit:txn] != 1)
				iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
		}
		[txn commit];

		struct netmsg_head *h = &c->out_messages;
		struct iproto_retcode *reply = iproto_reply(h, request);
		reply->data_len += sizeof(u32);
		net_add_iov_dup(h, &txn->obj_affected, sizeof(u32));
		if (txn->flags & BOX_RETURN_TUPLE && txn->obj)
			tuple_add(h, reply, txn->obj);
		if (request->msg_code == DELETE && txn->flags & BOX_RETURN_TUPLE && txn->old_obj)
			tuple_add(h, reply, txn->old_obj);

		stop = ev_now();
		if (stop - start > cfg.too_long_threshold)
			say_warn("too long %s: %.3f sec", ops[txn->op], stop - start);
	}
	@catch (Error *e) {
		if (e->file && strcmp(e->file, "src/paxos.m") != 0) {
			say_warn("aborting txn, [%s reason:\"%s\"] at %s:%d peer:%s",
				 [[e class] name], e->reason, e->file, e->line, conn_peer_name(c));
			if (e->backtrace)
				say_debug("backtrace:\n%s", e->backtrace);
		}
		[txn rollback];
		@throw;
	}
}

static void
box_select_cb(struct netmsg_head *h, struct iproto *request, struct conn *c __attribute__((unused)))
{
	struct tbuf data = TBUF(request->data, request->data_len, fiber->pool);
	struct iproto_retcode *reply = iproto_reply(h, request);
	struct object_space *object_space;

	i32 n = read_u32(&data);
	u32 i = read_u32(&data);
	u32 offset = read_u32(&data);
	u32 limit = read_u32(&data);

	if (n < 0 || n > object_space_count - 1)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad namespace number");

	if (!object_space_registry[n].enabled)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "object_space %i is not enabled", n);

	if (i > MAX_IDX)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index too big");

	object_space = &object_space_registry[n];

	if ((object_space->index[i]) == NULL)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index is invalid");

	process_select(h, reply, object_space->index[i], limit, offset, &data);
	stat_collect(stat_base, request->msg_code, 1);
}

static void
xlog_print(struct tbuf *out, u16 op, struct tbuf *b)
{
	u32 n, key_len;
	void *key;
	u32 cardinality, field_no;
	u32 flags;
	u32 op_cnt;

	n = read_u32(b);

	switch (op) {
	case INSERT:
		tbuf_printf(out, "%s n:%i ", ops[op], n);
		flags = read_u32(b);
		cardinality = read_u32(b);
		u32 data_len = tbuf_len(b);
		void *data= read_bytes(b, data_len);

		if (!valid_tuple(cardinality, data, data_len))
			abort();
		tuple_print(out, cardinality, data);
		break;

	case DELETE:
		(void)read_u32(b); /* drop unused flags */
	case DELETE_1_3:
		tbuf_printf(out, "%s n:%i ", ops[op], n);
		key_len = read_u32(b);
		key = read_field(b);
		if (tbuf_len(b) != 0)
			abort();
		tuple_print(out, key_len, key);
		break;

	case UPDATE_FIELDS:
		tbuf_printf(out, "%s n:%i ", ops[op], n);
		flags = read_u32(b);
		key_len = read_u32(b);
		key = read_field(b);
		op_cnt = read_u32(b);

		tbuf_printf(out, "flags:%08X ", flags);
		tuple_print(out, key_len, key);

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
	if (tag == wal_tag) {
		u16 op = read_u16(r);
		xlog_print(out, op, r);
		return;
	}
	if (tag_type == TAG_WAL) {
		u16 op = tag >> 5;
		xlog_print(out, op, r);
		return;
	}
	if (tag == snap_tag) {
		snap_print(out, r);
		return;
	}
}

static void
configure(void)
{
	if (cfg.object_space == NULL)
		panic("at least one object_space should be configured");

	for (int i = 0; i < object_space_count; i++) {
		if (cfg.object_space[i] == NULL)
			break;

		if (!CNF_STRUCT_DEFINED(cfg.object_space[i]))
			object_space_registry[i].enabled = false;
		else
			object_space_registry[i].enabled = !!cfg.object_space[i]->enabled;

		if (!object_space_registry[i].enabled)
			continue;

		object_space_registry[i].ignored = !!cfg.object_space[i]->ignored;
		object_space_registry[i].cardinality = cfg.object_space[i]->cardinality;

		if (cfg.object_space[i]->index == NULL)
			panic("(object_space = %" PRIu32 ") at least one index must be defined", i);

		for (int j = 0; j < nelem(object_space_registry[i].index); j++) {

			if (cfg.object_space[i]->index[j] == NULL)
				break;

			struct index_conf *ic = cfg_box2index_conf(cfg.object_space[i]->index[j]);
			if (ic == NULL)
				panic("(object_space = %" PRIu32 " index = %" PRIu32 ") "
				      "unknown index type `%s'", i, j, cfg.object_space[i]->index[j]->type);

			ic->n = j;
			Index *index = [Index new_conf:ic dtor:&box_tuple_dtor];

			if (index == nil)
				panic("(object_space = %" PRIu32 " index = %" PRIu32 ") "
				      "XXX unknown index type `%s'", i, j, cfg.object_space[i]->index[j]->type);

			/* FIXME: only reasonable for HASH indexes */
			if ([index respondsTo:@selector(resize:)])
				[(id)index resize:cfg.object_space[i]->estimated_rows];

			if (index->conf.type == TREE && j > 0)
				index = [[DummyIndex alloc] init_with_index:index];

			object_space_registry[i].index[j] = (Index<BasicIndex> *)index;
		}

		Index *pk = object_space_registry[i].index[0];

		if (pk->conf.unique == false)
			panic("(object_space = %" PRIu32 ") object_space PK index must be unique", i);

		object_space_registry[i].enabled = true;

		say_info("object space %i successfully configured", i);
		say_info("  PK %i:%s", pk->conf.n, [[pk class] name]);
	}
}

void
title(const char *fmt, ...)
{
	va_list ap;
	char buf[64];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	set_proc_title("box:%s%s pri:%s sec:%s adm:%s",
		       buf, custom_proc_title,
		       cfg.primary_addr, cfg.secondary_addr, cfg.admin_addr);
}

static void
build_object_space_trees(struct object_space *object_space)
{
	Index<BasicIndex> *pk = object_space->index[0];
	size_t n_tuples = [pk size];
        size_t estimated_tuples = n_tuples * 1.2;

	Tree *ts[MAX_IDX] = { nil, };
	void *nodes[MAX_IDX] = { NULL, };
	int i = 0, tree_count = 0;

	for (int j = 0; object_space->index[j]; j++)
		if ([object_space->index[j] isKindOf:[DummyIndex class]]) {
			DummyIndex *dummy = (id)object_space->index[j];
			if ([dummy is_wrapper_of:[Tree class]]) {
				object_space->index[j] = [dummy unwrap];
				ts[i++] = (id)object_space->index[j];
			}
		}
	tree_count = i;
	if (tree_count == 0)
		return;

	say_info("Building tree indexes of object space %i", object_space->n);

        if (n_tuples > 0) {
		for (int i = 0; i < tree_count; i++) {
                        nodes[i] = xmalloc(estimated_tuples * ts[i]->node_size);
			if (nodes[i] == NULL)
                                panic("can't allocate node array");
                }

		struct tnt_object *obj;
		u32 t = 0;
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			for (int i = 0; i < tree_count; i++) {
                                struct index_node *node = nodes[i] + t * ts[i]->node_size;
                                ts[i]->dtor(obj, node, ts[i]->dtor_arg);
                        }
                        t++;
		}
	}

	for (int i = 0; i < tree_count; i++) {
		say_info("  %i:%s", ts[i]->conf.n, [[ts[i] class] name]);
		[ts[i] set_nodes:nodes[i]
			   count:n_tuples
		       allocated:estimated_tuples];
	}
}

static void
build_secondary_indexes()
{
	title("building_indexes");
	@try {
		for (u32 n = 0; n < object_space_count; n++) {
			if (object_space_registry[n].enabled)
				build_object_space_trees(&object_space_registry[n]);
		}
	}
	@catch (Error *e) {
		raise("unable to built tree indexes: %s", e->reason);
	}

	for (u32 n = 0; n < object_space_count; n++) {
		if (!object_space_registry[n].enabled)
			continue;

		struct tbuf *i = tbuf_alloc(fiber->pool);
		foreach_index(index, &object_space_registry[n])
			tbuf_printf(i, " %i:%s", index->conf.n, [[index class] name]);

		say_info("Object space %i indexes:%.*s", n, tbuf_len(i), (char *)i->ptr);
	}
}

void
box_bound_to_primary(int fd)
{
	if (fd < 0) {
		if (!cfg.local_hot_standby)
			panic("unable bind to %s", cfg.primary_addr);
		return;
	}

	if (cfg.local_hot_standby) {
		@try {
			[recovery enable_local_writes];
			title("%s", [recovery status]);
		}
		@catch (Error *e) {
			panic("Recovery failure: %s", e->reason);
		}
	}
}

static void
box_service_register(struct service *s)
{
	service_iproto(s);

	service_register_iproto_stream(s, NOP, box_select_cb, 0);
	service_register_iproto_stream(s, SELECT, box_select_cb, 0);
	service_register_iproto_stream(s, SELECT_LIMIT, box_select_cb, 0);
	service_register_iproto_block(s, INSERT, box_cb, 0);
	service_register_iproto_block(s, UPDATE_FIELDS, box_cb, 0);
	service_register_iproto_block(s, DELETE, box_cb, 0);
	service_register_iproto_block(s, DELETE_1_3, box_cb, 0);
	service_register_iproto_block(s, EXEC_LUA, box_lua_cb, 0);
	service_register_iproto_block(s, PAXOS_LEADER, box_paxos_cb, 0);
}

static void
initialize_service()
{
	tcp_service(&box_primary, cfg.primary_addr, box_bound_to_primary, iproto_wakeup_workers);
	box_service_register(&box_primary);

	for (int i = 0; i < MAX(1, cfg.wal_writer_inbox_size); i++)
		fiber_create("box_worker", iproto_worker, &box_primary);

	if (cfg.secondary_addr != NULL && strcmp(cfg.secondary_addr, cfg.primary_addr) != 0) {
		tcp_service(&box_secondary, cfg.secondary_addr, NULL, iproto_wakeup_workers);
		box_service_register(&box_secondary);
		fiber_create("box_secondary_worker", iproto_worker, &box_secondary);
	}
	say_info("(silver)box initialized (%i workers)", cfg.wal_writer_inbox_size);
}


@implementation BoxTxn

static void
txn_cleanup(BoxTxn *txn)
{
	assert(!txn->closed);
	txn->closed = true;
	/* do not null tnx->obj & txn->old_obj, as there is
	   code that examines contents of txn after commit */

	for (int i = 0; i < nelem(txn->ref); i++) {
		if (txn->ref[i] == NULL)
			break;
		object_unlock(txn->ref[i]);
		object_decr_ref(txn->ref[i]);
	}
}

- (void)
prepare:(u16)op_ data:(const void *)data len:(u32)len
{
	wal.tag = wal_tag;
	op = op_;
	body = data;
	body_len = len;
	box_prepare_update(self);
}


- (void)
prepare:(const struct row_v12 *)row data:(const void *)data
{
	memcpy(&wal, row, sizeof(wal));
	wal.len = 0;

	say_debug("%s tag:%s data:%s", __func__, xlog_tag_to_a(row->tag),
		 tbuf_to_hex(&TBUF(data, row->len, fiber->pool)));

	switch (row->tag & TAG_MASK) {
	case wal_tag:
		op = *(u16 *)data;
		body = data + sizeof(u16);
		body_len = row->len - sizeof(u16);
		assert(op != 0);
		box_prepare_update(self);
		break;
	case snap_tag:
		op = INSERT;
		body = data;
		body_len = row->len;

		const struct box_snap_row *snap = body;
		object_space = &object_space_registry[snap->object_space];
		if (!object_space->enabled)
			raise("object_space %i is not configured", object_space->n);
		if (object_space->ignored) {
			object_space = NULL;
			return;
		}
		index = object_space->index[0];
		assert(index != nil);

		prepare_replace(self, snap->tuple_size, snap->data, snap->data_size);
		break;
	}
}

- (void)
append:(struct wal_pack *)pack
{
	wal.tag |= TAG_WAL;
	wal_pack_append_row(pack, &wal);
	if (wal.len == 0) {
		wal_pack_append_data(pack, &wal, &op, sizeof(op));
		wal_pack_append_data(pack, &wal, body, body_len);
	}
}

- (struct row_v12 *)
row
{
	assert(wal.len == 0);
	wal.tag |= TAG_WAL;
	struct row_v12 *r = palloc(fiber->pool, sizeof(*r) + sizeof(op) + body_len);
	memcpy(r, &wal, sizeof(*r));
	r->len += sizeof(op) + body_len;
	memcpy(r->data, &op, sizeof(op));
	memcpy(r->data + sizeof(op), body, body_len);
	return r;
}

- (void)
commit
{
	if (!object_space)
		goto cleanup;

	if (op == DELETE || op == DELETE_1_3)
		commit_delete(self);
	else
		commit_replace(self);

	stat_collect(stat_base, op, 1);
	say_debug("%s: old_obj:refs=%i,%p obj:ref=%i,%p", __func__,
		 old_obj ? old_obj->refs : 0, old_obj,
		 obj ? obj->refs : 0, obj);
cleanup:
	txn_cleanup(self);
}

- (void)
rollback
{
	if (!object_space)
		goto cleanup;

	say_debug("box_rollback(op:%s)", ops[op]);

	if (op == DELETE || op == DELETE_1_3)
		goto cleanup;

	if (op == INSERT || op == UPDATE_FIELDS)
		rollback_replace(self);

	say_debug("%s: old_obj:refs=%i,%p obj:ref=%i,%p", __func__,
		 old_obj ? old_obj->refs : 0, old_obj,
		 obj ? obj->refs : 0, obj);

cleanup:
	txn_cleanup(self);
}
@end

@implementation Recovery (Box)

- (void)
check_replica
{
	if ([self is_replica])
		iproto_raise(ERR_CODE_NONMASTER, "replica is readonly");
}

- (void)
wal_final_row
{
	if (box_primary.name == NULL) {
		build_secondary_indexes();
		initialize_service();
	}

	/* recovery of empty local_hot_standby & remote_hot_standby replica done in reverse:
	   first: primary port bound & service initialized (and proctitle set)
	   second: pull rows from remote (ans proctitle set to "loading %xx.yy")
	   in order to avoid stuck proctitle set it after every pull done,
	   not after service initialization */
	title("%s", [recovery status]);
}

- (u32)
snapshot_estimate
{
	size_t total_rows = 0;
	for (int n = 0; n < object_space_count; n++)
		if (object_space_registry[n].enabled)
			total_rows += [object_space_registry[n].index[0] size];
	return total_rows;
}

- (int)
snapshot_fold
{
	struct tnt_object *obj;
	struct box_tuple *tuple;

	u32 crc = 0;
#ifdef FOLD_DEBUG
	int count = 0;
#endif
	for (int n = 0; n < object_space_count; n++) {
		if (!object_space_registry[n].enabled)
			continue;

		id pk = object_space_registry[n].index[0];

		if ([pk respondsTo:@selector(ordered_iterator_init)])
			[pk ordered_iterator_init];
		else

			[pk iterator_init];

		while ((obj = [pk iterator_next])) {
			tuple = box_tuple(obj);
#ifdef FOLD_DEBUG
			struct tbuf *b = tbuf_alloc(fiber->pool);
			tuple_print(b, tuple->cardinality, tuple->data);
			say_info("row %i: %.*s", count++, tbuf_len(b), (char *)b->ptr);
#endif
			crc = crc32c(crc, (unsigned char *)&tuple->bsize,
				     tuple->bsize + sizeof(tuple->bsize) +
				     sizeof(tuple->cardinality));
		}
	}
	printf("CRC: 0x%08x\n", crc);
	return 0;
}

@end

@implementation SnapWriter (Box)
- (int)
snapshot_write_rows:(XLog *)l
{
	struct box_snap_row header;
	struct tnt_object *obj;
	struct box_tuple *tuple;
	struct palloc_pool *pool = palloc_create_pool(__func__);
	struct tbuf *row = tbuf_alloc(pool);
	int ret = 0;
	size_t rows = 0, pk_rows, total_rows = [self snapshot_estimate];

	for (int n = 0; n < object_space_count; n++) {
		if (!object_space_registry[n].enabled)
			continue;

		pk_rows = 0;
		id pk = object_space_registry[n].index[0];
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			if (obj->refs <= 0) {
				say_error("heap invariant violation: n:%i obj->refs == %i", n, obj->refs);
				errno = EINVAL;
				ret = -1;
				goto out;
			}

			tuple = box_tuple(obj);
			if (!valid_tuple(tuple->cardinality, tuple->data, tuple->bsize)) {
				say_error("heap invariant violation: n:%i invalid tuple %p", n, obj);
				errno = EINVAL;
				ret = -1;
				goto out;
			}

			header.object_space = n;
			header.tuple_size = tuple->cardinality;
			header.data_size = tuple->bsize;

			tbuf_reset(row);
			tbuf_append(row, &header, sizeof(header));
			tbuf_append(row, tuple->data, tuple->bsize);

			if (snapshot_write_row(l, snap_tag, row) < 0) {
				ret = -1;
				goto out;
			}

			pk_rows++;
			if (++rows % 100000 == 0) {
				float pct = (float)rows / total_rows * 100.;
				say_info("%.1fM/%.2f%% rows written", rows / 1000000., pct);
				set_proc_title("dumper %.2f%% (%" PRIu32 ")", pct, getppid());
			}
			if (rows % 10000 == 0)
				[l confirm_write];
		}

		foreach_index(index, &object_space_registry[n]) {
			if (index->conf.n == 0)
				continue;

			/* during initial load of replica secondary indexes isn't configured yet */
			if ([index isKindOf:[DummyIndex class]])
				continue;

			set_proc_title("dumper check index:%i ((%" PRIu32 ")", index->conf.n, getppid());

			size_t index_rows = 0;
			[index iterator_init];
			while ([index iterator_next])
				index_rows++;
			if (pk_rows != index_rows) {
				say_error("heap invariant violation: n:%i index:%i rows:%zi != pk_rows:%zi",
					  n, index->conf.n, index_rows, pk_rows);
				errno = EINVAL;
				ret = -1;
				goto out;
			}
		}
	}

out:
	palloc_destroy_pool(pool);
	return ret;
}

@end


static void init_second_stage(va_list ap __attribute__((unused)));

static void
init(void)
{
	stat_base = stat_register(ops, nelem(ops));
	primary_addr = cfg.primary_addr;

	object_space_registry = xcalloc(object_space_count, sizeof(struct object_space));
	for (int i = 0; i < object_space_count; i++)
		object_space_registry[i].n = i;

	title("loading");
	if (cfg.paxos_enabled) {
		if (cfg.wal_feeder_addr)
			panic("wal_feeder_addr is incompatible with paxos");
		if (cfg.local_hot_standby)
			panic("wal_hot_standby is incompatible with paxos");
	}

	recovery = [[Recovery alloc] init_snap_dir:strdup(cfg.snap_dir)
					   wal_dir:strdup(cfg.wal_dir)
				      rows_per_wal:cfg.rows_per_wal
				       feeder_addr:cfg.wal_feeder_addr
					     flags:init_storage ? RECOVER_READONLY : 0
					 txn_class:[BoxTxn class]];

	if (init_storage)
		return;

	/* fiber is required to successfully pull from remote */
	fiber_create("box_init", init_second_stage);
}

static void
init_second_stage(va_list ap __attribute__((unused)))
{
	luaT_openbox(root_L);
	if (luaT_require("box_init") == -1)
		panic("unable to load `box_init' lua module: %s", lua_tostring(fiber->L, -1));

	configure();

	@try {
		i64 local_lsn = [recovery recover_start];
		if (cfg.paxos_enabled) {
			[recovery enable_local_writes];
		} else {
			if (local_lsn == 0) {
				if (!cfg.wal_feeder_addr) {
					say_error("unable to find initial snapshot");
					say_info("don't you forget to initialize "
						 "storage with --init-storage switch?");
					exit(EX_USAGE);
				}

				/* Break circular dependency.
				   Remote recovery depends on [enable_local_writes] wich itself
				   depends on binding to primary port.
				   Binding to primary port depends on wal_final_row from
				   remote replication. (There is no data in local WALs yet)
				 */
				if (cfg.wal_feeder_addr && cfg.local_hot_standby)
					[recovery wal_final_row];
			}
			if (!cfg.local_hot_standby)
				[recovery enable_local_writes];
		}
	}
	@catch (Error *e) {
		panic("Recovery failure: %s", e->reason);
	}
	title("%s", [recovery status]);
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


static int
cat_scn(i64 stop_scn)
{
	[[[CatRecovery alloc] init_snap_dir:cfg.snap_dir
				    wal_dir:cfg.wal_dir
				   stop_scn:stop_scn] recover_start];
	return 0;
}

static int
cat(const char *filename)
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


static void
info(struct tbuf *out, const char *what)
{
	if (what == NULL) {
		tbuf_printf(out, "info:" CRLF);
		tbuf_printf(out, "  version: \"%s\"" CRLF, octopus_version());
		tbuf_printf(out, "  uptime: %i" CRLF, tnt_uptime());
		tbuf_printf(out, "  pid: %i" CRLF, getpid());
		struct child *wal_writer = [recovery wal_writer];
		if (wal_writer)
			tbuf_printf(out, "  wal_writer_pid: %" PRIi64 CRLF,
				    (i64)wal_writer->pid);
		tbuf_printf(out, "  lsn: %" PRIi64 CRLF, [recovery lsn]);
		tbuf_printf(out, "  scn: %" PRIi64 CRLF, [recovery scn]);
		if ([recovery is_replica]) {
			tbuf_printf(out, "  recovery_lag: %.3f" CRLF, [recovery lag]);
			tbuf_printf(out, "  recovery_last_update: %.3f" CRLF, [recovery last_update_tstamp]);
			if (!cfg.ignore_run_crc) {
				tbuf_printf(out, "  recovery_run_crc_lag: %.3f" CRLF, [recovery run_crc_lag]);
				tbuf_printf(out, "  recovery_run_crc_status: %s" CRLF, [recovery run_crc_status]);
			}
		}
		tbuf_printf(out, "  status: %s%s" CRLF, [recovery status], custom_proc_title);
		tbuf_printf(out, "  config: \"%s\""CRLF, cfg_filename);

		tbuf_printf(out, "  namespaces:" CRLF);
		for (uint32_t n = 0; n < object_space_count; ++n) {
			if (!object_space_registry[n].enabled)
				continue;
			tbuf_printf(out, "  - n: %i"CRLF, n);
			tbuf_printf(out, "    objects: %i"CRLF, [object_space_registry[n].index[0] size]);
			tbuf_printf(out, "    indexes:"CRLF);
			foreach_index(index, &object_space_registry[n])
				tbuf_printf(out, "    - { index: %i, slots: %i, bytes: %zi }" CRLF,
					    index->conf.n, [index slots], [index bytes]);
		}
		return;
	}

	if (strcmp(what, "net") == 0) {
		if (box_primary.name != NULL)
			service_info(out, &box_primary);
		if (box_secondary.name != NULL)
			service_info(out, &box_secondary);
		return;
	}
}

static void
reload_config(struct octopus_cfg *old,
	      struct octopus_cfg *new)
{
	if (!old->wal_feeder_addr && !new->wal_feeder_addr)
		return;

	[recovery feeder_change_from:old->wal_feeder_addr
				  to:new->wal_feeder_addr];
	title("%s", [recovery status]);
}

static struct tnt_module box = {
	.name = "box",
	.version = box_version_string,
	.init = init,
	.reload_config = reload_config,
	.cat = cat,
	.cat_scn = cat_scn,
	.info = info
};

register_module(box);
register_source();
