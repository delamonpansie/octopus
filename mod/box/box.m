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
#import <object.h>
#import <assoc.h>
#import <index.h>
#import <paxos.h>

#import <mod/box/box.h>
#import <mod/box/moonbox.h>

#include <third_party/crc32.h>

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sysexits.h>

static struct service *box_primary, *box_secondary;

static int stat_base;
STRS(messages, MESSAGES);

const int MEMCACHED_OBJECT_SPACE = 23;

struct object_space *object_space_registry;
const int object_space_count = 256;

static Recovery *recovery;

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

static void
field_print(struct tbuf *buf, void *f)
{
	uint32_t size;

	size = LOAD_VARINT32(f);

	if (size == 2)
		tbuf_printf(buf, "%i:", *(u16 *)f);

	if (size == 4)
		tbuf_printf(buf, "%i:", *(u32 *)f);

	tbuf_printf(buf, "\"");
	while (size-- > 0) {
		if (0x20 <= *(u8 *)f && *(u8 *)f < 0x7f)
			tbuf_printf(buf, "%c", *(u8 *)f++);
		else
			tbuf_printf(buf, "\\x%02X", *(u8 *)f++);
	}
	tbuf_printf(buf, "\"");

}

static void
tuple_print(struct tbuf *buf, uint8_t cardinality, void *f)
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
valid_tuple(struct tbuf *buf, u32 cardinality)
{
	struct tbuf tmp = *buf;
	for (int i = 0; i < cardinality; i++)
		read_field(&tmp);

	return tbuf_len(&tmp) == 0;
}

static void
validate_indexes(struct box_txn *txn)
{
	foreach_index(index, txn->object_space) {
                [index valid_object:txn->obj];

		if (index->unique) {
                        struct tnt_object *obj = [index find_by_obj:txn->obj];

                        if (obj != NULL && obj != txn->old_obj)
				iproto_raise_fmt(ERR_CODE_INDEX_VIOLATION,
						 "duplicate key value violates unique index %i:%s",
						 index->n, [index class]->name);
                }
	}
}

static struct tnt_object *
txn_acquire(struct box_txn *txn, struct tnt_object *obj)
{
	if (unlikely(obj == NULL))
		return NULL;

	int i;
	for (i = 0; i < nelem(txn->ref); i++)
		if (txn->ref[i] == NULL) {
			@try {
				object_incr_ref(obj);
				object_lock(obj);
				txn->ref[i] = obj;
				return obj;
			}
			@catch (id e) {
				object_decr_ref(obj);
				@throw;
			}
		}
	panic("txn->ref[] to small i:%i", i);
}


static void __attribute((noinline))
prepare_replace(struct box_txn *txn, size_t cardinality, struct tbuf *data)
{
	if (cardinality == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality can't be equal to 0");
	if (tbuf_len(data) == 0 || !valid_tuple(data, cardinality))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "tuple encoding error");

	txn->obj = txn_acquire(txn, tuple_alloc(cardinality, tbuf_len(data)));
	struct box_tuple *tuple = box_tuple(txn->obj);
	memcpy(tuple->data, data->ptr, tbuf_len(data));
	tbuf_ltrim(data, tbuf_len(data));

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
commit_replace(struct box_txn *txn)
{
	say_debug("%s: old_obj:%p obj:%p", __func__, txn->old_obj, txn->obj);
	if (txn->old_obj != NULL) {
		foreach_index(index, txn->object_space)
			[index remove: txn->old_obj];
		object_decr_ref(txn->old_obj);
	}

	if (txn->obj != NULL) {
		if (txn->obj->flags & GHOST) {
			txn->obj->flags &= ~GHOST;
		} else {
			foreach_index(index, txn->object_space)
				[index replace: txn->obj];
			object_incr_ref(txn->obj);
		}
	}

	if (txn->m) {
		net_add_iov_dup(&txn->m, &txn->obj_affected, sizeof(u32));

		if (txn->obj && txn->flags & BOX_RETURN_TUPLE)
			tuple_add_iov(&txn->m, txn->obj);
	}
}

static void
rollback_replace(struct box_txn *txn)
{
	say_debug("rollback_replace: txn->obj:%p", txn->obj);

	if (txn->obj && txn->obj->flags & GHOST) {
		foreach_index(index, txn->object_space)
			[index remove: txn->obj];
		object_decr_ref(txn->obj);
	}
}

static void
do_field_arith(u8 op, struct tbuf *field, void *arg, u32 arg_size)
{
	if (tbuf_len(field) != 4)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "num op on field with length != 4");
	if (arg_size != 4)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "num op with arg not u32");

	switch (op) {
	case 1:
		*(i32 *)field->ptr += *(i32 *)arg;
		break;
	case 2:
		*(u32 *)field->ptr &= *(u32 *)arg;
		break;
	case 3:
		*(u32 *)field->ptr ^= *(u32 *)arg;
		break;
	case 4:
		*(u32 *)field->ptr |= *(u32 *)arg;
		break;
	}
}

static size_t
do_field_splice(struct tbuf *field, void *args_data, u32 args_data_size)
{
	struct tbuf args = TBUF(args_data, args_data_size, NULL);
	struct tbuf *new_field = NULL;
	void *offset_field, *length_field, *list_field;
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
		offset = pick_u32(offset_field, &offset_field);
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

		length = pick_u32(length_field, &length_field);
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

	size_t diff = (varint32_sizeof(tbuf_len(new_field)) + tbuf_len(new_field)) -
		      (varint32_sizeof(tbuf_len(field)) + tbuf_len(field));

	*field = *new_field;
	return diff;
}

static void __attribute__((noinline))
prepare_update_fields(struct box_txn *txn, struct tbuf *data)
{
	struct tbuf *fields;
	void *field;
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
		void *src = field;
		int len = LOAD_VARINT32(field);
		/* .ptr  - start of varint
		   .end  - start of data
		   .free - len(data) */
		fields[i] = (struct tbuf){ .ptr = src, .end = field, .free = len, .pool = NULL };
		field += len;
	}

	int __attribute__((pure)) field_len(const struct tbuf *b)
	{
		return varint32_sizeof(tbuf_len(b)) + tbuf_len(b);
	}

	while (op_cnt-- > 0) {
		u8 op;
		u32 field_no, arg_size;
		void *arg;
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

			bsize -= varint32_sizeof(tbuf_len(field)) + tbuf_len(field);
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

void
tuple_add_iov(struct netmsg **m, struct tnt_object *obj)
{
	struct box_tuple *tuple = box_tuple(obj);

	net_add_ref_iov(m, obj, &tuple->bsize,
			tuple->bsize + sizeof(tuple->bsize) +
			sizeof(tuple->cardinality));
}

static void __attribute__((noinline))
process_select(struct box_txn *txn, u32 limit, u32 offset, struct tbuf *data)
{
	struct tnt_object *obj;
	uint32_t *found;
	u32 count = read_u32(data);

	say_debug("SELECT");
	found = palloc(txn->m->head->pool, sizeof(*found));
	net_add_iov(&txn->m, found, sizeof(*found));
	*found = 0;

	if (txn->index->unique) {
		for (u32 i = 0; i < count; i++) {
			u32 c = read_u32(data);
			obj = [txn->index find_key:data with_cardinalty:c];
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
			tuple_add_iov(&txn->m, obj);
			limit--;
		}
	} else {
		/* The only non unique index type is Tree */
		Tree *tree = (Tree *)txn->index;
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
				tuple_add_iov(&txn->m, obj);
				--limit;
			}
		}
	}

	if (tbuf_len(data) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	stat_collect(stat_base, SELECT_KEYS, count);
	stat_collect(stat_base, txn->op, 1);
}

static void __attribute__((noinline))
prepare_delete(struct box_txn *txn, struct tbuf *key_data)
{
	u32 c = read_u32(key_data);
	txn->old_obj = txn_acquire(txn, [txn->index find_key:key_data with_cardinalty:c]);
	txn->obj_affected = txn->old_obj != NULL;
}

static void
commit_delete(struct box_txn *txn)
{
	if (txn->old_obj) {
		foreach_index(index, txn->object_space)
			[index remove: txn->old_obj];
		object_ref(txn->old_obj, -1);
	}
	if (txn->m)
		net_add_iov_dup(&txn->m, &txn->obj_affected, sizeof(u32));
}

void
txn_init(struct iproto *req, struct box_txn *txn, struct netmsg *m)
{
	memset(txn, 0, sizeof(*txn));
	txn->op = req->msg_code;
	if (m) {
		txn->m = m;
		netmsg_getmark(txn->m, &txn->header_mark);
		iproto_reply(&txn->m, req->msg_code, req->sync);
	}
}

void
txn_cleanup(struct box_txn *txn)
{
	if (txn->op == 0) /* txn wasn't initialized, e.g. txn->op wasn't set by box_prepare_update */
		return;

	for (int i = 0; i < nelem(txn->ref); i++) {
		if (txn->ref[i] == NULL)
			break;
		object_unlock(txn->ref[i]);
		object_decr_ref(txn->ref[i]);
	}

	say_debug("%s: old_obj:refs=%i,%p obj:ref=%i,%p", __func__,
		 txn->old_obj ? txn->old_obj->refs : 0, txn->old_obj,
		 txn->obj ? txn->obj->refs : 0, txn->obj);

	/* mark txn as clean */
	memset(txn, 0, sizeof(*txn));
}


static void
update_crc(struct tnt_object *obj, u32 *crc)
{
	if (!obj)
		return;

	struct box_tuple *tuple = box_tuple(obj);
	u32 len = tuple->bsize + sizeof(tuple->bsize) + sizeof(tuple->cardinality);
	u32 old_crc = *crc;
	*crc = crc32c(*crc, (void *)obj, len);
	say_debug("%s: obj:%p tuple:%p, tuple->bsize:%i len:%i bytes:%s old_crc:0x%x crc:0x%x", __func__,
		  obj, tuple, tuple->bsize,
		  len, tbuf_to_hex(&TBUF(obj, len, fiber->pool)),
		  old_crc, *crc);
}

void
txn_commit(struct box_txn *txn)
{
	if (txn->op == DELETE)
		commit_delete(txn);
	else
		commit_replace(txn);

	if (unlikely(!txn->snap)) {
		update_crc(txn->old_obj, &recovery->run_crc_mod);
		update_crc(txn->obj, &recovery->run_crc_mod);
	}

	say_info("txn_commit(op:%s) scn:%"PRIi64 " run_crc_mod:0x%x",
		  messages_strs[txn->op], [recovery scn], recovery->run_crc_mod);
	stat_collect(stat_base, txn->op, 1);

	say_debug("%s: old_obj:refs=%i,%p obj:ref=%i,%p", __func__,
		 txn->old_obj ? txn->old_obj->refs : 0, txn->old_obj,
		 txn->obj ? txn->obj->refs : 0, txn->obj);
}

void
txn_abort(struct box_txn *txn)
{
	say_debug("box_rollback(op:%s)", messages_strs[txn->op]);

	if (txn->op == DELETE)
		return;

	if (txn->op == INSERT || txn->op == UPDATE_FIELDS)
		rollback_replace(txn);

	say_debug("%s: old_obj:refs=%i,%p obj:ref=%i,%p", __func__,
		 txn->old_obj ? txn->old_obj->refs : 0, txn->old_obj,
		 txn->obj ? txn->obj->refs : 0, txn->obj);
}

void
txn_submit_to_storage(struct box_txn *txn)
{
	if ([recovery submit:txn->wal_record->ptr
			 len:tbuf_len(txn->wal_record)] != 1)
		iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
	say_debug("%s: old_obj:refs=%i,%p obj:ref=%i,%p", __func__,
		 txn->old_obj ? txn->old_obj->refs : 0, txn->old_obj,
		 txn->obj ? txn->obj->refs : 0, txn->obj);
}

static void
txn_common_parser(struct box_txn *txn, struct tbuf *data)
{
	i32 n = read_u32(data);
	if (n < 0 || n > object_space_count - 1)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad namespace number");

	if (!object_space_registry[n].enabled)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "object_space %i is not enabled", n);

	txn->object_space = &object_space_registry[n];
	txn->index = txn->object_space->index[0];
}

static bool __attribute__((pure))
op_is_select(u32 op)
{
	return op == SELECT || op == SELECT_LIMIT;
}

static void
box_dispach_select(struct box_txn *txn, struct tbuf *data)
{
	txn_common_parser(txn, data);
	say_debug("box_dispach(%i)", txn->op);

	u32 i = read_u32(data);
	u32 offset = read_u32(data);
	u32 limit = read_u32(data);

	if (i > MAX_IDX)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index too big");

	if ((txn->index = txn->object_space->index[i]) == NULL)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index is invalid");

	process_select(txn, limit, offset, data);
}


void
box_prepare_update(struct box_txn *txn, struct tbuf *data)
{
	txn->wal_record = tbuf_alloc(fiber->pool);
	tbuf_append(txn->wal_record, &txn->op, sizeof(txn->op));
	tbuf_append(txn->wal_record, data->ptr, tbuf_len(data));

	say_debug("box_dispach(%i)", txn->op);

	switch (txn->op) {
	case INSERT:
		txn_common_parser(txn, data);
		txn->flags = read_u32(data);
		u32 cardinality = read_u32(data);
		if (txn->object_space->cardinality > 0
		    && txn->object_space->cardinality != cardinality)
		{
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
				  "tuple cardinality must match object_space cardinality");
		}
		prepare_replace(txn, cardinality, data);
		break;

	case DELETE:
		txn_common_parser(txn, data);
		prepare_delete(txn, data);
		break;

	case UPDATE_FIELDS:
		txn_common_parser(txn, data);
		txn->flags = read_u32(data);
		prepare_update_fields(txn, data);
		break;

	case NOP:
		txn_common_parser(txn, data);
		break;

	default:
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown opcode:%"PRIi32, txn->op);
	}

	if (txn->obj) {
		struct box_tuple *tuple = box_tuple(txn->obj);
		if (!valid_tuple(&TBUF(tuple->data, tuple->bsize, NULL), tuple->cardinality))
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "internal error");
	}
	if (tbuf_len(data) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");
}

static void
box_process(struct conn *c, struct tbuf *request, void *arg __attribute__((unused)))
{
	struct box_txn txn = { .op = 0 };
	u32 msg_code = iproto(request)->msg_code;
	struct tbuf request_data = TBUF(iproto(request)->data, iproto(request)->data_len, fiber->pool);
	say_debug("%s: c:%p", __func__, c);
	@try {
		if (op_is_select(msg_code)) {
			txn_init(iproto(request), &txn, netmsg_tail(&c->out_messages));
			box_dispach_select(&txn, &request_data);
			iproto_commit(&txn.header_mark, ERR_CODE_OK);
		} else {
			ev_tstamp start = ev_now(), stop;
			struct netmsg_head h = { TAILQ_HEAD_INITIALIZER(h.q), fiber->pool, 0 };
			txn_init(iproto(request), &txn, netmsg_tail(&h));

			if (unlikely(c->service != box_primary))
				iproto_raise(ERR_CODE_NONMASTER, "updates forbiden on secondary port");

			u32 rc = ERR_CODE_OK;
			switch (msg_code) {
			case EXEC_LUA:
				rc = box_dispach_lua(&txn, &request_data);
				stat_collect(stat_base, EXEC_LUA, 1);
				break;
			case PAXOS_LEADER:
				if ([recovery respondsTo:@selector(leader_redirect_raise)])
					[recovery perform:@selector(leader_redirect_raise)];
				else
					iproto_raise(ERR_CODE_UNSUPPORTED_COMMAND,
						     "PAXOS_LEADER unsupported in non cluster configuration");
				break;
			default:
				box_prepare_update(&txn, &request_data);
				/* we'r potentially block here */
				txn_submit_to_storage(&txn);
				txn_commit(&txn);
			}
			iproto_commit(&txn.header_mark, rc);
			netmsg_concat(&c->out_messages, &h);

			stop = ev_now();
			if (stop - start > cfg.too_long_threshold)
				say_warn("too long %s: %.3f sec", messages_strs[txn.op], stop - start);
		}
	}
	@catch (Error *e) {
		say_warn("aboring txn, [%s reason:\"%s\"] at %s:%d peer:%s",
			 [[e class] name], e->reason, e->file, e->line, conn_peer_name(c));
		if (e->backtrace)
			say_debug("backtrace:\n%s", e->backtrace);
		txn_abort(&txn);
		u32 rc = ERR_CODE_UNKNOWN_ERROR;
		if ([e respondsTo:@selector(code)])
			rc = [(id)e code];
		else if ([e isMemberOf:[IndexError class]])
			rc = ERR_CODE_ILLEGAL_PARAMS;
		iproto_error(&txn.m, &txn.header_mark, rc, e->reason);
		if (&c->out_messages != txn.m->head)
			netmsg_concat(&c->out_messages, txn.m->head);
	}
	@finally {
		say_debug("%s: @finally c:%p", __func__, c);
		txn_cleanup(&txn);
#ifdef NET_IO_PARANOIA
		netmsg_verify_ownership(&c->out_messages);
#endif
	}
}

static void
xlog_print(struct tbuf *out, struct tbuf *b)
{
	u16 op;
	u32 n, key_len;
	void *key;
	u32 cardinality, field_no;
	u32 flags;
	u32 op_cnt;

	op = read_u16(b);

	switch (op) {
	case INSERT:
		n = read_u32(b);
		tbuf_printf(out, "%s n:%i ", messages_strs[op], n);
		flags = read_u32(b);
		cardinality = read_u32(b);
		if (!valid_tuple(b, cardinality))
			abort();
		tuple_print(out, cardinality, b->ptr);
		break;

	case DELETE:
		n = read_u32(b);
		tbuf_printf(out, "%s n:%i ", messages_strs[op], n);
		key_len = read_u32(b);
		key = read_field(b);
		if (tbuf_len(b) != 0)
			abort();
		tuple_print(out, key_len, key);
		break;

	case UPDATE_FIELDS:
		n = read_u32(b);
		tbuf_printf(out, "%s n:%i ", messages_strs[op], n);
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
	tbuf_printf(out, "m:%i ", snap->object_space);
	tuple_print(out, snap->tuple_size, snap->data);
}


static void
print_row(struct tbuf *out, u16 tag, struct tbuf *r)
{
	if (tag == wal_tag)
		xlog_print(out, r);
	else if (tag == snap_tag)
		snap_print(out, r);
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

		object_space_registry[i].cardinality = cfg.object_space[i]->cardinality;

		if (cfg.object_space[i]->index == NULL)
			panic("(object_space = %" PRIu32 ") at least one index must be defined", i);

		for (int j = 0; j < nelem(object_space_registry[i].index); j++) {

			if (cfg.object_space[i]->index[j] == NULL)
				break;

                        typeof(cfg.object_space[i]->index[j]) index_cfg = cfg.object_space[i]->index[j];
			Index *index = [Index new_with_n:j cfg:index_cfg];
			object_space_registry[i].index[j] = (Index<BasicIndex> *)index;

			if (index == nil)
				panic("object_space = %" PRIu32 " index = %" PRIu32 ") "
				      "unknown index type `%s'", i, j, index_cfg->type);

			if ([index respondsTo:@selector(resize)])
				[(id)index resize:cfg.object_space[i]->estimated_rows];
		}

		Index *pk = object_space_registry[i].index[0];

		if (pk->unique == false)
			panic("(object_space = %" PRIu32 ") object_space PK index must be unique", i);

		if ([pk cardinality] != 1)
			panic("(object_space = %" PRIu32 ") object_space PK index must be 1 cardinality", i);

		object_space_registry[i].enabled = true;

		say_info("object space %i successfully configured", i);
		say_info("  PK %i:%s", pk->n, [pk class]->name);
	}
}

static void
title(const char *fmt, ...)
{
	va_list ap;
	char buf[64];

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (cfg.memcached)
		set_proc_title("memcached:%s%s pri:%i adm:%i",
			       buf, custom_proc_title, cfg.primary_port, cfg.admin_port);
	else
		set_proc_title("box:%s%s pri:%i sec:%i adm:%i",
			       buf, custom_proc_title,
			       cfg.primary_port, cfg.secondary_port, cfg.admin_port);
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
                        nodes[i] = malloc(estimated_tuples * ts[i]->node_size);
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
		say_info("  %i:%s", ts[i]->n, [ts[i] class]->name);
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
			tbuf_printf(i, " %i:%s", index->n, [index class]->name);

		say_info("Object space %i indexes:%.*s", n, tbuf_len(i), (char *)i->ptr);
	}
}

void
box_bound_to_primary(int fd)
{
	if (fd < 0) {
		if (!cfg.local_hot_standby)
			panic("unable bind server socket");
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
initialize_service()
{
	if (cfg.memcached != 0) {
		memcached_init();
	} else {
		box_primary = tcp_service(cfg.primary_port, box_bound_to_primary);
		for (int i = 0; i < cfg.wal_writer_inbox_size - 2; i++)
			fiber_create("box_worker", iproto_interact, box_primary, box_process, NULL);

		if (cfg.secondary_port > 0) {
			box_secondary = tcp_service(cfg.secondary_port, NULL);
			fiber_create("box_secondary_worker", iproto_interact, box_secondary, box_process);
		}

		say_info("(silver)box initialized (%i workers)", cfg.wal_writer_inbox_size);
	}
}

static void
snap_apply(struct box_txn *txn, struct tbuf *t)
{
	struct box_snap_row *row;

	row = box_snap_row(t);
	txn->object_space = &object_space_registry[row->object_space];

	if (!txn->object_space->enabled)
		raise("object_space %i is not configured", txn->object_space->n);

	txn->index = txn->object_space->index[0];
	assert(txn->index != nil);

	txn->snap = true;
	prepare_replace(txn, row->tuple_size, &TBUF(row->data, row->data_size, NULL));
	txn->op = INSERT;
}

static void
wal_apply(struct box_txn *txn, struct tbuf *t)
{
	txn->op = read_u16(t);
	box_prepare_update(txn, t);
}

@implementation Recovery (Box)

- (void)
apply:(struct tbuf *)op tag:(u16)tag
{
	struct box_txn txn;
	memset(&txn, 0, sizeof(txn));

	switch (tag) {
	case wal_tag:
		wal_apply(&txn, op);
		txn_commit(&txn);
		txn_cleanup(&txn);
		break;
	case snap_tag:
		snap_apply(&txn, op);
		txn_commit(&txn);
		txn_cleanup(&txn);
		break;
	case snap_initial_tag:
	case snap_final_tag:
	case run_crc:
	case nop:
		break;
	default:
		raise("unknown row tag: %u/%s", tag, xlog_tag_to_a(tag));
	}
}

- (void)
wal_final_row
{
	if (box_primary == NULL) {
		build_secondary_indexes();
		initialize_service();
		title("%s", [recovery status]);
	}
}

@end


static void init_second_stage(va_list ap __attribute__((unused)));

static void
init(void)
{
	stat_base = stat_register(messages_strs, messages_MAX);

	object_space_registry = calloc(object_space_count, sizeof(struct object_space));
	for (int i = 0; i < object_space_count; i++)
		object_space_registry[i].n = i;

	if (cfg.memcached != 0) {
		if (cfg.secondary_port != 0)
			panic("in memcached mode secondary_port must be 0");
		if (cfg.wal_feeder_addr)
			panic("remote replication is not supported in memcached mode.");
	}

	title("loading");
	if (cfg.paxos_enabled) {
		if (cfg.wal_feeder_addr)
			panic("wal_feeder_addr is incompatible with paxos");
		if (cfg.local_hot_standby)
			panic("wal_hot_standby is incompatible with paxos");
	}

	recovery = cfg.paxos_enabled ? [PaxosRecovery alloc] : [Recovery alloc];
	recovery = [recovery init_snap_dir:cfg.snap_dir
				   wal_dir:cfg.wal_dir
			      rows_per_wal:cfg.rows_per_wal
			       feeder_addr:cfg.wal_feeder_addr
			       fsync_delay:cfg.wal_fsync_delay
			     run_crc_delay:cfg.run_crc_delay
			      nop_hb_delay:cfg.nop_hb_delay
				     flags:init_storage ? RECOVER_READONLY : 0
			snap_io_rate_limit:cfg.snap_io_rate_limit * 1024 * 1024];

	/* initialize hashes _after_ starting wal writer */

	if (cfg.memcached != 0) {
		int n = cfg.memcached_object_space > 0 ? cfg.memcached_object_space : MEMCACHED_OBJECT_SPACE;

		cfg.object_space = calloc(n + 2, sizeof(cfg.object_space[0]));
		for (u32 i = 0; i <= n; ++i) {
			cfg.object_space[i] = calloc(1, sizeof(cfg.object_space[0][0]));
			cfg.object_space[i]->enabled = false;
		}

		cfg.object_space[n]->enabled = true;
		cfg.object_space[n]->cardinality = 4;
		cfg.object_space[n]->estimated_rows = 0;
		cfg.object_space[n]->index = calloc(2, sizeof(cfg.object_space[n]->index[0]));
		cfg.object_space[n]->index[0] = calloc(1, sizeof(cfg.object_space[n]->index[0][0]));
		cfg.object_space[n]->index[1] = NULL;
		cfg.object_space[n]->index[0]->type = "HASH";
		cfg.object_space[n]->index[0]->unique = 1;
		cfg.object_space[n]->index[0]->key_field =
			calloc(2, sizeof(cfg.object_space[n]->index[0]->key_field[0]));
		cfg.object_space[n]->index[0]->key_field[0] =
			calloc(1, sizeof(cfg.object_space[n]->index[0]->key_field[0][0]));
		cfg.object_space[n]->index[0]->key_field[1] = NULL;
		cfg.object_space[n]->index[0]->key_field[0]->fieldno = 0;
		cfg.object_space[n]->index[0]->key_field[0]->type = "STR";

		memcached_index = (StringHash *)object_space_registry[n].index[0];
	}

	configure();

	if (init_storage)
		return;

	/* fiber is required to successfully pull from remote */
	fiber_create("box_init", init_second_stage);
}

static void
init_second_stage(va_list ap __attribute__((unused)))
{
	luaT_openbox(root_L);
	luaT_dofile("box_init.lua");

	@try {
		i64 local_lsn = [recovery recover_start];
		if (cfg.paxos_enabled) {
			[recovery enable_local_writes];
		} else {
			if (local_lsn == 0) {
				if (!cfg.wal_feeder_addr) {
					say_crit("don't you forget to initialize "
						 "storage with --init-storage switch?");
					exit(EX_USAGE);
				}
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

static int
cat(const char *filename)
{
	return read_log(filename, print_row);
}

static u32
snapshot_rows(XLog *l)
{
	struct box_snap_row header;
	struct tnt_object *obj;
	struct box_tuple *tuple;
	struct tbuf *row;
	size_t rows = 0, total_rows = 0;

	for (int n = 0; n < object_space_count; n++)
		if (object_space_registry[n].enabled)
			total_rows += [object_space_registry[n].index[0] size];

	if (!l)
		return total_rows;

	for (int n = 0; n < object_space_count; n++) {
		if (!object_space_registry[n].enabled)
			continue;

		id pk = object_space_registry[n].index[0];
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			tuple = box_tuple(obj);

			header.object_space = n;
			header.tuple_size = tuple->cardinality;
			header.data_size = tuple->bsize;

			/* snapshot_write_row will release fiber->pool time to time */
			row = tbuf_alloc(fiber->pool);
			tbuf_append(row, &header, sizeof(header));
			tbuf_append(row, tuple->data, tuple->bsize);

			snapshot_write_row(l, snap_tag, row);

			if (++rows % 100000 == 0) {
				float pct = (float)rows / total_rows * 100.;
				say_crit("%.1fM/%.2f%% rows written", rows / 1000000., pct);
				set_proc_title("dumper %.2f%% (%" PRIu32 ")", pct, getppid());
			}
			if (rows % 10000 == 0)
				[l confirm_write];
		}
	}
	return total_rows;
}

static void
snapshot(bool initial)
{
	if (initial)
		[recovery initial];

	if ([recovery lsn] == 0) {
		say_warn("lsn == 0");
		_exit(EXIT_FAILURE);
	}
	[recovery snapshot_save:snapshot_rows];
}

static void
info(struct tbuf *out)
{
	tbuf_printf(out, "info:" CRLF);
	tbuf_printf(out, "  version: \"%s\"" CRLF, octopus_version());
	tbuf_printf(out, "  uptime: %i" CRLF, tnt_uptime());
	tbuf_printf(out, "  pid: %i" CRLF, getpid());
	tbuf_printf(out, "  wal_writer_pid: %" PRIi64 CRLF,
		    (i64) [recovery wal_writer]->pid);
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
				    index->n, [index slots], [index bytes]);
	}

	if (box_primary != NULL)
		service_info(out, box_primary);
	if (box_secondary != NULL)
		service_info(out, box_secondary);
}


static struct tnt_module box = {
        .name = "box",
        .init = init,
        .check_config = NULL,
        .reload_config = NULL,
        .cat = cat,
        .snapshot = snapshot,
        .info = info,
        .exec = NULL
};

register_module(box);
register_source();
