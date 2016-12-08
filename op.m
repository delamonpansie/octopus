/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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
#import <objc.h>
#import <index.h>
#import <shard.h>

#import <mod/box/box.h>
#if CFG_lua_path
#import <mod/box/src-lua/moonbox.h>
#endif
#if CFG_caml_path
#import <mod/box/src-ml/camlbox.h>
#endif

#include <stdint.h>

static int stat_base;
char const * const box_ops[] = ENUM_STR_INITIALIZER(MESSAGES);

static struct slab_cache phi_cache;

void __attribute__((noreturn))
bad_object_type()
{
	raise_fmt("bad object type");
}

void *
next_field(void *f)
{
	u32 size = LOAD_VARINT32(f);
	return (u8 *)f + size;
}

void *
tuple_field(struct tnt_object *obj, size_t i)
{
	void *field = tuple_data(obj);

	if (i >= tuple_cardinality(obj))
		return NULL;

	while (i-- > 0)
		field = next_field(field);

	return field;
}

static struct tnt_object *
tuple_alloc(unsigned cardinality, unsigned size)
{
	struct tnt_object *obj;
	if (cardinality < 256 && size < 256) {
		obj = object_alloc(BOX_SMALL_TUPLE, 0, sizeof(struct box_small_tuple) + size);
		struct box_small_tuple *tuple = box_small_tuple(obj);
		tuple->bsize = size;
		tuple->cardinality = cardinality;
	} else {
		obj = object_alloc(BOX_TUPLE, 1, sizeof(struct box_tuple) + size);
		object_incr_ref(obj);
		struct box_tuple *tuple = box_tuple(obj);
		tuple->bsize = size;
		tuple->cardinality = cardinality;
	}

	say_debug3("tuple_alloc(%u, %u) = %p", cardinality, size, obj + 1);
	return obj;
}

void
tuple_free(struct tnt_object *obj)
{
	switch (obj->type) {
	case BOX_TUPLE:
		object_decr_ref(obj);
		break;
	case BOX_SMALL_TUPLE:
		say_debug("object_free(%p)", obj);
		sfree(obj);
		break;
	default:
		assert(false);
	}
}

ssize_t
fields_bsize(u32 cardinality, const void *data, u32 max_len)
{
	struct tbuf tmp = TBUF(data, max_len, NULL);
	for (int i = 0; i < cardinality; i++)
		read_field(&tmp);

	return tmp.ptr - data;
}

int
tuple_valid(struct tnt_object *obj)
{
	@try {
		return fields_bsize(tuple_cardinality(obj), tuple_data(obj), tuple_bsize(obj)) ==
		tuple_bsize(obj);
	} @catch(Error* e) {
		say_error("%s", e->reason);
		[e release];
		return 0;
	}
}

void
net_tuple_add(struct netmsg_head *h, struct tnt_object *obj)
{
	switch (obj->type) {
	case BOX_TUPLE: {
		struct box_tuple *tuple = box_tuple(obj);
		size_t size = tuple->bsize + 8;
		net_add_obj_iov(h, obj, &tuple->bsize, size);
		break;
	}
	case BOX_SMALL_TUPLE: {
		struct box_small_tuple *tuple = box_small_tuple(obj);
		struct box_tuple *reply = palloc(h->pool, sizeof(*reply) + tuple->bsize);
		reply->bsize = tuple->bsize;
		reply->cardinality = tuple->cardinality;
		memcpy(reply->data, tuple->data, tuple->bsize);
		net_add_iov(h, (void *)reply, sizeof(*reply) + tuple->bsize);
		break;
	}
	case BOX_PHI:
		assert(false);
	default:
		bad_object_type();
	}
}

static struct box_phi_cell *
phi_cell_alloc(struct box_phi *phi, struct tnt_object *obj, struct box_op *bop)
{
	struct box_phi_cell *cell = slab_cache_alloc(&phi_cache);
	say_debug3("%s: %p phi:%p obj:%p", __func__, cell, phi, obj);
	*cell = (struct box_phi_cell) {
		.head = phi,
		.obj = obj,
		.bop = bop };
	TAILQ_INSERT_TAIL(&phi->tailq, cell, link);
	return cell;
}

static struct box_phi *
phi_alloc(Index<BasicIndex> *index, struct tnt_object *obj, struct box_op *bop)
{
	struct box_phi *head = slab_cache_alloc(&phi_cache);
	say_debug3("%s: %p index:%i obj:%p", __func__, head, index->conf.n, obj);
	*head = (struct box_phi) {
		.header = { .type = BOX_PHI },
		.index = index,
		.obj = obj,
		.tailq = TAILQ_HEAD_INITIALIZER(head->tailq),
		.bop = bop };
	return head;
}

#define box_phi(obj) container_of(obj, struct box_phi, header)

struct tnt_object *
phi_left(struct tnt_object *obj)
{
	if (obj && obj->type == BOX_PHI)
		obj = box_phi(obj)->obj;

	assert(obj == NULL || obj->type != BOX_PHI);
	return obj;
}

struct tnt_object *
phi_right(struct tnt_object *obj)
{
	if (obj && obj->type == BOX_PHI)
		obj = TAILQ_LAST(&box_phi(obj)->tailq, phi_tailq)->obj;

	assert(obj == NULL || obj->type != BOX_PHI);
	return obj;
}

struct tnt_object *
phi_obj(const struct tnt_object *obj)
{
	assert(obj->type == BOX_PHI);
	struct box_phi *phi = box_phi(obj);
	return phi->obj ?: TAILQ_FIRST(&phi->tailq)->obj;
}

static void
phi_insert(struct box_op *bop, id<BasicIndex> index,
	   struct tnt_object *old_obj, struct tnt_object *obj)
{
	struct phi_tailq *bop_tailq = &bop->phi;
	struct box_phi *phi;
	struct box_phi_cell *cell;
	assert(old_obj != NULL || obj != NULL);
	assert(obj == NULL || obj->type != BOX_PHI);

	if (old_obj && old_obj->type == BOX_PHI) {
		phi = box_phi(old_obj);
		assert(phi->index == index);
		cell = phi_cell_alloc(phi, obj, bop);
	} else {
		/* phi is not owned by box_op, but rather by index itself.
		   phi_cell is owned by box_op */
		phi = phi_alloc(index, old_obj, bop);
		cell = phi_cell_alloc(phi, obj, bop);
		@try {
			[index replace: &phi->header];
		}
		@catch (id e) {
			sfree(phi);
			sfree(cell);
			@throw;
		}
	}

	TAILQ_INSERT_TAIL(bop_tailq, cell, bop_link);
}

static void
phi_commit(struct box_phi_cell *cell)
{
	struct box_phi *phi = cell->head;
	assert(TAILQ_FIRST(&phi->tailq) == cell);
	assert(phi->obj != NULL || cell->obj != NULL);
	say_debug3("%s: cell:%p phi:%p obj:%p", __func__, cell, phi, cell->obj);

	if (cell == TAILQ_LAST(&phi->tailq, phi_tailq)) {
		/* we are last in a node,
		 * so replace phi with committed obj, or do delete it */
		if (cell->obj == NULL)
			[phi->index remove:&phi->header];
		else
			[phi->index replace:cell->obj];
		sfree(phi);
	} else {
		assert(cell->obj != NULL || TAILQ_NEXT(cell, link)->obj != NULL);
		/* remove self from chain, and copy committed obj to the head */
		phi->obj = cell->obj;
		TAILQ_REMOVE(&phi->tailq, cell, link);
	}
}

static void
phi_rollback(struct box_phi_cell *cell)
{
	struct box_phi *phi = cell->head;
	assert(cell == TAILQ_LAST(&phi->tailq, phi_tailq));
	say_debug3("%s: cell:%p phi:%p obj:%p", __func__, cell, phi, cell->obj);

	if (cell != TAILQ_FIRST(&phi->tailq)) {
		assert(TAILQ_PREV(cell, phi_tailq, link)->obj != NULL || cell->obj != NULL);
		/* unlink self from chain */
		TAILQ_REMOVE(&phi->tailq, cell, link);
	} else {
		assert(phi->obj != NULL || cell->obj != NULL);
		/* so put back original obj into index, or do delete */
		if (phi->obj == NULL)
			[phi->index remove:&phi->header];
		else
			[phi->index replace:phi->obj];
		sfree(phi);
	}
}

struct tnt_object *
tuple_visible_left(struct tnt_object *obj)
{
	return phi_left(obj);
}

struct tnt_object *
tuple_visible_right(struct tnt_object *obj)
{
	return phi_right(obj);
}

static void
object_space_delete(struct box_op *bop, struct tnt_object *index_obj, struct tnt_object *tuple)
{
	struct object_space *object_space = bop->object_space;
	if (tuple == NULL)
		return;

	id<BasicIndex> pk = object_space->index[0];
	phi_insert(bop, pk, index_obj , NULL);

	foreach_indexi(1, index, object_space) {
		struct tnt_object* old_obj = [index find_obj:tuple];
		assert(phi_right(old_obj) == tuple);
		phi_insert(bop, index, old_obj, NULL);
	}
}

static void
object_space_insert(struct box_op *bop, struct tnt_object *index_obj, struct tnt_object *tuple)
{
	struct object_space *object_space = bop->object_space;
	id<BasicIndex> pk = object_space->index[0];
	assert(phi_right(index_obj) == NULL);
	phi_insert(bop, pk, index_obj, tuple);

	foreach_indexi(1, index, object_space) {
		index_obj = [index find_obj:tuple];
		if (phi_right(index_obj) == NULL)
			phi_insert(bop, index, index_obj, tuple);
		else
			iproto_raise_fmt(ERR_CODE_INDEX_VIOLATION,
					 "duplicate key value violates unique index %i:%s",
					 index->conf.n, [[index class] name]);
	}
}

static void
object_space_replace(struct box_op *bop, int pk_affected, struct tnt_object *index_obj,
		     struct tnt_object *old_tuple, struct tnt_object *tuple)
{
	struct object_space *object_space = bop->object_space;
	id<BasicIndex> pk = object_space->index[0];
	uintptr_t i = 0;
	if (!pk_affected) {
		phi_insert(bop, pk, index_obj, tuple);
		i = 1;
	}

	foreach_indexi(i, index, object_space) {
		index_obj = [index find_obj:tuple];

		if (phi_right(index_obj) == NULL) {
			struct tnt_object *old_obj = [index find_obj:old_tuple];
			assert(phi_right(old_obj) == old_tuple);
			phi_insert(bop, index, old_obj, NULL);
			phi_insert(bop, index, index_obj, tuple);
		} else  if (phi_right(index_obj) == old_tuple) {
			phi_insert(bop, index, index_obj, tuple);
		} else {
			iproto_raise_fmt(ERR_CODE_INDEX_VIOLATION,
					 "duplicate key value violates unique index %i:%s",
					 index->conf.n, [[index class] name]);
		}
	}
}

void
prepare_replace(struct box_op *bop, size_t cardinality, const void *data, u32 data_len)
{
	if (cardinality == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality can't be equal to 0");
	if (data_len == 0 || fields_bsize(cardinality, data, data_len) != data_len)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "tuple encoding error");

	bop->obj = tuple_alloc(cardinality, data_len);
	memcpy(tuple_data(bop->obj), data, data_len);

	Index<BasicIndex> *pk = bop->object_space->index[0];
	struct tnt_object *old_root = [pk find_obj:bop->obj];
	bop->old_obj = phi_right(old_root);
	bop->obj_affected = bop->old_obj != NULL ? 2 : 1;

	if (bop->flags & BOX_ADD && bop->old_obj != NULL)
		iproto_raise(ERR_CODE_NODE_FOUND, "tuple found");
	if (bop->flags & BOX_REPLACE && bop->old_obj == NULL)
		iproto_raise(ERR_CODE_NODE_NOT_FOUND, "tuple not found");

	say_debug("%s: old_obj:%p obj:%p", __func__, bop->old_obj, bop->obj);
	if (bop->old_obj == NULL)
		object_space_insert(bop, old_root, bop->obj);
	else
		object_space_replace(bop, 0, old_root, bop->old_obj, bop->obj);
}

static void bytes_usage(struct object_space *object_space, struct tnt_object *obj, int sign);
void
snap_insert_row(struct object_space* object_space, size_t cardinality, const void *data, u32 data_len)
{
	if (cardinality == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality can't be equal to 0");
	if (data_len == 0 || fields_bsize(cardinality, data, data_len) != data_len)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "tuple encoding error");

	struct tnt_object *obj = tuple_alloc(cardinality, data_len);
	memcpy(tuple_data(obj), data, data_len);
	if (!tuple_valid(obj)) {
		raise_fmt("tuple misformatted");
	}
	Index<BasicIndex> *pk = object_space->index[0];
	@try {
		[pk replace: obj];
		bytes_usage(object_space, obj, +1);
	} @catch (id e) {
		tuple_free(obj);
		@throw;
	}
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
prepare_update_fields(struct box_op *bop, struct tbuf *data)
{
	struct tbuf *fields;
	const u8 *field;
	int i;
	u32 op_cnt;

	u32 key_cardinality = read_u32(data);
	if (key_cardinality != bop->object_space->index[0]->conf.cardinality)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "key fields count doesn't match");

	Index<BasicIndex> *pk = bop->object_space->index[0];
	struct tnt_object *old_root = [pk find_key:data cardinalty:key_cardinality];
	bop->old_obj = phi_right(old_root);

	op_cnt = read_u32(data);
	if (op_cnt == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "no ops");

	if (bop->old_obj == NULL) {
		/* pretend we parsed all data */
		tbuf_ltrim(data, tbuf_len(data));
		return;
	}
	bop->obj_affected = 1;

	size_t bsize = tuple_bsize(bop->old_obj);
	int cardinality = tuple_cardinality(bop->old_obj);
	void *tdata = tuple_data(bop->old_obj);
	int field_count = cardinality * 1.2;
	fields = palloc(fiber->pool, field_count * sizeof(struct tbuf));

	for (i = 0, field = tdata; i < cardinality; i++) {
		const void *src = field;
		int len = LOAD_VARINT32(field);
		/* .ptr  - start of varint
		   .end  - start of data
		   .free - len(data) */
		fields[i] = (struct tbuf){ .ptr = (void *)src, .end = (void *)field,
					   .free = len, .pool = NULL };
		field += len;
	}

	int pk_affected = 0;
	while (op_cnt-- > 0) {
		u8 op;
		u32 field_no, arg_size;
		const u8 *arg;
		struct tbuf *field = NULL;

		field_no = read_u32(data);
		op = read_u8(data);
		arg = read_field(data);
		arg_size = LOAD_VARINT32(arg);

		Index<BasicIndex> *pk = bop->object_space->index[0];
		for (int i = 0; i < pk->conf.cardinality; i++) {
			if (pk->conf.field[i].index == field_no) {
				pk_affected = 1;
				break;
			}
		}

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
				iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "unable to delete PK");

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
						 (cardinality + 128) * sizeof(struct tbuf));
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

	bop->obj = tuple_alloc(cardinality, bsize);

	u8 *p = tuple_data(bop->obj);
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

	if (![pk eq:bop->old_obj :bop->obj])
		bop->obj_affected++;

	object_space_replace(bop, pk_affected, old_root, bop->old_obj, bop->obj);
}

static void __attribute__((noinline))
process_select(struct netmsg_head *h, Index<BasicIndex> *index,
	       u32 limit, u32 offset, struct tbuf *data)
{
	struct tnt_object *obj;
	uint32_t *found;
	u32 count = read_u32(data);
	index_cmp cmp = NULL;

	say_debug("SELECT");
	found = palloc(h->pool, sizeof(*found));
	net_add_iov(h, found, sizeof(*found));
	*found = 0;

	for (u32 i = 0; i < count; i++) {
		u32 c = read_u32(data);
		if (index->conf.cardinality == c) {
			obj = [index find_key:data cardinalty:c];
			obj = tuple_visible_left(obj);
			if (obj == NULL)
				continue;
			if (unlikely(limit == 0))
				continue;
			if (unlikely(offset > 0)) {
				offset--;
				continue;
			}

			(*found)++;
			net_tuple_add(h, obj);
			limit--;
		} else if (index->conf.type == HASH || index->conf.type == NUMHASH || index->conf.type == PHASH) {
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "cardinality mismatch");
		} else {
			Tree *tree = (Tree *)index;
			cmp = cmp ?: [tree compare];
			[tree iterator_init_with_key:data cardinalty:c];
			if (unlikely(limit == 0))
				continue;

			while ((obj = [tree iterator_next_check:cmp]) != NULL) {
				obj = tuple_visible_left(obj);
				if (unlikely(obj == NULL))
					continue;
				if (unlikely(offset > 0)) {
					offset--;
					continue;
				}

				(*found)++;
				net_tuple_add(h, obj);

				if (--limit == 0)
					break;
			}
		}
	}

	if (tbuf_len(data) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	stat_collect(stat_base, SELECT_KEYS, count);
}

static void __attribute__((noinline))
prepare_delete(struct box_op *bop, struct tbuf *key_data)
{
	u32 c = read_u32(key_data);

	Index<BasicIndex> *pk = bop->object_space->index[0];
	struct tnt_object *old_root = [pk find_key:key_data cardinalty:c];
	bop->old_obj = phi_right(old_root);
	bop->obj_affected = bop->old_obj != NULL;
	object_space_delete(bop, old_root, bop->old_obj);
}

struct box_op *
box_op_alloc(struct box_txn *txn, int msg_code, const void *data, int data_len)
{
	struct box_op *bop = palloc(fiber->pool, sizeof(*bop) + data_len);
	*bop = (struct box_op) { .txn = txn,
				 .op = msg_code & 0xffff,
				 .data_len = data_len,
				 .phi = TAILQ_HEAD_INITIALIZER(bop->phi) };
	memcpy(bop->data, data, data_len);
	TAILQ_INSERT_TAIL(&txn->ops, bop, link);
	return bop;
}

struct box_op *
box_prepare(struct box_txn *txn, int op, const void *data, u32 data_len)
{
	say_debug("%s op:%i/%s", __func__, op, box_ops[op]);
	if (txn->mode == RO)
		iproto_raise(ERR_CODE_NONMASTER, "txn is readonly");

	struct box_op *bop = box_op_alloc(txn, op, data, data_len);
	struct tbuf buf = TBUF(data, data_len, NULL);

	i32 n = read_u32(&buf);
	bop->object_space = object_space(txn->box, n);
	if (bop->object_space->ignored) {
		/* bop->object_space == NULL means this txn will be ignored */
		bop->object_space = NULL;
		return bop;
	}

	switch (op) {
	case INSERT:
		bop->flags = read_u32(&buf);
		u32 cardinality = read_u32(&buf);
		u32 tuple_blen = tbuf_len(&buf);
		void *tuple_bytes = read_bytes(&buf, tuple_blen);
		prepare_replace(bop, cardinality, tuple_bytes, tuple_blen);
		bop->ret_obj = bop->obj;
		break;

	case DELETE:
		bop->flags = read_u32(&buf); /* RETURN_TUPLE */
	case DELETE_1_3:
		prepare_delete(bop, &buf);
		bop->ret_obj = bop->old_obj;
		break;

	case UPDATE_FIELDS:
		bop->flags = read_u32(&buf);
		prepare_update_fields(bop, &buf);
		bop->ret_obj = bop->obj;
		break;

	case NOP:
		break;

	default:
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "unknown opcode:%"PRIi32, op);
	}


	if (bop->obj) {
		if (bop->object_space->cardinality > 0 &&
		    bop->object_space->cardinality != tuple_cardinality(bop->obj))
		{
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS,
				     "tuple cardinality must match object_space cardinality");
		}

		if (!tuple_valid(bop->obj))
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "internal error");
	}
	if (tbuf_len(&buf) != 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	return bop;
}

static void
box_op_cleanup(enum txn_state state, struct box_op *bop)
{
	say_debug3("%s: old_obj:%p obj:%p", __func__, bop->old_obj, bop->obj);
	switch (state) {
	case COMMIT:
		if (bop->old_obj)
			tuple_free(bop->old_obj);
		break;
	case ROLLBACK:
		if (bop->obj)
			tuple_free(bop->obj);
		break;
	default:
		if (!bop->object_space)
			return;
		assert(false);
	}
}

static void
txn_cleanup(struct box_txn *txn)
{
	say_debug3("%s: txn:%i/%p", __func__, txn->id, txn);
	assert(fiber->txn == txn);
	fiber->txn = NULL;

	struct box_op *bop;
	TAILQ_FOREACH(bop, &txn->ops, link)
		box_op_cleanup(txn->state, bop);
}

static void
bytes_usage(struct object_space *object_space, struct tnt_object *obj, int sign)
{
#define small_tuple_overhead (sizeof(struct tnt_object) + sizeof(struct box_small_tuple))
#define tuple_overhead (sizeof(struct gc_oct_object) + sizeof(struct box_tuple))

	switch (obj->type) {
	case BOX_TUPLE:
		object_space->obj_bytes += sign * (tuple_bsize(obj) + tuple_overhead);
		break;
	case BOX_SMALL_TUPLE:
		object_space->obj_bytes += sign * (tuple_bsize(obj) + small_tuple_overhead);
		break;
	default:
		assert(false);
	}
	object_space->slab_bytes += sign * salloc_usable_size(obj);
}

static void
box_op_commit(struct box_op *bop)
{
	if (!bop->object_space)
		return;

	say_debug2("%s: old_obj:%p obj:%p", __func__, bop->old_obj, bop->obj);
	if (bop->obj) {
		bytes_usage(bop->object_space, bop->obj, +1);
	}
	if (bop->old_obj)
		bytes_usage(bop->object_space, bop->old_obj, -1);

	struct box_phi_cell *cell, *tmp;
	TAILQ_FOREACH_SAFE(cell, &bop->phi, bop_link, tmp) {
		phi_commit(cell);
		sfree(cell);
	}
	stat_collect(stat_base, bop->op, 1);
}

void
box_commit(struct box_txn *txn)
{
	@try {
		assert(txn->state == UNDECIDED);
		txn->state = COMMIT;
		struct box_op *bop;
		TAILQ_FOREACH(bop, &txn->ops, link)
			box_op_commit(bop);
		txn_cleanup(txn);
	}
	@catch (Error *e) {
		panic_exc_fmt(e, "can't handle exception after WAL write: %s", e->reason);
	}
	@catch (id e) {
		panic_exc_fmt(e, "can't handle unknown exception after WAL write");
	}
}

static void
box_op_rollback(struct box_op *bop)
{
	say_debug3("%s:", __func__);
	if (!bop->object_space)
		return;

	struct box_phi_cell *cell, *tmp;
	TAILQ_FOREACH_REVERSE_SAFE(cell, &bop->phi, phi_tailq, bop_link, tmp) {
		phi_rollback(cell);
		sfree(cell);
	}
}

void
box_rollback(struct box_txn *txn)
{
	say_debug2("%s: txn:%i/%p state:%i", __func__,
		   txn->id, txn, txn->state);
	if (txn->state == ROLLBACK)
		goto cleanup;
	assert(txn->state == UNDECIDED);
	txn->state = ROLLBACK;

	struct box_op *bop;
	TAILQ_FOREACH_REVERSE(bop, &txn->ops, box_op_tailq, link)
		box_op_rollback(bop);
cleanup:
	txn_cleanup(txn);
}


static int
tlv_add(struct tbuf *buf, int tag)
{
	struct tlv tlv = { .tag = tag };
	int offt = buf->end - buf->ptr;
	tbuf_append(buf, &tlv, sizeof(tlv));
	return offt;
}

static void
tlv_end(struct tbuf *buf, int offt)
{
	struct tlv *tlv = buf->ptr + offt;
	tlv->len = buf->end - buf->ptr - offt - sizeof(struct tlv);
}

int
box_submit(struct box_txn *txn)
{
	say_debug2("%s: txn:%i/%p state:%i", __func__, txn->id, txn, txn->state);
	int len = 0, count = 0;
	struct box_op *bop, *single;

	if (txn->mode == RO) {
		assert(TAILQ_FIRST(&txn->ops) == NULL);
		return 0;
	}

	TAILQ_FOREACH(bop, &txn->ops, link) {
		assert(bop->object_space);
		if (bop->data_len > 0) {
			single = bop;
			len += bop->data_len;
			count++;
		} else {
			struct box_phi_cell *cell;
			TAILQ_FOREACH(cell, &bop->phi, bop_link)
				assert(cell->obj == NULL);
		}
		txn->obj_affected += bop->obj_affected;
	}

	if (len == 0) {
		txn->submit = 0;
		return 0;
	}

	if (count == 1) {
		txn->submit = [txn->box->shard submit:single->data
						  len:single->data_len
						  tag:single->op<<5|TAG_WAL];
	} else {
		struct tbuf *buf = tbuf_alloc(fiber->pool);
		int multi = tlv_add(buf, BOX_MULTI_OP);
		TAILQ_FOREACH(bop, &txn->ops, link) {
			int offt = tlv_add(buf, BOX_OP);
			tbuf_append(buf, &bop->op, 2);
			tbuf_append(buf, bop->data, bop->data_len);
			tlv_end(buf, offt);
		}
		tlv_end(buf, multi);

		txn->submit = [txn->box->shard submit:buf->ptr
						  len:tbuf_len(buf)
						  tag:tlv|TAG_WAL];
	}

	return txn->submit ?: -1;
}

struct box_txn *
box_txn_alloc(int shard_id, enum txn_mode mode)
{
	static int cnt;
	struct box_txn *txn = p0alloc(fiber->pool, sizeof(*txn));
	txn->box = (shard_rt + shard_id)->shard->executor;
	txn->id = cnt++;
	txn->mode = mode;
	txn->fiber = fiber;
	TAILQ_INIT(&txn->ops);
	assert(fiber->txn == NULL);
	fiber->txn = txn;
	say_debug2("%s: txn:%i/%p", __func__, txn->id, txn);
	return txn;
}

#if CFG_lua_path || CFG_caml_path
static void
box_proc_cb(struct netmsg_head *wbuf, struct iproto *request)
{
	say_debug("%s: op:0x%02x sync:%u", __func__, request->msg_code, request->sync);
	struct box_txn *txn = box_txn_alloc(request->shard_id, RO);
	if (![txn->box->shard is_replica])
		txn->mode = RW;

	@try {
#if CFG_caml_path
		int ret = box_dispach_ocaml(wbuf, request);
		if (ret == 0) {
			box_commit(txn);
			return;
		}
  #if !CFG_lua_path
		struct tbuf req = TBUF(request->data, request->data_len, NULL);
		tbuf_ltrim(&req, sizeof(u32)); /* ignore flags */
		int len = read_varint32(&req);
		char *proc = req.ptr;
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "no such proc '%.*s'", len, proc);
  #endif
#endif

#if CFG_lua_path
		box_dispach_lua(wbuf, request);
		box_commit(txn);
#endif
	}
	@catch (Error *e) {
		box_rollback(txn);
		say_warn("aborting proc request, [%s reason:\"%s\"] at %s:%d",
			 [[e class] name], e->reason, e->file, e->line);
		if (e->backtrace)
			say_debug("backtrace:\n%s", e->backtrace);
		@throw;
	}
	@finally {
		stat_collect(stat_base, EXEC_LUA, 1);
	}
}
#endif


void
box_meta_cb(struct netmsg_head *wbuf, struct iproto *request)
{
	Box *box = (shard_rt + request->shard_id)->shard->executor;

	say_debug2("%s: op:0x%02x sync:%u", __func__, request->msg_code, request->sync);
	if ([box->shard is_replica])
		iproto_raise(ERR_CODE_NONMASTER, "replica is readonly");

	if (box->shard->dummy // && cfg.object_space
		)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "metadata updates are forbidden because cfg.object_space is configured");

	if (cfg.object_space)
		say_warn("metadata updates with configured cfg.object_space");

	struct box_meta_txn txn = { .op = request->msg_code,
				    .box = box };
	@try {
		box_prepare_meta(&txn, &TBUF(request->data, request->data_len, NULL));
		if ([box->shard submit:request->data
				   len:request->data_len
				   tag:request->msg_code<<5|TAG_WAL] != 1)
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
	}
	@catch (id e) {
		box_rollback_meta(&txn);
		@throw;
	}
	@try {
		box_commit_meta(&txn);
		iproto_reply_small(wbuf, request, ERR_CODE_OK);
	}
	@catch (Error *e) {
		panic_exc_fmt(e, "can't handle exception after WAL write: %s", e->reason);
	}
	@catch (id e) {
		panic_exc_fmt(e, "can't handle unknown exception after WAL write");
	}
}

static void
box_cb(struct netmsg_head *wbuf, struct iproto *request)
{
	struct box_txn *txn = box_txn_alloc(request->shard_id, RW);
	say_debug2("%s: c:%p op:0x%02x sync:%u", __func__, NULL, request->msg_code, request->sync);

	struct box_op *bop = NULL;
	@try {
		if ([txn->box->shard is_replica])
			iproto_raise(ERR_CODE_NONMASTER, "replica is readonly");

		bop = box_prepare(txn, request->msg_code, request->data, request->data_len);
		if (!bop->object_space)
			iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "ignored object space");

		if (box_submit(txn) == -1)
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

		struct iproto_retcode *reply = iproto_reply(wbuf, request, ERR_CODE_OK);
		net_add_iov_dup(wbuf, &txn->obj_affected, sizeof(u32));
		if (bop->flags & BOX_RETURN_TUPLE && bop->ret_obj)
			net_tuple_add(wbuf, bop->ret_obj);
		iproto_reply_fixup(wbuf, reply);

		box_commit(txn);
	}
	@catch (Error *e) {
		if (e->file && strcmp(e->file, "src/paxos.m") != 0) {
			say_warn("aborting txn, [%s reason:\"%s\"] at %s:%d peer:%s",
				 [[e class] name], e->reason, e->file, e->line,
				 net_fd_name(container_of(wbuf, struct netmsg_io, wbuf)->fd));
			if (e->backtrace)
				say_debug("backtrace:\n%s", e->backtrace);
		}
		box_rollback(txn);
		@throw;
	}
}

static void
box_select_cb(struct netmsg_head *wbuf, struct iproto *request)
{
	Box *box = (shard_rt + request->shard_id)->shard->executor;
	struct tbuf data = TBUF(request->data, request->data_len, fiber->pool);
	struct iproto_retcode *reply = iproto_reply(wbuf, request, ERR_CODE_OK);
	struct object_space *obj_spc;

	i32 n = read_u32(&data);
	u32 i = read_u32(&data);
	u32 offset = read_u32(&data);
	u32 limit = read_u32(&data);

	obj_spc = object_space(box, n);

	if (i > MAX_IDX)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index too big");

	if ((obj_spc->index[i]) == NULL)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index is invalid");

	process_select(wbuf, obj_spc->index[i], limit, offset, &data);
	iproto_reply_fixup(wbuf, reply);
	stat_collect(stat_base, request->msg_code & 0xffff, 1);
}

#define foreach_op(...) for(int *op = (int[]){__VA_ARGS__, 0}; *op; op++)

void
box_service(struct iproto_service *s)
{
	foreach_op(NOP, SELECT, SELECT_LIMIT)
		service_register_iproto(s, *op, box_select_cb, IPROTO_NONBLOCK);
	foreach_op(INSERT, UPDATE_FIELDS, DELETE, DELETE_1_3)
		service_register_iproto(s, *op, box_cb, IPROTO_ON_MASTER);
	foreach_op(CREATE_OBJECT_SPACE, CREATE_INDEX, DROP_OBJECT_SPACE, DROP_INDEX, TRUNCATE)
		service_register_iproto(s, *op, box_meta_cb, IPROTO_ON_MASTER|IPROTO_WLOCK);

#if CFG_lua_path || CFG_caml_path
	/* allow select only lua procedures
	   updates are blocked by luaT_box_dispatch() */
	service_register_iproto(s, EXEC_LUA, box_proc_cb, 0);
#endif
}

static void
box_roerr(struct netmsg_head *h __attribute__((unused)),
	  struct iproto *request __attribute__((unused)))
{
	iproto_raise(ERR_CODE_NONMASTER, "updates are forbidden");
}

void
box_service_ro(struct iproto_service *s)
{
	service_register_iproto(s, SELECT, box_select_cb, IPROTO_NONBLOCK);
	service_register_iproto(s, SELECT_LIMIT, box_select_cb, IPROTO_NONBLOCK);

	foreach_op(INSERT, UPDATE_FIELDS, DELETE, DELETE_1_3, PAXOS_LEADER,
		   CREATE_OBJECT_SPACE, CREATE_INDEX, DROP_OBJECT_SPACE, DROP_INDEX, TRUNCATE)
		service_register_iproto(s, *op, box_roerr, IPROTO_NONBLOCK);

#if CFG_lua_path || CFG_caml_path
	/* allow select only lua procedures
	   updates are blocked by luaT_box_dispatch() */
	service_register_iproto(s, EXEC_LUA, box_proc_cb, 0);
#endif
}

void
box_init_phi_cache(void)
{
	slab_cache_init(&phi_cache, sizeof(union box_phi_union), SLAB_GROW, "phi_cache");
}

void __attribute__((constructor))
box_op_init(void)
{
	stat_base = stat_register(box_ops, nelem(box_ops));
}

register_source();
