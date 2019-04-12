/*
 * Copyright (C) 2015, 2016 Mail.RU
 * Copyright (C) 2015, 2016 Yuriy Vostrikov
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

#import <pickle.h>
#import <say.h>
#include <stdint.h>

#import <mod/box/box.h>


static void __attribute__((noinline))
prepare_create_object_space(struct box_meta_txn *txn, int n, struct tbuf *data)
{
	say_debug("%s n:%i", __func__, n);
	char cardinalty = read_u8(data);
	struct index_conf ic = { .n = 0 };
	index_conf_read(data, &ic);
	index_conf_validate(&ic);

	if (ic.unique == false)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index must be unique");
	txn->index = [Index new_conf:&ic dtor:&box_tuple_dtor];
	if (txn->index == nil)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't create index");

	if (n < 0 || n > nelem(txn->box->object_space_registry) - 1)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "bad namespace number %i", n);

	if (txn->box->object_space_registry[(int)n])
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "object_space %i is exists", n);

	txn->object_space = xcalloc(1, sizeof(struct object_space));
	txn->object_space->n = n;
	txn->object_space->cardinality = cardinalty;
	txn->object_space->snap = txn->flags & 1;
	txn->object_space->wal = txn->flags & 2;
	txn->object_space->index[0] = txn->index;
	object_space_fill_stat_names(txn->object_space);
	assert(txn->object_space->snap);
	assert(txn->object_space->wal);
}

static void __attribute__((noinline))
prepare_create_index(struct box_meta_txn *txn, struct tbuf *data)
{
	say_debug("%s", __func__);
	struct index_conf ic = { .n = read_i8(data) };
	index_conf_read(data, &ic);
	index_conf_validate(&ic);

	if (txn->object_space->index[(int)ic.n])
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "index already exists");

	if (ic.n > 0 && ic.unique == false)
		index_conf_merge_unique(&ic, &txn->object_space->index[0]->conf);

	txn->index = [Index new_conf:&ic dtor:&box_tuple_dtor];
	if (txn->index == nil)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't create index");

	Index<BasicIndex> *pk = txn->object_space->index[0];
	struct tnt_object *obj;

	if ([pk size] == 0)
		return;

	if ([txn->index respondsTo:@selector(set_sorted_nodes:count:)]) {

		size_t n_tuples = [pk size];

		void *nodes = xmalloc(n_tuples * txn->index->node_size);
		int node_size = txn->index->node_size;
		int i = 0;
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			struct index_node *node = nodes + i * node_size;
			txn->index->dtor(obj, node, txn->index->dtor_arg);
			i++;
		}
		say_debug("n_tuples:%i", (int)n_tuples);
		struct print_dups_arg arg = {
			.space = txn->object_space->n,
			.index = ic.n,
		};
		if (![(Tree*)txn->index sort_nodes:nodes
					count:n_tuples
				  onduplicate:box_idx_print_dups
					  arg:(void*)&arg]) {
			free(nodes);
			iproto_raise(ERR_CODE_INDEX_VIOLATION, "duplicate values for unique index");
		}
		[(Tree*)txn->index set_sorted_nodes:nodes count:n_tuples];
	} else {
		[pk iterator_init];
		while ((obj = [pk iterator_next]))
			[txn->index replace:obj];
	}
}

static void __attribute__((noinline))
prepare_drop_index(struct box_meta_txn *txn, struct tbuf *data)
{
	say_debug("%s", __func__);
	int i = read_i8(data);
	if (i < 0 || i > nelem(txn->object_space->index))
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "bad index num");
	if (i == 0)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "can't drop primary key");
	txn->index = txn->object_space->index[i];
	if (txn->index == NULL)
		iproto_raise(ERR_CODE_ILLEGAL_PARAMS, "attemp to drop non existent index");
}
void
box_prepare_meta(struct box_meta_txn *txn, struct tbuf *data)
{
	i32 n = read_u32(data);
	txn->flags = read_u32(data);

	switch(txn->op) {
	case CREATE_OBJECT_SPACE:
		prepare_create_object_space(txn, n, data);
		break;
	case CREATE_INDEX:
		txn->object_space = object_space(txn->box, n);
		prepare_create_index(txn, data);
		break;
	case DROP_OBJECT_SPACE:
	case TRUNCATE:
		txn->object_space = object_space(txn->box, n);
		break;
	case DROP_INDEX:
		txn->object_space = object_space(txn->box, n);
		prepare_drop_index(txn, data);
		break;
	default:
		raise_fmt("unknown op");
	}
}

static void
link_index(struct object_space *object_space)
{
	Index *index = object_space->index[0];

	for (int i = 1; i < nelem(object_space->index); i++) {
		Index *next = object_space->index[i];
		if (next) {
			index->next = next;
			index = next;
		}
	}
	index->next = nil;
}

void
box_commit_meta(struct box_meta_txn *txn)
{
	id<BasicIndex> pk = txn->object_space->index[0];
	struct tnt_object *obj;

	switch (txn->op) {
	case CREATE_OBJECT_SPACE:
		say_info("CREATE object_space n:%i 0:%s",
			 txn->object_space->n, [txn->object_space->index[0] info]);
		txn->box->object_space_registry[txn->object_space->n] = txn->object_space;
		break;
	case CREATE_INDEX:
		say_info("CREATE index n:%i %i:%s",
			 txn->object_space->n, txn->index->conf.n, [txn->index info]);
		txn->object_space->index[(int)txn->index->conf.n] = txn->index;
		link_index(txn->object_space);
		break;
	case DROP_INDEX:
		say_info("DROP index n:%i %i", txn->object_space->n, txn->index->conf.n);
		txn->object_space->index[(int)txn->index->conf.n] = NULL;
		[txn->index free];
		link_index(txn->object_space);
		break;
	case DROP_OBJECT_SPACE:
		say_info("DROP object_space n:%i", txn->object_space->n);
		[pk iterator_init];
		while ((obj = [pk iterator_next]) != NULL) {
			assert(tuple_visible_left(obj) == obj);
			object_decr_ref(obj);
		}
		foreach_index(index, txn->object_space)
			[txn->index free];
		txn->box->object_space_registry[txn->object_space->n] = NULL;
		object_space_clear_stat_names(txn->object_space);
		free(txn->object_space);
		break;
	case TRUNCATE:
		pk = txn->object_space->index[0];
		[pk iterator_init];
		while ((obj = [pk iterator_next]) != NULL) {
			assert(tuple_visible_left(obj) == obj);
			tuple_free(obj);
		}
		foreach_index(index, txn->object_space)
			[index clear];
		break;
	default:
		assert(0);
	}
	txn->box->version++;
}

void
box_rollback_meta(struct box_meta_txn *txn)
{
	switch (txn->op) {
	case CREATE_OBJECT_SPACE:
		free(txn->object_space);
		txn->object_space = NULL; // fall through
	case CREATE_INDEX:
		[txn->index free];
		txn->index = nil;
	}
}


register_source();
