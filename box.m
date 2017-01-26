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
#import <say.h>
#import <stat.h>
#import <octopus.h>
#import <tbuf.h>
#import <util.h>
#import <objc.h>
#import <index.h>
#import <spawn_child.h>
#import <shard.h>

#import <mod/box/box.h>
#import <mod/box/src-lua/moonbox.h>
#import <mod/box/box_version.h>
#import <mod/feeder/feeder.h>

#if CFG_lua_path
#import <src-lua/octopus_lua.h>
#endif

#include <third_party/crc32.h>

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sysexits.h>

static struct iproto_service box_primary, box_secondary;
extern void tuple_print(struct tbuf *buf, u32 cardinality, void *f);

struct object_space *
object_space(Box *box, int n)
{
	if (n < 0 || n > nelem(box->object_space_registry) - 1)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "bad namespace number %i", n);

	if (!box->object_space_registry[n])
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "object_space %i is not enabled", n);

	return box->object_space_registry[n];
}

static Index *
configure_index(int i, int j, Index* pk)
{
	struct index_conf *ic = cfg_box2index_conf(cfg.object_space[i]->index[j], i, j, 1);
	if (ic == NULL)
		panic("(object_space = %" PRIu32 " index = %" PRIu32 ") "
		      "unknown index type `%s'", i, j, cfg.object_space[i]->index[j]->type);
	ic->n = j;

	if (j == 0 && ic->unique == false)
		panic("(object_space = %" PRIu32 ") object_space PK index must be unique", i);

	if (j > 0 && ic->unique == false) {
		assert(pk != NULL);
		index_conf_merge_unique(ic, &pk->conf);
	}

	Index *index = [Index new_conf:ic dtor:&box_tuple_dtor];
	free(ic);

	if (index == nil)
		panic("(object_space = %" PRIu32 " index = %" PRIu32 ") "
		      "XXX unknown index type `%s'", i, j, cfg.object_space[i]->index[j]->type);

	/* FIXME: only reasonable for HASH indexes */
	if ([index respondsTo:@selector(resize:)]) {
		if (pk == NULL)
			[(id)index resize:cfg.object_space[i]->estimated_rows];
		else
			[(id)index resize:[pk size]];
	}

	return index;
}

static void
configure_pk(Box *box)
{
	for (int i = 0; i < nelem(box->object_space_registry); i++) {
		if (cfg.object_space[i] == NULL)
			break;

		struct object_space *obj_spc;
		if (!CNF_STRUCT_DEFINED(cfg.object_space[i]))
			continue;

		obj_spc = box->object_space_registry[i] = xcalloc(1, sizeof(struct object_space));

		obj_spc->n = i;
		obj_spc->ignored = !!cfg.object_space[i]->ignored;
		obj_spc->snap = !!cfg.object_space[i]->snap;
		obj_spc->wal = obj_spc->snap && !!cfg.object_space[i]->wal;
		obj_spc->cardinality = cfg.object_space[i]->cardinality;

		if (cfg.object_space[i]->index == NULL)
			panic("(object_space = %" PRIu32 ") at least one index must be defined", i);

		obj_spc->index[0] = configure_index(i, 0, NULL);
		Index *pk = obj_spc->index[0];
		say_info("object space %i PK %i:%s ", i, pk->conf.n, [[pk class] name]);

	}
}

void
box_idx_print_dups(void *varg, struct index_node* a, struct index_node* b, uint32_t position)
{
	struct print_dups_arg* arg = varg;
	struct tbuf out = TBUF(NULL, 0, fiber->pool);
	tbuf_printf(&out, "Duplicate values space %d index %d : ", arg->space, arg->index);
	tuple_print(&out, tuple_cardinality(a->obj), tuple_data(a->obj));
	tbuf_printf(&out, " ");
	tuple_print(&out, tuple_cardinality(b->obj), tuple_data(b->obj));
	say_error("%.*s", (int)tbuf_len(&out), (char*)out.ptr);
	if (arg->positions) {
		write_i32(arg->positions, position);
	}
}

enum dup_action { DUP_PANIC, DUP_IGNORE, DUP_DELETE };
struct dup_conf {
	int spaceno, indexno;
	enum dup_action action;
} *dup_conf = NULL;
static enum dup_action
on_duplicate_action(int spaceno, int indexno)
{
	if (dup_conf != NULL) {
		for (int i = 0; dup_conf[i].spaceno >= 0; i++) {
			if (dup_conf[i].spaceno == spaceno && dup_conf[i].indexno == indexno)
				return dup_conf[i].action;
		}
	}
	return cfg.no_panic_on_snapshot_duplicates ? DUP_IGNORE : DUP_PANIC;
}

static void
delete_duplicates(struct object_space* object_space, size_t node_size, uint32_t *indexes, uint32_t icount, void* nodes)
{
	/* attention: indexes[icount] should be equal to number of nodes */
	struct index_node* node;
	int i;
	uint32_t olda, oldb, newa;
	struct tbuf out = TBUF(NULL, 0, fiber->pool);
	for (i = 0; i < icount; i++) {
		node = nodes + node_size * indexes[i];
		tuple_print(&out, tuple_cardinality(node->obj), tuple_data(node->obj));
		say_warn("space %d delete duplicate %.*s",
				object_space->n, (int)tbuf_len(&out), (char*)out.ptr);
		tbuf_reset(&out);
		foreach_index(index, object_space)
			[index remove: node->obj];
	}
	for (i = 0; i < icount; i++) {
		olda = indexes[i] + 1;
		oldb = indexes[i+1]; /* here is why indexes[icount] should == number of nodes */
		newa = indexes[i] - i;
		memmove(nodes + newa * node_size,
			nodes + olda * node_size,
			(oldb - olda) * node_size);
	}
}

static void
build_secondary(struct object_space *object_space)
{
	Index<BasicIndex> *pk = object_space->index[0];
	size_t n_tuples = [pk size];

	Tree *tree[MAX_IDX] = { nil, };
	id<HashIndex> hash[MAX_IDX] = { nil, };
	int tree_count = 0, hash_count = 0;

	for (int j = 1; j < MAX_IDX; j++) {
		if (object_space->index[j] == nil)
			continue;
		if ([object_space->index[j] isKindOf:[Tree class]])
			tree[tree_count++] = (id)object_space->index[j];
		else
			hash[hash_count++] = (id)object_space->index[j];
	}

	if (tree_count == 0 && hash_count == 0)
		return;

	say_info("Building secondary indexes of object space %i", object_space->n);

	if (n_tuples > 0 && cfg.on_snapshot_duplicates) {
		int cnt = 0, k = 0;
		for (int i = 0; cfg.on_snapshot_duplicates[i]; i++)
			if (CNF_STRUCT_DEFINED(cfg.on_snapshot_duplicates[i]))
				cnt++;
		dup_conf = xcalloc(cnt+1, sizeof(struct dup_conf));
		dup_conf[cnt].spaceno = -1;
		for (int i = 0; cfg.on_snapshot_duplicates[i]; i++) {
			if (!CNF_STRUCT_DEFINED(cfg.on_snapshot_duplicates[i]))
				continue;
			typeof(cfg.on_snapshot_duplicates[i]->index) indexes =
					cfg.on_snapshot_duplicates[i]->index;
			for (int j = 0; indexes[j]; j++) {
				if (!CNF_STRUCT_DEFINED(indexes[j]))
					continue;
				const char* action = indexes[j]->action;
				dup_conf[k].spaceno = i;
				dup_conf[k].indexno = j;
				if (strcmp(action, "IGNORE") == 0)
					dup_conf[k].action = DUP_IGNORE;
				else if (strcmp(action, "DELETE") == 0)
					dup_conf[k].action = DUP_DELETE;
				else
					abort();
				say_debug2("dup_conf[%d]={.spaceno=%d, .indexno=%d, .action=%d}", k, i, j, dup_conf[k].action);
				k++;
			}
		}
	}

        if (n_tuples > 0) {
		title("building_indexes/object_space:%i ", object_space->n);
		struct tnt_object *obj;
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			for (int i = 0; i < hash_count; i++)
				[hash[i] replace:obj];
		}
		for (int i = 0; i < tree_count; i++) {
			say_info("  %i:%s", tree[i]->conf.n, [[tree[i] class] name]);
			void *nodes = xmalloc(n_tuples * tree[i]->node_size);
			u32 t = 0;
			[pk iterator_init];
			while ((obj = [pk iterator_next])) {
				struct index_node *node = nodes + t * tree[i]->node_size;
				tree[i]->dtor(obj, node, tree[i]->dtor_arg);
				t++;
			}
			struct print_dups_arg arg = {
				.space = object_space->n,
				.index = tree[i]->conf.n,
			};
			enum dup_action action = on_duplicate_action(arg.space, arg.index);
			if (action == DUP_DELETE) {
				arg.positions = tbuf_alloc(fiber->pool);
			}
			if (![tree[i] sort_nodes:nodes
				      count:n_tuples
				onduplicate:box_idx_print_dups
					arg:(void*)&arg]) {
				say_debug("space %d index %d FOUND DUPS!!! action is %s",
						arg.space, arg.index,
						(action == DUP_PANIC ? "PANIC" :
					         action == DUP_IGNORE ? "IGNORE" : "DELETE"));
				if (action != DUP_IGNORE) {
					say_error("if you want to ignore this duplicates, add " \
						"on_snapshot_duplicates[%d].index[%d].action=\"IGNORE\"",
					       arg.space, arg.index);
				}
				if (action != DUP_DELETE) {
					say_error("if you want to delete duplicates rows, add " \
						"on_snapshot_duplicates[%d].index[%d].action=\"DELETE\"",
					       arg.space, arg.index);
				}
				if (action == DUP_PANIC) {
					panic("duplicate tuples");
				}
				if (action == DUP_DELETE) {
					uint32_t npos = tbuf_len(arg.positions)/sizeof(uint32_t);
					write_i32(arg.positions, n_tuples);
					delete_duplicates(object_space,
							tree[i]->node_size,
							arg.positions->ptr,
							npos, nodes);
					n_tuples -= npos;
					say_error("DON'T FORGET TO SAVE SNAPSHOT AS SOON AS POSSIBLE!!!!!!!!");
				}
			}
			[tree[i] set_sorted_nodes:nodes count:n_tuples];
		}
	}
	if (dup_conf != NULL)
		free(dup_conf);
	title(NULL);
}

static void
configure_secondary(Box *box)
{

	for (int i = 0; i < nelem(box->object_space_registry); i++) {
		struct object_space *obj_spc = box->object_space_registry[i];
		if (obj_spc == NULL)
			continue;

		Index *prev = obj_spc->index[0];
		Index *pk = obj_spc->index[0];
		say_info("object space %i", i);
		for (int j = 1; j < nelem(obj_spc->index); j++) {
			if (cfg.object_space[i]->index[j] == NULL)
				break;

			if (!CNF_STRUCT_DEFINED(cfg.object_space[i]->index[j]))
				continue;

			obj_spc->index[j] = configure_index(i, j, pk);
			prev->next = obj_spc->index[j];
			prev = obj_spc->index[j];
			say_info("\tindex %i:%s", j, [[obj_spc->index[j] class] name]);
		}

		build_secondary(obj_spc);
	}
}

@implementation Box

- (void)
set_shard:(Shard<Shard> *)shard_
{
	shard = shard_;
	if (cfg.object_space != NULL) {
		if (shard->dummy)
			configure_pk(self);
		else
			say_warn("cfg.object_space ignored");
	}
}

static void
prepare_tlv(struct box_txn *txn, struct tlv *tlv)
{
	switch (tlv->tag) {
	case BOX_MULTI_OP: {
		void *ptr = tlv->val;
		int len = tlv->len;
		while (len) {
			struct tlv *nested = ptr;
			ptr += sizeof(*nested) + nested->len;
			len -= sizeof(*nested) + nested->len;
			prepare_tlv(txn, nested);
		}
		break;
	}
	case BOX_OP:
		box_prepare(txn, *(u16 *)tlv->val, tlv->val + 2, tlv->len - 2);
		break;
	default:
		break;
	}
}

- (void)
apply:(struct tbuf *)data tag:(u16)tag
{
	say_debug2("%s tag:%s", __func__, xlog_tag_to_a(tag));
	say_debug3("%s row:%s", __func__, box_row_to_a(tag, data));

	int tag_type = tag & ~TAG_MASK;
	tag &= TAG_MASK;

	if (tag >= CREATE_OBJECT_SPACE << 5) {
		struct box_meta_txn txn = { .op = tag >> 5,
					    .box = self };
		@try {
			box_prepare_meta(&txn, data);
			box_commit_meta(&txn);
		}
		@catch (id e) {
			box_rollback_meta(&txn);
			@throw;
		}
		return;
	}

	switch (tag_type) {
	case TAG_WAL: ;
		struct box_txn txn = { .box = self,
				       .mode = RW,
				       .ops = TAILQ_HEAD_INITIALIZER(txn.ops) };
		fiber->txn = &txn;
		@try {
			if (tag == wal_data) {
				int op = read_u16(data);
				box_prepare(&txn, op, data->ptr, tbuf_len(data));
			} else if (tag == tlv) {
				while (tbuf_len(data)) {
					struct tlv *tlv = read_bytes(data, sizeof(*tlv));
					tbuf_ltrim(data, tlv->len);
					prepare_tlv(&txn, tlv);
				}
			} else if (tag >= user_tag) {
				box_prepare(&txn, tag >> 5, data->ptr, tbuf_len(data));
			} else {
				return;
			}
			box_commit(&txn);
		}
		@catch (id e) {
			box_rollback(&txn);
			@throw;
		}
		break;
	case TAG_SNAP:
		if (tag != snap_data)
			return;

		const struct box_snap_row *snap = box_snap_row(data);
		struct object_space *object_space = object_space_registry[snap->object_space];
		if (object_space == NULL)
			raise_fmt("object_space %i is not configured", snap->object_space);
		if (object_space->ignored) {
			break;
		}

		assert(object_space->index[0] != nil);
		snap_insert_row(object_space, snap->tuple_size, snap->data, snap->data_size);
		break;
	case TAG_SYS:
		abort();
	}
}

- (void)
snap_final_row
{
	/* this called only when shard is dummy (legace mode) */
	configure_secondary(self);
}

- (void)
wal_final_row
{
	for (u32 n = 0; n < nelem(object_space_registry); n++) {
		struct object_space *obj_spc = object_space_registry[n];
		if (obj_spc == NULL)
			continue;

		say_info("Object space %i", n);
		foreach_index(index, obj_spc)
			say_info("\tindex[%i]: %s", index->conf.n, [index info]);
	}
}

- (void)
status_changed
{
}

- (void)
print:(const struct row_v12 *)row into:(struct tbuf *)buf
{
	print_row(buf, row, box_print_row);
}

- (int)
snapshot_fold
{
	struct tnt_object *obj;

	u32 crc = 0;
#ifdef FOLD_DEBUG
	int count = 0;
#endif
	for (int n = 0; n < nelem(object_space_registry); n++) {
		if (object_space_registry[n] == NULL || !object_space_registry[n]->snap)
			continue;

		id pk = object_space_registry[n]->index[0];

		if ([pk respondsTo:@selector(ordered_iterator_init)])
			[pk ordered_iterator_init];
		else
			[pk iterator_init];

		while ((obj = [pk iterator_next])) {
#ifdef FOLD_DEBUG
			struct tbuf *b = tbuf_alloc(fiber->pool);
			tuple_print(b, tuple->cardinality, tuple->data);
			say_info("row %i: %.*s", count++, tbuf_len(b), (char *)b->ptr);
#endif
			u32 header[2] = { tuple_bsize(obj) ,tuple_cardinality(obj) };
			crc = crc32c(crc, (unsigned char *)header, 8);
			crc = crc32c(crc, tuple_data(obj), header[0] /* bsize */);
		}
	}
	printf("CRC: 0x%08x\n", crc);
	return 0;
}

- (u32)
snapshot_estimate
{
	size_t total_rows = 0;
	for (int n = 0; n < nelem(object_space_registry); n++)
		if (object_space_registry[n] && object_space_registry[n]->snap)
			total_rows += [object_space_registry[n]->index[0] size];
	return total_rows;
}

static int verify_indexes(struct object_space *o, size_t pk_rows)
{
	struct tnt_object *obj;
	foreach_index(index, o) {
		if (index->conf.n == 0)
			continue;

		title("snap_dump/check index:%i", index->conf.n);

		size_t index_rows = 0;
		[index iterator_init];
		while ((obj = [index iterator_next])) {
			obj = tuple_visible_left(obj);
			if (obj == NULL)
				continue;
			index_rows++;
		}
		if (pk_rows != index_rows) {
			say_error("heap invariant violation: n:%i index:%i rows:%zi != pk_rows:%zi",
				  o->n, index->conf.n, index_rows, pk_rows);
			return -1;
		}
	}
	return 0;
}
- (int)
snapshot_write_rows:(XLog *)l
{
	struct box_snap_row header;
	struct tnt_object *obj;
	struct palloc_pool *pool = palloc_create_pool((struct palloc_config){.name = __func__});
	struct tbuf *row = tbuf_alloc(pool);
	int ret = 0;
	size_t rows = 0, pk_rows, total_rows = [self snapshot_estimate];

	for (int n = 0; n < nelem(object_space_registry); n++) {
		if (object_space_registry[n] == NULL || !object_space_registry[n]->snap)
			continue;

		struct object_space *o = object_space_registry[n];
		Index<BasicIndex> *pk = o->index[0];

		assert(n == o->n);

		if (!shard->dummy) {
			tbuf_reset(row);
			write_i32(row, n);
			int flags = (o->snap ? 1 : 0) | (o->wal ? 2 : 0);
			write_i32(row, flags);
			write_i8(row, o->cardinality);
			index_conf_write(row, &pk->conf);

			if ([l append_row:row->ptr len:tbuf_len(row)
				    shard:shard tag:(CREATE_OBJECT_SPACE << 5)|TAG_SNAP] == NULL)
			{
				ret = -1;
				goto out;
			}
		}

		pk_rows = 0;
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			obj = tuple_visible_left(obj);
			if (obj == NULL)
				continue;

			if (obj->type == BOX_TUPLE && container_of(obj, struct gc_oct_object, obj)->refs <= 0) {
				say_error("heap invariant violation: n:%i obj->refs == %i", n,
					  container_of(obj, struct gc_oct_object, obj)->refs);
				errno = EINVAL;
				ret = -1;
				goto out;
			}

			if (!tuple_valid(obj)) {
				say_error("heap invariant violation: n:%i invalid tuple %p", n, obj);
				errno = EINVAL;
				ret = -1;
				goto out;
			}

			header.object_space = n;
			header.tuple_size = tuple_cardinality(obj);
			header.data_size = tuple_bsize(obj);

			tbuf_reset(row);
			tbuf_append(row, &header, sizeof(header));
			tbuf_append(row, tuple_data(obj), header.data_size);

			if ([l append_row:row->ptr len:tbuf_len(row)
				    shard:shard tag:snap_data|TAG_SNAP] == NULL)
			{
				ret = -1;
				goto out;
			}

			pk_rows++;
			if (++rows % 100000 == 0) {
				float pct = (float)rows / total_rows * 100.;
				say_info("%.1fM/%.2f%% rows written", rows / 1000000., pct);
				title("snap_dump %.2f%%", pct);
			}
		}

		if (!shard->dummy) {
			foreach_index(index, o) {
				if (index->conf.n == 0)
					continue;
				tbuf_reset(row);
				write_i32(row, n);
				write_i32(row, 0); // flags
				write_i8(row, index->conf.n);
				index_conf_write(row, &index->conf);

				if ([l append_row:row->ptr len:tbuf_len(row)
					    shard:shard tag:(CREATE_INDEX << 5)|TAG_SNAP] == NULL)
				{
					ret = -1;
					goto out;
				}
			}
		}

		if (verify_indexes(o, pk_rows) < 0) {
			errno = EINVAL;
			ret = -1;
			goto out;
		}
	}

out:
	palloc_destroy_pool(pool);
	return ret;
}

@end

static void
initialize_service()
{
	iproto_service(&box_primary, cfg.primary_addr);
	box_primary.options = SERVICE_SHARDED;
	box_service(&box_primary);
	feeder_service(&box_primary);

	for (int i = 0; i < MAX(1, cfg.wal_writer_inbox_size); i++)
		fiber_create("box_worker", iproto_worker, &box_primary);

	if (cfg.secondary_addr != NULL && strcmp(cfg.secondary_addr, cfg.primary_addr) != 0) {
		iproto_service(&box_secondary, cfg.secondary_addr);
		box_secondary.options = SERVICE_SHARDED;
		box_service_ro(&box_secondary);
		fiber_create("box_secondary_worker", iproto_worker, &box_secondary);
	}
	say_info("(silver)box initialized (%i workers)", cfg.wal_writer_inbox_size);
}

static void
init_second_stage(va_list ap __attribute__((unused)))
{
#if CFG_lua_path
	luaT_openbox(root_L);
	luaO_require_or_panic("box_init", false, NULL);
#endif
#if CFG_caml_path
	extern void oct_caml_plugins();
	oct_caml_plugins();
#endif
	[recovery simple:&box_primary];
}


static void
init(void)
{
	title("loading");

	recovery = [[Recovery alloc] init];
	recovery->default_exec_class = [Box class];

	if (init_storage) {
		if (cfg.object_space)
			[recovery shard_create_dummy:NULL];
		return;
	}

	box_init_phi_cache();

	initialize_service();

	/* fiber is required to successfully pull from remote */
	fiber_create("box_init", init_second_stage);
}

static void
info(struct tbuf *out, const char *what)
{
	if (what == NULL) {
		tbuf_printf(out, "info:" CRLF);
		tbuf_printf(out, "  version: \"%s\"" CRLF, octopus_version());
		tbuf_printf(out, "  uptime: %i" CRLF, tnt_uptime());
		tbuf_printf(out, "  pid: %i" CRLF, getpid());
		extern Recovery *recovery;
		tbuf_printf(out, "  lsn: %" PRIi64 CRLF, [recovery lsn]);
		tbuf_printf(out, "  shards:" CRLF);
		for (int i = 0; i < MAX_SHARD; i++) {
			id<Shard> shard = [recovery shard:i];
			if (shard == nil)
				continue;
			tbuf_printf(out, "  - shard_id: %i" CRLF, i);
			tbuf_printf(out, "    scn: %" PRIi64 CRLF, [shard scn]);
			tbuf_printf(out, "    status: %s%s%s" CRLF, [shard status],
				    cfg.custom_proc_title ? "@" : "",
				    cfg.custom_proc_title ?: "");
			if ([shard is_replica]) {
				tbuf_printf(out, "    recovery_lag: %.3f" CRLF, [shard lag]);
				tbuf_printf(out, "    recovery_last_update: %.3f" CRLF, [shard last_update_tstamp]);
				if (!cfg.ignore_run_crc)
					tbuf_printf(out, "    recovery_run_crc_status: %s" CRLF, [shard run_crc_status]);
			}
			Box *box = [shard executor];
			tbuf_printf(out, "    namespaces:" CRLF);
			for (uint32_t n = 0; n < nelem(box->object_space_registry); ++n) {
				if (box->object_space_registry[n] == NULL)
					continue;
				struct object_space *sp = box->object_space_registry[n];
				tbuf_printf(out, "    - n: %i"CRLF, n);
				tbuf_printf(out, "      objects: %i"CRLF, [sp->index[0] size]);
				tbuf_printf(out, "      obj_bytes: %zi"CRLF, sp->obj_bytes);
				tbuf_printf(out, "      slab_bytes: %zi"CRLF, sp->slab_bytes);
				tbuf_printf(out, "      indexes:"CRLF);
				foreach_index(index, sp)
					tbuf_printf(out, "      - { index: %i, slots: %i, bytes: %zi }" CRLF,
						    index->conf.n, [index slots], [index bytes]);
			}
		}
		tbuf_printf(out, "  config: \"%s\""CRLF, cfg_filename);

		return;
	}

	if (strcmp(what, "net") == 0) {
		if (box_primary.name != NULL)
			iproto_service_info(out, &box_primary);
		if (box_secondary.name != NULL)
			iproto_service_info(out, &box_secondary);
		return;
	}
}

static int
check_config(struct octopus_cfg *new)
{
	extern void out_warning(int v, char *format, ...);
	struct feeder_param feeder;
	enum feeder_cfg_e e = feeder_param_fill_from_cfg(&feeder, new);
	bool errors = false;
	if (e) {
		out_warning(0, "wal_feeder config is wrong");
		errors = true;
	}

	if (new->object_space != NULL) {
		for (int i = 0; i < OBJECT_SPACE_MAX; i++) {
			if (new->object_space[i] == NULL)
				break;
			if (!CNF_STRUCT_DEFINED(new->object_space[i]))
				continue;
			if (new->object_space[i]->index == NULL) {
				out_warning(0, "(object_space = %" PRIu32 ") at least one index must be defined", i);
				errors = true;
			}
			for (int j = 0; j < MAX_IDX; j++) {
				if (new->object_space[i]->index[j] == NULL)
					break;
				if (!CNF_STRUCT_DEFINED(new->object_space[i]->index[j]))
					continue;

				struct index_conf* ic = cfg_box2index_conf(new->object_space[i]->index[j], i, j, 0);
				if (ic == NULL) {
					errors = true;
				} else {
					free(ic);
				}
			}
		}
	}

	if (new->on_snapshot_duplicates) {
		for (int i = 0; new->on_snapshot_duplicates[i]; i++) {
			if (!CNF_STRUCT_DEFINED(new->on_snapshot_duplicates[i]))
				continue;
			typeof(new->on_snapshot_duplicates[i]->index) indexes =
				new->on_snapshot_duplicates[i]->index;
			if (indexes == NULL) {
				out_warning(0, "no indexes for on_snapshot_duplicates[%d]", i);
				errors = true;
			} else {
				for (int j = 0; indexes[j]; j++) {
					if (!CNF_STRUCT_DEFINED(indexes[j]))
						continue;
#define eq(t, s) (strcmp((t),(s)) == 0)
					const char* action = indexes[j]->action;
					if (!action || !(eq(action, "DELETE") || !eq(action, "IGNORE")))
						out_warning(0, "on_snapshot_duplicates[%d].index[%d].action unknown (=\"%s\")",
								i, j, action ?: "<null>");
#undef eq
				}
			}
		}
	}

	return errors ? -1 : 0;
}

static void
reload_config(struct octopus_cfg *old __attribute__((unused)),
	      struct octopus_cfg *new __attribute__((unused)))
{
	Shard<Shard> *shard = [recovery shard:0];
	if (shard == nil || !shard->dummy) {
		say_error("ignoring legacy configuration request");
		return;
	}

	if ([(id)shard respondsTo:@selector(adjust_route)])
		[(id)shard perform:@selector(adjust_route)];
	else
		say_error("ignoring unsupported configuration request");
}

static struct tnt_module box_mod = {
	.name = "box",
	.version = box_version_string,
	.depend_on = (const char*[]){"onlineconf", NULL},
	.init = init,
	.check_config = check_config,
	.reload_config = reload_config,
	.cat = box_cat,
	.cat_scn = box_cat_scn,
	.info = info,
};

register_module(box_mod);
register_source();
