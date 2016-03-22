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
#import <paxos.h>

#import <mod/box/box.h>
#import <mod/box/moonbox.h>
#import <mod/box/box_version.h>
#import <mod/feeder/feeder.h>

#include <third_party/crc32.h>

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sysexits.h>

static struct iproto_service box_primary, box_secondary;
const int object_space_max_idx = MAX_IDX;

struct object_space *
object_space(Box *box, int n)
{
	if (n < 0 || n > nelem(box->object_space_registry) - 1)
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "bad namespace number %i", n);

	if (!box->object_space_registry[n])
		iproto_raise_fmt(ERR_CODE_ILLEGAL_PARAMS, "object_space %i is not enabled", n);

	return box->object_space_registry[n];
}

int
box_version(Box* box)
{
	return box->version;
}

static void
configure(Box *box)
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

		for (int j = 0; j < nelem(obj_spc->index); j++) {

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

			if ([index isKindOf: [Tree class]] && j > 0)
				index = [[DummyIndex alloc] init_with_index:index];

			obj_spc->index[j] = (Index<BasicIndex> *)index;
		}

		Index *pk = obj_spc->index[0];

		if (pk->conf.unique == false)
			panic("(object_space = %" PRIu32 ") object_space PK index must be unique", i);

		say_info("object space %i successfully configured", i);
		say_info("  PK %i:%s", pk->conf.n, [[pk class] name]);
	}
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
		title("building_indexes/object_space:%i ", object_space->n);
		for (int i = 0; i < tree_count; i++)
                        nodes[i] = xmalloc(estimated_tuples * ts[i]->node_size);
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
	title(NULL);
}

@implementation Box

- (id)
init_shard:(Shard<Shard> *)shard_
{
	[super init];
	shard = shard_;

	if (cfg.object_space != NULL) {
		if (shard->dummy)
			configure(self);
		else
			say_warn("cfg.object_space ignored");
	}

	return self;
}

- (void)
set_shard:(Shard<Shard> *)shard_
{
	shard = shard_;
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
	} else {
		struct box_txn txn = { .box = self };
		@try {
			switch (tag_type) {
			case TAG_WAL:
				if (tag == wal_data)
					txn.op = read_u16(data);
				else if(tag >= user_tag)
					txn.op = tag >> 5;
				else
					return;

				box_prepare(&txn, data);
				break;
			case TAG_SNAP:
				if (tag != snap_data)
					return;

				const struct box_snap_row *snap = box_snap_row(data);
				txn.object_space = object_space_registry[snap->object_space];
				if (txn.object_space == NULL)
					raise_fmt("object_space %i is not configured", snap->object_space);
				if (txn.object_space->ignored) {
					txn.object_space = NULL;
					return;
				}

				txn.op = INSERT;
				txn.index = txn.object_space->index[0];
				assert(txn.index != nil);

				prepare_replace(&txn, snap->tuple_size, snap->data, snap->data_size);
				break;
			case TAG_SYS:
				abort();
			}

			box_commit(&txn);
		}
		@catch (id e) {
			box_rollback(&txn);
			@throw;
		}
		@finally {
			box_cleanup(&txn);
		}
	}
}

- (void)
build_secondary_indexes
{
	if (built_seconday_indexes)
		return;
	built_seconday_indexes = true;

	@try {
		for (u32 n = 0; n < nelem(object_space_registry); n++) {
			if (object_space_registry[n])
				build_object_space_trees(object_space_registry[n]);
		}
	}
	@catch (Error *e) {
		raise_fmt("unable to built tree indexes: %s", e->reason);
	}

	for (u32 n = 0; n < nelem(object_space_registry); n++) {
		struct object_space *obj_spc = object_space_registry[n];
		if (obj_spc == NULL)
			continue;

		say_info("Object space %i", n);
		foreach_index(index, obj_spc)
			say_info("\tindex[%i]: %s", index->conf.n, [[index class] name]);
	}
}


- (void)
wal_final_row
{
	[self build_secondary_indexes];
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
			extern void tuple_print(struct tbuf *buf, u32 cardinality, void *f);
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

		/* during initial load of replica secondary indexes isn't configured yet */
		if ([index isKindOf:[DummyIndex class]])
			continue;

		title("snap_dump/check index:%i", index->conf.n);

		size_t index_rows = 0;
		[index iterator_init];
		while ((obj = [index iterator_next])) {
			if (unlikely(object_ghost(obj)))
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
			write_i32(row, 0); // flags
			write_i8(row, o->cardinality);
			write_i8(row, o->snap);
			write_i8(row, o->wal);
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
			if (unlikely(object_ghost(obj)))
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
	luaT_openbox(root_L);
	luaT_require_or_panic("box_init", false, NULL);

	[recovery simple:&box_primary];
}


static void
init(void)
{
	title("loading");

	recovery = [[Recovery alloc] init];
	recovery->default_exec_class = [Box class];

	if (init_storage)
		return;

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
				if (!cfg.ignore_run_crc) {
					tbuf_printf(out, "    recovery_run_crc_lag: %.3f" CRLF, [shard run_crc_lag]);
					tbuf_printf(out, "    recovery_run_crc_status: %s" CRLF, [shard run_crc_status]);
				}
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
	if (e) {
		out_warning(0, "wal_feeder config is wrong");
		return -1;
	}

	return 0;
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

	if ([(id)shard respondsTo:@selector(remote_hot_standby)])
		[(id)shard perform:@selector(remote_hot_standby)];
	else
		say_error("ignoring unsupported configuration request");
}

static struct tnt_module box_mod = {
	.name = "box",
	.version = box_version_string,
	.init = init,
	.check_config = check_config,
	.reload_config = reload_config,
	.cat = box_cat,
	.cat_scn = box_cat_scn,
	.info = info,
};

register_module(box_mod);
register_source();
