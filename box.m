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
struct object_space *object_space_registry;

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
		object_space_registry[i].snap = !!cfg.object_space[i]->snap;
		object_space_registry[i].wal = object_space_registry[i].snap &&
						!!cfg.object_space[i]->wal;
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

			if ([index isKindOf: [Tree class]] && j > 0)
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
	@try {
		for (u32 n = 0; n < object_space_count; n++) {
			if (object_space_registry[n].enabled)
				build_object_space_trees(&object_space_registry[n]);
		}
	}
	@catch (Error *e) {
		raise_fmt("unable to built tree indexes: %s", e->reason);
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

static void
initialize_service()
{
	tcp_iproto_service(&box_primary, cfg.primary_addr, NULL, NULL);
	box_service_ro(&box_primary);

	for (int i = 0; i < MAX(1, cfg.wal_writer_inbox_size); i++)
		fiber_create("box_worker", iproto_worker, &box_primary);

	if (cfg.secondary_addr != NULL && strcmp(cfg.secondary_addr, cfg.primary_addr) != 0) {
		tcp_iproto_service(&box_secondary, cfg.secondary_addr, NULL, NULL);
		box_service_ro(&box_secondary);
		fiber_create("box_secondary_worker", iproto_worker, &box_secondary);
	}
	say_info("(silver)box initialized (%i workers)", cfg.wal_writer_inbox_size);
}

@implementation Box

- (void)
apply:(struct tbuf *)data tag:(u16)tag
{
	struct box_txn txn = { .op = 0 };

	@try {

		say_debug("%s tag:%s data:%s", __func__,
			  xlog_tag_to_a(tag), tbuf_to_hex(data));

		int tag_type = tag & ~TAG_MASK;
		tag &= TAG_MASK;

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
			txn.object_space = &object_space_registry[snap->object_space];
			if (!txn.object_space->enabled)
				raise_fmt("object_space %i is not configured", txn.object_space->n);
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
			return;
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


- (void)
wal_final_row
{
	if (box_primary.name == NULL) {
		build_secondary_indexes();
		initialize_service();
	}
}

- (void)
status_changed
{
	if (recovery->status == PRIMARY) {
		box_service(&box_primary);
		return;
	}
	if (recovery->prev_status == PRIMARY)
		box_service_ro(&box_primary);
}

- (void)
print:(const struct row_v12 *)row into:(struct tbuf *)buf
{
	if (print_sys_row(buf, row))
		return;

	print_row_header(buf, row);
	box_print_row(buf, row->tag, &TBUF(row->data, row->len, fiber->pool));
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
		if (!object_space_registry[n].enabled || !object_space_registry[n].snap)
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
			extern void tuple_print(struct tbuf *buf, u32 cardinality, void *f);
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

@interface BoxSnapWriter : SnapWriter
@end

@implementation BoxSnapWriter
- (u32)
snapshot_estimate
{
	size_t total_rows = 0;
	for (int n = 0; n < object_space_count; n++)
		if (object_space_registry[n].enabled && object_space_registry[n].snap)
			total_rows += [object_space_registry[n].index[0] size];
	return total_rows;
}

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
		if (!object_space_registry[n].enabled || !object_space_registry[n].snap)
			continue;

		pk_rows = 0;
		id pk = object_space_registry[n].index[0];
		[pk iterator_init];
		while ((obj = [pk iterator_next])) {
			if (unlikely(ghost(obj)))
				continue;

			if (obj->refs <= 0) {
				say_error("heap invariant violation: n:%i obj->refs == %i", n, obj->refs);
				errno = EINVAL;
				ret = -1;
				goto out;
			}

			tuple = box_tuple(obj);
			if (tuple_bsize(tuple->cardinality, tuple->data, tuple->bsize) != tuple->bsize) {
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

			if (snapshot_write_row(l, snap_data, row) < 0) {
				ret = -1;
				goto out;
			}

			pk_rows++;
			if (++rows % 100000 == 0) {
				float pct = (float)rows / total_rows * 100.;
				say_info("%.1fM/%.2f%% rows written", rows / 1000000., pct);
				title("snap_dump %.2f%%", pct);
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

			title("snap_dump/check index:%i", index->conf.n);

			size_t index_rows = 0;
			[index iterator_init];
			while ((obj = [index iterator_next])) {
				if (unlikely(ghost(obj)))
					continue;
				index_rows++;
			}
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
	object_space_registry = xcalloc(object_space_count, sizeof(struct object_space));
	for (int i = 0; i < object_space_count; i++)
		object_space_registry[i].n = i;

	title("loading");
	if (cfg.paxos_enabled) {
		if (cfg.local_hot_standby)
			panic("wal_hot_standby is incompatible with paxos");
	}

	struct feeder_param feeder;
	enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
	if (fid_err) panic("wrong feeder conf");

	recovery = [[Recovery alloc] init_feeder_param:&feeder];
	[recovery set_client:[[Box alloc] init]];
	[recovery set_snap_writer:[BoxSnapWriter class]];

	if (init_storage)
		return;

	/* fiber is required to successfully pull from remote */
	fiber_create("box_init", init_second_stage);
}

static void
init_second_stage(va_list ap __attribute__((unused)))
{
	luaT_openbox(root_L);
	luaT_require_or_panic("box_init", false, NULL);

	configure();

	if (cfg.paxos_enabled) {
		[recovery load_from_local];
		[recovery enable_local_writes];
	} else {
		[recovery simple];
	}
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
		tbuf_printf(out, "  status: %s%s%s" CRLF, [recovery status],
			    cfg.custom_proc_title ? "@" : "",
			    cfg.custom_proc_title ?: "");
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
reload_config(struct octopus_cfg *old _unused_,
	      struct octopus_cfg *new)
{
	struct feeder_param feeder;
	feeder_param_fill_from_cfg(&feeder, new);
	[recovery feeder_changed:&feeder];
}

static struct tnt_module box = {
	.name = "box",
	.version = box_version_string,
	.init = init,
	.check_config = check_config,
	.reload_config = reload_config,
	.cat = box_cat,
	.cat_scn = box_cat_scn,
	.info = info
};

register_module(box);
register_source();
