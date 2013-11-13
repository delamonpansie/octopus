/*
 * Copyright (C) 2010, 2011, 2012, 2013 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013 Yuriy Vostrikov
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
#import <log_io.h>
#import <index.h>
#import <say.h>
#include <stat.h>
#include <salloc.h>
#import <pickle.h>

#include <sysexits.h>

#import <mod/memcached/memcached_version.h>
#import <mod/memcached/store.h>

CStringHash *mc_index;
static u64 cas;

struct mc_stats mc_stats;

static struct tnt_object *
mc_alloc(const char *key, u32 exptime, u32 flags, u32 value_len, const char *value)
{
	char suffix[43];
	sprintf(suffix, " %"PRIu32" %"PRIu32"\r\n", flags, value_len);
	int suffix_len = strlen(suffix);
	int key_len = strlen(key) + 1;

	struct tnt_object *obj = NULL;

	obj = object_alloc(MC_OBJ, sizeof(struct mc_obj) +
			   key_len + suffix_len + value_len);

	struct mc_obj *m = mc_obj(obj);
	*m = (struct mc_obj){ .exptime = exptime,
			      .flags = flags,
			      .cas = ++cas,
			      .key_len = key_len,
			      .suffix_len = suffix_len,
			      .value_len = value_len };
	memcpy(m->data, key, key_len);
	memcpy(m->data + key_len, suffix, suffix_len);
	memcpy(m->data + key_len + suffix_len, value, value_len);
	return obj;
}


enum tag { STORE = user_tag, DELETE };
Recovery *recovery;
@implementation Recovery (Memcached)

static void
store_compat(struct tbuf *op)
{
	int key_len = read_varint32(op);
	char *key = read_bytes(op, key_len);

	int meta_len = read_varint32(op);
	assert(meta_len == 16);
	u32 exptime = read_u32(op);
	u32 flags = read_u32(op);
	u64 cas = read_u64(op);

	int suffix_len = read_varint32(op);
	read_bytes(op, suffix_len);

	int value_len = read_varint32(op);
	char *value = read_bytes(op, value_len);

	struct tnt_object *obj = mc_alloc(key, exptime, flags, value_len, value);
	object_incr_ref(obj);
	[mc_index replace:obj];

	struct mc_obj *m = mc_obj(obj);
	if (m->cas > cas)
		cas = m->cas + 1;

	say_info("STORE(c)	%s", m->data);
}

- (void)
apply:(struct tbuf *)op tag:(u16)tag
{
	/* row format is dead simple:
	   STORE -> op is mc_obj itself.
	   DELETE -> op is key array  */

	struct tnt_object *obj;
	struct mc_obj *m;
	const char *key;

	switch(tag & TAG_MASK) {
	case STORE:
		m = op->ptr;
		obj = object_alloc(MC_OBJ, mc_len(m));
		object_incr_ref(obj);
		memcpy(obj->data, m, mc_len(m));
		[mc_index replace:obj];
		if (m->cas > cas)
			cas = m->cas + 1;
		say_debug("STORE	%s", m->data);
		break;
	case DELETE:
		while (tbuf_len(op) > 0) {
			key = op->ptr;
			obj = [mc_index find:key];
			if (obj) {
				[mc_index remove:obj];
				object_decr_ref(obj);
			}
			tbuf_ltrim(op, strlen(key) + 1);
			say_debug("DELETE	%s", key);
		}
		break;

	/* compat with box emulation */
	case wal_tag: {
		u16 code = read_u16(op);
		read_u32(op); /* obj_space */
		switch(code) {
		case 13: {
			read_u32(op); /* flags */
			read_u32(op); /* cardinality */
			store_compat(op);
			break;
		}
		case 20: {
			read_u32(op); /* key cardinality */
			int key_len = read_varint32(op);
			char *key = palloc(fiber->pool, key_len + 1);
			memcpy(key, op->ptr, key_len);
			key[key_len] = 0;
			obj = [mc_index find:key];
			if (obj) {
				[mc_index remove:obj];
				object_decr_ref(obj);
			}
			say_debug("DELETE(c)	%s", key);
			break;
		}
		default:
			abort();
		}
		assert(tbuf_len(op) == 0);
		break;
	}
	case snap_tag:
		read_u32(op); /* obj_space */
		read_u32(op); /* cardinality */
		read_u32(op);  /* data_size */
		store_compat(op);
		break;

	default:
		break;
	}
}

- (int)
snapshot_write_rows: (XLog *)l
{
	u32			i = 0;
	struct tnt_object	*obj;

	set_proc_title("dumper of pid %" PRIu32 ": dumping actions", getppid());

	[mc_index iterator_init];

	while((obj = [mc_index iterator_next]) != NULL) {
		struct mc_obj *m = mc_obj(obj);
		if (snapshot_write_row(l, STORE, &TBUF(m, mc_len(m), NULL)) < 0)
			return -1;

		prelease_after(fiber->pool, 128 * 1024);

		if ((++i)% 100000 == 0) {
			say_info("%.1fM rows written", i / 1000000. );
			set_proc_title("dumper of pid %" PRIu32 ": dumping actions (%.1fM  rows )",
				       getppid(), i / 1000000.);
		}

		if (i % 10000 == 0)
			[l confirm_write];
	}

	say_info("Snapshot finished");

	return 0;
}
@end

int
store(const char *key, u32 exptime, u32 flags, u32 value_len, char *value)
{
	struct tnt_object *old_obj = NULL, *obj = NULL;

	if ([recovery is_replica])
		return 0;

	@try {
		obj = mc_alloc(key, exptime, flags, value_len, value);
		struct mc_obj *m = mc_obj(obj);

		old_obj = [mc_index find_by_obj:obj];
		if (old_obj) {
			object_lock(old_obj); /* may throw exception */
		} else {
			object_lock(obj);
			obj->flags |= GHOST;
			[mc_index replace:obj];
		}

		if ([recovery submit:m len:mc_len(m) tag:STORE|TAG_WAL] == 1) {
			if ((obj->flags & GHOST) == 0) {
				[mc_index replace:obj];
				object_unlock(old_obj);
				object_decr_ref(old_obj);
			} else {
				obj->flags &= ~GHOST;
				object_unlock(obj);
			}
			object_incr_ref(obj);
			obj = NULL;
			return 1;
		} else {
			say_warn("can't write WAL row");
			return 0;
		}
	}
	@catch (Error *e) {
		say_warn("got exception: %s", e->reason);
		return 0;
	}
	@finally {
		if (obj)
			sfree(obj);
	}
}

int
delete(char **keys, int n)
{
	struct tnt_object **obj = palloc(fiber->pool, sizeof(*obj) * n);
	int k = 0;
	int ret = 0;

	if ([recovery is_replica])
		return 0;

	for (int i = 0; i < n; i++) {
		obj[k] = [mc_index find:*keys++];
		if (obj[k] == NULL)
			continue;
		@try {
			object_lock(obj[k]);
		}
		@catch (id e) {
			continue;
		}
		k++;
	}

	if (k == 0)
		return 0;

	struct tbuf *b = tbuf_alloc(fiber->pool);
	for (int i = 0; i < k; i++) {
		struct mc_obj *m = mc_obj(obj[i]);
		tbuf_append(b, m->data, m->key_len);
	}

	if ([recovery submit:b->ptr len:tbuf_len(b) tag:DELETE|TAG_WAL] == 1) {
		for (int i = 0; i < k; i++) {
			[mc_index remove:obj[i]];
			object_unlock(obj[i]);
			object_decr_ref(obj[i]);
		}
		ret += k;
	}
	return ret;
}

#ifndef MEMCACHE_NO_EXPIRE
static void
memcached_expire(va_list va __attribute__((unused)))
{
	u32 i = 0;
	say_info("memcached expire fiber started");
	char **keys = malloc(cfg.memcached_expire_per_loop * sizeof(void *));

	for (;;) {
		double delay = (double)cfg.memcached_expire_per_loop *
				       cfg.memcached_expire_full_sweep /
			       ([mc_index slots] + 1);
		if (delay > 1)
			delay = 1;
		fiber_sleep(delay);

		say_debug("expire loop");
		if ([recovery is_replica])
			continue;

		if (i >= [mc_index slots])
			i = 0;

		int k = 0;
		for (int j = 0; j < cfg.memcached_expire_per_loop; j++, i++) {
			struct tnt_object *obj = [mc_index get:i];
			if (obj == NULL || ghost(obj))
				continue;

			if (!expired(obj))
				continue;

			struct mc_obj *m = mc_obj(obj);

			keys[k] = palloc(fiber->pool, m->key_len);
			strcpy(keys[k], m->data);
			k++;
		}

		delete(keys, k);
		say_debug("expired %i keys", k);

		fiber_gc();
	}
}
#endif


void
print_stats(struct conn *c)
{
	u64 bytes_used, items;
	struct tbuf *out = tbuf_alloc(fiber->pool);
	slab_stat2(&bytes_used, &items);

	tbuf_printf(out, "STAT pid %"PRIu32"\r\n", (u32)getpid());
	tbuf_printf(out, "STAT uptime %"PRIu32"\r\n", (u32)tnt_uptime());
	tbuf_printf(out, "STAT time %"PRIu32"\r\n", (u32)ev_now());
	tbuf_printf(out, "STAT version 1.2.5 (octopus/(silver)box)\r\n");
	tbuf_printf(out, "STAT pointer_size %zu\r\n", sizeof(void *)*8);
	tbuf_printf(out, "STAT curr_items %"PRIu64"\r\n", items);
	tbuf_printf(out, "STAT total_items %"PRIu64"\r\n", mc_stats.total_items);
	tbuf_printf(out, "STAT bytes %"PRIu64"\r\n", bytes_used);
	tbuf_printf(out, "STAT curr_connections %"PRIu32"\r\n", mc_stats.curr_connections);
	tbuf_printf(out, "STAT total_connections %"PRIu32"\r\n", mc_stats.total_connections);
	tbuf_printf(out, "STAT connection_structures %"PRIu32"\r\n", mc_stats.curr_connections); /* lie a bit */
	tbuf_printf(out, "STAT cmd_get %"PRIu64"\r\n", mc_stats.cmd_get);
	tbuf_printf(out, "STAT cmd_set %"PRIu64"\r\n", mc_stats.cmd_set);
	tbuf_printf(out, "STAT get_hits %"PRIu64"\r\n", mc_stats.get_hits);
	tbuf_printf(out, "STAT get_misses %"PRIu64"\r\n", mc_stats.get_misses);
	tbuf_printf(out, "STAT evictions %"PRIu64"\r\n", mc_stats.evictions);
	tbuf_printf(out, "STAT bytes_read %"PRIu64"\r\n", mc_stats.bytes_read);
	tbuf_printf(out, "STAT bytes_written %"PRIu64"\r\n", mc_stats.bytes_written);
	tbuf_printf(out, "STAT limit_maxbytes %"PRIu64"\r\n", (u64)(cfg.slab_alloc_arena * (1 << 30)));
	tbuf_printf(out, "STAT threads 1\r\n");
	tbuf_printf(out, "END\r\n");

	net_add_iov_dup(&c->out_messages, out->ptr, tbuf_len(out));
}

void
flush_all(va_list ap)
{
	i32 delay = va_arg(ap, u32);;
	if (delay > ev_now())
		fiber_sleep(delay - ev_now());
	u32 slots = [mc_index slots];
	for (u32 i = 0; i < slots; i++) {
		struct tnt_object *obj = [mc_index get:i];
		if (obj != NULL)
			mc_obj(obj)->exptime = 1;
	}
}

static void
memcached_bound_to_primary(int fd)
{
	if (fd < 0) {
		if (!cfg.local_hot_standby)
			panic("unable bind to %s", cfg.primary_addr);
		return;
	}

	if (cfg.local_hot_standby) {
		@try {
			[recovery enable_local_writes];
			set_proc_title("memcached:%s%s pri:%s adm:%s",
				       [recovery status], custom_proc_title,
				       cfg.primary_addr, cfg.admin_addr);
		}
		@catch (Error *e) {
			panic("Recovery failure: %s", e->reason);
		}
	}

#ifndef MEMCACHE_NO_EXPIRE
	if (fd > 0 && fiber_create("memecached_expire", memcached_expire) == NULL)
		panic("can't start the expire fiber");
#endif
}

static void
memcached_handler(va_list ap)
{
	int fd = va_arg(ap, int);
	struct conn *c;
	int r, p;
	int batch_count;

	mc_stats.total_connections++;
	mc_stats.curr_connections++;

	c = conn_init(NULL, fiber->pool, fd, fiber, fiber, MO_SLAB);
	palloc_register_gc_root(fiber->pool, c, conn_gc);

	@try {
		for (;;) {
			batch_count = 0;
			if (conn_recv(c) <= 0)
				return;

		dispatch:
			p = memcached_dispatch(c);
			if (p < 0) {
				say_debug("negative dispatch, closing connection");
				return;
			}

			if (p == 0 && batch_count == 0) /* we havn't successfully parsed any requests */
				continue;

			if (p == 1) {
				batch_count++;
				/* some unparsed commands remain and batch count less than 20 */
				if (tbuf_len(c->rbuf) > 0 && batch_count < 20)
					goto dispatch;
			}

			mc_stats.bytes_written += c->out_messages.bytes;
			r = conn_flush(c);
			if (r < 0) {
				say_debug("flush_output failed, closing connection");
				return;
			}

			fiber_gc();

			if (p == 1 && tbuf_len(c->rbuf) > 0) {
				batch_count = 0;
				goto dispatch;
			}
		}
	}
	@catch (Error *e) {
		say_debug("got error %s", e->reason);
	}
	@finally {
		palloc_unregister_gc_root(fiber->pool, c);
		conn_close(c);
		mc_stats.curr_connections--;
	}
}

static void
memcached_accept(int fd, void *data __attribute__((unused)))
{
	if (fiber_create("memcached/handler", memcached_handler, fd) == NULL) {
		say_error("unable create fiber");
		close(fd);
	}
}

static struct index_node *
dtor(struct tnt_object *obj, struct index_node *node, void *arg __attribute__((unused)))
{
	struct mc_obj *m = mc_obj(obj);
	node->obj = obj;
	node->key.ptr = m->data;
	return node;
}

void
init()
{
	mc_index = [[CStringHash alloc] init];
	mc_index->dtor = dtor;

	recovery = [Recovery alloc];
	recovery = [recovery init_snap_dir:cfg.snap_dir
				   wal_dir:cfg.wal_dir
			      rows_per_wal:cfg.rows_per_wal
			       feeder_addr:cfg.wal_feeder_addr
				     flags:init_storage ? RECOVER_READONLY : 0
				 txn_class:nil];

	if (init_storage)
		return;

	i64 local_lsn = [recovery recover_start];
	if (local_lsn == 0) {
		if (!cfg.wal_feeder_addr) {
			say_error("unable to find initial snapshot");
			say_info("don't you forget to initialize "
				 "storage with --init-storage switch?");
			exit(EX_USAGE);
		}
	}
	if (!cfg.local_hot_standby)
		[recovery enable_local_writes];

	if (fiber_create("memcached/acceptor", tcp_server, cfg.primary_addr,
			 memcached_accept, memcached_bound_to_primary, NULL) == NULL)
	{
		say_error("can't start tcp_server on `%s'", cfg.primary_addr);
		exit(EX_OSERR);
	}

	say_info("memcached initialized");
}

static void
print_row(struct tbuf *out, u16 tag, struct tbuf *op)
{
	switch(tag & TAG_MASK) {
	case STORE: {
		struct mc_obj *m = op->ptr;
		const char *key = m->data;
		tbuf_printf(out, "STORE %.*s %.*s", m->key_len, key, m->value_len, mc_value(m));
		break;
	}

	case DELETE:
		tbuf_printf(out, "DELETE");
		while (tbuf_len(op) > 0) {
			const char *key = op->ptr;
			tbuf_printf(out, " %s", key);
			tbuf_ltrim(op, strlen(key) + 1);
		}
		break;

	case snap_final_tag:
		break;
	default:
		tbuf_printf(out, "++UNKNOWN++");
		return;
	}
}

static int
cat(const char *filename)
{
	read_log(filename, print_row);
	return 0; /* ignore return status of read_log */
}


static struct tnt_module memcached = {
	.name = "memcached",
	.version = memcached_version_string,
	.init = init,
	.cat = cat,
};

register_module(memcached);
register_source();
