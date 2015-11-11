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
#import <log_io.h>
#import <index.h>
#import <say.h>
#include <stat.h>
#include <salloc.h>
#import <pickle.h>

#include <sysexits.h>

#import <mod/memcached/memcached_version.h>
#import <mod/memcached/store.h>

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

static struct index_node *
dtor(struct tnt_object *obj, struct index_node *node, void *arg __attribute__((unused)))
{
	struct mc_obj *m = mc_obj(obj);
	node->obj = obj;
	node->key.ptr = m->data;
	return node;
}

@implementation Memcached
- (id)
init_shard:(Shard<Shard> *)shard_
{
	[super init];
	shard = shard_;
	mc_index = [[CStringHash alloc] init:NULL];
	mc_index->dtor = dtor;

	return self;
}

- (void)
set_shard:(Shard<Shard> *)shard_
{
	shard = shard_;
}

static void
store_compat(CStringHash *mc_index, struct tbuf *op)
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
	case wal_data: {
		u16 code = read_u16(op);
		read_u32(op); /* obj_space */
		switch(code) {
		case 13: {
			read_u32(op); /* flags */
			read_u32(op); /* cardinality */
			store_compat(mc_index, op);
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
	case snap_data:
		read_u32(op); /* obj_space */
		read_u32(op); /* cardinality */
		read_u32(op);  /* data_size */
		store_compat(mc_index, op);
		break;

	default:
		break;
	}
}

static void memcached_expire(va_list va __attribute__((unused)));

- (void)
wal_final_row
{
}

- (void)
status_changed
{
	if (cfg.memcached_no_expire)
		return;

	if (shard->status == PRIMARY) {
		fiber_create("memecached_expire", memcached_expire);
		return;
	}
	if (shard->prev_status ==  PRIMARY)
		panic("can't downgrade from primary");
}

- (int)
snapshot_write_rows: (XLog *)l
{
	u32			i = 0;
	struct tnt_object	*obj;

	title("dumper of pid %" PRIu32 ": dumping actions", getppid());

	[mc_index iterator_init];

	while((obj = [mc_index iterator_next]) != NULL) {
		struct mc_obj *m = mc_obj(obj);
		if ([l append_row:m len:mc_len(m)
			    shard:nil tag:STORE|TAG_SNAP] == NULL)
			return -1;

		if ((++i)% 100000 == 0) {
			say_info("%.1fM rows written", i / 1000000. );
			title("dumper of pid %" PRIu32 ": dumping actions (%.1fM  rows )",
			      getppid(), i / 1000000.);
		}

		if (i % 10000 == 0)
			[l confirm_write];
	}

	say_info("Snapshot finished");
	return 0;
}

- (u32)
snapshot_estimate
{
	return [mc_index size];
}

static void mc_print_row(struct tbuf *out, u16 tag, struct tbuf *op);
- (void)
print:(const struct row_v12 *)row into:(struct tbuf *)buf
{
	print_row(buf, row, mc_print_row);
}

@end

int
store(Memcached *memc, const char *key, u32 exptime, u32 flags, u32 value_len, char *value)
{
	struct tnt_object *old_obj = NULL, *obj = NULL;
	CStringHash *mc_index = memc->mc_index;
	if ([memc->shard is_replica])
		return 0;

	@try {
		obj = mc_alloc(key, exptime, flags, value_len, value);
		struct mc_obj *m = mc_obj(obj);

		old_obj = [mc_index find_obj:obj];
		if (old_obj) {
			object_lock(old_obj); /* may throw exception */
		} else {
			object_lock(obj);
			obj->flags |= GHOST;
			[mc_index replace:obj];
		}

		if ([memc->shard submit:m len:mc_len(m) tag:STORE|TAG_WAL] == 1) {
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
		[e release];
		return 0;
	}
	@finally {
		if (obj)
			sfree(obj);
	}
}

int
delete(Memcached *memc, char **keys, int n)
{
	CStringHash *mc_index = memc->mc_index;
	struct tnt_object **obj = palloc(fiber->pool, sizeof(*obj) * n);
	int k = 0;
	int ret = 0;

	if ([memc->shard is_replica])
		return 0;

	for (int i = 0; i < n; i++) {
		obj[k] = [mc_index find:*keys++];
		if (obj[k] == NULL)
			continue;
		@try {
			object_lock(obj[k]);
		}
		@catch (Error* e) {
			[e release];
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

	if ([memc->shard submit:b->ptr len:tbuf_len(b) tag:DELETE|TAG_WAL] == 1) {
		for (int i = 0; i < k; i++) {
			[mc_index remove:obj[i]];
			object_unlock(obj[i]);
			object_decr_ref(obj[i]);
		}
		ret += k;
	}
	return ret;
}

static void
memcached_expire(va_list ap)
{
	Recovery *recovery = va_arg(ap, Recovery *);
	Shard<Shard> *shard;
	Memcached *memc;
	CStringHash *mc_index;
	u32 i = 0;

	if (cfg.memcached_no_expire)
		return;

	say_info("memcached expire fiber started");
	char **keys = malloc(cfg.memcached_expire_per_loop * sizeof(void *));

	for (;;) {
		shard = [recovery shard:0];
		memc = [shard executor];
		mc_index = memc->mc_index;

		double delay = (double)cfg.memcached_expire_per_loop *
				       cfg.memcached_expire_full_sweep /
			       ([mc_index slots] + 1);
		if (delay > 1)
			delay = 1;
		fiber_sleep(delay);

		say_debug("expire loop");
		if ([shard is_replica])
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

		delete(memc, keys, k);
		say_debug("expired %i keys", k);

		fiber_gc();
	}
}

void
print_stats(struct netmsg_head *wbuf)
{
	u64 bytes_used, items;
	struct tbuf *out = tbuf_alloc(wbuf->pool);
	slab_total_stat(&bytes_used, &items);

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

	net_add_iov(wbuf, out->ptr, tbuf_len(out));
}

void
flush_all(va_list ap)
{
	Memcached *memc = va_arg(ap, Memcached *);
	CStringHash *mc_index = memc->mc_index;
	i32 delay = va_arg(ap, u32);
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
memcached_handler(va_list ap)
{
	int fd = va_arg(ap, int);
	Memcached *memc = va_arg(ap, Memcached *);
	int r, p;
	int batch_count;

	mc_stats.total_connections++;
	mc_stats.curr_connections++;

	struct tbuf rbuf = TBUF(NULL, 0, fiber->pool);
	struct netmsg_head wbuf;
	netmsg_head_init(&wbuf, fiber->pool);
	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);
	palloc_register_gc_root(fiber->pool, &wbuf, netmsg_head_gc);

	@try {
		for (;;) {
			batch_count = 0;
			if (fiber_recv(fd, &rbuf) <= 0)
				return;

		dispatch:
			p = memcached_dispatch(memc, fd, &rbuf, &wbuf);
			if (p < 0) {
				say_debug("negative dispatch, closing connection");
				return;
			}

			if (p == 0 && batch_count == 0) /* we havn't successfully parsed any requests */
				continue;

			if (p == 1) {
				batch_count++;
				/* some unparsed commands remain and batch count less than 20 */
				if (tbuf_len(&rbuf) > 0 && batch_count < 20)
					goto dispatch;
			}

			mc_stats.bytes_written += wbuf.bytes;
			r = fiber_writev(fd, &wbuf);
			if (r < 0) {
				say_debug("flush_output failed, closing connection");
				return;
			}

			fiber_gc();

			if (p == 1 && tbuf_len(&rbuf) > 0) {
				batch_count = 0;
				goto dispatch;
			}
		}
	}
	@catch (Error *e) {
		say_debug("got error %s", e->reason);
		[e release];
	}
	@finally {
		palloc_unregister_gc_root(fiber->pool, &rbuf);
		palloc_unregister_gc_root(fiber->pool, &wbuf);
		close(fd);
		mc_stats.curr_connections--;
	}
}

static void
memcached_accept(int fd, void *data)
{
	fiber_create("memcached/handler", memcached_handler, fd, data);
}

static void
init_second_stage(va_list ap)
{
	Recovery *recovery = va_arg(ap, Recovery *);
	[recovery simple];
	Memcached *memc = [[recovery shard:0] executor];

	if (fiber_create("memcached/acceptor", tcp_server, cfg.primary_addr,
			 memcached_accept, NULL, memc) == NULL)
	{
		say_error("can't start tcp_server on `%s'", cfg.primary_addr);
		exit(EX_OSERR);
	}

	say_info("memcached initialized");
}
void
memcached_init()
{
	struct feeder_param feeder;
	enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg(&feeder, NULL);
	if (fid_err) panic("wrong feeder conf");

	extern Recovery *recovery;
	recovery = [[Recovery alloc] init];
	recovery->default_exec_class = [Memcached class];

	if (init_storage)
		return;

	fiber_create("memecached_expire", memcached_expire, recovery);
	/* fiber is required to successfully pull from remote */
	fiber_create("memcached_init", init_second_stage, recovery);
}

static void
mc_print_row(struct tbuf *out, u16 tag, struct tbuf *op)
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

	case snap_final:
		break;
	default:
		tbuf_printf(out, "++UNKNOWN++");
		return;
	}
}

static int
memcached_cat(const char *filename)
{
	read_log(filename, mc_print_row);
	return 0; /* ignore return status of read_log */
}


static struct tnt_module memcached = {
	.name = "memcached",
	.version = memcached_version_string,
	.init = memcached_init,
	.cat = memcached_cat,
};

register_module(memcached);
register_source();
