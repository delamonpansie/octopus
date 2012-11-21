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
#import <object.h>
#import <net_io.h>
#import <pickle.h>
#import <say.h>

#include <stdio.h>
#include <string.h>

#include <tbuf.h>
#include <iproto.h>
#include <salloc.h>
#import <assoc.h>

#include <mod/box/box.h>
#include <stat.h>


#define STAT(_)					\
        _(MEMC_GET, 1)				\
        _(MEMC_GET_MISS, 2)			\
	_(MEMC_GET_HIT, 3)			\
	_(MEMC_EXPIRED_KEYS, 4)

enum memcached_stat ENUM_INITIALIZER(STAT);
const char *memcached_stat[] = ENUM_STR_INITIALISER(STAT);
int stat_base;


StringHash *memcached_index;

/* memcached tuple format:
   <key, meta, data> */

struct meta {
	u32 exptime;
	u32 flags;
	u64 cas;
} __attribute__((packed));

%%{
	machine memcached;
	write data;
}%%


static u64
natoq(const u8 *start, const u8 *end)
{
	u64 num = 0;
	while (start < end)
		num = num * 10 + (*start++ - '0');
	return num;
}

/* FIXME: "flags bytes" should be passed precooked */
static int
store(void *key, u32 exptime, u32 flags, u32 bytes, u8 *data)
{
	u32 box_flags = 0, cardinality = 4;
	static u64 cas = 42;
	struct meta m;

	struct tbuf *req = tbuf_alloc(fiber->pool);

	tbuf_append(req, &cfg.memcached_object_space, sizeof(u32));
	tbuf_append(req, &box_flags, sizeof(box_flags));
	tbuf_append(req, &cardinality, sizeof(cardinality));

	tbuf_append_field(req, key);

	m.exptime = exptime;
	m.flags = flags;
	m.cas = cas++;
	write_varint32(req, sizeof(m));
	tbuf_append(req, &m, sizeof(m));

	char b[43];
	sprintf(b, " %"PRIu32" %"PRIu32"\r\n", flags, bytes);
	write_varint32(req, strlen(b));
	tbuf_append(req, b, strlen(b));

	write_varint32(req, bytes);
	tbuf_append(req, data, bytes);

	struct box_txn txn;
	@try {
		ev_tstamp start = ev_now(), stop;
		struct iproto r = { .msg_code = INSERT };
		txn_init(&r, &txn, NULL);
		box_prepare_update(&txn, req);
		txn_submit_to_storage(&txn);
		txn_commit(&txn);

		int key_len = LOAD_VARINT32(key);
		say_debug("memcached/store key:(%i)'%.*s' exptime:%"PRIu32" flags:%"PRIu32
			  " cas:%"PRIu64 " bytes:%i",
			  key_len, key_len, (u8 *)key, exptime, flags,
			  cas, bytes);

		stop = ev_now();
		if (stop - start > cfg.too_long_threshold)
			say_warn("too long store: %.3f sec", stop - start);

		return 0;
	}
	@catch (id e) {
		txn_abort(&txn);
		return 1;
	}
	@finally {
		txn_cleanup(&txn);
	}
}

static int
delete(void *key)
{
	u32 key_len = 1;
	struct tbuf *req = tbuf_alloc(fiber->pool);

	tbuf_append(req, &cfg.memcached_object_space, sizeof(u32));
	tbuf_append(req, &key_len, sizeof(key_len));
	tbuf_append_field(req, key);

	struct box_txn txn;
	@try {
		struct iproto r = { .msg_code = DELETE };
		txn_init(&r, &txn, NULL);
		box_prepare_update(&txn, req);
		txn_submit_to_storage(&txn);
		txn_commit(&txn);
		return 0;
	}
	@catch (id e) {
		txn_abort(&txn);
		return 1;
	}
	@finally {
		txn_cleanup(&txn);
	}
}


static struct meta *
obj_meta(struct tnt_object *obj)
{
	struct box_tuple *tuple = box_tuple(obj);
	void *field = tuple_field(tuple, 1);
	return field + 1;
}

static bool
expired(struct tnt_object *obj)
{
#ifdef MEMCACHE_NO_EXPIRE
	(void)obj;
	return 0;
#else
	struct meta *m = obj_meta(obj);
 	return m->exptime == 0 ? 0 : m->exptime < ev_now();
#endif
}

static bool
is_numeric(void *field, u32 value_len)
{
	for (int i = 0; i < value_len; i++)
		if (*((u8 *)field + i) < '0' || '9' < *((u8 *)field + i))
			return false;
	return true;
}

static struct stats {
	u64 total_items;
	u32 curr_connections;
	u32 total_connections;
	u64 cmd_get;
	u64 cmd_set;
	u64 get_hits;
	u64 get_misses;
	u64 evictions;
	u64 bytes_read;
	u64 bytes_written;
} stats;

static void
print_stats(struct netmsg **m)
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
	tbuf_printf(out, "STAT total_items %"PRIu64"\r\n", stats.total_items);
	tbuf_printf(out, "STAT bytes %"PRIu64"\r\n", bytes_used);
	tbuf_printf(out, "STAT curr_connections %"PRIu32"\r\n", stats.curr_connections);
	tbuf_printf(out, "STAT total_connections %"PRIu32"\r\n", stats.total_connections);
	tbuf_printf(out, "STAT connection_structures %"PRIu32"\r\n", stats.curr_connections); /* lie a bit */
	tbuf_printf(out, "STAT cmd_get %"PRIu64"\r\n", stats.cmd_get);
	tbuf_printf(out, "STAT cmd_set %"PRIu64"\r\n", stats.cmd_set);
	tbuf_printf(out, "STAT get_hits %"PRIu64"\r\n", stats.get_hits);
	tbuf_printf(out, "STAT get_misses %"PRIu64"\r\n", stats.get_misses);
	tbuf_printf(out, "STAT evictions %"PRIu64"\r\n", stats.evictions);
	tbuf_printf(out, "STAT bytes_read %"PRIu64"\r\n", stats.bytes_read);
	tbuf_printf(out, "STAT bytes_written %"PRIu64"\r\n", stats.bytes_written);
	tbuf_printf(out, "STAT limit_maxbytes %"PRIu64"\r\n", (u64)(cfg.slab_alloc_arena * (1 << 30)));
	tbuf_printf(out, "STAT threads 1\r\n");
	tbuf_printf(out, "END\r\n");
	net_add_iov(m, out->ptr, tbuf_len(out));
}

static void
flush_all(va_list ap)
{
	i32 delay = va_arg(ap, u32);;
	if (delay > ev_now())
		fiber_sleep(delay - ev_now());
	u32 slots = [memcached_index slots];
	for (u32 i = 0; i < slots; i++) {
		struct tnt_object *obj = [memcached_index get:i];
		if (obj != NULL)
			obj_meta(obj)->exptime = 1;
	}
}


static int __attribute__((noinline))
memcached_dispatch(struct conn *c)
{
	int cs;
	u8 *p, *pe;
	u8 *fstart;
	struct tbuf *keys = tbuf_alloc(fiber->pool);
	void *key;
	bool append, show_cas;
	int incr_sign;
	u64 cas, incr;
	u32 flags, exptime, bytes;
	bool noreply = false;
	u8 *data = NULL;
	bool done = false;
	int r;
	i32 flush_delay = 0;
	size_t keys_count = 0;

	p = c->rbuf->ptr;
	pe = c->rbuf->end;

	say_debug("memcached_dispatch '%.*s'", MIN((int)(pe - p), 40) , p);

#define ADD_IOV_LITERAL(s) ({						\
	if (unlikely(!noreply)) {					\
		struct netmsg *m = netmsg_tail(&c->out_messages);	\
		net_add_iov(&m, (s), sizeof(s) - 1);			\
	}								\
})

#define STORE() ({							\
	stats.cmd_set++;						\
	if (bytes > (1<<20)) {						\
		ADD_IOV_LITERAL("SERVER_ERROR object too large for cache\r\n"); \
	} else {							\
		if (store(key, exptime, flags, bytes, data) == 0) {	\
			stats.total_items++;				\
			ADD_IOV_LITERAL("STORED\r\n");		\
		} else {						\
			ADD_IOV_LITERAL("SERVER_ERROR\r\n");	\
		}							\
	}								\
})

	%%{
		action set {
			key = read_field(keys);
			STORE();
		}

		action add {
			key = read_field(keys);
			struct tnt_object *obj = [memcached_index find:key];
			if (obj != NULL && !ghost(obj) && !expired(obj))
				ADD_IOV_LITERAL("NOT_STORED\r\n");
			else
				STORE();
		}

		action replace {
			key = read_field(keys);
			struct tnt_object *obj = [memcached_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj))
				ADD_IOV_LITERAL("NOT_STORED\r\n");
			else
				STORE();
		}

		action cas {
			key = read_field(keys);
			struct tnt_object *obj = [memcached_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj))
				ADD_IOV_LITERAL("NOT_FOUND\r\n");
			else if (obj_meta(obj)->cas != cas)
				ADD_IOV_LITERAL("EXISTS\r\n");
			else
				STORE();
		}

		action append_prepend {
			struct tbuf *b;
			void *value;
			u32 value_len;

			key = read_field(keys);
			struct tnt_object *obj = [memcached_index find:key];
			if (obj == NULL || ghost(obj)) {
				ADD_IOV_LITERAL("NOT_STORED\r\n");
			} else {
				struct box_tuple *tuple = box_tuple(obj);
				value = tuple_field(tuple, 3);
				value_len = LOAD_VARINT32(value);
				b = tbuf_alloc(fiber->pool);
				if (append) {
					tbuf_append(b, value, value_len);
					tbuf_append(b, data, bytes);
				} else {
					tbuf_append(b, data, bytes);
					tbuf_append(b, value, value_len);
				}

				bytes += value_len;
				data = b->ptr;
				STORE();
			}
		}

		action incr_decr {
			struct meta *meta;
			struct tbuf *b;
			void *field;
			u32 value_len;
			u64 value;

			key = read_field(keys);
			struct tnt_object *obj = [memcached_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj)) {
				ADD_IOV_LITERAL("NOT_FOUND\r\n");
			} else {
				struct box_tuple *tuple = box_tuple(obj);
				meta = obj_meta(obj);
				field = tuple_field(tuple, 3);
				value_len = LOAD_VARINT32(field);

				if (is_numeric(field, value_len)) {
					value = natoq(field, field + value_len);

					if (incr_sign > 0) {
						value += incr;
					} else {
						if (incr > value)
							value = 0;
						else
							value -= incr;
					}

					exptime = meta->exptime;
					flags = meta->flags;

					b = tbuf_alloc(fiber->pool);
					tbuf_printf(b, "%"PRIu64, value);
					data = b->ptr;
					bytes = tbuf_len(b);

					stats.cmd_set++;
					if (store(key, exptime, flags, bytes, data) == 0) {
						stats.total_items++;
						if (!noreply) {
							struct netmsg *m = netmsg_tail(&c->out_messages);
							net_add_iov(&m, b->ptr, tbuf_len(b));
							net_add_iov(&m, "\r\n", 2);
						}
					} else {
						ADD_IOV_LITERAL("SERVER_ERROR\r\n");
					}
				} else {
					ADD_IOV_LITERAL("CLIENT_ERROR cannot increment or decrement"
							" non-numeric value\r\n");
				}
			}

		}

		action delete {
			key = read_field(keys);
			struct tnt_object *obj = [memcached_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj)) {
				ADD_IOV_LITERAL("NOT_FOUND\r\n");
			} else {
				if (delete(key) == 0) {
					ADD_IOV_LITERAL("DELETED\r\n");
				} else {
					ADD_IOV_LITERAL("SERVER_ERROR\r\n");
				}
			}
		}

		action get {
			stat_collect(stat_base, MEMC_GET, 1);
			stats.cmd_get++;
			struct netmsg *m = netmsg_tail(&c->out_messages);
			while (keys_count-- > 0) {
				struct tnt_object *obj;
				struct box_tuple *tuple;
				struct meta *meta;
				void *field;
				void *value;
				void *suffix;
				u32 key_len;
				u32 value_len;
				u32 suffix_len;
				u32 _l;

				key = read_field(keys);
				obj = [memcached_index find:key];
				key_len = LOAD_VARINT32(key);

				if (obj == NULL || ghost(obj)) {
					stat_collect(stat_base, MEMC_GET_MISS, 1);
					stats.get_misses++;
					continue;
				}

				meta = obj_meta(obj);
				tuple = box_tuple(obj);
				field = tuple->data;

				/* skip key */
				_l = LOAD_VARINT32(field);
				field += _l;

				/* metainfo */
				_l = LOAD_VARINT32(field);
				meta = field;
				field += _l;

				/* suffix */
				suffix_len = LOAD_VARINT32(field);
				suffix = field;
				field += suffix_len;

				/* value */
				value_len = LOAD_VARINT32(field);
				value = field;

				if (meta->exptime > 0 && meta->exptime < ev_now()) {
					stats.get_misses++;
					stat_collect(stat_base, MEMC_GET_MISS, 1);
					continue;
				} else {
					stats.get_hits++;
					stat_collect(stat_base, MEMC_GET_HIT, 1);
				}

				if (show_cas) {
					struct tbuf *b = tbuf_alloc(fiber->pool);
					tbuf_printf(b, "VALUE %.*s %"PRIu32" %"PRIu32" %"PRIu64"\r\n",
						    (int)key_len, (u8 *)key, meta->flags, value_len, meta->cas);
					net_add_iov(&m, b->ptr, tbuf_len(b));
					stats.bytes_written += tbuf_len(b);
				} else {
					net_add_iov(&m, "VALUE ", 6);
					net_add_iov(&m, key, key_len);
					net_add_iov(&m, suffix, suffix_len);
				}
				net_add_ref_iov(&m, obj, value, value_len);
				net_add_iov(&m, "\r\n", 2);
				stats.bytes_written += value_len + 2;
			}
			net_add_iov(&m, "END\r\n", 5);
			stats.bytes_written += 5;
		}

		action flush_all {
			fiber_create("flush_all", flush_all, flush_delay);
			ADD_IOV_LITERAL("OK\r\n");
		}

		action stats {
			struct netmsg *m = netmsg_tail(&c->out_messages);
			print_stats(&m);
		}

		action quit {
			return 0;
		}

		action fstart { fstart = p; }
		action key_start {
			fstart = p;
			for (; p < pe && *p != ' ' && *p != '\r' && *p != '\n'; p++);
			if ( *p == ' ' || *p == '\r' || *p == '\n') {
				write_varint32(keys, p - fstart);
				tbuf_append(keys, fstart, p - fstart);
				keys_count++;
				p--;
			} else
				p = fstart;
 		}


		printable = [^ \t\r\n];
		key = printable >key_start ;

		action exptime {
			exptime = natoq(fstart, p);
			if (exptime > 0 && exptime <= 60*60*24*30)
				exptime = exptime + ev_now();
		}
		exptime = digit+ >fstart %exptime;

		flags = digit+ >fstart %{flags = natoq(fstart, p);};
		bytes = digit+ >fstart %{bytes = natoq(fstart, p);};
		cas_value = digit+ >fstart %{cas = natoq(fstart, p);};
		incr_value = digit+ >fstart %{incr = natoq(fstart, p);};
		flush_delay = digit+ >fstart %{flush_delay = natoq(fstart, p);};

		action read_data {
			size_t parsed = p - (u8 *)c->rbuf->ptr;
			while (tbuf_len(c->rbuf) - parsed < bytes + 2) {
				if ((r = conn_recv(c)) <= 0) {
					say_debug("read returned %i, closing connection", r);
					return -1;
				}
			}

			p = c->rbuf->ptr + parsed;
			pe = c->rbuf->end;

			data = p;

			if (strncmp((char *)(p + bytes), "\r\n", 2) == 0) {
				p += bytes + 2;
			} else {
				goto exit;
			}
		}

		action done {
			done = true;
			stats.bytes_read += p - (u8 *)c->rbuf->ptr;
			tbuf_ltrim(c->rbuf, p - (u8 *)c->rbuf->ptr);
		}

		eol = ("\r\n" | "\n") @{ p++; };
		spc = " "+;
		noreply = (spc "noreply"i %{ noreply = true; })?;
		store_command_body = spc key spc flags spc exptime spc bytes noreply eol;

		set = ("set"i store_command_body) @read_data @done @set;
		add = ("add"i store_command_body) @read_data @done @add;
		replace = ("replace"i store_command_body) @read_data @done @replace;
		append  = ("append"i  %{append = true; } store_command_body) @read_data @done @append_prepend;
		prepend = ("prepend"i %{append = false;} store_command_body) @read_data @done @append_prepend;
		cas = ("cas"i spc key spc flags spc exptime spc bytes spc cas_value noreply spc?) eol @read_data @done @cas;


		get = "get"i %{show_cas = false;} spc key (spc key)* spc? eol @done @get;
		gets = "gets"i %{show_cas = true;} spc key (spc key)* spc? eol @done @get;
		delete = "delete"i spc key (spc exptime)? noreply spc? eol @done @delete;
		incr = "incr"i %{incr_sign = 1; } spc key spc incr_value noreply spc? eol @done @incr_decr;
		decr = "decr"i %{incr_sign = -1;} spc key spc incr_value noreply spc? eol @done @incr_decr;

		stats = "stats"i eol @done @stats;
		flush_all = "flush_all"i (spc flush_delay)? noreply spc? eol @done @flush_all;
		quit = "quit"i eol @done @quit;

	        main := set | cas | add | replace | append | prepend |
			get | gets | delete | incr | decr | stats | flush_all | quit;
		write init;
		write exec;
	}%%

	if (!done) {
		say_debug("parse failed after: `%.*s'", (int)(pe - p), p);
		if (pe - p > (1 << 20)) {
		exit:
			say_warn("memcached proto error");
			ADD_IOV_LITERAL("ERROR\r\n");
			stats.bytes_written += 7;
			return -1;
		}
		char *r;
		if ((r = memmem(p, pe - p, "\r\n", 2)) != NULL) {
			tbuf_ltrim(c->rbuf, r + 2 - (char *)c->rbuf->ptr);
			ADD_IOV_LITERAL("CLIENT_ERROR bad command line format\r\n");
			return 1;
		}
		return 0;
	}

	return 1;
}

#ifndef MEMCACHE_NO_EXPIRE
static void
memcached_expire(va_list va __attribute__((unused)))
{
	u32 i = 0;
	say_info("memcached expire fiber started");
	for (;;) {
		if (i >= [memcached_index slots])
			i = 0;

		struct tbuf *keys_to_delete = tbuf_alloc(fiber->pool);
		int expired_keys = 0;

		for (int j = 0; j < cfg.memcached_expire_per_loop; j++, i++) {
			struct tnt_object *obj = [memcached_index get:i];
			if (obj == NULL)
				continue;

			if (!expired(obj))
				continue;

			say_debug("expire tuple %p", obj);
			tbuf_append_field(keys_to_delete, box_tuple(obj)->data);
		}

		while (tbuf_len(keys_to_delete) > 0) {
			delete(read_field(keys_to_delete));
			expired_keys++;
		}
		stat_collect(stat_base, MEMC_EXPIRED_KEYS, expired_keys);

		fiber_gc();

		double delay = (double)cfg.memcached_expire_per_loop *
				       cfg.memcached_expire_full_sweep /
			       ([memcached_index slots] + 1);
		if (delay > 1)
			delay = 1;
		fiber_sleep(delay);
	}
}
#endif

static void
memcached_bound_to_primary(int fd)
{
	box_bound_to_primary(fd);
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
	stats.total_connections++;
	stats.curr_connections++;
	int r, p;
	int batch_count;

	c = conn_init(NULL, fiber->pool, fd, fiber, fiber, 0);
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

			r = conn_flush(c);
			if (r < 0) {
				say_debug("flush_output failed, closing connection");
				return;
			}

			stats.bytes_written += r;
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
		stats.curr_connections--;
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

void
memcached_init()
{
	stat_base = stat_register(memcached_stat, nelem(memcached_stat));
	fiber_create("memcached/acceptor", tcp_server,
		     cfg.primary_port, memcached_accept, memcached_bound_to_primary, NULL);

	say_info("memcached initialized");

	int n = cfg.memcached_object_space > 0 ? cfg.memcached_object_space : 23;
	memcached_index = (StringHash *)object_space_registry[n].index[0];
}

register_source();

/*
 * Local Variables:
 * mode: c
 * End:
 */
