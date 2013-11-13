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
#import <net_io.h>
#import <say.h>
#import <tbuf.h>

#include <string.h>

#import <mod/memcached/store.h>

%%{
	machine memcached;
	write data;
}%%


static u64
natoq(const char *start, const char *end)
{
	u64 num = 0;
	while (start < end)
		num = num * 10 + (*start++ - '0');
	return num;
}


static bool
is_numeric(const char *field, u32 value_len)
{
	for (int i = 0; i < value_len; i++)
		if (field[i] < '0' || '9' < field[i])
			return false;
	return true;
}


static char *
next_key(char **k)
{
	char *r = *k, *p, *s;
	if (unlikely(!r))
		return NULL;
	for (p = r; *p != ' ' && *p != '\r' && *p != '\n'; p++);
	s = p;
	while (*p == ' ') p++;
	if (*p != '\r' && *p != '\n')
		*k = p;
	else
		*k = NULL;
	*s = 0;
	return r;
}

static const char *
quote(const char *p, int len)
{
	const int qlen = 40;
	static char buf[40 * 2 + 3 + 1]; /* qlen * 2 + '...' + \0 */
	char *b = buf;
	for (int i = 0; i < MIN(len, qlen); i++) {
		if (' ' <= p[i] && p[i] <= 'z') {
			*b++ = p[i];
			continue;
		}
		if (p[i] == '\r') {
			*b++ = '\\';
			*b++ = 'r';
			continue;
		}
		if (p[i] == '\n') {
			*b++ = '\\';
			*b++ = 'n';
			continue;
		}
		*b++ = '?';
	}
	if (len > qlen) {
		*b++ = '.'; *b++ = '.'; *b++ = '.';
	}
	*b = 0;
	return buf;
}

int __attribute__((noinline))
memcached_dispatch(struct conn *c)
{
	int cs;
	char *p, *pe;
	char *fstart, *kstart;
	bool append, show_cas;
	int incr_sign;
	u64 cas, incr;
	u32 flags, exptime, bytes;
	bool noreply = false;
	char *data = NULL;
	bool done = false;
	i32 flush_delay = 0;

	p = c->rbuf->ptr;
	pe = c->rbuf->end;

	say_debug("memcached_dispatch '%s'", quote(p, (int)(pe - p)));

#define ADD_IOV_LITERAL(s) ({						\
	if (unlikely(!noreply))						\
		net_add_iov(&c->out_messages, (s), sizeof(s) - 1);	\
})

#define STORE() ({							\
	mc_stats.cmd_set++;						\
	if (bytes > (1<<20)) {						\
		ADD_IOV_LITERAL("SERVER_ERROR object too large for cache\r\n"); \
	} else {							\
		if (store(key, exptime, flags, bytes, data)) {	\
			mc_stats.total_items++;				\
			ADD_IOV_LITERAL("STORED\r\n");		\
		} else {						\
			ADD_IOV_LITERAL("SERVER_ERROR\r\n");	\
		}							\
	}								\
})

	%%{
		action set {
			char *key = next_key(&kstart);
			STORE();
		}

		action add {
			const char *key = next_key(&kstart);
			struct tnt_object *obj = [mc_index find:key];
			if (obj != NULL && !ghost(obj) && !expired(obj))
				ADD_IOV_LITERAL("NOT_STORED\r\n");
			else
				STORE();
		}

		action replace {
			char *key = next_key(&kstart);
			struct tnt_object *obj = [mc_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj))
				ADD_IOV_LITERAL("NOT_STORED\r\n");
			else
				STORE();
		}

		action cas {
			char *key = next_key(&kstart);
			struct tnt_object *obj = [mc_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj))
				ADD_IOV_LITERAL("NOT_FOUND\r\n");
			else if (mc_obj(obj)->cas != cas)
				ADD_IOV_LITERAL("EXISTS\r\n");
			else
				STORE();
		}

		action append_prepend {
			char *key = next_key(&kstart);
			struct tnt_object *obj = [mc_index find:key];
			if (obj == NULL || ghost(obj)) {
				ADD_IOV_LITERAL("NOT_STORED\r\n");
			} else {
				struct mc_obj *m = mc_obj(obj);
				struct tbuf *b = tbuf_alloc(fiber->pool);
				if (append) {
					tbuf_append(b, mc_value(m), m->value_len);
					tbuf_append(b, data, bytes);
				} else {
					tbuf_append(b, data, bytes);
					tbuf_append(b, mc_value(m), m->value_len);
				}

				bytes += m->value_len;
				data = b->ptr;
				STORE();
			}
		}

		action incr_decr {
			char *key = next_key(&kstart);
			struct tnt_object *obj = [mc_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj)) {
				ADD_IOV_LITERAL("NOT_FOUND\r\n");
			} else {
				struct mc_obj *m = mc_obj(obj);

				if (is_numeric(mc_value(m), m->value_len)) {
					u64 value = natoq(mc_value(m), mc_value(m) + m->value_len);

					if (incr_sign > 0) {
						value += incr;
					} else {
						if (incr > value)
							value = 0;
						else
							value -= incr;
					}

					exptime = m->exptime;
					flags = m->flags;

					struct tbuf *b = tbuf_alloc(fiber->pool);
					tbuf_printf(b, "%"PRIu64, value);
					data = b->ptr;
					bytes = tbuf_len(b);

					mc_stats.cmd_set++;
					if (store(key, exptime, flags, bytes, data)) {
						mc_stats.total_items++;
						if (!noreply) {
							struct netmsg_head *h = &c->out_messages;
							net_add_iov(h, b->ptr, tbuf_len(b));
							ADD_IOV_LITERAL("\r\n");
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
			char *key = next_key(&kstart);
			struct tnt_object *obj = [mc_index find:key];
			if (obj == NULL || ghost(obj) || expired(obj)) {
				ADD_IOV_LITERAL("NOT_FOUND\r\n");
			} else {
				if (delete(&key, 1)) {
					ADD_IOV_LITERAL("DELETED\r\n");
				} else {
					ADD_IOV_LITERAL("SERVER_ERROR\r\n");
				}
			}
		}

		action get {
			mc_stats.cmd_get++;
			struct netmsg_head *h = &c->out_messages;
			char *key;
			while ((key = next_key(&kstart))) {
				struct tnt_object *obj = [mc_index find:key];

				if (obj == NULL || ghost(obj) || expired(obj)) {
					mc_stats.get_misses++;
					continue;
				}
				mc_stats.get_hits++;

				struct mc_obj *m = mc_obj(obj);
				const char *suffix = m->data + m->key_len;
				const char *value = mc_value(m);

				if (show_cas) {
					struct tbuf *b = tbuf_alloc(fiber->pool);
					tbuf_printf(b, "VALUE %s %"PRIu32" %"PRIu32" %"PRIu64"\r\n",
						    key, m->flags, m->value_len, m->cas);
					net_add_iov(h, b->ptr, tbuf_len(b));
					mc_stats.bytes_written += tbuf_len(b);
				} else {
					ADD_IOV_LITERAL("VALUE ");
					net_add_iov(h, key, strlen(key));
					net_add_iov(h, suffix, m->suffix_len);
				}
				net_add_obj_iov(h, obj, value, m->value_len);
				ADD_IOV_LITERAL("\r\n");
				mc_stats.bytes_written += m->value_len + 2;
			}
			ADD_IOV_LITERAL("END\r\n");
			mc_stats.bytes_written += 5;
		}

		action flush_all {
			fiber_create("flush_all", flush_all, flush_delay);
			ADD_IOV_LITERAL("OK\r\n");
		}

		action stats {
			print_stats(c);
		}

		action quit {
			return 0;
		}

		action fstart { fstart = p; }
		action key_start {
			kstart = p;
			for (; p < pe && *p != ' ' && *p != '\r' && *p != '\n'; p++);
			if (*p == ' ' || *p == '\r' || *p == '\n')
				p--;
			else
				p = kstart;
		}
		action keys_start {
			kstart = p;
			for (; p < pe && *p != '\r' && *p != '\n'; p++);
			if (*p == '\r' || *p == '\n')
				p--;
			else
				p = kstart;
		}

		printable = [^ \t\r\n];
		key = printable >key_start ;
		keys = printable >keys_start ;

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
			size_t parsed = p - (char *)c->rbuf->ptr;
			while (tbuf_len(c->rbuf) - parsed < bytes + 2) {
				int r = conn_recv(c);
				if (r <= 0) {
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
			mc_stats.bytes_read += p - (char *)c->rbuf->ptr;
			tbuf_ltrim(c->rbuf, p - (char *)c->rbuf->ptr);
		}

		eol = "\r\n" @{ p++; };
		spc = " "+;
		noreply = (spc "noreply"i %{ noreply = true; })?;
		store_command_body = spc key spc flags spc exptime spc bytes noreply eol;

		set = ("set"i store_command_body) @read_data @done @set;
		add = ("add"i store_command_body) @read_data @done @add;
		replace = ("replace"i store_command_body) @read_data @done @replace;
		append  = ("append"i  %{append = true; } store_command_body) @read_data @done @append_prepend;
		prepend = ("prepend"i %{append = false;} store_command_body) @read_data @done @append_prepend;
		cas = ("cas"i spc key spc flags spc exptime spc bytes spc cas_value noreply spc?) eol @read_data @done @cas;


		get = "get"i %{show_cas = false;} spc keys spc? eol @done @get;
		gets = "gets"i %{show_cas = true;} spc keys spc? eol @done @get;
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
		say_debug("parse failed at: `%s'", quote(p, (int)(pe - p)));
		if (pe - p > (1 << 20)) {
		exit:
			say_warn("memcached proto error");
			ADD_IOV_LITERAL("ERROR\r\n");
			mc_stats.bytes_written += 7;
			return -1;
		}
		char *r;
		if ((r = memmem(p, pe - p, "\r\n", 2)) != NULL) {
			tbuf_ltrim(c->rbuf, r + 2 - (char *)c->rbuf->ptr);
			while (tbuf_len(c->rbuf) >= 2 && memcmp(c->rbuf->ptr, "\r\n", 2) == 0)
				tbuf_ltrim(c->rbuf, 2);
			ADD_IOV_LITERAL("CLIENT_ERROR bad command line format\r\n");
			return 1;
		}
		return 0;
	}

	return 1;
}


register_source();

/*
 * Local Variables:
 * mode: c
 * End:
 */
