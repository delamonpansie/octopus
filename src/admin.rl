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

#import <config.h>
#import <fiber.h>
#import <palloc.h>
#import <salloc.h>
#import <say.h>
#import <stat.h>
#import <tarantool.h>
#import <tbuf.h>
#import <util.h>
#import <net_io.h>

#include <third_party/luajit/src/lua.h>

#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>

static const char help[] =
	"available commands:" CRLF
	" - help" CRLF
	" - exit" CRLF
	" - show info" CRLF
	" - show fiber" CRLF
	" - show configuration" CRLF
	" - show slab" CRLF
	" - show palloc" CRLF
	" - show stat" CRLF
	" - save coredump" CRLF
	" - enable coredump" CRLF
	" - save snapshot" CRLF
	" - exec mod <command>" CRLF
	" - exec lua <code>" CRLF
	" - incr log_level" CRLF
	" - decr log_level" CRLF
	" - reload configuration" CRLF;


static const char unknown_command[] = "unknown command. try typing help." CRLF;

%%{
	machine admin;
	write data;
}%%


static void
end(struct tbuf *out)
{
	tbuf_printf(out, "..." CRLF);
}

static void
start(struct tbuf *out)
{
	tbuf_printf(out, "---" CRLF);
}

static void
ok(struct tbuf *out)
{
	start(out);
	tbuf_printf(out, "ok" CRLF);
	end(out);
}

static void
fail(struct tbuf *out, struct tbuf *err)
{
	start(out);
	tbuf_printf(out, "fail:%.*s" CRLF, (int)tbuf_len(err), (char *)err->data);
	end(out);
}

static const char *
tbuf_reader(lua_State *L __attribute__((unused)), void *data, size_t *size)
{
	struct tbuf *code = data;
	*size = tbuf_len(code);
	return tbuf_peek(code, tbuf_len(code));
}

void
exec_lua(lua_State *L, struct tbuf *code, struct tbuf *out)
{
	int r = lua_load(L, tbuf_reader, code, "network_input");
	if (r != 0) {
		if (r == LUA_ERRSYNTAX)
			tbuf_printf(out, "error: syntax");
		if (r == LUA_ERRMEM)
			tbuf_printf(out, "error: memory");
		return;
	}

	if (lua_pcall(L, 0, 0, 0)) {
		tbuf_printf(out, "error: pcall");
		return;
	}
}

static int
admin_dispatch(struct conn *c)
{
	struct tbuf *out = tbuf_alloc(fiber->pool);
	struct tbuf *err = tbuf_alloc(fiber->pool);
	int cs;
	char *p, *pe;
	char *strstart, *strend;
	while ((pe = memchr(c->rbuf->data, '\n', tbuf_len(c->rbuf))) == NULL) {
		if (conn_readahead(c, 1) <= 0)
			return 0;
	}

	pe++;
	p = c->rbuf->data;

	%%{
		action show_configuration {
			tarantool_cfg_iterator_t *i;
			char *key, *value;

			start(out);
			tbuf_printf(out, "configuration:" CRLF);
			i = tarantool_cfg_iterator_init();
			while ((key = tarantool_cfg_iterator_next(i, &cfg, &value)) != NULL) {
				if (value) {
					tbuf_printf(out, "  %s: \"%s\"" CRLF, key, value);
					free(value);
				} else {
					tbuf_printf(out, "  %s: (null)" CRLF, key);
				}
			}
			end(out);
		}

		action show_info {
			start(out);
			if (module(NULL)->info != NULL)
				module(NULL)->info(out);
			end(out);
		}

		action help {
			start(out);
			tbuf_append(out, help, sizeof(help) - 1);
			end(out);
		}

		action show_stat {
			start(out);
			stat_print(fiber->L, out);
			end(out);
		}

		action lua_exec {
			struct tbuf *code = tbuf_alloc(fiber->pool);
			tbuf_append(code, strstart, strend - strstart);

			start(out);
			exec_lua(fiber->L, code, out);
			end(out);
		}

		action mod_exec {
			start(out);
			if (module(NULL)->exec != NULL)
				module(NULL)->exec(strstart, strend - strstart, out);
			else
				tbuf_printf(out, "unimplemented" CRLF);
			end(out);
		}

		action reload_configuration {
			if (reload_cfg(err))
				fail(out, err);
			else
				ok(out);
		}

		action save_snapshot {
			int ret = save_snapshot(NULL, 0);

			if (ret == 0)
				ok(out);
			else {
				tbuf_printf(err, " can't save snapshot, errno %d (%s)",
					    ret, strerror(ret));

				fail(out, err);
			}
		}

		eol = "\n" | "\r\n";
		show = "sh"("o"("w")?)?;
		info = "in"("f"("o")?)?;
		check = "ch"("e"("c"("k")?)?)?;
		configuration = "co"("n"("f"("i"("g"("u"("r"("a"("t"("i"("o"("n")?)?)?)?)?)?)?)?)?)?)?;
		fiber = "fi"("b"("e"("r")?)?)?;
		slab = "sl"("a"("b")?)?;
		mod = "mo"("d")?;
		lua = "lu"("a")?;
		palloc = "pa"("l"("l"("o"("c")?)?)?)?;
		stat = "st"("a"("t")?)?;
		help = "h"("e"("l"("p")?)?)?;
		exit = "e"("x"("i"("t")?)?)? | "q"("u"("i"("t")?)?)?;
		save = "sa"("v"("e")?)?;
		enable = "en"("a"("b"("l"("e")?)?)?)?;
		coredump = "co"("r"("e"("d"("u"("m"("p")?)?)?)?)?)?;
		snapshot = "sn"("a"("p"("s"("h"("o"("t")?)?)?)?)?)?;
		exec = "ex"("e"("c")?)?;
		string = [^\r\n]+ >{strstart = p;}  %{strend = p;};
		reload = "re"("l"("o"("a"("d")?)?)?)?;
		incr = "inc"("r")?;
		decr = "dec"("r")?;
		log_level = "log"("_"("l"("e"("v"("e"("l")?)?)?)?)?)?;

		commands = (help			%help						|
			    exit			%{return 0;}					|
			    show " "+ info		%show_info					|
			    show " "+ fiber		%{start(out); fiber_info(out); end(out);}	|
			    show " "+ configuration 	%show_configuration				|
			    show " "+ slab		%{start(out); slab_stat(out); end(out);}	|
			    show " "+ palloc		%{start(out); palloc_stat(out); end(out);}	|
			    show " "+ stat		%show_stat					|
			    enable " "+ coredump        %{maximize_core_rlimit(); ok(out);}		|
			    save " "+ coredump		%{coredump(60); ok(out);}			|
			    save " "+ snapshot		%save_snapshot					|
			    incr " "+ log_level         %{cfg.log_level++; ok(out);}			|
			    decr " "+ log_level         %{cfg.log_level--; ok(out);}			|
			    exec " "+ mod " " + string	%mod_exec					|
			    exec " "+ lua " "+ string	%lua_exec					|
			    check " "+ slab		%{slab_validate(); ok(out);}			|
			    reload " "+ configuration	%reload_configuration);

	        main := commands eol;
		write init;
		write exec;
	}%%

	size_t parsed = (void *)pe - (void *)c->rbuf->data;
	c->rbuf->len -= parsed;
	c->rbuf->size -= parsed;
	c->rbuf->data = pe;

	if (p != pe) {
		start(out);
		tbuf_append(out, unknown_command, sizeof(unknown_command) - 1);
		end(out);
	}

	return conn_write(c, out->data, tbuf_len(out));
}


static void
admin_handler(va_list ap)
{
	int fd = va_arg(ap, int);
	struct conn *c = NULL;

	c = conn_create(fiber->pool, fd);
	palloc_register_gc_root(fiber->pool, c, conn_gc);
	@try {
		for (;;) {
			if (admin_dispatch(c) <= 0)
				break;
			fiber_gc();
		}
	}
	@finally {
		palloc_unregister_gc_root(fiber->pool, c);
		conn_close(c);
	}
}

static void
admin_accept(int fd, void *data __attribute__((unused)))
{
	if (fiber_create("admin/handler", admin_handler, fd) == NULL) {
		say_error("unable create fiber");
		close(fd);
	}
}

int
admin_init(void)
{
	if (fiber_create("admin/acceptor", tcp_server,
			 cfg.admin_port, admin_accept, NULL, NULL) == NULL)
	{
		say_syserror("can't bind to %d", cfg.admin_port);
		return -1;
	}
	return 0;
}



/*
 * Local Variables:
 * mode: c
 * End:
 */
