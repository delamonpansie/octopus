/*
 * Copyright (C) 2010-2016 Mail.RU
 * Copyright (C) 2010-2016 Yury Vostrikov
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
#import <palloc.h>
#import <salloc.h>
#import <say.h>
#import <stat.h>
#import <log_io.h>
#import <octopus.h>
#import <tbuf.h>
#import <net_io.h>
#import <pickle.h>

#include <third_party/luajit/src/lua.h>

#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>

static const char help[] =
	"available commands:" CRLF
	" - help" CRLF
	" - exit" CRLF
	" - show info [net]" CRLF
	" - show fiber" CRLF
	" - show configuration" CRLF
	" - show slab" CRLF
	" - show palloc" CRLF
	" - show stat" CRLF
	" - show shard" CRLF
	" - save coredump" CRLF
	" - enable coredump" CRLF
	" - save snapshot" CRLF
	" - exec mod <command>" CRLF
	" - exec lua <code>" CRLF
	" - incr log_level" CRLF
	" - decr log_level" CRLF
	" - incr log_level source_name" CRLF
	" - decr log_level source_name" CRLF
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
	/* dirty hack to compile with clang */
	(void)admin_error;
	(void)admin_en_main;
	(void)admin_first_final;
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
fail(struct tbuf *out, const struct tbuf *err)
{
	start(out);
	tbuf_printf(out, "fail:%.*s" CRLF, (int)tbuf_len(err), (char *)err->ptr);
	end(out);
}

#if CFG_lua_path
static lua_State *
luaT_make_repl_env(int fd)
{
	lua_State *L = lua_newthread(fiber->L);
	lua_getglobal(L, "make_repl_env");
	lua_pushinteger(L, fd);
	lua_call(L, 1, 0);
	return L;
}

static const char *
tbuf_reader(lua_State *L __attribute__((unused)), void *data, size_t *size)
{
	struct tbuf *code = data;
	*size = tbuf_len(code);
	return read_bytes(code, tbuf_len(code));
}

static void
exec_lua(const char *str, size_t len, struct tbuf *out)
{
	lua_State* L = fiber->L;
	if (!cfg.admin_exec_lua) {
		tbuf_printf(out, "error: command is disabled" CRLF);
		return;
	}

	struct tbuf *code = tbuf_alloc(fiber->pool);
	if (*str == '=' || *str == '!') {
		if (*str == '=')
			tbuf_append_lit(code, "print(");
		else
			tbuf_append_lit(code, "ddump(");
		tbuf_append(code, str + 1, len - 1);
		tbuf_append_lit(code, ")");
	} else {
		tbuf_append(code, str, len);
	}

	int r = lua_load(L, tbuf_reader, code, "network_input");
	if (r != 0) {
		if (r == LUA_ERRSYNTAX)
			tbuf_printf(out, "error: syntax, %s" CRLF, lua_tostring(L, -1));
		if (r == LUA_ERRMEM)
			tbuf_printf(out, "error: memory, %s" CRLF, lua_tostring(L, -1));
		return;
	}

	if (lua_pcall(L, 0, 1, 0) != 0) {
		tbuf_printf(out, "error: pcall: %s" CRLF, lua_tostring(L, -1));
		return;
	}

	str = lua_tolstring(L, -1, &len);
	tbuf_append(out, str, len);
	if (len)
		tbuf_append(out, CRLF, 2);
	lua_pop(L, 1);
}
#endif


static void
log_level(struct tbuf *out, const char *strstart, const char *strend, int diff)
{
	const char *too_long = "too long source";
	const char *not_found = "unknown source";
	int len = strend ? strend - strstart : strlen(strstart);
	if (len > 64) {
		fail(out, &TBUF(too_long, strlen(too_long), NULL));
	} else {
		char *filename = alloca(len + 1);
		memcpy(filename, strstart, len);
		filename[len] = 0;
		if (say_level_source(filename, diff))
			ok(out);
		else
			fail(out, &TBUF(not_found, strlen(not_found), NULL));
	}
}


static char *
rbuf_getline(int fd, struct tbuf *rbuf)
{
	char *endline;
	while ((endline = memchr(rbuf->ptr, '\n', tbuf_len(rbuf))) == NULL) {
		if (tbuf_len(rbuf) > 0 && *(char*)(rbuf->ptr) == 0x04 /* Ctrl-D */)
			return NULL;
		ssize_t r = fiber_recv(fd, rbuf);
		if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK ||
			      errno == EINTR))
			continue;
		if (r <= 0)
			return NULL;
	}
	return endline;
}

static int
admin_dispatch(int fd, struct tbuf *rbuf)
{
	struct tbuf *out = tbuf_alloc(fiber->pool);
	struct tbuf *err = tbuf_alloc(fiber->pool);
	int cs;
	char *p, *pe;
	char *strstart = NULL, *strend = NULL;
	int info_net = 0;
	int info_string = 0;

	pe = rbuf_getline(fd, rbuf);
	if (pe == NULL)
		return 0;

	pe++;
	p = rbuf->ptr;

	%%{
		action show_configuration {
			octopus_cfg_iterator_t *i;
			char *key, *value;

			start(out);
			tbuf_printf(out, "configuration:" CRLF);
			i = octopus_cfg_iterator_init();
			while ((key = octopus_cfg_iterator_next(i, &cfg, &value)) != NULL) {
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
			struct tbuf *code;
			const char* opt = NULL;
			if (info_net) opt = "net";
			else if (info_string) {
				code = tbuf_alloc(fiber->pool);
				tbuf_append(code, strstart, strend - strstart);
				opt = code->ptr;
			}
			start(out);
			if (module(NULL)->info != NULL)
				module(NULL)->info(out, opt);
			end(out);
		}

		action help {
			start(out);
			tbuf_append(out, help, sizeof(help) - 1);
			end(out);
		}

		action show_stat {
			start(out);
			stat_print(out);
			end(out);
		}

		action show_shard {
			start(out);
			[recovery shard_info:out];
			end(out);
		}
		action lua_exec {
#if CFG_lua_path
			start(out);
			exec_lua(strstart, strend - strstart, out);
			end(out);
#endif
		}
		action lua_repl {
#if CFG_lua_path
			tbuf_ltrim(rbuf, pe - (char *)rbuf->ptr); // trim "exec lua"
			start(out);
			tbuf_append_lit(out, "-- lua repl, type ### to exit" CRLF "> ");
			fiber_write(fd, out->ptr, tbuf_len(out));
			tbuf_reset(out);

			struct tbuf *code = tbuf_alloc(fiber->pool);

			lua_State *oldL = fiber->L;
			fiber->L = luaT_make_repl_env(fd);

			while ((pe = rbuf_getline(fd, rbuf)) != NULL) {
				*pe++ = 0;
				char *line = rbuf->ptr;
				int len = pe - line;
				tbuf_ltrim(rbuf, len);

				if (strcmp(line, "###") == 0) {
					end(out);
					break;
				}

				exec_lua(line, len - 1, out); /* without trailing \0 */
				fiber_write(fd, out->ptr, tbuf_len(out));
				fiber_write(fd, "> ", 2);
				tbuf_reset(out);
				tbuf_reset(code);
			}

			fiber->L = oldL;
			lua_pop(fiber->L, 1);
			if (pe == NULL)
				return 0;
			p = pe - 1;
#endif
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
			if (reload_cfg() < 0)
				fail(out, &TBUF(cfg_err, cfg_err_len, NULL));
			else
				ok(out);
		}

		action save_snapshot {
			int ret = [recovery fork_and_snapshot];

			if (ret == 0)
				ok(out);
			else {
				tbuf_printf(err, " can't save snapshot, errno %d (%s)",
					    ret, strerror_o(ret));

				fail(out, err);
			}
		}

		action save_core {
			if (coredump(60) >= 0) {
				ok(out);
			} else {
				tbuf_printf(err, "%s", strerror_o(errno));
				fail(out,err);
			}
		}

		eol = "\n" | "\r\n";
		check = "ch"("e"("c"("k")?)?)?;
		configuration = "co"("n"("f"("i"("g"("u"("r"("a"("t"("i"("o"("n")?)?)?)?)?)?)?)?)?)?)?;
		coredump = "co"("r"("e"("d"("u"("m"("p")?)?)?)?)?)?;
		decr = "dec"("r")?;
		enable = "en"("a"("b"("l"("e")?)?)?)?;
		exec = "ex"("e"("c")?)?;
		exit = "e"("x"("i"("t")?)?)? | "q"("u"("i"("t")?)?)?;
		fiber = "fi"("b"("e"("r")?)?)?;
		help = "h"("e"("l"("p")?)?)?;
		incr = "inc"("r")?;
		info = "in"("f"("o")?)?;
		log = "log"("_"("l"("e"("v"("e"("l")?)?)?)?)?)?;
		lua = "lu"("a")?;
		mod = "mo"("d")?;
		palloc = "pa"("l"("l"("o"("c")?)?)?)?;
		reload = "re"("l"("o"("a"("d")?)?)?)?;
		save = "sa"("v"("e")?)?;
		shard = "sh"("a"("r"("d")?)?)?;
		show = "sh"("o"("w")?)?;
		slab = "sl"("a"("b")?)?;
		snapshot = "sn"("a"("p"("s"("h"("o"("t")?)?)?)?)?)?;
		stat = "st"("a"("t")?)?;
		string = [^\r\n]+ >{strstart = p;}  %{strend = p;};
                net = "n"("e"("t")?)? %{ info_net = 1;};

		info_string = string %{ info_string = 1;};
		info_option = (" "+ (net | info_string))?;

		commands = (help			%help						|
			    exit			%{return 0;}					|
			    show " "+ info info_option 	%show_info					|
			    show " "+ fiber		%{start(out); fiber_info(out); end(out);}	|
			    show " "+ configuration 	%show_configuration				|
			    show " "+ slab		%{start(out); slab_stat(out); end(out);}	|
			    show " "+ palloc		%{start(out); palloc_stat_info(out); end(out);}	|
			    show " "+ stat		%show_stat					|
			    show " "+ shard		%show_shard					|
			    enable " "+ coredump        %{maximize_core_rlimit(); ok(out);}		|
			    save " "+ coredump		%save_core					|
			    save " "+ snapshot		%save_snapshot					|
			    incr " "+ log         	%{log_level(out, "ALL", NULL, 1); }		|
			    decr " "+ log         	%{log_level(out, "ALL", NULL, -1); }		|
			    incr " "+ log " "+ string	%{log_level(out, strstart, strend, 1); }	|
			    decr " "+ log " "+ string	%{log_level(out, strstart, strend, -1); }	|
			    exec " "+ mod " "+ string	%mod_exec					|
			    exec " "+ lua " "+ string	%lua_exec					|
			    exec " " + lua		%lua_repl					|
			    check " "+ slab		%{slab_validate(); ok(out);}			|
			    reload " "+ configuration	%reload_configuration);

	        main := space* commands <: space* eol;
		write init;
		write exec;
	}%%

	size_t parsed = (void *)pe - (void *)rbuf->ptr;
	tbuf_ltrim(rbuf, parsed);

	if (p != pe) {
		start(out);
		tbuf_append(out, unknown_command, sizeof(unknown_command) - 1);
		end(out);
	}

	return fiber_write(fd, out->ptr, tbuf_len(out));
}


static void
admin_handler(va_list ap)
{
	int fd = va_arg(ap, int);
	struct tbuf rbuf = TBUF(NULL, 0, fiber->pool);

	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);
	@try {
		for (;;) {
			if (admin_dispatch(fd, &rbuf) <= 0)
				break;
			fiber_gc();
		}
	}
	@finally {
		palloc_unregister_gc_root(fiber->pool, &rbuf);
		close(fd);
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

static void
admin_init(void)
{
	if (!cfg.admin_addr)
		return;

	if (fiber_create("admin/acceptor", tcp_server,
			 cfg.admin_addr, admin_accept, NULL, NULL) == NULL)
	{
		say_syserror("can't start tcp_server on :`%s'", cfg.admin_addr);
	}
}

static int
admin_fixup_addr(struct octopus_cfg *conf)
{
	extern void out_warning(int v, char *format, ...);
	if (net_fixup_addr(&conf->admin_addr, conf->admin_port) < 0)
		out_warning(0, "Option 'admin_addr' is overridden by 'admin_port'");

	return 0;
}

static struct tnt_module admin_mod = {
	.init = admin_init,
	.check_config = admin_fixup_addr
};

register_module(admin_mod);


/*
 * Local Variables:
 * mode: objc
 * End:
 */
