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
#import <admin.h>
#import <fiber.h>
#import <iproto.h>
#import <log_io.h>
#import <palloc.h>
#import <salloc.h>
#import <say.h>
#import <net_io.h>
#import <stat.h>
#import <tarantool.h>
#import <util.h>
#import <tarantool_version.h>

#include <third_party/gopt/gopt.h>
#include <third_party/luajit/src/lua.h>
#include <third_party/luajit/src/lualib.h>
#include <third_party/luajit/src/lauxlib.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <pwd.h>
#include <unistd.h>
#include <getopt.h>
#include <libgen.h>
#include <sysexits.h>
#if HAVE_SYS_PRCTL_H
# include <sys/prctl.h>
#endif

#define DEFAULT_CFG_FILENAME "tarantool.cfg"
const char *cfg_filename = DEFAULT_CFG_FILENAME;
char *cfg_filename_fullpath = NULL;
char *binary_filename;
bool init_storage, booting = true;
static void *opt = NULL;

lua_State *root_L;

char cfg_err_buf[1024], *cfg_err;
int cfg_err_len;
struct tarantool_cfg cfg;

Recovery *recovery_state;

extern int daemonize(int nochdir, int noclose);
void out_warning(int v, char *format, ...);

static void
reset_cfg_err()
{
	cfg_err = cfg_err_buf;
	*cfg_err = 0;
	cfg_err_len = sizeof(cfg_err_buf);
}

static i32
load_cfg(struct tarantool_cfg *conf, i32 check_rdonly)
{
	FILE *f;
	i32 n_accepted, n_skipped;

	reset_cfg_err();
	if (cfg_filename_fullpath != NULL)
		f = fopen(cfg_filename_fullpath, "r");
	else
		f = fopen(cfg_filename, "r");

	if (f == NULL) {
		out_warning(0, "can't open config `%s'", cfg_filename);

		return -1;
	}

	parse_cfg_file_tarantool_cfg(conf, f, check_rdonly, &n_accepted, &n_skipped);
	fclose(f);

	if (check_cfg_tarantool_cfg(conf) != 0)
		return -1;

	if (n_accepted == 0 || n_skipped != 0)
		return -1;

	if (module(NULL)->check_config)
		return module(NULL)->check_config(conf);
	else
		return 0;
}

i32
reload_cfg()
{
	struct tarantool_cfg new_cfg1, new_cfg2;
	i32 ret;

	// Load with checking readonly params
	if (dup_tarantool_cfg(&new_cfg1, &cfg) != 0) {
		destroy_tarantool_cfg(&new_cfg1);
		return -1;
	}
	ret = load_cfg(&new_cfg1, 1);
	if (ret == -1) {
		destroy_tarantool_cfg(&new_cfg1);
		return -1;
	}
	// Load without checking readonly params
	if (fill_default_tarantool_cfg(&new_cfg2) != 0) {
		destroy_tarantool_cfg(&new_cfg2);
		return -1;
	}
	ret = load_cfg(&new_cfg2, 0);
	if (ret == -1) {
		destroy_tarantool_cfg(&new_cfg1);
		return -1;
	}
	// Compare only readonly params
	char *diff = cmp_tarantool_cfg(&new_cfg1, &new_cfg2, 1);
	if (diff != NULL) {
		destroy_tarantool_cfg(&new_cfg1);
		destroy_tarantool_cfg(&new_cfg2);
		out_warning(0, "Could not accept read only '%s' option", diff);
		return -1;
	}
	destroy_tarantool_cfg(&new_cfg1);
	if (module(NULL)->reload_config)
		module(NULL)->reload_config(&cfg, &new_cfg2);
	destroy_tarantool_cfg(&cfg);
	cfg = new_cfg2;
	return 0;
}

#define foreach_module(m) for (struct tnt_module *m = modules_head; m != NULL; m = m->next)

struct tnt_module *
module(const char *name)
{
        for (struct tnt_module *m = modules_head; m != NULL; m = m->next)
                if (name == NULL || strcmp(name, m->name) == 0)
                        return m;
        return NULL;
}

void
register_module_(struct tnt_module *m)
{
        m->next = modules_head;
        modules_head = m;
}

const char *
tarantool_version(void)
{
	return tarantool_version_string;
}


unsigned
tnt_uptime(void)
{
	static double boot;
	if (unlikely(boot == 0))
		boot = ev_now();

	return (unsigned)(ev_now() - boot);
}

#ifdef STORAGE
int
save_snapshot(void *ev, int events __attribute__((unused)))
{
	pid_t p;
	switch ((p = tnt_fork())) {
	case -1:
		say_syserror("fork");
		return -1;

	case 0: /* child, the dumper */
		fiber->name = "dumper";
		set_proc_title("dumper (%" PRIu32 ")", getppid());
		fiber_destroy_all();
		palloc_unmap_unused();
		close_all_xcpt(1, sayfd);

		foreach_module(m)
			if (m->snapshot != NULL)
				m->snapshot(false);

#ifdef COVERAGE
		__gcov_flush();
#endif
		_exit(EXIT_SUCCESS);

	default: /* parent, may wait for child */
		if (ev != NULL) /* dump on sigusr1 is async, don't wait */
			return 0;

		return wait_for_child(p);
	}
}
#endif

static void
sig_int(int sig)
{
	say_info("SIGINT or SIGTERM recieved, terminating");

#ifdef COVERAGE
	__gcov_flush();
#endif
	if (master_pid == getpid()) {
		if (sig == SIGINT)
			sig = SIGTERM;

		signal(sig, SIG_IGN);
		kill(0, sig);
		exit(EXIT_SUCCESS);
	} else
		_exit(EXIT_SUCCESS);
}

static void
signal_init(void)
{
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sigemptyset(&sa.sa_mask);

	sa.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &sa, 0) == -1)
		goto error;

	sa.sa_handler = sig_int;
	if (sigaction(SIGINT, &sa, 0) == -1 ||
	    sigaction(SIGTERM, &sa, 0) == -1 ||
	    sigaction(SIGHUP, &sa, 0) == -1)
		goto error;

	return;
      error:
	say_syserror("sigaction");
	exit(EX_OSERR);
}

static void
create_pid(void)
{
	FILE *f;
	char buf[16] = { 0 };
	pid_t pid;

	f = fopen(cfg.pid_file, "a+");
	if (f == NULL)
		panic_syserror("can't open pid file");

        fseeko(f, 0, SEEK_SET);
	if (fgets(buf, sizeof(buf), f) != NULL && strlen(buf) > 0) {
		pid = strtol(buf, NULL, 10);
		if (pid > 0 && kill(pid, 0) == 0)
			panic("the daemon is already running");
		else
			say_info("updating a stale pid file");
		if (ftruncate(fileno(f), 0) == -1)
			panic_syserror("ftruncate(`%s')", cfg.pid_file);
	}

        fseeko(f, 0, SEEK_SET);
	fprintf(f, "%i\n", getpid());
	fclose(f);
}

static void
remove_pid(void)
{
	unlink(cfg.pid_file);
}

static int
luaT_print(struct lua_State *L)
{
	int n = lua_gettop(L);
	for (int i = 1; i <= n; i++)
		say_info("%s", lua_tostring(L, i));
	return 0;
}

static int
luaT_panic(struct lua_State *L)
{
	const char *err = "unknown error";
	if (lua_isstring(L, 1))
		err = lua_tostring(L, 1);
	panic("lua failed with: %s", err);
}


static int
luaT_error(struct lua_State *L)
{
	const char *err = "unknown error";
	if (lua_isstring(L, 1))
		err = lua_tostring(L, 1);

	say_error("lua failed with: %s", err);
	panic("%s", err); // FIXME: tnt_raise(tnt_Exception, reason:err);
}


static int
luaT_static_module(lua_State *L)
{
    const char *_name = luaL_checkstring(L, 1);
    char *name = alloca(strlen(_name));
    strcpy(name, _name);

    for (char *p = name; *p; p++)
	    if (*p == '.')
		    *p = '_';

    for (struct lua_src *s = lua_src; s->name; s++)
	    if (strcmp(name, s->name) == 0) {
		    if (luaL_loadbuffer(L, s->start, s->size, name) != 0)
			    panic("luaL_loadbuffer: %s", lua_tostring(L, 1));
		    return 1;
	    }


    lua_pushnil(L);
    return 1;
}


static void
luaT_init()
{
	struct lua_State *L;
	L = root_L = luaL_newstate();

	/* any lua error during initial load is fatal */
	lua_atpanic(L, luaT_panic);

	luaL_openlibs(L);
	lua_register(L, "print", luaT_print);

	luaT_opentbuf(L);
	luaT_openfiber(L);

	lua_getglobal(L, "package");
	lua_getfield(L, -1, "loaders");
	lua_pushinteger(L, lua_objlen(L, -1));
	lua_pushcfunction(L, luaT_static_module);
	lua_settable(L, -3);
	lua_pop(L, 1);

        lua_getglobal(L, "package");
        lua_pushstring(L, cfg.lua_path);
        lua_setfield(L, -2, "path");
        lua_pop(L, 1);

        lua_getglobal(L, "require");
        lua_pushliteral(L, "prelude");

	if (lua_pcall(L, 1, 0, 0))
		panic("lua_pcall() failed: %s", lua_tostring(L, -1));

	lua_atpanic(L, luaT_error);
}

static void
initialize(double slab_alloc_arena, int slab_alloc_minimal, double slab_alloc_factor)
{

	salloc_init(slab_alloc_arena * (1 << 30), slab_alloc_minimal, slab_alloc_factor);
	fiber_init();
}

static void
initialize_minimal()
{
	initialize(0.1, 4, 2);
}

#ifdef STORAGE
static void
ev_panic(const char *msg)
{
	/* panic is a macro */
	panic("%s", msg);
}
#endif

int
main(int argc, char **argv)
{
#ifdef STORAGE
	const char *cat_filename = NULL;
#endif
	const char *cfg_paramname = NULL;

#ifndef HAVE_STACK_END_ADDRESS
        STACK_END_ADDRESS = &argc;
#endif
	cfg.log_level = S_INFO;
	master_pid = getpid();
	palloc_init();
#ifdef HAVE_LIBELF
	load_symbols(argv[0]);
#endif
	argv = init_set_proc_title(argc, argv);

	const void *opt_def =
		gopt_start(gopt_option('g', GOPT_ARG, gopt_shorts(0),
				       gopt_longs("cfg-get", "cfg_get"),
				       "=KEY", "return a value from configuration file described by KEY"),
			   gopt_option('k', 0, gopt_shorts(0),
				       gopt_longs("check-config"),
				       NULL, "Check configuration file for errors"),
			   gopt_option('c', GOPT_ARG, gopt_shorts('c'),
				       gopt_longs("config"),
				       "=FILE", "path to configuration file (default: " DEFAULT_CFG_FILENAME ")"),
#ifdef STORAGE
			   gopt_option('C', GOPT_ARG, gopt_shorts(0), gopt_longs("cat"),
				       "=FILE", "cat snapshot file to stdout in readable format and exit"),
			   gopt_option('I', 0, gopt_shorts(0),
				       gopt_longs("init-storage"),
				       NULL, "initialize storage (an empty snapshot file) and exit"),
#endif
			   gopt_option('v', GOPT_REPEAT, gopt_shorts('v'), gopt_longs("verbose"),
				       NULL, "increase verbosity level in log messages"),
			   gopt_option('e', 0, gopt_shorts('e'), gopt_longs("stderr"),
				       NULL, "Duplicate log output to stderr"),
			   gopt_option('D', 0, gopt_shorts('D'), gopt_longs("daemonize"),
				       NULL, "redirect input/output streams to a log file and run as daemon"),
			   gopt_option('h', 0, gopt_shorts('h', '?'), gopt_longs("help"),
				       NULL, "display this help and exit"),
			   gopt_option('V', 0, gopt_shorts('V'), gopt_longs("version"),
				       NULL, "print program version and exit"));

	opt = gopt_sort(&argc, (const char **)argv, opt_def);
	binary_filename = argv[0];

	if (gopt(opt, 'V')) {
		puts(tarantool_version());
		return 0;
	}

	if (gopt(opt, 'h')) {
		puts("Tarantool -- an efficient in-memory data store.");
		printf("Usage: %s [OPTIONS]\n", basename(argv[0]));
		puts("");
		gopt_help(opt_def);
		puts("");
		puts("Please visit project home page at http://launchpad.net/tarantool");
		puts("to see online documentation, submit bugs or contribute a patch.");
		return 0;
	}

#ifdef STORAGE
	if (gopt_arg(opt, 'C', &cat_filename)) {
		initialize_minimal();
		if (access(cat_filename, R_OK) == -1) {
			say_syserror("access(\"%s\")", cat_filename);
			exit(EX_OSFILE);
		}
		return module(NULL)->cat(cat_filename);
	}
#endif

	/* If config filename given in command line it will override the default */
	gopt_arg(opt, 'c', &cfg_filename);

	if (argc != 1) {
		fprintf(stderr, "Can't parse command line: try --help or -h for help.\n");
		exit(EX_USAGE);
	}

	if (cfg_filename[0] != '/') {
		cfg_filename_fullpath = malloc(PATH_MAX);
		if (getcwd(cfg_filename_fullpath, PATH_MAX - strlen(cfg_filename) - 1) == NULL) {
			say_syserror("getcwd");
			exit(EX_OSERR);
		}

		strcat(cfg_filename_fullpath, "/");
		strcat(cfg_filename_fullpath, cfg_filename);
	}

	if (gopt(opt, 'k')) {
		if (fill_default_tarantool_cfg(&cfg) != 0 || load_cfg(&cfg, 0) != 0) {
			say_error("check_config FAILED%s", cfg_err);

			return 1;
		}

		return 0;
	}

	if (fill_default_tarantool_cfg(&cfg) != 0 || load_cfg(&cfg, 0) != 0)
		panic("can't load config: %s", cfg_err);

	cfg.log_level += gopt(opt, 'v');
	dup_to_stderr = gopt(opt, 'e');

	if (gopt_arg(opt, 'g', &cfg_paramname)) {
		tarantool_cfg_iterator_t *i;
		char *key, *value;

		i = tarantool_cfg_iterator_init();
		while ((key = tarantool_cfg_iterator_next(i, &cfg, &value)) != NULL) {
			if (strcmp(key, cfg_paramname) == 0) {
				printf("%s\n", value);
				free(value);

				return 0;
			}

			free(value);
		}

		return 1;
	}
	if (cfg.work_dir != NULL && chdir(cfg.work_dir) == -1)
		say_syserror("can't chdir to `%s'", cfg.work_dir);

	if (cfg.username != NULL) {
		if (getuid() == 0 || geteuid() == 0) {
			struct passwd *pw;
			if ((pw = getpwnam(cfg.username)) == 0) {
				say_syserror("getpwnam: %s", cfg.username);
				exit(EX_NOUSER);
			}
			if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0 || seteuid(pw->pw_uid)) {
				say_syserror("setgit/setuid");
				exit(EX_OSERR);
			}
		} else {
			say_error("can't swith to %s: i'm not root", cfg.username);
		}
	}

	if (cfg.coredump) {
		struct rlimit c = { 0, 0 };
		if (getrlimit(RLIMIT_CORE, &c) < 0) {
			say_syserror("getrlimit");
			exit(EX_OSERR);
		}
		c.rlim_cur = c.rlim_max;
		if (setrlimit(RLIMIT_CORE, &c) < 0) {
			say_syserror("setrlimit");
			exit(EX_OSERR);
		}
#if HAVE_PRCTL
		if (prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) < 0) {
			say_syserror("prctl");
			exit(EX_OSERR);
		}
#endif
	}

	if (gopt(opt, 'D'))
		if (daemonize(1, 0) < 0)
			panic("unable to daemonize");

	if (cfg.pid_file != NULL) {
		create_pid();
		atexit(remove_pid);
	}

	say_logger_init(cfg.logger_nonblock);
	booting = false;

	@try {
#ifdef STORAGE
	if (gopt(opt, 'I')) {
		init_storage = true;
		initialize_minimal();
		luaT_init();
		module(NULL)->init();
		module(NULL)->snapshot(true);
		exit(EXIT_SUCCESS);
	}
#endif

#if defined(UTILITY)
	initialize_minimal();
	luaT_init();
	signal_init();
	module(NULL)->init();
#elif defined(STORAGE)
	signal_init();
	ev_set_syserr_cb(ev_panic);
	ev_default_loop(ev_recommended_backends() | EVFLAG_SIGNALFD);
	say_debug("ev_loop initialized");

	if (module("WAL feeder"))
		module("WAL feeder")->init();

	ev_signal ev_sig = { .coro = 0 };
	ev_signal_init(&ev_sig, (void *)save_snapshot, SIGUSR1);
	ev_signal_start(&ev_sig);

	initialize(cfg.slab_alloc_arena, cfg.slab_alloc_minimal, cfg.slab_alloc_factor);

	luaT_init();
	stat_init();

	if (module("(silver)box"))
		module("(silver)box")->init();

	admin_init();
	prelease(fiber->pool);
	say_crit("log level %i", cfg.log_level);
	say_crit("entering event loop");
	if (cfg.io_collect_interval > 0)
		ev_set_io_collect_interval(cfg.io_collect_interval);
	ev_run(0);
	ev_loop_destroy();
	say_crit("exiting loop");
#else
#error UTILITY or STORAGE must be defined
#endif
	}
	@catch (Error *e) {
		panic_exc(e);
	}
	@catch (id e) {
		panic("unknown exception");
	}

	return 0;
}
