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
#import <admin.h>
#import <fiber.h>
#import <iproto.h>
#import <log_io.h>
#import <palloc.h>
#import <salloc.h>
#import <say.h>
#import <net_io.h>
#import <stat.h>
#import <octopus.h>
#import <index.h>
#import <octopus_version.h>

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
#include <sys/ioctl.h>
#include <pwd.h>
#include <unistd.h>
#include <getopt.h>
#include <libgen.h>
#include <sysexits.h>
#if HAVE_SYS_PRCTL_H
# include <sys/prctl.h>
#endif

#define DEFAULT_CFG_FILENAME "octopus.cfg"
const char *cfg_filename = DEFAULT_CFG_FILENAME;
char *cfg_filename_fullpath = NULL;
char *binary_filename;
bool init_storage, booting = true;
static void *opt = NULL;

lua_State *root_L;

char cfg_err_buf[1024], *cfg_err;
int cfg_err_len, cfg_err_offt;
struct octopus_cfg cfg;
char *custom_proc_title;


ev_io keepalive_ev = { .coro = 0 };
Recovery *recovery;
int keepalive_pipe[2];

extern int daemonize(int nochdir, int noclose);
void out_warning(int v, char *format, ...);

static void
reset_cfg_err()
{
	cfg_err = cfg_err_buf;
	*cfg_err = 0;
	cfg_err_offt = 0;
	cfg_err_len = sizeof(cfg_err_buf);
}

static i32
load_cfg(struct octopus_cfg *conf, i32 check_rdonly)
{
	FILE *f;
	i32 n_accepted, n_skipped, n_optional;

	reset_cfg_err();
	if (cfg_filename_fullpath != NULL)
		f = fopen(cfg_filename_fullpath, "r");
	else
		f = fopen(cfg_filename, "r");

	if (f == NULL) {
		out_warning(0, "can't open config `%s'", cfg_filename);

		return -1;
	}

	parse_cfg_file_octopus_cfg(conf, f, check_rdonly, &n_accepted, &n_skipped, &n_optional);
	fclose(f);

	if (check_cfg_octopus_cfg(conf) != 0)
		return -1;

	if (n_accepted == 0 || n_skipped != 0)
		return -1;

	if (module(NULL)->check_config)
		return module(NULL)->check_config(conf);
	else
		return 0;
}

int
reload_cfg(void)
{
	struct octopus_cfg new_cfg1, new_cfg2;
	int ret;

	memset(&new_cfg1, 0, sizeof(new_cfg1));
	memset(&new_cfg2, 0, sizeof(new_cfg2));

	// Load with checking readonly params
	if (dup_octopus_cfg(&new_cfg1, &cfg) != 0) {
		destroy_octopus_cfg(&new_cfg1);
		return -1;
	}
	ret = load_cfg(&new_cfg1, 1);
	if (ret == -1) {
		destroy_octopus_cfg(&new_cfg1);
		return -1;
	}
	// Load without checking readonly params
	if (fill_default_octopus_cfg(&new_cfg2) != 0) {
		destroy_octopus_cfg(&new_cfg2);
		return -1;
	}
	ret = load_cfg(&new_cfg2, 0);
	if (ret == -1) {
		destroy_octopus_cfg(&new_cfg1);
		return -1;
	}
	// Compare only readonly params
	char *diff = cmp_octopus_cfg(&new_cfg1, &new_cfg2, 1);
	if (diff != NULL) {
		destroy_octopus_cfg(&new_cfg1);
		destroy_octopus_cfg(&new_cfg2);
		out_warning(0, "Could not accept read only '%s' option", diff);
		return -1;
	}
	destroy_octopus_cfg(&new_cfg1);
	foreach_module (m)
		if (m->reload_config != NULL)
			m->reload_config(&cfg, &new_cfg2);
	destroy_octopus_cfg(&cfg);
	cfg = new_cfg2;
	return 0;
}

struct tnt_module *
module(const char *name)
{
	if (name == NULL)
		name = PRIMARY_MOD;
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
octopus_version(void)
{
	return octopus_version_string;
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
static void
save_snapshot(void *ev __attribute__((unused)), int events __attribute__((unused)))
{
	[recovery snapshot:false];
}
#endif

static void
sig_int(int sig __attribute__((unused)))
{
	say_info("SIGINT or SIGTERM recieved, terminating");

#ifdef COVERAGE
	__gcov_flush();
#endif
	if (master_pid == getpid()) {
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

	sa.sa_handler = maximize_core_rlimit;
	if (sigaction(SIGUSR2, &sa, 0) == -1)
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
	fprintf(f, "%i\n", master_pid);
	fclose(f);
}

static void
remove_pid(void)
{
	if (getpid() == master_pid)
		unlink(cfg.pid_file);
	else
		say_warn("%s: not a master", __func__);
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
	const char *err = "unknown lua error";
	if (lua_isstring(L, -1))
		err = lua_tostring(L, -1);

	/* FIXME: use native exceptions ? */
	iproto_raise_fmt(ERR_CODE_UNKNOWN_ERROR, "%s", err);
}


static int
luaT_static_module(lua_State *L)
{
    const char *_name = luaL_checkstring(L, 1);
    char *name = alloca(strlen(_name) + 1);
    strcpy(name, _name);

    for (char *p = name; *p; p++)
	    if (*p == '.')
		    *p = '_';

    for (struct lua_src *s = lua_src; s->name; s++)
	    if (strcmp(name, s->name) == 0) {
		    if (luaL_loadbuffer(L, s->start, s->size, name) != 0)
			    panic("luaL_loadbuffer: %s", lua_tostring(L, 2));
		    return 1;
	    }


    lua_pushnil(L);
    return 1;
}

static int
luaT_os_ctime(lua_State *L)
{
	const char *filename = luaL_checkstring(L, 1);
	struct stat buf;

	if (stat(filename, &buf) < 0)
		luaL_error(L, "stat(`%s'): %s", filename, strerror(errno));
	lua_pushinteger(L, buf.st_ctime);
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

	luaT_openfiber(L);
	luaT_opennet(L);

	lua_getglobal(L, "package");
	lua_getfield(L, -1, "loaders");
	lua_pushinteger(L, lua_objlen(L, -1));
	lua_pushcfunction(L, luaT_static_module);
	lua_settable(L, -3);
	lua_pop(L, 2);

        lua_getglobal(L, "package");
        lua_pushstring(L, cfg.lua_path);
        lua_setfield(L, -2, "path");
        lua_pop(L, 1);

	lua_getglobal(L, "os");
	lua_pushcfunction(L, luaT_os_ctime);
	lua_setfield(L, -2, "ctime");
	lua_pop(L, 1);

	luaT_objinit(L);
	luaT_indexinit(L);

        lua_getglobal(L, "require");
        lua_pushliteral(L, "prelude");
	if (lua_pcall(L, 1, 0, 0))
		panic("lua_pcall() failed: %s", lua_tostring(L, -1));

	lua_atpanic(L, luaT_error);
}

int
luaT_find_proc(lua_State *L, char *fname, i32 len)
{
	lua_pushvalue(L, LUA_GLOBALSINDEX);
	do {
		char *e = memchr(fname, '.', len);
		if (e == NULL)
			e = fname + len;

		if (lua_isnil(L, -1))
			return 0;
		lua_pushlstring(L, fname, e - fname);
		lua_gettable(L, -2);
		lua_remove(L, -2);

		len -= e - fname + 1;
		fname = e + 1;
	} while (len > 0);
	if (lua_isnil(L, -1))
		return 0;
	return 1;
}

void
luaT_dofile(const char *filename)
{
	if (access(filename, R_OK) == 0) {
		lua_getglobal(root_L, "dofile");
		lua_pushstring(root_L, filename);
		if (lua_pcall(root_L, 1, 0, 0))
			panic("lua_pcall() failed: %s", lua_tostring(root_L, -1));
		say_info("%s loaded", filename);
	}
}


#ifdef STORAGE
static void
keepalive_read(ev_io *e, int events __attribute__((unused)))
{
	char buf[16];
	ssize_t r = read(e->fd, buf, sizeof(buf));
	if (r > 0)
		return;
	if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
		return;

	panic("read from keepalive_pipe failed");
}

static void
ev_panic(const char *msg)
{
	/* panic is a macro */
	panic("%s", msg);
}
#endif

static int
octopus(int argc, char **argv)
{
#ifdef STORAGE
	const char *cat_filename = NULL;
#endif
	const char *cfg_paramname = NULL;

	master_pid = getpid();
	srand(master_pid);
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
				       "=FILE|SCN", "cat xlog to stdout in readable format and exit"),
			   gopt_option('F', GOPT_ARG, gopt_shorts(0), gopt_longs("fold"),
				       "=SCN", "calculate CRC32C of storage at given SCN and exit"),
			   gopt_option('i', 0, gopt_shorts('i'),
				       gopt_longs("init-storage"),
				       NULL, "initialize storage (an empty snapshot file) and exit"),
#endif
			   gopt_option('v', GOPT_ARG|GOPT_REPEAT, gopt_shorts('v'), gopt_longs("verbose"),
				       "=LEVEL", "increase verbosity level of particular source or ALL; where LEVEL is n|ALL[=n]|filename[=n] , n = 1..6"),
			   gopt_option('H', 0, gopt_shorts(0), gopt_longs("list-sources"),
				       NULL, "list known sources"),
			   gopt_option('e', GOPT_REPEAT, gopt_shorts('e'), gopt_longs("stderr"),
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
		puts(octopus_version());
		return 0;
	}

	if (gopt(opt, 'h')) {
		puts("Tarantool -- an efficient in-memory data store.");
		printf("Usage: %s [OPTIONS]\n", basename(argv[0]));
		puts("");
		gopt_help(opt_def);
		puts("");
		return 0;
	}

	if (gopt(opt, 'H')) {
		say_list_sources();
		return 0;
	}
#ifdef STORAGE
	if (gopt_arg(opt, 'C', &cat_filename)) {
		salloc_init(0, 0, 0);
		fiber_init();
		set_proc_title("cat %s", cat_filename);

		if (strchr(cat_filename, '.') || strchr(cat_filename, '/')) {
			if (access(cat_filename, R_OK) == -1) {
				say_syserror("access(\"%s\")", cat_filename);
				exit(EX_OSFILE);
			}
			/* TODO: sane module selection */
			foreach_module (m)
				if (m->cat != NULL)
					return m->cat(cat_filename);
			panic("no --cat actions defined");
		} else {
			i64 stop_scn = atol(cat_filename);
			if (!stop_scn) {
				say_error("invalid SCN: `%s'", cat_filename);
				exit(EX_USAGE);
			}

			gopt_arg(opt, 'c', &cfg_filename);
			if (fill_default_octopus_cfg(&cfg) != 0 || load_cfg(&cfg, 0) != 0)
				panic("can't load config: %s", cfg_err);
			if (cfg.work_dir != NULL && chdir(cfg.work_dir) == -1)
				say_syserror("can't chdir to `%s'", cfg.work_dir);

			foreach_module (m)
				if (m->cat_scn != NULL)
					return m->cat_scn(stop_scn);
		}
	}

	const char *opt_text;
	if (gopt_arg(opt, 'F', &opt_text))
		fold_scn = atol(opt_text);
#endif

	/* If config filename given in command line it will override the default */
	gopt_arg(opt, 'c', &cfg_filename);

	if (argc != 1) {
		fprintf(stderr, "Can't parse command line: try --help or -h for help.\n");
		exit(EX_USAGE);
	}

	if (cfg_filename[0] != '/') {
		cfg_filename_fullpath = xmalloc(PATH_MAX);
		if (getcwd(cfg_filename_fullpath, PATH_MAX - strlen(cfg_filename) - 1) == NULL) {
			say_syserror("getcwd");
			exit(EX_OSERR);
		}

		strcat(cfg_filename_fullpath, "/");
		strcat(cfg_filename_fullpath, cfg_filename);
	}

	if (gopt(opt, 'k')) {
		if (fill_default_octopus_cfg(&cfg) != 0 || load_cfg(&cfg, 0) != 0) {
			say_error("check_config FAILED%s", cfg_err);

			return 1;
		}

		return 0;
	}

	if (fill_default_octopus_cfg(&cfg) != 0 || load_cfg(&cfg, 0) != 0)
		panic("can't load config: %s", cfg_err);

	if (gopt(opt, 'e'))
		dup_to_stderr = gopt(opt, 'e') + INFO - 1;

	const char *filename;
	int i = 0;
	say_level_source("ALL", cfg.log_level - default_level);
	while ((filename = gopt_arg_i(opt, 'v', i++))) {
		if (strlen(filename) == 1) {
			say_level_source("ALL", atoi(filename));
			continue;
		}
		if (strchr(filename, '=') != NULL) {
			char *dup = strdup(filename);
			char *eq = strchr(dup, '=');
			*eq++ = 0;
			say_level_source(dup, atoi(eq));
			free(dup);
			continue;
		}
		say_level_source(filename, 1);
	}

	if (gopt_arg(opt, 'g', &cfg_paramname)) {
		octopus_cfg_iterator_t *i;
		char *key, *value;

		i = octopus_cfg_iterator_init();
		while ((key = octopus_cfg_iterator_next(i, &cfg, &value)) != NULL) {
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

	if (cfg.coredump > 0) {
#if HAVE_PRCTL
		if (prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) < 0) {
			say_syserror("prctl");
			exit(EX_OSERR);
		}
#endif
	}

	if (fold_scn) {
		custom_proc_title = "fold";
	} else {
		if (cfg.custom_proc_title == NULL)
			custom_proc_title = "";
		else {
			custom_proc_title = xcalloc(strlen(cfg.custom_proc_title) + 2, 1);
			strcat(custom_proc_title, "@");
			strcat(custom_proc_title, cfg.custom_proc_title);
		}

		if (gopt(opt, 'D')) {
			if (daemonize(1, 0) < 0)
				panic("unable to daemonize");
			master_pid = getpid();
		}

		if (cfg.pid_file != NULL) {
			create_pid();
			atexit(remove_pid);
		}

		say_logger_init(cfg.logger_nonblock);
		booting = false;
	}

#ifdef STORAGE
	if (gopt(opt, 'i')) {
		init_storage = true;
		salloc_init(0, 0, 0);
		fiber_init();
		luaT_init();
		if (cfg.wal_feeder_addr) {
			say_warn("--init-storage is no op in replica");
			exit(EX_USAGE);
		}

		module(NULL)->init();
		exit([recovery snapshot_initial]);
	}
#endif

#if defined(UTILITY)
	initialize_minimal();
	fiber_init();
	luaT_init();
	signal_init();
	module(NULL)->init();
#elif defined(STORAGE)
	say_info("octopus version: %s", octopus_version());
	signal_init();
	ev_set_syserr_cb(ev_panic);
	ev_default_loop(ev_recommended_backends() | EVFLAG_SIGNALFD);
	char *evb = NULL;
	switch(ev_backend()) {
		case    EVBACKEND_SELECT:   evb = "select"; break;
		case    EVBACKEND_POLL:     evb = "poll"; break;
		case    EVBACKEND_EPOLL:    evb = "epoll"; break;
		case    EVBACKEND_KQUEUE:   evb = "kqueue"; break;
		case    EVBACKEND_DEVPOLL:  evb = "dev/poll"; break;
		case    EVBACKEND_PORT:     evb = "port"; break;
		default:                    evb = "unknown";
	}

	say_info("ev_loop initialized using '%s' backend, libev version is %d.%d", 
		 evb, ev_version_major(), ev_version_minor());

	ev_timer coredump_timer = { .coro = 0 };
	if (cfg.coredump > 0) {
		ev_timer_init(&coredump_timer, maximize_core_rlimit,
			      cfg.coredump * 60, 0);
		ev_timer_start(&coredump_timer);
	}

	int one = 1;
	if (pipe(keepalive_pipe) == -1 || ioctl(keepalive_pipe[0], FIONBIO, &one) == -1) {
		say_syserror("can't create keepalive pipe");
		exit(1);
	}

	ev_io_init(&keepalive_ev, keepalive_read, keepalive_pipe[0], EV_READ);
	ev_io_start(&keepalive_ev);

	fiber_init(); /* must be initialized before Lua */
	luaT_init();

	if (module("WAL feeder") && fold_scn == 0)
		module("WAL feeder")->init();

	ev_signal ev_sig = { .coro = 0 };
	ev_signal_init(&ev_sig, (void *)save_snapshot, SIGUSR1);
	ev_signal_start(&ev_sig);

	u64 fixed_arena = cfg.slab_alloc_arena * (1 << 30);
	if ((size_t)fixed_arena != fixed_arena)
		panic("slab_alloc_arena overflow");

	salloc_init(fixed_arena, cfg.slab_alloc_minimal, cfg.slab_alloc_factor);

	stat_init();
	@try {
		module(NULL)->init();
	}
	@catch (id e) {
		if ([e respondsTo:@selector(code)] && [e code] == ERR_CODE_MEMORY_ISSUE) {
			say_error("Can't allocate memory. Is slab_arena too small?");
			exit(EX_OSFILE);
		}
		@throw e;
	}
	admin_init();
	luaT_dofile("init.lua"); /* run Lua init _after_ module init */

	prelease(fiber->pool);
	say_debug("entering event loop");
	if (cfg.io_collect_interval > 0)
		ev_set_io_collect_interval(cfg.io_collect_interval);

	ev_run(0);
	ev_loop_destroy();
	say_debug("exiting loop");
#else
#error UTILITY or STORAGE must be defined
#endif
	return 0;
}

int main(int argc, char **argv)
{
	@try {
		return octopus(argc, argv);
	}
	@catch (Error *e) {
		panic_exc(e);
	}
	@catch (id e) {
		panic("unknown exception %s", [[e class] name]);
	}
}
register_source();
