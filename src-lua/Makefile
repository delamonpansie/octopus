# some lua files are generated with CPP, they also need XCPPFLAGS

LUA=1

cfg_tmpl += src-lua/octopus_lua_cfg.cfg_tmpl

%.lua: XCPPFLAGS += -I. -Iinclude
ifneq (.,$(srcdir))
  %.o %.lua: XCPPFLAGS += -I$(srcdir) -I$(srcdir)/include
  # ../../../ is because of 'make -C third_party/luajit/src'
  LUAJIT_VPATH=$(if $(filter /%,$(srcdir)),$(srcdir)/third_party/luajit/src/,../../../$(srcdir)/third_party/luajit/src/)
endif

# prevent parallel building of this targets
third_party/luajit/src/libluajit.a:|third_party/luajit/src/jit/vmdef.lua
third_party/luajit/src/jit/vmdef.lua:|third_party/luajit/src/luajit

third_party/luajit/src/luajit third_party/luajit/src/jit/vmdef.lua third_party/luajit/src/libluajit.a:
	@mkdir -p third_party/luajit/src/host third_party/luajit/src/jit
	$E "MAKE	$@"
	$(Q)echo 'include $$(VPATH)Makefile' | $(MAKE) -C third_party/luajit/src -f - \
		VPATH=$(LUAJIT_VPATH) \
		CC="$(CC)" E="$(E)" Q="$(Q)" \
		CCDEBUG="$(filter -g%,$(CFLAGS))" BUILDMODE=static \
		TARGET_CFLAGS='-funwind-tables -DLUAJIT_UNWIND_EXTERNAL' \
		$(subst third_party/luajit/src/,,$@)

$(binary): third_party/luajit/src/libluajit.a
define LUAJIT_B
LUA_PATH='$(srcdir)/third_party/luajit/src/?.lua' \
	./third_party/luajit/src/luajit -b \
	$(if $(DCFLAGS),-g) \
	-n $(subst /,.,$(basename $(lastword $(subst src-lua/, ,$<)))) \
	$< $@
endef

%.o: %.lua third_party/luajit/src/luajit
	@mkdir -p $(dir $@)
	$(E) "LUAJIT	$@"
	$(Q)$(LUAJIT_B)


obj += src-lua/octopus_lua.o
obj += src-lua/prelude.o
obj += src-lua/packer.o
obj += src-lua/pp.o
obj += src-lua/ddump.o
obj += src-lua/objc.o
obj += src-lua/index.o
obj += src-lua/stat.o
obj += src-lua/graphite.o
obj += src-lua/net.o
obj += src-lua/cdef.o
obj += src-lua/cdef_base.o
obj += src-lua/wal.o
obj += src-lua/fiber_lock.o
obj += src-lua/fiber_loop.o
obj += src-lua/reloadfile.o
obj += src-lua/repl.o
obj += src-lua/jit/v.o
obj += src-lua/jit/dump.o
obj += src-lua/jit/bc.o
obj += src-lua/jit/vmdef.o
obj += src-lua/jit/p.o
obj += src-lua/jit/dis_x86.o
obj += src-lua/jit/dis_x64.o

src-lua/jit/vmdef.lua: third_party/luajit/src/jit/vmdef.lua
	@mkdir -p $(dir $@)
	@test -f $@ || cp $^ $@


# this dependency is required for $(CPP) calls below
include/net_io.h: cfg/octopus.h

src-lua/cdef.lua: CPPFLAGS += -Wno-deprecated
dist-clean += src-lua/cdef.lua

src-lua/cdef.lua: cfg/octopus.h include/config.h include/fiber.h include/index.h include/iproto_def.h include/log_io.h include/net_io.h include/octopus.h include/octopus_ev.h include/palloc.h include/pickle.h include/say.h include/tbuf.h include/stat.h include/graphite.h cfg/defs.h src-lua/gen_cdef.sh
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)CPP="$(CPP) -P $(XCPPFLAGS) $(CPPFLAGS)" SED="$(SED)" srcdir="$(srcdir)" sh $(srcdir)/src-lua/gen_cdef.sh > $@

