obj += src-lua/prelude.o
obj += src-lua/packer.o
obj += src-lua/pp.o
obj += src-lua/ddump.o
obj += src-lua/objc.o
obj += src-lua/index.o
obj += src-lua/stat.o
obj += src-lua/net.o
obj += src-lua/cdef.o
obj += src-lua/cdef_base.o
obj += src-lua/wal.o
obj += src-lua/fiber_lock.o
obj += src-lua/fiber_loop.o
obj += src-lua/reloadfile.o
obj += src-lua/shard.o
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

dist-clean += src-lua/cdef.lua

src-lua/cdef.lua: cfg/octopus.h include/config.h include/fiber.h include/index.h include/iproto_def.h include/log_io.h include/net_io.h include/octopus.h include/octopus_ev.h include/palloc.h include/pickle.h include/say.h include/tbuf.h src-lua/gen_cdef.sh
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)CPP="$(CPP) -P $(XCPPFLAGS) $(CPPFLAGS)" SED="$(SED)" srcdir="$(srcdir)" sh $(srcdir)/src-lua/gen_cdef.sh > $@

ifneq (,$(GRAPHITE_ADDR))
obj += src-lua/graphite.o
src-lua/graphite.lua: extra/graphite.lua
	$(E) "GEN	$@"
	$(Q)echo "local graphite_addr = '$(GRAPHITE_ADDR)'" > $@
	$(Q)$(CAT) $^ >> $@
endif
