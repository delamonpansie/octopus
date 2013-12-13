obj += src-lua/prelude.o
obj += src-lua/packer.o
obj += src-lua/pp.o
obj += src-lua/objc.o
obj += src-lua/index.o
obj += src-lua/stat.o
obj += src-lua/net.o
obj += src-lua/cdef.o
obj += src-lua/cdef_base.o
obj += src-lua/jit/v.o
obj += src-lua/jit/dump.o
obj += src-lua/jit/bc.o
obj += src-lua/jit/vmdef.o
obj += src-lua/jit/p.o
obj += src-lua/jit/dis_x86.o
obj += src-lua/jit/dis_x64.o

src-lua/jit/vmdef.lua: .luajit
	@mkdir -p $(dir $@)
	@test -f $@ || ln -s ../../$(srcdir)/third_party/luajit/src/jit/vmdef.lua $@


# this dependency is required for $(CPP) calls below
include/net_io.h: cfg/octopus.h

dist-clean += src-lua/cdef.lua
src-lua/cdef.lua: include/octopus.h include/index.h include/fiber.h include/octopus_ev.h include/net_io.h include/iproto_def.h include/pickle.h src-lua/gen_cdef.sh
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)CPP="$(CPP) -P $(CPPFLAGS)" SED="$(SED)" srcdir="$(srcdir)" sh $(srcdir)/src-lua/gen_cdef.sh > $@

ifneq (,$(GRAPHITE_ADDR))
obj += src-lua/graphite.o
src-lua/graphite.lua: extra/graphite.lua
	$(E) "SED	$@"
	$(Q)$(SED) 's/127\.0\.0\.1:2003/$(GRAPHITE_ADDR)/' < $^ > $@
endif
