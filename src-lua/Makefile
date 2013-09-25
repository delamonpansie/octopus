obj += src-lua/prelude.o
obj += src-lua/index.o
obj += src-lua/stat.o
obj += src-lua/net.o
obj += src-lua/net_cdef.o
obj += src-lua/jit/v.o
obj += src-lua/jit/dump.o
obj += src-lua/jit/bc.o
obj += src-lua/jit/vmdef.o
obj += src-lua/jit/p.o

src-lua/jit/vmdef.lua: .luajit
	@mkdir -p $(dir $@)
	@test -f $@ || ln -s ../../$(srcdir)/third_party/luajit/src/jit/vmdef.lua $@


# this dependency is required for $(CPP) calls below
include/net_io.h: cfg/octopus.h

src-lua/net_cdef.lua: include/octopus_ev.h include/net_io.h include/iproto_def.h Makefile
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)echo "local ffi = require 'ffi'" > $@
	$(Q)echo "module (...)" >> $@
	$(Q)echo "ffi.cdef [[ " >> $@
	$(Q)echo "struct palloc_pool;" >> $@
	$(Q)$(CPP) -P $(CPPFLAGS) $(srcdir)/include/octopus_ev.h | $(SED) -n '/^typedef [a-z]\+ ev_tstamp/p; /typedef struct ev_\(io\|timer\)/,/^}/p;' >> $@
	$(Q)$(CPP) -P $(CPPFLAGS) $(srcdir)/include/iproto_def.h | $(SED) -n '/^struct iproto\(_retcode\)\? \+{/,/^}/{s/\[\]/[?]/;p;}'  >> $@
	$(Q)$(CPP) -P $(CPPFLAGS) $(srcdir)/include/net_io.h | $(SED) -n '/^struct netmsg\(_mark\|_head\|_tailq\)\? \+{/,/^}/p; /^enum conn_memory_ownership \+{/,/^}/p; /^struct conn \+{/,/^}/p' >> $@
	$(Q)$(CPP) -P $(CPPFLAGS) -DLUA_DEF='@@@' $(srcdir)/include/net_io.h | $(SED) -n '/@@@/!d; s/@@@//; p' >> $@
	$(Q)echo "]]" >> $@

ifneq (,$(GRAPHITE_ADDR))
obj += src-lua/graphite.o
src-lua/graphite.lua: extra/graphite.lua
	$(E) "SED	$@"
	$(Q)$(SED) 's/127\.0\.0\.1:2003/$(GRAPHITE_ADDR)/' < $^ > $@
endif
