src-lua += src-lua/prelude.lua
src-lua += src-lua/index.lua
src-lua += src-lua/stat.lua
src-lua += src-lua/net.lua
src-lua += src-lua/net_cdef.lua
src-lua += src-lua/jit/v.lua
src-lua += src-lua/jit/dump.lua
src-lua += src-lua/jit/vmdef.lua

src-lua/jit/vmdef.lua: third_party/luajit/src/jit/vmdef.lua


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
	$(Q)$(CPP) -P $(CPPFLAGS) $(srcdir)/include/iproto_def.h | $(SED) -n '/^struct iproto\(_retcode\)\? \+{/,/^}/p;' >> $@
	$(Q)$(CPP) -P $(CPPFLAGS) $(srcdir)/include/net_io.h | $(SED) -n '/^struct netmsg\(_head\|_tailq\)\? \+{/,/^}/p; /^enum conn_memory_ownership \+{/,/^}/p; /^struct conn \+{/,/^}/p' >> $@
	$(Q)$(CPP) -P $(CPPFLAGS) -DLUA_DEF='@@@' $(srcdir)/include/net_io.h | $(SED) -n '/@@@/!d; s/@@@//; p' >> $@
	$(Q)echo "]]" >> $@

ifneq (,$(GRAPHITE_ADDR))
src-lua += src-lua/graphite.lua
src-lua/graphite.lua: extra/graphite.lua
	$(E) "SED	$@"
	$(Q)$(SED) 's/127\.0\.0\.1:2003/$(GRAPHITE_ADDR)/' < $^ > $@
endif

obj += src-lua/registry.o
dist-clean += src-lua/registry.m
src-lua/registry.m: $(src-lua) scripts/stringify Makefile
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)sh $(srcdir)/scripts/gen-lua-registry.sh $(foreach src,$(src-lua),$(srcdir)/$(src)) > $@.tmp
	$(Q)mv $@.tmp $@
