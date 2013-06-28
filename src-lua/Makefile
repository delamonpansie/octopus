src-lua += src-lua/prelude.lua
src-lua += src-lua/index.lua
src-lua += src-lua/stat.lua
src-lua += src-lua/net.lua
src-lua += src-lua/jit/v.lua
src-lua += src-lua/jit/dump.lua
src-lua += src-lua/jit/vmdef.lua

src-lua/jit/vmdef.lua: third_party/luajit/src/jit/vmdef.lua

obj += src-lua/registry.o

dist-clean += src-lua/registry.m

src-lua/registry.m: $(src-lua) scripts/stringify Makefile
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)sh $(srcdir)/scripts/gen-lua-registry.sh $(src-lua) > $@
