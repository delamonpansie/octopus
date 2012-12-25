src-lua += src-lua/prelude.lua
src-lua += src-lua/index.lua
src-lua += src-lua/stat.lua

obj += src-lua/registry.o

dist-clean += src-lua/registry.m

src-lua/registry.m: $(src-lua-c) scripts/stringify
	@mkdir -p $(dir $@)
	$(Q)sh $(srcdir)/scripts/gen-lua-registry.sh $(src-lua) > $@
