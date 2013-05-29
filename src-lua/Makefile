src-lua += src-lua/prelude.lua
src-lua += src-lua/index.lua
src-lua += src-lua/stat.lua
src-lua += src-lua/net.lua

obj += src-lua/registry.o

dist-clean += src-lua/registry.m

src-lua/registry.m: $(src-lua) scripts/stringify Makefile
	@mkdir -p $(dir $@)
	$(E) "GEN	$@"
	$(Q)sh $(srcdir)/scripts/gen-lua-registry.sh $(src-lua) > $@
