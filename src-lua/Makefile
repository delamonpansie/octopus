src-lua += src-lua/prelude.lua
src-lua += src-lua/index.lua
src-lua += src-lua/stat.lua

obj += src-lua/bundle.o
obj += src-lua/registry.o

dist-clean += src-lua/bundle.o
dist-clean += src-lua/bundle.o.redefine
dist-clean += src-lua/bundle.nm
dist-clean += src-lua/registry.m

src-lua/bundle.o: $(src-lua)
	@mkdir -p $(dir $@)
	$(LD) -r -b binary $(filter %.lua,$^) -o $@
	$(NM) $@ | $(SED) 's/.* //; h; s/src_lua_/\n/; s/.*\n//; x; G; s/\n/ /' > $@.redefine
	$(OBJCOPY) --redefine-syms $@.redefine $@

src-lua/bundle.nm: src-lua/bundle.o
	@mkdir -p $(dir $@)
	$(NM) $^ | $(SED) '/end$$/d; s/.* //;' | sort -r > $@

src-lua/registry.m: src-lua/bundle.nm
	@mkdir -p $(dir $@)
	@sh $(srcdir)/scripts/gen-lua-registry.sh $< > $@