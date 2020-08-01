ifeq (-O0,$(filter -O%,$(CFLAGS) $(XCFLAGS)))
	CARGO_PROFILE=debug
else
	CARGO_PROFILE=release
endif

LDFLAGS += -Lsrc-rs/target/$(CARGO_PROFILE)
LIBS += -lrustyoctopus
CFLAGS += -pthread

$(binary): src-rs/target/$(CARGO_PROFILE)/librustyoctopus.a
-include src-rs/target/$(CARGO_PROFILE)/librustyoctopus.pd

src-rs/target/$(CARGO_PROFILE)/librustyoctopus.a: src-rs/src/octopus_ev.rs
src-rs/src/octopus_ev.rs: include/octopus_ev.h Makefile
	$E GEN	octopus_ev.rs
	$Q bindgen \
		--no-layout-tests \
		--rust-target=nightly \
		--no-recursive-whitelist \
		--ctypes-prefix libc \
		--whitelist-function "ev_.*" \
		--whitelist-type "(__)?sig_atomic_t" \
		--whitelist-type "ev_.*" \
		--blacklist-type 'ev_any_watcher' \
		--opaque-type ev_watcher_list \
		--whitelist-var "EV_.*" \
		--raw-line 'type stat = libc::stat;' \
		$< -o $@ -- -Iinclude -I.

dist-clean += src-rs/src/octopus_ev.rs


# For whatever reason cargo lists 'build.rs' as a dependency. Surely,
# there is no such source file and make complains that it can't build
# it.
src-rs/target/$(CARGO_PROFILE)/librustyoctopus.pd: src-rs/target/$(CARGO_PROFILE)/librustyoctopus.d
	@sed -e 's/\<build\.rs\>//' <$< >$@

src-rs/target/$(CARGO_PROFILE)/librustyoctopus.a:
	$E "CARGO	$(notdir $@)"
	$Q cd $(srcdir)/src-rs && \
		CARGO_TARGET_DIR=$(CURDIR)/src-rs/target \
		CARGO_BUILD_DEP_INFO_BASEDIR=$(realpath $(srcdir)) \
		$(CARGO) -q build -Z unstable-options --profile $(CARGO_PROFILE)
	$Q touch $@
