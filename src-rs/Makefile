ifeq (-O0,$(filter -O%,$(CFLAGS) $(XCFLAGS)))
	CARGO_PROFILE=debug
else
	CARGO_PROFILE=release
endif

obj += src-rs/target/$(CARGO_PROFILE)/liboctopus.a

src-rs/target/$(CARGO_PROFILE)/liboctopus.a:
	$E "CARGO	$(notdir $@)"
	$Q cd $(srcdir)/src-rs && \
		CARGO_TARGET_DIR=$(CURDIR)/src-rs/target \
		CARGO_BUILD_DEP_INFO_BASEDIR=$(realpath $(srcdir)) \
		$(CARGO) -q build -Z unstable-options --profile $(CARGO_PROFILE)
	$Q touch $@
