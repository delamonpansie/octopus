ifeq (-O0,$(filter -O%,$(CFLAGS) $(XCFLAGS)))
	CARGO_PROFILE=debug
else
	CARGO_PROFILE=release
endif

LDFLAGS += -Lsrc-rs/target/$(CARGO_PROFILE)
LIBS += -lrustyoctopus
CFLAGS += -pthread

$(binary): src-rs/target/$(CARGO_PROFILE)/librustyoctopus.a
-include src-rs/target/$(CARGO_PROFILE)/librustyoctopus.d

src-rs/target/$(CARGO_PROFILE)/librustyoctopus.a:
	$E "CARGO	$(notdir $@)"
	$Q cd $(srcdir)/src-rs && \
		CARGO_TARGET_DIR=$(CURDIR)/src-rs/target \
		CARGO_BUILD_DEP_INFO_BASEDIR=$(realpath $(srcdir)) \
		$(CARGO) -q build -Z unstable-options --profile $(CARGO_PROFILE)
	$Q touch $@
