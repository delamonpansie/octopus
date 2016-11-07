NRCPU := $(shell (getconf _NPROCESSORS_ONLN || lscpu -p | grep -c ^[0-9] || echo 1) 2>/dev/null)
ifeq ($(NRCPU),0)
  NRCPU := 1
endif
unexport MAKEFLAGS
define make
	@$(MAKE) --no-print-directory -j$(NRCPU) -f Makefile $(MAKECMDGOALS)
endef

ifeq ($(wildcard Makefile),Makefile)
all:
	$(make)
%:
	$(make)
else
all $(filter-out configure Makefile distclean,$(MAKECMDGOALS)): Makefile
	$(make)
Makefile: configure
	./configure
configure: configure.ac third_party/libev/libev.m4
	autoconf
distclean:
	@echo "configure wasn't run, aborting"
endif
