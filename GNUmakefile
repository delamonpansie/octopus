ifeq ($(wildcard Makefile),Makefile)
include Makefile
else
all $(filter-out configure Makefile distclean,$(MAKECMDGOALS)): Makefile
	$(MAKE) -f Makefile $(MAKECMDGOALS)
Makefile: configure
	./configure
configure: configure.ac third_party/libev/libev.m4
	autoconf
distclean:
	@echo "configure wasn't run, aborting"
endif
