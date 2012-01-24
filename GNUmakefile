ifeq ($(wildcard Makefile),Makefile)
include Makefile
else
all $(filter-out configure Makefile,$(MAKECMDGOALS)): Makefile
	$(MAKE) -f Makefile $(MAKECMDGOALS)
Makefile: configure
	./configure
configure:
	autoconf
endif
