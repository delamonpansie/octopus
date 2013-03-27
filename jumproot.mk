ifeq ($(ROOT),)
all ::
	@$(MAKE) --no-print-directory -C ../.. -f Makefile octopus
% ::
	@$(MAKE) --no-print-directory -C ../.. -f Makefile $(MAKECMDGOALS)
endif
