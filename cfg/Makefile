obj += cfg/warning.o
obj += cfg/prscfg.o
obj += cfg/octopus.o

disable-warns-obj += cfg/octopus.o
disable-warns-obj += cfg/prscfg.o

cfg/warning.o: cfg/prscfg.h

ifeq (1, $(HAVE_CONFETTI))
    dist-clean += cfg/prscfg.h
    dist-clean += cfg/prscfg.c
    dist-clean += cfg/octopus.h
    dist-clean += cfg/octopus.c
    dist-clean += cfg/octopus.cfg
    dist-clean += cfg/octopus.cfg_tmpl

    dist += cfg/prscfg.h
    dist += cfg/prscfg.c
    dist += cfg/octopus.h
    dist += cfg/octopus.c
    dist += cfg/octopus.cfg

    defered += $$(obj): cfg/octopus.h cfg/octopus.cfg

    cfg/octopus.cfg_tmpl: cfg/core.cfg_tmpl $(cfg_tmpl) Makefile
	@mkdir -p $(dir $@)
	@echo '%{ ' > $@
	@echo "#include <cfg/prscfg.h>" >> $@
	@echo "#include <cfg/octopus.h>" >> $@
	@echo "void out_warning(ConfettyError r, char *format, ...);" >> $@
	@echo '%}' >> $@
	$(E) "GEN	$@"
	$(Q)$(CAT) $(filter-out Makefile,$^) >> $@

    cfg/prscfg.c: cfg/octopus.cfg_tmpl
	$(E) "CFTY	$@"
	$(Q)$(CONFETTI) -i $< -p $@

    cfg/prscfg.h: cfg/octopus.cfg_tmpl
	$(E) "CFTY	$@"
	$(Q)$(CONFETTI) -i $< -H $@
endif
