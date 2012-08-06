obj += cfg/warning.o
obj += cfg/octopus.o

ifeq (1, $(HAVE_CONFETTI))
    dist-clean += cfg/octopus.h
    dist-clean += cfg/octopus.c
    dist-clean += cfg/octopus.cfg
    dist-clean += cfg/octopus.cfg_tmpl

    dist += cfg/octopus.h
    dist += cfg/octopus.c
    dist += cfg/octopus.cfg

    defered += $$(obj): cfg/octopus.h cfg/octopus.cfg

    cfg/octopus.cfg_tmpl: cfg/core.cfg_tmpl $(cfg_tmpl) Makefile
	@mkdir -p $(dir $@)
	@echo '%{ ' > $@
	@echo "#include <third_party/confetti/prscfg.h>" >> $@
	@echo "#include <cfg/octopus.h>" >> $@
	@echo "void out_warning(ConfettyError r, char *format, ...);" >> $@
	@echo '%}' >> $@
	$(E) "GEN	$@"
	$(Q)$(CAT) $(filter-out Makefile,$^) >> $@
endif

obj += cfg/octopus.o
disable-warns-obj += cfg/octopus.o
