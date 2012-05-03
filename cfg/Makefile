obj += cfg/warning.o
obj += cfg/tarantool.o

ifeq (1, $(HAVE_CONFETTI))
    dist-clean += cfg/tarantool.h
    dist-clean += cfg/tarantool.c
    dist-clean += cfg/tarantool.cfg
    dist-clean += cfg/tarantool.cfg_tmpl

    dist += cfg/tarantool.h
    dist += cfg/tarantool.c
    dist += cfg/tarantool.cfg

    defered += $$(obj): cfg/tarantool.h cfg/tarantool.cfg

    cfg/tarantool.cfg_tmpl: cfg/core.cfg_tmpl $(cfg_tmpl) Makefile
	@mkdir -p $(dir $@)
	@echo '%{ ' > $@
	@echo "#include <third_party/confetti/prscfg.h>" >> $@
	@echo "#include <cfg/tarantool.h>" >> $@
	@echo "void out_warning(ConfettyError r, char *format, ...);" >> $@
	@echo '%}' >> $@
	$(E) "GEN	$@"
	$(Q)$(CAT) $(filter-out Makefile,$^) >> $@
endif

obj += cfg/tarantool.o
disable-warns-obj += cfg/tarantool.o
