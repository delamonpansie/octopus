obj += cfg/warning.o
obj += cfg/tarantool_cfg.o

ifeq (1, $(HAVE_CONFETTI))
    dist-clean += cfg/tarantool_cfg.h
    dist-clean += cfg/tarantool_cfg.c
    dist-clean += cfg/tarantool_cfg.cfg
    dist-clean += cfg/tarantool_cfg.cfg_tmpl

    defered += $$(obj): cfg/tarantool_cfg.h cfg/tarantool_cfg.cfg

    cfg/tarantool_cfg.cfg_tmpl: cfg/core_cfg.cfg_tmpl $(cfg_tmpl) Makefile
	@mkdir -p $(dir $@)
	@echo '%{ ' > $@
	@echo "#include <third_party/confetti/prscfg.h>" >> $@
	@echo "#include <cfg/tarantool_cfg.h>" >> $@
	@echo "void out_warning(ConfettyError r, char *format, ...);" >> $@
	@echo '%}' >> $@
	$(CAT) $(filter-out Makefile,$^) >> $@
endif

obj += cfg/tarantool_cfg.o
disable-warns-obj += cfg/tarantool_cfg.o