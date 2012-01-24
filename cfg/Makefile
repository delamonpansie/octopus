obj += cfg/warning.o
obj += cfg/tarantool_$(module)_cfg.o

ifeq (1, $(HAVE_CONFETTI))
    dist-clean += cfg/tarantool_$(module)_cfg.h
    dist-clean += cfg/tarantool_$(module)_cfg.c
    dist-clean += cfg/tarantool_$(module)_cfg.cfg
    dist-clean += cfg/tarantool_$(module)_cfg.cfg_tmpl

    defered += $$(obj): cfg/tarantool_$(module)_cfg.h cfg/tarantool_$(module)_cfg.cfg

    cfg/tarantool_$(module)_cfg.cfg_tmpl: cfg/core_cfg.cfg_tmpl $(cfg_tmpl)
	@mkdir -p $(dir $@)
	@echo '%{ ' > $@
	@echo "#include <third_party/confetti/prscfg.h>" >> $@
	@echo "#include <cfg/tarantool_$(module)_cfg.h>" >> $@
	@echo "void out_warning(ConfettyError r, char *format, ...);" >> $@
	@echo '%}' >> $@
	$(CAT) $^ >> $@
endif

CFLAGS += -DTARANTOOL_CONFIG='<cfg/tarantool_$(module)_cfg.h>'
obj += cfg/tarantool_$(module)_cfg.o
disable-warns-obj += cfg/tarantool_$(module)_cfg.o