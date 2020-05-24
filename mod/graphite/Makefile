cfg_tmpl += cfg/graphite.cfg_tmpl
obj += mod/graphite/graphite.o

ifneq (,$(GRAPHITE_ADDR))
  mod/graphite/graphite.o: XCFLAGS += -DGRAPHITE_ADDR=$(GRAPHITE_ADDR)
endif
