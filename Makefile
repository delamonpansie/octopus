binary_type ?= FEEDER

obj += mod/feeder/feeder.o
obj += mod/feeder/src-lua/feeder.o
obj += src/log_io.o
obj += src/log_io_recovery.o
obj += src/log_io_writers.o

cfg_tmpl += mod/feeder/feeder_cfg.cfg_tmpl
cfg_tmpl += cfg/log_io.cfg_tmpl

mod/feeder/feeder.o: mod/feeder/feeder_version.h
