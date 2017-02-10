obj += mod/feeder/feeder.o
obj += mod/feeder/src-lua/feeder.o
obj += src/log_io.o
obj += src/log_io_reader.o


# standalone feeder
ifeq ($(modules),feeder)
 obj += src/iproto.o
 obj += src/iproto_client.o
 obj += src/spawn_child.o
 cfg_tmpl += cfg/replication.cfg_tmpl
 XCFLAGS += -DOCT_CHILDREN=1
endif

cfg_tmpl += mod/feeder/feeder_cfg.cfg_tmpl
cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += cfg/opengraph_xlogs.cfg_tmpl

mod/feeder/feeder.o: mod/feeder/feeder_version.h
