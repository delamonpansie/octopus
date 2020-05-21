XCFLAGS += -DOCT_CHILDREN=1

obj += $(obj-log-io)
obj += src/iproto.o
obj += src/iproto_client.o
obj += src/admin.o

obj += mod/example/main.o
mod/example/main.o: CFLAGS += -Wno-unused-parameter
mod/example/main.o: mod/example/example_version.h

obj += mod/feeder/feeder.o

cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += cfg/iproto.cfg_tmpl
cfg_tmpl += cfg/admin.cfg_tmpl
cfg_tmpl += mod/feeder/feeder_cfg.cfg_tmpl
cfg_tmpl += cfg/replication.cfg_tmpl

-include ../../jumproot.mk
