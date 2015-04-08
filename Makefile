binary_type = STORAGE

obj += src/admin.o
obj += $(obj-log-io)
obj += src/paxos.o
obj += src/tnt_obj.o
obj += src/iproto.o
obj += src/iproto_client.o

src/octopus.o src/net_io.o: XCFLAGS += -DOCT_OBJECT

obj += mod/box/box.o
obj += mod/box/op.o
obj += mod/box/meta_op.o
obj += mod/box/print.o
obj += mod/box/moonbox.o
obj += mod/box/tuple_index.o
obj += third_party/qsort_arg.o

mod/box/box.o: mod/box/box_version.h

obj += mod/box/src-lua/box.o
obj += mod/box/src-lua/box_prelude.o
obj += mod/box/src-lua/box/object_space_info.o
obj += mod/box/src-lua/box/example_proc.o
obj += mod/box/src-lua/box/expire.o
obj += mod/box/src-lua/box/dyn_tuple.o
obj += mod/box/src-lua/box/op.o

cfg_tmpl += cfg/admin.cfg_tmpl
cfg_tmpl += cfg/iproto.cfg_tmpl
cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += cfg/paxos.cfg_tmpl
cfg_tmpl += mod/box/object_space.cfg_tmpl


test: box_test

.PHONY: box_test
box_test: $(binary)
	@cd mod/box && ./test/run.rb

-include ../../jumproot.mk
