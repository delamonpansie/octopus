XCFLAGS += -DOCT_CHILDREN=1

obj += src/admin.o
obj += $(obj-log-io)
obj += $(obj-index)
obj += src/tnt_obj.o
obj += src/iproto.o
obj += src/iproto_client.o
obj += src/onlineconf.o
obj += src-lua/onlineconf.o

src/octopus.o src/net_io.o src/fiber.o: XCFLAGS += -DOCT_OBJECT

obj += mod/box/box.o
obj += mod/box/op.o
obj += mod/box/meta_op.o
obj += mod/box/print.o
obj += mod/box/tuple_index.o
obj += third_party/qsort_arg.o

mod/box/box.o: mod/box/box_version.h

ifeq ($(LUA),1)
obj += mod/box/src-lua/moonbox.o
obj += mod/box/src-lua/box.o
obj += mod/box/src-lua/box_prelude.o
obj += mod/box/src-lua/box/object_space_info.o
obj += mod/box/src-lua/box/example_proc.o
obj += mod/box/src-lua/box/expire.o
obj += mod/box/src-lua/box/dyn_tuple.o
obj += mod/box/src-lua/box/op.o
obj += mod/box/src-lua/box/string_ext.o
obj += mod/box/src-lua/box/cast.o
endif

ifeq ($(OCAML),1)
obj += $(obj-caml)
cfg_tmpl += src-ml/ocaml.cfg_tmpl

OCAMLINC += -I mod/box/src-ml
cmx += mod/box/src-ml/box.cmx
cmx += mod/box/src-ml/box_space.cmx
cmx += mod/box/src-ml/box_tuple.cmx
cmx += mod/box/src-ml/box_index.cmx
cmx += mod/box/src-ml/box_op.cmx
cmx += mod/box/src-ml/box1.cmx
obj += mod/box/src-ml/box_tuple_stubs.o
obj += mod/box/src-ml/camlbox.o

mod/box/src-ml/box1.cmx : OCAMLIFLAGS += -opaque
endif

cfg_tmpl += cfg/admin.cfg_tmpl
cfg_tmpl += cfg/iproto.cfg_tmpl
cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += cfg/replication.cfg_tmpl
cfg_tmpl += mod/box/object_space.cfg_tmpl


test: box_test

.PHONY: box_test
box_test: $(binary)
	@cd mod/box && ./test/run.rb

-include ../../jumproot.mk
