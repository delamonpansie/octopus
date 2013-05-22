binary_type = STORAGE

obj += src/admin.o
obj += $(obj-log-io)
obj += src/paxos.o

obj += mod/box/box.o
obj += mod/box/moonbox.o
obj += mod/box/tuple_index.o
obj += mod/box/memcached.o
obj += third_party/qsort_arg.o

mod/box/box.o: mod/box/box_version.h

src-lua += mod/box/src-lua/box.lua
src-lua += mod/box/src-lua/box_prelude.lua
src-lua += mod/box/src-lua/box/object_space_info.lua
src-lua += mod/box/src-lua/box/example_proc.lua


ifeq (1,$(HAVE_RAGEL))
dist-clean += mod/box/memcached.m
dist += mod/box/memcached.m
endif

cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += mod/box/box.cfg_tmpl
cfg_tmpl += cfg/paxos.cfg_tmpl

-include ../../jumproot.mk
