binary_type = STORAGE

obj += src/admin.o
obj += $(obj-log-io)
obj += src/paxos.o

obj += mod/box/box.o
obj += mod/box/moonbox.o
obj += mod/box/tuple_index.o
obj += third_party/qsort_arg.o

mod/box/box.o: mod/box/box_version.h

src-lua += mod/box/src-lua/box.lua
src-lua += mod/box/src-lua/box_prelude.lua
src-lua += mod/box/src-lua/box/object_space_info.lua
src-lua += mod/box/src-lua/box/example_proc.lua
src-lua += mod/box/src-lua/box/expire.lua

cfg_tmpl += cfg/iproto.cfg_tmpl
cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += cfg/paxos.cfg_tmpl
cfg_tmpl += mod/box/box.cfg_tmpl


test: box_test

.PHONY: box_test
box_test:
	@cd mod/box && ./test/run.rb

-include ../../jumproot.mk
