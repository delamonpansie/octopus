src/tarantool.o: CFLAGS += -DSTORAGE

obj += src/admin.o
obj += mod/box/box.o
obj += mod/box/moonbox.o
obj += mod/box/tuple_index.o
obj += mod/box/memcached.o
obj += third_party/qsort_arg.o

src-lua += mod/box/src-lua/box.lua
src-lua += mod/box/src-lua/box_prelude.lua
src-lua += mod/box/src-lua/box/index.lua
src-lua += mod/box/src-lua/box/user_proc.lua
src-lua += mod/box/src-lua/box/user_proc_unsafe.lua



ifeq (1,$(HAVE_RAGEL))
	dist-clean += mod/box/memcached.m
endif

cfg_tmpl += mod/box/box_cfg.cfg_tmpl