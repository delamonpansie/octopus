binary_type = STORAGE

obj += src/admin.o
obj += $(obj-log-io)

obj += mod/memcached/store.o
obj += mod/memcached/proto.o

mod/memcached/store.o: mod/memcached/memcached_version.h

ifeq (1,$(HAVE_RAGEL))
dist-clean += mod/memcached/proto.m
dist += mod/memcached/proto.m
endif

cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += mod/memcached/memcached.cfg_tmpl

-include ../../jumproot.mk
