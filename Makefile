binary_type = STORAGE

obj += src/admin.o
obj += $(obj-log-io)
obj += src/tnt_obj.o
obj += src/iproto.o

src/octopus.o src/net_io.o: XCFLAGS += -DOCT_OBJECT

obj += mod/memcached/store.o
obj += mod/memcached/proto.o

mod/memcached/store.o: mod/memcached/memcached_version.h

ifeq (1,$(HAVE_RAGEL))
dist-clean += mod/memcached/proto.m
dist += mod/memcached/proto.m
endif

cfg_tmpl += cfg/log_io.cfg_tmpl
cfg_tmpl += cfg/admin.cfg_tmpl
cfg_tmpl += mod/memcached/memcached.cfg_tmpl


test: test_memcached $(binary)

00000000000000000001.snap:
	./$(binary) -c mod/memcached/test/octopus.cfg -i

test_memcached: 00000000000000000001.snap
	./$(binary) -D -c mod/memcached/test/octopus.cfg && sleep 1
	T_MEMD_USE_DAEMON="127.0.0.1:11211" prove mod/memcached/test/memcached/t
	kill `cat octopus.pid`

-include ../../jumproot.mk
