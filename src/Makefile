include src/index/Makefile

obj += src/object.o
obj += src/tnt_obj.o
obj += src/net_io.o
obj += src/coro.o
obj += src/fiber.o
obj += src/iproto.o
obj += src/log_io.o
obj += src/log_io_writers.o
obj += src/log_io_recovery.o
obj += src/log_io_puller.o
obj += src/palloc.o
obj += src/pickle.o
obj += src/salloc.o
obj += src/say.o
obj += src/stat.o
obj += src/tarantool_ev.o
obj += src/tbuf.o
obj += src/util.o
obj += src/assoc.o
obj += src/paxos.o
obj += src/tarantool.o

ifeq (1,$(HAVE_RAGEL))
	dist-clean += src/admin.m
	dist += src/admin.m
endif

src/tarantool.o: tarantool_version.h
src/tarantool.o: CFLAGS += -DPRIMARY_MOD='"$(firstword $(modules))"'

src/palloc.o: CFLAGS += -DPALLOC_STAT_INFO -DHAVE_VALGRIND_H=1 -DHAVE_THIRD_PARTY_QUEUE_H=1
disable-warns-obj += src/tarantool_ev.o
src/tarantool_ev.o: CFLAGS += -U_FORTIFY_SOURCE
ifeq (1,$(HAVE_CC_FLAG_W_NO_COMMENT))
  src/tarantool_ev.o: CFLAGS += -Wno-comment
endif
ifeq (1,$(HAVE_CC_FLAG_W_NO_UNUSED_VALUE))
  src/tarantool_ev.o: CFLAGS += -Wno-unused-value
endif

ifeq (1,$(RESOLVE_SYMBOLS))
	LDFLAGS += -rdynamic -lbfd
	CFLAGS += -DRESOLVE_SYMBOLS
endif

ifneq (,$(TRACE))
	obj += src/trace.o
	$(TRACE): CFLAGS += -finstrument-functions
	LDFLAGS += -Wl,-Map=tarantool.map
endif
