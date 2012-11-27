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
obj += src/octopus_ev.o
obj += src/tbuf.o
obj += src/util.o
obj += src/assoc.o
obj += src/octopus.o
obj += src/errcode.o

ifeq (1,$(HAVE_RAGEL))
	dist-clean += src/admin.m
	dist += src/admin.m
endif

$(obj): CFLAGS += -DOCTOPUS

src/octopus.o: octopus_version.h
src/octopus.o: CFLAGS += -DPRIMARY_MOD='"$(firstword $(modules))"' -D$(binary_type)
ifeq ($(binary_type),)
  $(error binary_type must be defined)
endif

disable-warns-obj += src/octopus_ev.o
src/octopus_ev.o: CFLAGS += -U_FORTIFY_SOURCE
ifeq (1,$(HAVE_CC_FLAG_W_NO_COMMENT))
  src/octopus_ev.o: CFLAGS += -Wno-comment
endif
ifeq (1,$(HAVE_CC_FLAG_W_NO_UNUSED_VALUE))
  src/octopus_ev.o: CFLAGS += -Wno-unused-value
endif

ifneq (,$(TRACE))
	obj += src/trace.o
	$(TRACE): CFLAGS += -finstrument-functions
	LDFLAGS += -Wl,-Map=octopus.map
endif
