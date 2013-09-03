include $(VPATH)/src/index/Makefile

obj += src/objc.o
obj += src/tnt_obj.o
obj += src/net_io.o
obj += src/coro.o
obj += src/fiber.o
obj += src/iproto.o

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

obj-log-io += src/log_io.o
obj-log-io += src/log_io_writers.o
obj-log-io += src/log_io_recovery.o
obj-log-io += src/log_io_puller.o

ifeq (1,$(HAVE_RAGEL))
	dist-clean += src/admin.m
	dist += src/admin.m
endif

$(obj): CFLAGS_EXTRA += -DOCTOPUS

src/octopus.o: octopus_version.h
src/octopus.o: CFLAGS_EXTRA += -DPRIMARY_MOD='"$(primary_module)"' -D$(binary_type)
ifeq ($(binary_type),)
  $(error binary_type must be defined)
endif

# some modules might be built without paxos
ifneq ($(findstring src/paxos.o,$(obj)),)
  src/log_io_recovery.o: CFLAGS_EXTRA += -DPAXOS
endif

# no warns at all: it complains even with -Wall
no-extra-warns += src/octopus_ev.o
src/octopus_ev.o: CFLAGS_EXTRA := -U_FORTIFY_SOURCE $(filter-out -W%,$(CFLAGS_EXTRA))

ifneq (,$(TRACE))
	obj += src/trace.o
	$(TRACE): CFLAGS_EXTRA += -finstrument-functions
	LDFLAGS += -Wl,-Map=octopus.map
endif
