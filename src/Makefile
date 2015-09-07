include $(VPATH)/src/index/Makefile

obj += src/objc.o
obj += src/net_io.o
obj += src/coro.o
obj += src/fiber.o

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
obj += src/spawn_child.o

obj-log-io += src/log_io.o
obj-log-io += src/log_io_reader.o
obj-log-io += src/log_io_remote.o
obj-log-io += src/log_io_writers.o
obj-log-io += src/log_io_recovery.o
obj-log-io += src/log_io_por.o
obj-log-io += src/log_io_puller.o
obj-log-io += src/log_io_run_crc.o
obj-log-io += src/paxos.o

ifeq (1,$(HAVE_RAGEL))
	dist-clean += src/admin.m
	dist += src/admin.m
endif

$(obj): XCFLAGS += -DOCTOPUS

src/octopus.o: octopus_version.h
src/octopus.o: XCFLAGS += -DPRIMARY_MOD='"$(primary_module)"'

# no warns at all: it complains even with -Wall
no-extra-warns += src/octopus_ev.o
src/octopus_ev.o: XCFLAGS := -U_FORTIFY_SOURCE $(filter-out -W%,$(CFLAGS_EXTRA))

# link with pthread if thread_pool used
ifneq ($(findstring src/thread_pool.o,$(obj)),)
  XCFLAGS += -DTHREADS
  src/octopus_ev.o: XCFLAGS += -DTHREADS
  LIBS += -pthread -lrt
endif

ifneq (,$(TRACE))
	obj += src/trace.o
	$(TRACE): XCFLAGS += -finstrument-functions
	XLDFLAGS += -Wl,-Map=octopus.map
endif
