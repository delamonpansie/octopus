OBDIR=client/octobench

benchobj += $(OBDIR)/octobench.o
dep += $(call depend,$(benchobj))

$(OBDIR)/octobench.o: CFLAGS += -pthread

all: $(OBDIR)/octobench
$(OBDIR)/octobench: $(benchobj) client/libiproto/libiproto.a
	$(E) "CC	$@"
	$(Q)$(CC) -pthread $(LDFLAGS) $^ $(LIBS) -o $@

clean: clean_bench
clean_bench:
	$(E) "CLEAN	client/octobench"
	$(Q)rm -f $(benchobj)
	$(Q)rm -f $(OBDIR)/octobench
