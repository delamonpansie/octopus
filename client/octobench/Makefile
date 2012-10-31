CLDIR?=client/libiproto
OBDIR?=client/octobench

benchobj += $(OBDIR)/octobench.o
$(OBDIR)/octobench.o: CFLAGS += -pthread

all: $(OBDIR)/octobench

clean: clean_bench

$(OBDIR)/octobench: $(benchobj) $(CLDIR)/libiproto.a
	    $E "CC  $@"
		    $Q$(CC) -pthread $(LDFLAGS) $^ $(LIBS) -o $@

clean_bench:
	rm -f $(benchobj)
	rm -f $(OBDIR)/octobench
