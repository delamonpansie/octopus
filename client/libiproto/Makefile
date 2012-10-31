CLDIR=client/libiproto
libobj += $(CLDIR)/iproto.o

AR=ar
RANLIB=ranlib

ARFLAGS=rcv

$(CLDIR)/libiproto.a: $(libobj)
	$(AR) $(ARFLAGS) $@ $<
	$(RANLIB) $@

all: $(CLDIR)/libiproto.a

clean: clean_lib

clean_lib:
	rm -f $(libobj)
	rm -f $(CLDIR)/libiproto.a
