CLDIR=client/libiproto
libobj += $(CLDIR)/iproto.o

all: $(CLDIR)/libiproto.a
$(CLDIR)/libiproto.a: $(libobj)
	$(E) "AR	$@"
	$(Q)$(AR) $(ARFLAGS) $@ $<
	$(E) "RANLIB	$@"
	$(Q)$(RANLIB) $@

clean: clean_lib
clean_lib:
	$(E) "CLEAN	$(CLDIR)"
	$(Q)rm -f $(libobj)
	$(Q)rm -f $(CLDIR)/libiproto.a
