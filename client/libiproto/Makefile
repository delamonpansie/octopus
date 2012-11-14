CLDIR=client/libiproto
libobj += $(CLDIR)/iproto.o $(CLDIR)/errcode.o
dep += $(call depend,$(libobj))

libiproto_all: $(CLDIR)/libiproto.a
$(CLDIR)/libiproto.a: $(libobj) third_party/qsort_arg.o
	$(E) "AR	$@"
	$(Q)$(AR) $(ARFLAGS) $@ $^
	$(E) "RANLIB	$@"
	$(Q)$(RANLIB) $@

clean: clean_lib
clean_lib:
	$(E) "CLEAN	$(CLDIR)"
	$(Q)rm -f $(libobj)
	$(Q)rm -f $(CLDIR)/libiproto.a
