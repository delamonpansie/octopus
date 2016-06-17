ifneq ($(OCAMLC),)
OCAML = 1

OCAMLLIB := $(dir $(shell $(OCAMLC) -where))
XCFLAGS += -I$(OCAMLLIB)/ocaml -DCAML_NAME_SPACE
XLDFLAGS += -L$(OCAMLLIB)/ocaml
XLIBS += -lunix -lasmrun -lcamlstr -lnums -lbigarray

OCAMLINC += -I src-ml -I +../batteries 
OCAMLLIBS += dynlink.cmxa str.cmxa unix.cmxa bigarray.cmxa nums.cmxa batteries.cmxa

OCAMLFLAGS += -annot

cfg_tmpl += src-ml/ocaml.cfg_tmpl

obj += src-ml/caml.o
obj += src-ml/plugin.o

obj += src-ml/exc_stubs.o
obj += src-ml/say_stubs.o
obj += src-ml/fiber_stubs.o
obj += src-ml/net_io_stubs.o
obj += src-ml/packer_stubs.o
obj += src-ml/index_stubs.o

cmx += src-ml/say.cmx
cmx += src-ml/fiber.cmx
cmx += src-ml/net_io.cmx
cmx += src-ml/packer.cmx
cmx += src-ml/octopus.cmx
cmx += src-ml/load_plugins.cmx

sorted-ml = $(shell $(OCAMLDEP) $(OCAMLINC) -sort $(cmx:.cmx=.ml))
sorted-cmx = $(sorted-ml:.ml=.cmx)

%.cmx: %.ml %.mli
	$(E) "CAML	$@"
	$(Q)$(OCAMLOPT) -g $(OCAMLINC) $(OCAMLFLAGS) $(OCAMLIFLAGS) -c $(<:.ml=.mli)
	$(Q)$(OCAMLOPT) -g $(OCAMLINC) $(OCAMLFLAGS) -c $<

%.cmx: %.ml
	$(E) "CAML	$@"
	$(Q)$(OCAMLOPT) -g $(OCAMLINC) $(OCAMLFLAGS) -c $<

%.i.d: %.mli	
	@$(OCAMLDEP) -native -one-line $(OCAMLINC) $< | \
	$(SED) '/^[^ ]\+\.cmx : /{s/\.cmi\>/.cmx/g; p; s/.*\.cmx : //; s/ \|$$/:\n/g}' | \
	$(SED) 's/^\([^ ]\+\.cmx\) : \(.*\)\1\(.*\)/\1 : \2\3/' > $(<:.mli=.i.d)

%.d: %.ml
	@$(OCAMLDEP) -native -one-line $(OCAMLINC) $< | \
	$(SED) '/^[^ ]\+\.cmx : /{s/\.cmi\>/.cmx/g; p; s/.*\.cmx : //; s/ \|$$/:\n/g}' | \
	$(SED) 's/^\([^ ]\+\.cmx\) : \(.*\)\1\(.*\)/\1 : \2\3/'> $(<:.ml=.d)

clean: mlclean
mlclean:
	$(E) "CLEAN	ml"
	$(Q)rm -f $(cmx) $(cmx:.cmx=.cmi) $(cmx:.cmx=.o) $(cmx:.cmx=.annot)


.SECONDEXPANSION:
src-ml/caml.o: $$(cmx)
	$E "CAML	$@"
	$Q$(OCAMLOPT) -g $(OCAMLINC) $(OCAMLFLAGS) $(OCAMLLIBS) $(DERIVE) $(sorted-cmx) -output-obj -o $@
endif
