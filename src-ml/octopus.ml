module Arg = Arg
module ArrayLabels = ArrayLabels
module CamlinternalOO = CamlinternalOO
module Complex = Complex
module Format = Format
module Gc = Gc
module Genlex = Genlex
module Lazy = Lazy
module Lexing = Lexing
module ListLabels = ListLabels
module MoreLabels = MoreLabels
module Oo = Oo
module Parsing = Parsing
module Queue = Queue
module Scanf = Scanf
module Set = Set
(* module Sort = Sort *)
module Stack = Stack
module StdLabels = StdLabels
module Stream = Stream
module StringLabels = StringLabels
module Weak = Weak
module Batteries = Batteries

type objc
type oct_obj
type tbuf

exception IProto_Failure of int * string


let load_plugin file_name =
  let err = Say.error "Error while loading plugin '%s': %s" file_name in
  try
    Dynlink.loadfile file_name
  with
    Dynlink.Error e -> err (Dynlink.error_message e)
  | e -> err (Printexc.to_string e)

let _ =
  Callback.register_exception "exn_failure" (Failure "");
  Callback.register_exception "exn_iproto_failure" (IProto_Failure (0, ""));
  Callback.register "load_plugin" load_plugin
