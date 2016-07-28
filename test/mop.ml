(* ocamlopt.opt -O3 -g -annot -I . -I +../batteries -shared -ccopt "-Wl,-Bsymbolic -Wl,-z,now" comter3.ml -o comet3.cmxs  *)

open Batteries
open Box1
module P = Packer


module Descr = struct
    type key = int
    let obj_space_no = 0
    let index_no = 0
    let node_pack = Index.node_pack_u32
    let tuple_of_key key = Tuple.(of_list [I32 key])
end

module O = ObjSpace.Make(Descr)

let mop () =
  O.update 1 [Set (1, "1")];
  O.update 1 [Set (1, "22")];
  O.update 1 [Set (1, "33")];
  O.update 1 [Set (1, "44")];
  O.delete 2;
  O.add Tuple.(of_list [I32 2; Bytes "42!"]);
  []

let _ =
  register_cb0 "user_proc.mop" mop;
  Box1.Say.info "OCAML test proc loaded"
