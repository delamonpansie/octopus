(* ocamlopt.opt -O3 -g -annot -I . -I +../batteries -shared -ccopt "-Wl,-Bsymbolic -Wl,-z,now" comter3.ml -o comet3.cmxs  *)

open Batteries
open Box1
module P = Packer


module S0Descr = struct
    type key = string
    let obj_space_no = 0
    let index_no = 0
    let node_pack = Index.node_pack_string
    let tuple_of_key key = Tuple.(of_list [Bytes key])
end

module S0I1Descr = struct
  type key = string * int * string
  let obj_space_no = 0
  let index_no = 1
  let node_pack index (a, b, c) =
    Index.node_pack_string index a;
    Index.node_pack_int index  b;
    Index.node_pack_string index  c
end

module S1Descr = struct
  type key = int
  let obj_space_no = 1
  let index_no = 0
  let node_pack = Index.node_pack_int
  let tuple_of_key a = Tuple.(of_list [I32 a])
end

module S1I1Descr = struct
  type key = int
  let obj_space_no = 1
  let index_no = 1
  let node_pack = Index.node_pack_int
end

module S1I2Descr = struct
  type key = Int64.t
  let obj_space_no = 1
  let index_no = 2
  let node_pack = Index.node_pack_u64
end

module S1I3Descr = struct
  type key = int
  let obj_space_no = 1
  let index_no = 3
  let node_pack = Index.node_pack_int
end

module S2Descr = struct
    type key  = int * int
    let obj_space_no = 2	(* Space for user sessions *)
    let index_no = 0
    let node_pack index (a,b) = Index.node_pack_int index a; Index.node_pack_int index b
    let tuple_of_key (a,b) = Tuple.(of_list [I32 a; I32 b])
end


module S0 = ObjSpace.Make(S0Descr)
module S0I1 = Index.Make(S0I1Descr)

module S1 = ObjSpace.Make(S1Descr)
module S1I0 = S1.PK
module S1I1 = Index.Make(S1I1Descr)
module S1I2 = Index.Make(S1I2Descr)
module S1I3 = Index.Make(S1I3Descr)

module S2 = ObjSpace.Make(S2Descr)

let test1 () =
  [Tuple.(of_list[Bytes "abc"; Bytes "defg"; Bytes "foobar"]);
   Tuple.(of_list[Bytes "abc"; Bytes "defg"]);
   Tuple.(of_list[Bytes "abc"]);]

let test0 = test1

let test2 () =
  [Tuple.(of_list [Bytes "cdata<const struct BasicIndex *>: 0xPTR";
                   Bytes "cdata<const struct BasicIndex *>: 0xPTR"])]

let test3 () =
  let t = S0.find "11" in
  [Tuple.(of_list [Bytes "cdata<const struct box_small_tuple *>: 0xPTR";
                   Bytes (Tuple.strfield 0 t);
                   Bytes (Tuple.strfield 0 t)])]

let test4 () =
  let k = Array.make 1001 "" in
  for i = 0 to 1000 do
    k.(i) <- string_of_int i
  done;

  let n = ref 0 in

  for i = 0 to 1000 do
    match S0.find k.(i) with
      _ -> incr n
    | exception Not_found -> ()
  done;

  for i = 0 to 1000 do
    match S0I1.find (k.(i), i, k.(i)) with
      _ -> incr n
    | exception Not_found -> ()
  done;

  for i = 0 to 1000 do
    match S0I1.find_dyn [Tuple.Bytes k.(i); Tuple.I32 i] with
      _ -> incr n
    | exception Not_found -> ()
  done;

  [Tuple.(of_list [Bytes (string_of_int !n)])]

let test5 () =
  [S1I0.find 0; S1I1.find 0; S1I2.find 0L; S1I3.find 0]


let test6 () =
  S0.upsert Tuple.(of_list [I32 0; I32 0; I32 0]);
  match get_affected_tuple () with
    Some t -> [t]
  | None -> []


let push_affected q =
  match get_affected_tuple () with
    Some t -> Queue.push t q
  | None -> ()

let nil = Tuple.(of_list [Bytes "nil"])

let test7 _ =
  let q = Queue.create () in
  Queue.push (S0.find "99") q;
  S0.update "99" [Set (2, "9999")];
  push_affected q;
  S0.delete "99";
  push_affected q;
  Queue.push (try S0.find "99" with Not_found -> nil) q;
  Queue.enum q |> List.of_enum

let test8 () =
  let q = Queue.create () in
  Queue.push (S2.find (0, 0)) q;
  S2.update (0, 0) [Set (2, "9999")];
  push_affected q;
  S2.delete (0, 0);
  push_affected q;
  Queue.push (try S2.find (0, 0) with Not_found -> nil) q;
  Queue.enum q |> List.of_enum

let test9 () =
  let q = Queue.create () in
  S2.upsert Tuple.(of_list [I32 0; I32 0; Bytes ""; Bytes ""; Bytes ""]);
  push_affected q;
  Queue.push (S2.find (0, 0)) q;
  S2.update (0,0) [Set16 (2, 0); Set32 (3, 0); Set64 (4, 0L)];
  push_affected q;
  S2.update (0,0) [Add16 (2, 0); Or32 (3, 14); Xor64 (4, 99L)];
  push_affected q;
  Queue.push (S2.find (0, 0)) q;
  Queue.enum q |> List.of_enum


let test10 () =
  S0.replace Tuple.(of_list [I32 0; Bytes "dead"; Bytes "beef"]);
  Fiber.sleep 1.0;
  S0.replace Tuple.(of_list [I32 0; Bytes "dead"; Bytes "beef"]);
  []


let _ =
  let l = ["test0", test0;
            "test1", test1;
            "test2", test2;
            "test3", test3;
            "test4", test4;
            "test5", test5;
            "test6", test6;
            "test7", test7;
            "test8", test8;
            "test9", test9;
            "test10", test10] in
  List.iter (fun (name, cb) -> register_cb0 ("user_proc." ^ name) cb) l;
  Box1.Say.info "OCAML test proc loaded"
