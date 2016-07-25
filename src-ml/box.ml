open Printf

type box
external box_shard : int -> box = "stub_box_shard"

module Hashtbl = Hashtbl.Make (struct
    type t = string
    let equal (a:string) (b:string) = a = b
    let hash = Hashtbl.hash
  end)

let registry = Hashtbl.create 10

let assert_count proc_name args n =
  if Array.length args != n then begin
    Say.error "invalid argument count for %s: want %i, got %i" proc_name n (Array.length args);
    raise (Octopus.IProto_Failure (0x2702, "Invalid argument count"))
  end

let register_cb0 name cb =
  Hashtbl.replace registry name (fun args -> assert_count name args 0;
                                  cb ())
let register_cb1 name cb =
  Hashtbl.replace registry name (fun args -> assert_count name args 1;
                                  cb args.(0))
let register_cb2 name cb =
  Hashtbl.replace registry name (fun args -> assert_count name args 2;
                                  cb args.(0) args.(1))
let register_cb3 name cb =
  Hashtbl.replace registry name (fun args -> assert_count name args 3;
                                  cb args.(0) args.(1) args.(2))
let register_cb4 name cb =
  Hashtbl.replace registry name (fun args -> assert_count name args 4;
                                  cb args.(0) args.(1) args.(2) args.(3))
let register_cb5 name cb =
  Hashtbl.replace registry name (fun args -> assert_count name args 5;
                                  cb args.(0) args.(1) args.(2) args.(3) args.(4))
let register_cbN name cb =
  Hashtbl.replace registry name cb

let dispatch (wbuf, request) (name:string) (args:string array) =
  let cb = Hashtbl.find registry name in
  try
    let out = cb args in
    let iproto = Net_io.reply wbuf request in
    Net_io.add_i32 wbuf (List.length out);
    List.iter (fun tup -> Box_tuple.net_add wbuf tup) out;
    Net_io.fixup wbuf iproto
  with
    Octopus.IProto_Failure (code, msg) -> Net_io.error wbuf request code msg
  | e -> begin
      let open Printexc in
      Say.error "Exception in %s : %s\nBacktrace: %s" name (to_string e) (get_backtrace ());
      Net_io.error wbuf request 0x2702 (sprintf "Exception: %s" (to_string e))
    end

external stub_get_affected_obj : unit -> Octopus.oct_obj = "stub_get_affected_obj" [@@noalloc]
let get_affected_tuple () =
  try Some (Box_tuple.of_oct_obj (stub_get_affected_obj ()))
  with Not_found -> None

let _ =
  Callback.register "box_dispatch" dispatch
