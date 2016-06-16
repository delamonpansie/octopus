open Printf

type box
external box_shard : int -> box = "stub_box_shard"

let registry = Hashtbl.create 10

let assert_count proc_name args n =
  if Array.length args != n then begin
    Say.error "invalid argument count for %s: want %i, got %i" proc_name n (Array.length args);
    raise (Octopus.IProto_Failure (0x2702, "Invalid argument count"))
  end

let register_cb0 name ctx cb =
  Hashtbl.replace registry name (fun box args -> assert_count name args 0;
                                  cb (ctx box))
let register_cb1 name ctx cb =
  Hashtbl.replace registry name (fun box args -> assert_count name args 1;
                                  cb (ctx box) args.(0))
let register_cb2 name ctx cb =
  Hashtbl.replace registry name (fun box args -> assert_count name args 2;
                                  cb (ctx box) args.(0) args.(1))
let register_cb3 name ctx cb =
  Hashtbl.replace registry name (fun box args -> assert_count name args 3;
                                  cb (ctx box) args.(0) args.(1) args.(2))
let register_cb4 name ctx cb =
  Hashtbl.replace registry name (fun box args -> assert_count name args 4;
                                  cb (ctx box) args.(0) args.(1) args.(2) args.(3))
let register_cb5 name ctx cb =
  Hashtbl.replace registry name (fun box args -> assert_count name args 5;
                                  cb (ctx box) args.(0) args.(1) args.(2) args.(3) args.(4))
let register_cbN name ctx cb = Hashtbl.replace registry name
    (fun box args -> cb (ctx box) args)

let box_dispatch (wbuf, request, (box:box)) (name:string) (args:string array) =
  let cb = Hashtbl.find registry name in
  try
    let out = cb box args in
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

let _ =
  Callback.register "box_dispatch" box_dispatch
