open Packer

type mop = | Set16 of (int * int) | Set32 of (int * int) | Set64 of (int * Int64.t)
           | Add16 of (int * int) | Add32 of (int * int) | Add64 of (int * Int64.t)
           | And16 of (int * int) | And32 of (int * int) | And64 of (int * Int64.t)
           | Or16  of (int * int) | Or32  of (int * int) | Or64  of (int * Int64.t)
           | Xor16 of (int * int) | Xor32 of (int * int) | Xor64 of (int * Int64.t)
           | Set of (int * bytes) | Splice of int | Delete of int
           | Insert of (int * bytes)


let msg_nop = 1
let msg_insert = 13
let msg_update_fields = 19
let msg_delete = 21

external dispatch : Box.box -> int -> Packer.t -> unit = "stub_box_dispatch"

let pack_tuple pa tuple =
  let open Box_tuple in
  let cardinal, _ = tuple_cardinal_and_bsize tuple in
  Packer.add_i32 pa cardinal;
  match tuple with
    Heap o ->
      for i = 0 to cardinal - 1 do
        Packer.add_bytes pa (heap_tuple_field o FRaw i)
      done
  | Gc o ->
    let rec pack pa f =
      let open Packer in
      match f with
        I8 v -> add_i8 pa 1; add_i8 pa v
      | I16 v -> add_i8 pa 2; add_i16 pa v
      | I32 v -> add_i8 pa 4; add_i32 pa v
      | I64 v -> add_i8 pa 8; add_i64 pa v
      | Bytes v -> add_field_bytes pa v
      | Field (Heap o, n) -> add_bytes pa (heap_tuple_field o FRaw n)
      | Field (Gc o, n) -> pack pa (List.nth o n)
      | FieldRange _ -> failwith "not implemented" in
    List.iter (pack pa) o

let upsert box ?(flags=0) n tuple =
  let pa = create 128 in
  add_i32 pa n;
  add_i32 pa flags;
  pack_tuple pa tuple;
  dispatch box msg_insert pa

let insert box n tuple =
  upsert box ~flags:1 n tuple

let add box n tuple =
  upsert box ~flags:3 n tuple

let replace box n tuple =
  upsert box ~flags:5 n tuple

let delete box n key =
  let pa = create 32 in
  add_i32 pa n;
  add_i32 pa 1; (* flags *)
  pack_tuple pa key;
  dispatch box msg_delete pa

let pack_mop pa mop =
  match mop with
  | Set (idx, v)    -> add_i32 pa idx; add_i8 pa 0; add_field_bytes pa v

  | Set16 (idx, v)  -> add_i32 pa idx; add_i8 pa 0; add_i8 pa 2; add_i16 pa v
  | Set32 (idx, v)  -> add_i32 pa idx; add_i8 pa 0; add_i8 pa 4; add_i32 pa v
  | Set64 (idx, v)  -> add_i32 pa idx; add_i8 pa 0; add_i8 pa 8; add_i64 pa v

  | Add16 (idx, v)  -> add_i32 pa idx; add_i8 pa 1; add_i8 pa 2; add_i16 pa v
  | Add32 (idx, v)  -> add_i32 pa idx; add_i8 pa 1; add_i8 pa 4; add_i32 pa v
  | Add64 (idx, v)  -> add_i32 pa idx; add_i8 pa 1; add_i8 pa 8; add_i64 pa v

  | And16 (idx, v)  -> add_i32 pa idx; add_i8 pa 2; add_i8 pa 2; add_i16 pa v
  | And32 (idx, v)  -> add_i32 pa idx; add_i8 pa 2; add_i8 pa 4; add_i32 pa v
  | And64 (idx, v)  -> add_i32 pa idx; add_i8 pa 2; add_i8 pa 8; add_i64 pa v

  | Or16 (idx, v)   -> add_i32 pa idx; add_i8 pa 3; add_i8 pa 2; add_i16 pa v
  | Or32 (idx, v)   -> add_i32 pa idx; add_i8 pa 3; add_i8 pa 4; add_i32 pa v
  | Or64 (idx, v)   -> add_i32 pa idx; add_i8 pa 3; add_i8 pa 8; add_i64 pa v

  | Xor16 (idx, v)  -> add_i32 pa idx; add_i8 pa 4; add_i8 pa 2; add_i16 pa v
  | Xor32 (idx, v)  -> add_i32 pa idx; add_i8 pa 4; add_i8 pa 4; add_i32 pa v
  | Xor64 (idx, v)  -> add_i32 pa idx; add_i8 pa 4; add_i8 pa 8; add_i64 pa v

  | Splice idx      -> add_i32 pa idx; add_i8 pa 5; failwith "not implemented"

  | Delete idx      -> add_i32 pa idx; add_i8 pa 6; add_i8 pa 0
  | Insert (idx, v) -> add_i32 pa idx; add_i8 pa 7; add_field_bytes pa v

let update box n key mops =
  let count = List.length mops in
  let pa = create (32 + count * 8) in
  add_i32 pa n;
  add_i32 pa 1; (* flags = return tuple *)
  pack_tuple pa key;
  add_i32 pa count;
  List.iter (fun mop -> pack_mop pa mop) mops;
  dispatch box msg_update_fields pa

