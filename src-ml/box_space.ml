type 'a key_info =  {
  pack: Box_index.objc_ptr -> 'a -> unit;
  tuple_of_key: 'a -> Box_tuple.t;
}

type 'a obj_space = { box: Box.box;
                      no: int;
                      pk_key: 'a key_info;
                      pk: 'a Box_index.t }

let obj_space_pk {pk} = pk
let box_find {pk} key = Box_index.index_find pk key

external stub_obj_space_index : Box.box -> int -> int -> Box_index.objc_ptr = "stub_obj_space_index"

let obj_space box no pk_key =
  let pk_ptr box no = stub_obj_space_index box no 0 in
  let pk = Box_index.mk (pk_ptr box no) pk_key.pack in
  { box; no; pk; pk_key }

let obj_space_index {box; no} idx {pack} =
  let idx_ptr = stub_obj_space_index box no idx in
  Box_index.mk idx_ptr pack

let packer_of_field (type a) (ty: a Box_index.field) : (a -> Box_tuple.field) =
  match ty with
    Box_index.NUM16 -> fun v -> Box_tuple.I16 v
  | Box_index.NUM32 -> fun v -> Box_tuple.I32 v
  | Box_index.NUM64 -> fun v -> Box_tuple.I64 v
  | Box_index.STRING -> fun v -> Box_tuple.Bytes v

let key_info1 (t0 as ty) =
  let pack = Box_index.pack1 ty in
  let pack_field0 = packer_of_field t0 in
  let tuple_of_key = fun (key0) -> Box_tuple.Gc [ pack_field0 key0 ] in
  { pack; tuple_of_key; }

let key_info2 ((t0, t1) as ty) =
  let pack_field0 = packer_of_field t0 in
  let pack_field1 = packer_of_field t1 in
  let tuple_of_key = fun (k0, k1) -> Box_tuple.Gc [ pack_field0 k0; pack_field1 k1 ] in
  let pack = Box_index.pack2 ty in
  { pack; tuple_of_key }

let key_info3 ((t0, t1, t2) as ty) =
  let pack_field0 = packer_of_field t0 in
  let pack_field1 = packer_of_field t1 in
  let pack_field2 = packer_of_field t2 in
  let tuple_of_key = fun (k0, k1, k2) -> Box_tuple.Gc [ pack_field0 k0; pack_field1 k1;
                                                        pack_field2 k2 ] in
  let pack = Box_index.pack3 ty in
  { pack; tuple_of_key }

let key_info4 ((t0, t1, t2, t3) as ty) =
  let pack_field0 = packer_of_field t0 in
  let pack_field1 = packer_of_field t1 in
  let pack_field2 = packer_of_field t2 in
  let pack_field3 = packer_of_field t3 in
  let tuple_of_key = fun (k0, k1, k2, k3 ) -> Box_tuple.Gc [ pack_field0 k0; pack_field1 k1;
                                                             pack_field2 k2; pack_field3 k3 ] in
  let pack = Box_index.pack4 ty in
  { pack; tuple_of_key }


let box_insert {box; no} tuple = Box_op.insert box no tuple
let box_replace {box; no} tuple = Box_op.replace box no tuple
let box_add {box; no} tuple = Box_op.add box no tuple
let box_delete {box; no; pk_key = { tuple_of_key }} key = Box_op.delete box no (tuple_of_key key)
let box_update {box; no; pk_key = { tuple_of_key }} key mops = Box_op.update box no (tuple_of_key key) mops
let box_get_affected_tuple = Box_op.get_affected_tuple
