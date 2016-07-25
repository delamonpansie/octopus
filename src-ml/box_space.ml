module type Descr = sig
  include Box_index.Descr
  val tuple_of_key : key -> Box_tuple.t
end

module Make (Descr : Descr) = struct
  module PK = Box_index.Make(Descr)
  type tuple = Box_tuple.t

  let find = PK.find
  let insert tuple = Box_op.insert (Box.box_shard (-1)) Descr.obj_space_no tuple
  let replace tuple = Box_op.replace (Box.box_shard (-1)) Descr.obj_space_no tuple
  let add tuple = Box_op.add (Box.box_shard (-1)) Descr.obj_space_no tuple
  let delete key = Box_op.delete (Box.box_shard (-1)) Descr.obj_space_no (Descr.tuple_of_key key)
  let update key mops = Box_op.update (Box.box_shard (-1)) Descr.obj_space_no (Descr.tuple_of_key key) mops
end
