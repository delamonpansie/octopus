type index
type index_type = HASH
                | NUMHASH
                | SPTREE
                | FASTTREE
                | COMPACTTREE
                | POSTREE
type iter_dir = Iter_forward | Iter_backward

external node_pack_u16 : index -> int -> unit = "stub_index_node_pack_u16"
external node_pack_u32 : index -> int -> unit = "stub_index_node_pack_u32"
external node_pack_u64 : index -> Int64.t -> unit = "stub_index_node_pack_u64"
external node_pack_string : index -> string -> unit = "stub_index_node_pack_string"

module type Descr = sig
  type key
  val obj_space_no : int
  val index_no: int
  val node_pack : index -> key -> unit
end

module MakeInternal (Descr : Descr) = struct
  type iter_init = Iter_empty
                 | Iter_key of Descr.key
                 | Iter_partkey of (int * Descr.key)
                 | Iter_tuple of Box_tuple.t


  external node_pack_begin : index -> unit = "stub_index_node_pack_begin"

  external node_set_cardinality : int -> unit = "stub_index_node_set_cardinality"

  external stub_find_node : index -> Octopus.oct_obj = "stub_index_find_node"
  external stub_iterator_init_with_direction : index -> int -> unit = "stub_index_iterator_init_with_direction"
  external stub_iterator_init_with_node_direction : index -> int -> unit = "stub_index_iterator_init_with_node_direction"
  external stub_iterator_init_with_object_direction : index -> Octopus.oct_obj -> int -> unit = "stub_index_iterator_init_with_object_direction"
  external stub_iterator_next : index -> Octopus.oct_obj = "stub_index_iterator_next"

  external stub_index_get : index -> int -> Octopus.oct_obj = "stub_index_get"
  external stub_index_slots : index -> int = "stub_index_slots"
  external stub_index_type : index -> index_type = "stub_index_type"

  let iterator_init ptr init dir =
    let dir = match dir with
        Iter_forward -> 1
      | Iter_backward -> -1 in
    match init with
      Iter_empty -> stub_iterator_init_with_direction ptr dir
    | Iter_key key -> begin
        node_pack_begin ptr;
        Descr.node_pack ptr key;
        stub_iterator_init_with_node_direction ptr dir;
      end
    | Iter_partkey (n, key) -> begin
        node_pack_begin ptr;
        Descr.node_pack ptr key;
        node_set_cardinality n;
        stub_iterator_init_with_node_direction ptr dir
      end
    | Iter_tuple t ->
      stub_iterator_init_with_object_direction ptr (Box_tuple.to_oct_obj t) dir

  let iterator_next ptr =
    Box_tuple.of_oct_obj (stub_iterator_next ptr)

  let iterator_skip ptr =
    ignore(stub_iterator_next ptr)

  let iterator_take index init dir lim =
    let rec loop index = function
        0 -> []
      | n -> match iterator_next index with
          a -> a :: loop index (n - 1)
        | exception Not_found -> [] in
    iterator_init index init dir;
    loop index lim

  let find ptr key =
    node_pack_begin ptr;
    Descr.node_pack ptr key;
    Box_tuple.of_oct_obj (stub_find_node ptr)

  let find_dyn ptr tuple =
    let rec pack ptr = function
        Box_tuple.I8 _ -> raise (Invalid_argument "find_by_tuple")
      | Box_tuple.I16 v -> node_pack_u16 ptr v
      | Box_tuple.I32 v -> node_pack_u32 ptr v
      | Box_tuple.I64 v -> node_pack_u64 ptr v
      | Box_tuple.Bytes v -> node_pack_string ptr v
      | Box_tuple.Field (t, n) -> node_pack_string ptr (Box_tuple.strfield n t)
      | Box_tuple.FieldRange (t, n, count) -> begin
          for i = n to n + count - 1 do
            node_pack_string ptr (Box_tuple.strfield (n + i) t)
          done
        end in
    node_pack_begin ptr;
    List.iter (pack ptr) tuple;
    Box_tuple.of_oct_obj (stub_find_node ptr)

  let get ptr slot =
    Box_tuple.of_oct_obj (stub_index_get ptr slot)

  let slots ptr =
    stub_index_slots ptr

  let typ ptr =
    stub_index_type ptr
end

module Make (Descr : Descr) = struct
  module Index = MakeInternal(Descr)

  type iter_init = Index.iter_init = Iter_empty
                                   | Iter_key of Descr.key
                                   | Iter_partkey of (int * Descr.key)
                                   | Iter_tuple of Box_tuple.t

  external index : int -> int -> index = "stub_obj_space_index"

  let iterator_init init dir = Index.iterator_init (index Descr.obj_space_no Descr.index_no) init dir
  and iterator_next () = Index.iterator_next (index Descr.obj_space_no Descr.index_no)
  and iterator_skip () = Index.iterator_skip (index Descr.obj_space_no Descr.index_no)
  and iterator_take init dir n = Index.iterator_take (index Descr.obj_space_no Descr.index_no) init dir n
  and find key = Index.find (index Descr.obj_space_no Descr.index_no) key
  and find_dyn list = Index.find_dyn (index Descr.obj_space_no Descr.index_no) list
  and get n = Index.get (index Descr.obj_space_no Descr.index_no) n
  and slots () = Index.slots (index Descr.obj_space_no Descr.index_no)
  and typ () = Index.typ (index Descr.obj_space_no Descr.index_no)
end
