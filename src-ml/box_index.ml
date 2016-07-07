type objc_ptr
type _ field = NUM16 : int field
	     | NUM32 : int field
	     | NUM64 : Int64.t field
	     | STRING : string field

type 'a t = { ptr : objc_ptr;
              node_pack : objc_ptr -> 'a -> unit; }

type index_type = HASH | NUMHASH | SPTREE | FASTTREE | COMPACTTREE | POSTREE
type 'a iter_init = Iter_empty
                  | Iter_key of 'a
                  | Iter_partkey of (int * 'a)
                  | Iter_tuple of Box_tuple.t

type iter_dir = Iter_forward | Iter_backward

external node_pack_field : objc_ptr -> int -> 'a field -> 'a -> unit = "stub_index_node_pack_field"
external node_set_cardinality : int -> unit = "stub_index_node_set_cardinality"

external stub_find_node : objc_ptr -> Octopus.oct_obj = "stub_index_find_node"
external stub_iterator_init_with_direction : objc_ptr -> int -> unit = "stub_index_iterator_init_with_direction"
external stub_iterator_init_with_node_direction : objc_ptr -> int -> unit = "stub_index_iterator_init_with_node_direction"
external stub_iterator_init_with_object_direction : objc_ptr -> Octopus.oct_obj -> int -> unit = "stub_index_iterator_init_with_object_direction"
external stub_iterator_next : objc_ptr -> Octopus.oct_obj = "stub_index_iterator_next"

external stub_index_get : objc_ptr -> int -> Octopus.oct_obj = "stub_index_get"
external stub_index_slots : objc_ptr -> int = "stub_index_slots"
external stub_index_type : objc_ptr -> index_type = "stub_index_type"

let iterator_init { ptr; node_pack } init dir =
  let dir = match dir with
      Iter_forward -> 1
    | Iter_backward -> -1 in
  match init with
    Iter_empty -> stub_iterator_init_with_direction ptr dir
  | Iter_key key -> begin
      node_pack ptr key;
      stub_iterator_init_with_node_direction ptr dir;
    end
  | Iter_partkey (n, key) -> begin
      node_pack ptr key;
      node_set_cardinality n;
      stub_iterator_init_with_node_direction ptr dir
    end
  | Iter_tuple t ->
    stub_iterator_init_with_object_direction ptr (Box_tuple.to_oct_obj t) dir

let iterator_next index =
  Box_tuple.of_oct_obj (stub_iterator_next index.ptr)

let iterator_skip index =
  ignore(stub_iterator_next index.ptr)

let iterator_take index init dir lim =
  let rec loop index = function
      0 -> []
    | n -> match iterator_next index with
        a -> a :: loop index (n - 1)
      | exception Not_found -> [] in
  iterator_init index init dir;
  loop index lim

let index_find { ptr; node_pack } key =
  node_pack ptr key;
  Box_tuple.of_oct_obj (stub_find_node ptr)

let index_get { ptr } slot =
  Box_tuple.of_oct_obj (stub_index_get ptr slot)

let index_slots { ptr } =
  stub_index_slots ptr

let index_type { ptr } =
  stub_index_type ptr

let pack1 t0 ptr v0 =
  node_pack_field ptr 0 t0 v0

let pack2 (t0, t1) ptr (v0, v1) =
  node_pack_field ptr 0 t0 v0;
  node_pack_field ptr 1 t1 v1

let pack3 (t0, t1, t2) ptr (v0, v1, v2) =
  node_pack_field ptr 0 t0 v0;
  node_pack_field ptr 1 t1 v1;
  node_pack_field ptr 2 t2 v2

let pack4 (t0, t1, t2, t3) ptr (v0, v1, v2, v3) =
  node_pack_field ptr 0 t0 v0;
  node_pack_field ptr 1 t1 v1;
  node_pack_field ptr 2 t2 v2;
  node_pack_field ptr 3 t3 v3

let mk ptr node_pack = { ptr; node_pack }
