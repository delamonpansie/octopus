type heap_tuple
type field = I8 of int | I16 of int | I32 of int | I64 of Int64.t
           | Bytes of bytes | Field of t * int | FieldRange of t * int * int
and t = Heap of heap_tuple | Gc of field list
type _ ftype = FI8 : int ftype
             | FI16 : int ftype
             | FI32 : Int32.t ftype
             | FI64 : Int64.t ftype
             | FInt : int ftype
             | FStr : string ftype
             | FRaw : string ftype

let heap = function Heap o -> o | Gc _ -> failwith "accesing constructed tuple not implemented"

external heap_tuple_alloc : Octopus.oct_obj -> heap_tuple = "box_tuple_custom_alloc"
external oct_obj_of_heap_tuple : heap_tuple -> Octopus.oct_obj = "stub_box_tuple_obj" [@@noalloc]
external heap_tuple_raw_field_size : heap_tuple -> int -> int = "stub_box_tuple_raw_field_size" [@@noalloc]
external heap_tuple_bsize : heap_tuple -> int = "stub_box_tuple_bsize" [@@noalloc]
external heap_tuple_cardinal : heap_tuple -> int = "stub_box_tuple_cardinality" [@@noalloc]
external heap_tuple_field : heap_tuple -> 'a ftype -> int -> 'a = "stub_box_tuple_field"
external heap_tuple_net_add : Net_io.wbuf -> heap_tuple -> unit = "stub_net_tuple_add" [@@noalloc]

let of_oct_obj o = Heap (heap_tuple_alloc o)  (* will raise Not_found if obj == NULL *)
let to_oct_obj o = oct_obj_of_heap_tuple (heap o)
let of_list a = Gc a

let i8field n tup = heap_tuple_field (heap tup) FI8 n
let i16field n tup = heap_tuple_field (heap tup) FI16 n
let i32field n tup = heap_tuple_field (heap tup) FI32 n
let i64field n tup = heap_tuple_field (heap tup) FI64 n
let numfield n tup = heap_tuple_field (heap tup) FInt n
let strfield n tup = heap_tuple_field (heap tup) FStr n
let rawfield n tup = heap_tuple_field (heap tup) FRaw n


let cardinal = function
    Heap o -> heap_tuple_cardinal o
  | Gc o -> List.length o

let rec tuple_raw_field_size tuple n =
  match tuple with
    Heap o -> heap_tuple_raw_field_size o n
  | Gc o -> gc_tuple_raw_field_size (List.nth o n)
and gc_tuple_raw_field_size = function
    I8 _ -> 1 + 1
  | I16 _ -> 1 + 2
  | I32 _ -> 1 + 4
  | I64 _ -> 1 + 8
  | Bytes b -> let len = Bytes.length b in (Packer.Bytes.varint32_size len) + len
  | Field (t, n) -> tuple_raw_field_size t n
  | FieldRange (t, n, count) -> begin
      let sum = ref 0 in
      for i = 0 to count - 1 do
        sum := !sum + tuple_raw_field_size t (n + 1)
      done;
      !sum
    end

let rec tuple_cardinal_and_bsize = function
    Heap o -> heap_tuple_cardinal o, heap_tuple_bsize o
  | Gc o -> begin
      let cardinal = ref 0 in
      let bsize = ref 0 in
      List.iter (fun f -> incr cardinal;
                          bsize := !bsize + gc_tuple_raw_field_size f) o;
      !cardinal, !bsize
    end

external unsafe_blit_tuple_field : bytes -> int -> heap_tuple -> int -> int -> int = "stub_box_tuple_blit_field" [@@noalloc]

let bytes_of_gc_tuple a =
  let cardinal, bsize = tuple_cardinal_and_bsize (Gc a) in
  let buf = Bytes.create (8 + bsize) in
  let pos = ref 8 in
  let open Packer.Bytes in
  let rec blit_field buf = function
      I8 v -> pos := unsafe_blit_field_i8 buf !pos v
    | I16 v -> pos := unsafe_blit_field_i16 buf !pos v
    | I32 v -> pos := unsafe_blit_field_i32 buf !pos v
    | I64 v -> pos := unsafe_blit_field_i64 buf !pos v
    | Bytes v -> pos := unsafe_blit_field_bytes buf !pos v
    | Field (Heap o, n) -> pos := unsafe_blit_tuple_field buf !pos o n 1
    | Field (Gc o, n) -> blit_field buf (List.nth o n)
    | FieldRange (Heap o, n, count) -> pos := unsafe_blit_tuple_field buf !pos o n count
    | FieldRange (Gc o, n, count) -> List.iter (blit_field buf) (BatList.take count (BatList.drop n o))
  in
  unsafe_blit_i32 buf 0 bsize;
  unsafe_blit_i32 buf 4 cardinal;
  List.iter (blit_field buf) a;
  buf

let net_add wbuf = function
    Heap o -> heap_tuple_net_add wbuf o
  | Gc a -> Net_io.add wbuf (bytes_of_gc_tuple a)
