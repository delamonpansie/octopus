let expires_per_second = 1000
let batch_size = 100


include Box_space
include Box_index

let hash_loop pk pred ini =
  let slots = index_slots pk in
  let batch = ref [] in
  let i = ref ini in
  let j = ref batch_size in
  while !i < slots && !j > 0 do
    (try
       match pred (index_get pk !i) with
         Some key -> batch := key :: !batch
       | None -> ()
     with Not_found -> ());
    incr i;
    decr j;
  done;
  match !batch with
    [] -> None, []
  | list -> Some !i, list

let tree_loop pk pred ini =
  let rec loop i batch =
    match pred (iterator_next pk 1) with
      Some key  ->
        if i = 1
        then key :: batch
        else loop (i - 1) (key :: batch)
    | None -> loop i batch
    | exception Not_found -> batch in
  iterator_init pk ini Iter_forward;
  match loop batch_size [] with
    hd :: tl -> Some (Iter_key hd), hd :: tl
  | [] -> None, []

(* every space modification must be done _outside_ of iterator running *)
let delete space batch =
  let cb key =
    try box_delete space key
    with Octopus.IProto_Failure (code, reason) ->
      Say.warn "delete failed: %s" reason in
  List.iter cb batch

let delay batch =
  Fiber.sleep (float ((List.length batch + 1) * batch_size) /.
               float ((batch_size + 1) * expires_per_second))

type expire_state = Running | Stop | Empty
let state = ref Empty

let loop obj_space pred =
  let pk = obj_space_pk obj_space in
  let rec aux inner_loop ini =
    if !state = Running then
      match inner_loop pk pred ini with
        Some next, batch -> begin
          delete obj_space batch;
          delay batch;
          aux inner_loop next
        end
      | None, _ -> () in
  match index_type pk  with
    HASH | NUMHASH -> aux hash_loop 0
  | _ -> aux tree_loop Iter_empty


external stub_next_primary_box : unit -> Box.box = "stub_box_next_primary_shard"

let expire no key_info pred info =
  (try
     while !state <> Empty do
       if !state = Running then
         state := Stop;
       Fiber.sleep 1.0
     done
   with Not_found -> ());
  Fiber.create (fun (no, key_info, pred) ->
      state := Running;
      while !state = Running do
        (try
           let box = stub_next_primary_box () in
           let obj_space = Box_space.obj_space box no key_info in
           loop obj_space pred;
         with e -> Say.warn "%s" (Printexc.to_string e));
        Fiber.sleep 1.0
      done;
      state := Empty;
    ) (no, key_info, pred)

