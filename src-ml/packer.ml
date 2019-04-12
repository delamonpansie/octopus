module Bytes = struct
  include Bytes
  external unsafe_int_of_bits : bytes -> int -> int -> int = "stub_int_of_bits" [@@noalloc]
  (** [unsafe_int_of_bits str pos size]  [size] = 1, 2, 4 or 8 *)

  let int8_of_bits buf pos =
    if Bytes.length buf < pos + 1 then
      invalid_arg "int_of_bits";
    unsafe_int_of_bits buf pos 1

  let int16_of_bits buf pos =
    if Bytes.length buf < pos + 2 then
      invalid_arg "int_of_bits";
    unsafe_int_of_bits buf pos 2

  let int32_of_bits buf pos =
    if Bytes.length buf < pos + 4 then
      invalid_arg "int_of_bits";
    unsafe_int_of_bits buf pos 4

  external int64_of_bits : bytes -> int -> Int64.t = "stub_int64_of_bits"
  external bits_of_int16 : int -> bytes = "stub_bits_of_i16"
  external bits_of_int32 : int -> bytes = "stub_bits_of_i32"
  external bits_of_int64 : Int64.t -> bytes = "stub_bits_of_i64"

  external unsafe_blit_i8  : bytes -> int -> int -> unit = "stub_blit_i8" [@@noalloc]
  external unsafe_blit_i16 : bytes -> int -> int -> unit = "stub_blit_i16" [@@noalloc]
  external unsafe_blit_i32 : bytes -> int -> int -> unit = "stub_blit_i32" [@@noalloc]
  external unsafe_blit_i64 : bytes -> int -> Int64.t -> unit = "stub_blit_i64" [@@noalloc]
  external unsafe_blit_varint : bytes -> int -> int -> int = "stub_blit_varint" [@@noalloc]

  external unsafe_blit_field_i8  : bytes -> int -> int -> int = "stub_blit_field_i8" [@@noalloc]
  external unsafe_blit_field_i16 : bytes -> int -> int -> int = "stub_blit_field_i16" [@@noalloc]
  external unsafe_blit_field_i32 : bytes -> int -> int -> int = "stub_blit_field_i32" [@@noalloc]
  external unsafe_blit_field_i64 : bytes -> int -> Int64.t -> int = "stub_blit_field_i64" [@@noalloc]
  external unsafe_blit_field_bytes : bytes -> int -> bytes -> int = "stub_blit_field_bytes" [@@noalloc]

  let blit_i8 str pos n =
    if Bytes.length str < pos + 1 then invalid_arg "blit_i8";
    unsafe_blit_i8 str pos n

  let blit_i16 str pos n =
    if Bytes.length str < pos + 2 then invalid_arg "blit_i16";
    unsafe_blit_i16 str pos n

  let blit_i32 str pos n =
    if Bytes.length str < pos + 4 then invalid_arg "blit_i32";
    unsafe_blit_i32 str pos n

  let blit_i64 str pos n =
    if Bytes.length str < pos + 8 then invalid_arg "blit_i64";
    unsafe_blit_i64 str pos n

  let blit_varint32 str pos n =
    if Bytes.length str < pos + 5 then invalid_arg "blit_ber";
    unsafe_blit_varint str pos n

  external varint32_size : (int [@untagged]) -> (int [@untagged]) = "abort" "varint32_sizeof" [@@noalloc]
end


type t = { mutable buf : bytes;
	   mutable used : int } (* stub_box_dispatch depends on layout of t *)

(* FIXME: вместо Bytes.length pa.buf надо самостоятельно хранить pa.length,
          т.к. вычисления Bytes.length небесплатное *)
let create n = { buf = Bytes.create n; used = 0 }
let clear pa = pa.used <- 0

let contents pa =
  if Bytes.length pa.buf = pa.used then begin
    let v = pa.buf in
    pa.buf <- Bytes.create 0;
    pa.used <- 0;
    v
  end else begin
    let len = pa.used in
    pa.used <- 0;
    Bytes.sub pa.buf 0 len
  end

let need pa n =
  if Bytes.length pa.buf - pa.used < n then begin
    let size = ref (max 16 (Bytes.length pa.buf)) in
    while !size - pa.used < n do
      size := !size * 2
    done;
    let buf' = Bytes.extend pa.buf 0 (!size - Bytes.length pa.buf) in
    pa.buf <- buf'
  end;
  let ret = pa.used in
  pa.used <- pa.used + n;
  ret

let blit {buf} offt s = Bytes.blit s 0 buf offt (Bytes.length s)

let add_i8 pa n  = Bytes.unsafe_blit_i8 pa.buf (need pa 1) n
let add_i16 pa n = Bytes.unsafe_blit_i16 pa.buf (need pa 2) n
let add_i32 pa n = Bytes.unsafe_blit_i32 pa.buf (need pa 4) n
let add_i64 pa n = Bytes.unsafe_blit_i64 pa.buf (need pa 8) n


let add_varint32 pa n =
  let offt = need pa 5 in
  pa.used <- Bytes.unsafe_blit_varint pa.buf offt n

let add_bytes pa s =
  let len = Bytes.length s in
  let offt = need pa len in
  Bytes.unsafe_blit s 0 pa.buf offt len

let add_string pa s =
  let len = String.length s in
  let offt = need pa len in
  String.unsafe_blit s 0 pa.buf offt len

let add_packer pa {buf; used} =
  let offt = need pa used in
  Bytes.blit buf 0 pa.buf offt used

let add_field_bytes pa s =
  let offt = need pa (5 + Bytes.length s) in
  pa.used <- Bytes.unsafe_blit_field_bytes pa.buf offt s

let blit_i8 {buf} offt n = Bytes.blit_i8 buf offt n
let blit_i16 {buf} offt n = Bytes.blit_i16 buf offt n
let blit_i32 {buf} offt n = Bytes.blit_i32 buf offt n
let blit_i64 {buf} offt n = Bytes.blit_i64 buf offt n
let blit_varint32 {buf} offt n = Bytes.blit_varint32 buf offt n
let blit_bytes {buf} offt n = Bytes.blit buf offt n


let bits_of_int64 = Bytes.bits_of_int64
let bits_of_int32 = Bytes.bits_of_int32
let bits_of_int16 = Bytes.bits_of_int16

let int64_of_bits = Bytes.int64_of_bits
let int32_of_bits = Bytes.int32_of_bits
let int16_of_bits = Bytes.int16_of_bits
let int8_of_bits = Bytes.int8_of_bits


let hexdump {buf;used} =
  if used = 0 then
    ""
  else
    let str = Buffer.create 100 in
    for i = 0 to used - 2 do
      Printf.bprintf str "%02x " (Char.code (Bytes.get buf i))
    done;
    Printf.bprintf str "%02x" (Char.code (Bytes.get buf (used - 1)));
    Buffer.contents str
