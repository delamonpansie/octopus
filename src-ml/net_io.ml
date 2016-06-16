type wbuf
type request
type reply

external reply : wbuf -> request -> reply = "stub_net_io_reply" [@@noalloc]
external fixup : wbuf -> reply -> unit = "stub_net_io_fixup" [@@noalloc]
external error : wbuf -> request -> int -> string -> unit = "stub_net_io_error" [@@noalloc]

external blit : wbuf -> bytes -> int -> int -> unit  = "stub_net_io_blit_bytes" [@@noalloc]
external add : wbuf -> bytes -> unit  = "stub_net_io_add_bytes" [@@noalloc]
external add_i8 : wbuf -> int -> unit  = "stub_net_io_add_i8" [@@noalloc]
external add_i16 : wbuf -> int -> unit  = "stub_net_io_add_i16" [@@noalloc]
external add_i32 : wbuf -> int -> unit  = "stub_net_io_add_i32" [@@noalloc]
external add_i64 : wbuf -> int -> unit  = "stub_net_io_add_i64" [@@noalloc]
