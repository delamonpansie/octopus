external stub_error : string -> unit = "stub_say_error" [@@noalloc]
external stub_warn : string -> unit = "stub_say_warn" [@@noalloc]
external stub_info : string -> unit = "stub_say_info" [@@noalloc]
external stub_debug : string -> unit = "stub_say_debug" [@@noalloc]

let error a = Printf.ksprintf stub_error a
let warn a = Printf.ksprintf stub_warn a
let info a = Printf.ksprintf stub_info a
let debug a = Printf.ksprintf stub_debug a
