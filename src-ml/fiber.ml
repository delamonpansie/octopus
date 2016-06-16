external sleep : float -> unit = "stub_fiber_sleep"
external create : ('a -> unit) -> 'a -> unit = "stub_fiber_create"

let loops : (string, (unit -> unit)) Hashtbl.t = Hashtbl.create 2

let loop name cb =
  if Hashtbl.mem loops name then
    Hashtbl.replace loops name cb
  else
    create (fun () ->
        Hashtbl.add loops name cb;
        (try
           while true do
             cb ();
           done
         with e -> Say.error "loop exception %s" (Printexc.to_string e));
        Hashtbl.remove loops name;
      ) ()
