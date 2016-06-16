open Batteries

let loaded = Hashtbl.create 10

type plugin = { name : string;
                version : int;
                path : string; }

let load {name; path; version} =
  try
    if not (Hashtbl.mem loaded path) then begin
        Say.info "Loading caml plugin %s, version %i" name version;
        Hashtbl.add loaded path true;
        Dynlink.loadfile path
      end
  with
    Dynlink.Error e -> Say.error "Error while loading plugin '%s': %s" path (Dynlink.error_message e)
  | e -> Say.error "Error while loading plugin '%s': %s" path (Printexc.to_string e)

let cmxs = Str.regexp "\\(.*\\)\\.\\([0-9]+\\)\\.cmxs$"
let is_plugin dir_name file_name =
  try
    ignore(Str.search_forward cmxs file_name 0);
    Some { name = Str.matched_group 1 file_name;
           version = int_of_string (Str.matched_group 2 file_name);
           path = dir_name ^ "/" ^ file_name }
  with Not_found -> None

let readdir dir_name =
  Sys.readdir dir_name
  |> Array.to_list
  |> List.filter_map (is_plugin dir_name)

let plugin_loader path =
  let loader pathlist =
    let hash = Hashtbl.create 5 in
    let max a =
      try
        if (Hashtbl.find hash a.name).version < a.version then
          Hashtbl.replace hash a.name a
      with
        Not_found -> Hashtbl.add hash a.name a in
    while true do
      Hashtbl.clear hash;
      List.iter (List.iter max % readdir) pathlist;
      Hashtbl.iter (fun _ v -> load v) hash;
      Fiber.sleep 1.0
    done in
  Fiber.create loader (Str.split (Str.regexp ":") path)

let _ =
  Callback.register "plugin_loader" plugin_loader
