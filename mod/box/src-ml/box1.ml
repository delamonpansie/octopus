(* common modules *)
module Say = Say
module Fiber = Fiber
module Packer = Packer

(* Box API *)
exception IProto_Failure = Octopus.IProto_Failure

include Box
module ObjSpace = Box_space
module Tuple = Box_tuple
module Index = Box_index
type tuple = Tuple.t
type mop = Box_op.mop = Set16 of (int * int)
                      | Set32 of (int * int)
                      | Set64 of (int * Int64.t)
                      | Add16 of (int * int)
                      | Add32 of (int * int)
                      | Add64 of (int * Int64.t)
                      | And16 of (int * int)
                      | And32 of (int * int)
                      | And64 of (int * Int64.t)
                      | Or16 of (int * int)
                      | Or32 of (int * int)
                      | Or64 of (int * Int64.t)
                      | Xor16 of (int * int)
                      | Xor32 of (int * int)
                      | Xor64 of (int * Int64.t)
                      | Set of (int * bytes)
                      | Splice of int
                      | Delete of int
                      | Insert of (int * bytes)
