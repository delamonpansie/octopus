module Say : sig
  val error : ('a, unit, string, unit) format4 -> 'a
  val warn : ('a, unit, string, unit) format4 -> 'a
  val info : ('a, unit, string, unit) format4 -> 'a
  val debug : ('a, unit, string, unit) format4 -> 'a

  (** Printf.printf аналоги для печати в журнал октопуса *)
end

module Fiber : sig
  external create : ('a -> unit) -> 'a -> unit = "stub_fiber_create"
  (** [create cb arg] запускает фибер и выполняет внутри него
      [cb arg] *)

  external sleep : float -> unit = "stub_fiber_sleep"
  (** [sleep delay]приостанаваливает выполнение на [delay]
      секунд. Другие фиберы продолжат испольнятся *)

  val loop : string -> (unit -> unit) -> unit
  (** [loop name cb] создает фибер и вызывает в бесконечном цикле
      cb (), при повторном вызове с тем же [name] заменяет [cb] в
      существующем фибере. Замена произходит после того, как [cb]
      вернет управление. Поэтому не стоит застревать в нем очень
      надолго. *)
end

module Packer : sig
  type t
  (** Автоматически расширяемый буфер, предназначенный для упаковки
      бинарных данных. *)

  val create : int -> t
  (** [create n] создает пустой packer. Параметр [n] указывает,
      сколько будет предвыделнно байт в буфере. Для оптимальной
      производительности он должен быть примерно равен результирующему
      размеру *)

  val contents : t -> bytes
  (** Возвращает копию содержимого packer *)

  val clear : t -> unit
  (** [clear pa] очищает packer. Внутренний буфер при этом не
      освобождается *)

  val need : t -> int -> int
  (** [need pa size] резервирует [size] байт во внутреннем
      буфере и возвращает смещение на их начало. Данное смещение потом
      можно использовать как аргумент [pos] в семействе функций
      Packer.blit_{i8,i16,i32,i64,ber} *)

  val blit_i8 : t -> int -> int -> unit
  (** [blit_i8 pa pos n] сохраняет [n] как 1-байтное число по
      смещению [pos]. *)

  val blit_i16 : t -> int -> int -> unit
  (** [blit_i16 pa pos n] сохраняет [n] как 2-байтное число по
      смещению [pos] *)

  val blit_i32 : t -> int -> int -> unit
  (** [blit_i32 pa pos n] сохраняет [n] как 4-байтоное число по
      смещению [pos] *)

  val blit_i64 : t -> int -> Int64.t -> unit
  (** [blit_i64 pa pos n] сохраняет [n] как 8-байтное число по
      смещению [pos] *)

  val blit_varint32 : t -> int -> int -> int
  (** [blit_varint32 pa pos n] сохраняет [n] как число в формате BER
      (perl pack 'w') по смещению [pos] *)

  val blit_bytes : t -> int -> bytes -> int -> int -> unit
  (** [blit_bytes pa pos src srcoff len] копирует [len] байтов из [src] по смещению
      [srcoff] в буфер по смещению [pos] *)

  val add_i8 : t -> int -> unit
  (** [add_i8 pa n] дописывает [n] в конец буфера как 1-байтное
      число *)

  val add_i16 : t -> int -> unit
  (** [add_i16 pa n] дописывает [n] в конец буфера как
      2-байтное число *)

  val add_i32 : t -> int -> unit
  (** [add_i32 pa n] дописывает [n] в конец буфера как
      4-байтное число *)

  val add_i64 : t -> Int64.t -> unit
  (** [add_i64 pa n] дописывает [n] в конец буфера как
      8-байтное число *)

  val add_varint32 : t -> int -> unit
  (** [add_varint32 pa n] дописывает [n] в конец буфера как число в
      формате BER (perl pack 'w') *)

  val add_bytes : t -> bytes -> unit
  (** [add_bytes pa bytes] дописывет содержимое [bytes] в конец
      буфера *)

  val add_packer : t -> t -> unit
  (** [add_packer pa pa2] дописывет содержимое другого packer в
      конец буфера *)

  val add_field_bytes : t -> bytes -> unit
  (** [add_field_bytes pa bytes] дописывает [bytes], в виде поля
      кортежа octopus/silverbox, то есть сперва длина [bytes]
      в закодированная в BER, а потом содержимое [bytes] *)

  val int8_of_bits : bytes -> int -> int
  (** [int_of_bits bytes pos] преобразует 1 байт по смещению
      [pos] в число, так как это бы сделал сделующий C код:
      *(int8_t * )([bytes] + [pos]) *)

  val int16_of_bits : bytes -> int -> int
  (** [int_of_bits bytes pos] преобразует 2 байта по смещению
      [pos] в число, так как это бы сделал сделующий C код:
      *(int16_t * )([bytes] + [pos]) *)

  val int32_of_bits : bytes -> int -> int
  (** [int_of_bits bytes pos] преобразует 4 байта по смещению
      [pos] в число, так как это бы сделал сделующий C код:
      *(int32_t * )([bytes] + [pos]) *)

  val int64_of_bits : bytes -> int -> Int64.t
  (** [int64_of_bits bytes pos] преобразует 8 байт по смещению
      [pos] в число, так как это бы сделал сделующий C код:
      *(int64_t * )([bytes] + [pos]) *)

  val bits_of_int16 : int -> bytes
  (** [bits_of_int16 n] возвращает 2-байтовое представление [n]. Если
      число не влезает в 2 байта, он будет обрезано *)

  val bits_of_int32 : int -> bytes
  (** [bits_of_int32 n] возвращает 4-байтовое представление [n]. Если
      число не влезает в 4-байта, он будет обрезано *)

  val bits_of_int64 : Int64.t -> bytes
  (** [bits_of_int64 n] возвращает 8-байтовое представление [n]. *)
end

type box
(** абстрактный тип микрошарда. Может использоваться только внутри
    коллбека, попытка сохранить его где-нибудь и использовать вне коллбека
    приведет к SEGV *)

type tuple
(** абстракнтый тип кортежа, который хранится в box. Для доступа к
    полям надо использовать соответствующие аккцесоры из module
    Tuple *)

exception IProto_Failure of int * string
(** [IProto_Failure of code * reason] в это исключение преобразуются
    ObjC исключение IProtoError *)


external box_shard : int -> box = "stub_box_shard"
(** [box_shard n] возвращает ushard [n]. *)

module Tuple : sig
  type field = I8 of int | I16 of int | I32 of int | I64 of Int64.t
             | Bytes of bytes | Field of tuple * int | FieldRange of tuple * int * int

  val of_list : field list -> tuple
  (** [of_list ;] преобразует список значений типа [field] в
      кортеж *)

  val cardinal : tuple -> int
  (** [cardinal возвращает количество полей в кортеже. *)

  val i8field : int -> tuple -> int
  (** [i8field tuple idx] возвращает числовое значение
      1-байтного поля, если длина поля не равна 1, то кидает
      исключение *)

  val i16field : int -> tuple -> int
  (** [i16field tuple idx] возвращает числовое значение
      2-байтного поля, если длина поля не равна 2, то кидает
      исключение *)

  val i32field : int -> tuple -> Int32.t
  (** [i32field tuple idx] возвращает числовое значение
      4-байтного поля, если длина поля не равна 4, то кидает
      исключение *)

  val i64field : int -> tuple -> Int64.t
  (** [i64field tuple idx] возвращает числовое значение
      8-байтного поля, если длина поля не равна 8, то кидает
      исключение *)

  val numfield : int -> tuple -> int
  (** [numfield tuple idx] возвращает числовое значение 1,2,4
      или 8-байтного поля, если длина поля не равна 1,2,4 или 8, то
      кидает исключение. 64 битное значение обрезается до размерности
      int *)

  val strfield : int -> tuple -> bytes
  (** [strfield tuple idx] возвращает байтовое представление поля *)

  val rawfield : int -> tuple -> bytes
  (** [strfield tuple idx] возвращает байтовое представление
      поля включая (!) BER-закодированную длину *)
end

module Index : sig
  type index
  type index_type = HASH
                  | NUMHASH
                  | SPTREE
                  | FASTTREE
                  | COMPACTTREE
                  | POSTREE

  type iter_dir = Iter_forward | Iter_backward
  (** направление итератора. Iter_backward поддерживается только для
      деревьев. *)

  external node_pack_u16 : index -> int -> unit = "stub_index_node_pack_u16"
  external node_pack_u32 : index -> int -> unit = "stub_index_node_pack_u32"
  external node_pack_u64 : index -> Int64.t -> unit = "stub_index_node_pack_u64"
  external node_pack_string : index -> string -> unit = "stub_index_node_pack_string"

  module type Descr = sig
    type key
    val obj_space_no : int
    val index_no : int
    val node_pack : index -> key -> unit
  end


  module Make : functor (Descr : Descr) -> sig
    type iter_init = Iter_empty
                   | Iter_key of Descr.key
                   | Iter_partkey of (int * Descr.key)
                   | Iter_tuple of tuple
    (** алгебраический тип для инициализации итератора:
        Iter_empty для итерации с самого начала индекса,
        Iter_key 'key для произвольного ключа,
        Iter_partkey (int * 'key) для частичного ключа и
        Iter_tuple tuple для старта с [tuple] *)

    val iterator_init : iter_init -> iter_dir -> unit
    (** [iterator_init init dir] инициализирует итератор используя
        [init] в качестве начального значения и [dir] как
        направление. Если индекс это хеш, то [dir] должен быть
        Iter_forward *)

    val iterator_next : unit -> tuple
    (** [iterator_next ()] возвращает текущий кортеж; перемещает
        итератор на следующий *)

    val iterator_skip : unit -> unit
    (** [iterator_skip ()] пропускает текущий кортеж; перемещает
        итератор на следующий *)

    val iterator_take : iter_init -> iter_dir -> int -> tuple list
    (** [iterator_take init dir count] возвращает список из [count]
        кортежей начания с [init] *)

    val find : Descr.key -> tuple
    (** [find key] находит кортеж в [index] по ключу
        [key]. Кидает исключение Not_found если не находит *)

    val find_dyn : Tuple.field list -> tuple
    (** [find_by_tuple key_part_list] находит кортеж в [index] по
        полному или частичному ключу [key_part_list]. Функция чуть
        менее эффективна чем [find] т.к. требуется промежуточная
        структура, описывающая ключ. В сулчае если тип ключа не
        совпадет с типом индекса кинет исключение Invalid_argument.
        Кидает исключение Not_found если не находит *)

    val get : int -> tuple
    val slots : unit -> int
    val typ : unit -> index_type
  end
end

type mop =
    Set16 of (int * int)
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
  (** алгебраический тип, описывающий микрооперации в [box_update] *)

module ObjSpace : sig
  module type Descr = sig
    type key
    val obj_space_no : int
    val index_no : int
    val node_pack : Index.index -> key -> unit
    val tuple_of_key : key -> tuple
  end

  module Make : functor (Descr : Descr) -> sig
    module PK : sig
      type iter_init = Iter_empty
                     | Iter_key of Descr.key
                     | Iter_partkey of (int * Descr.key)
                     | Iter_tuple of tuple
      val iterator_init : iter_init -> Index.iter_dir -> unit
      val iterator_next : unit -> tuple
      val iterator_skip : unit -> unit
      val iterator_take : iter_init -> Index.iter_dir -> int -> tuple list
      val find : Descr.key -> tuple
      val get : int -> tuple
      val slots : unit -> int
      val typ : unit -> Index.index_type
    end
    (** См. описание Index.Make *)

    val find : Descr.key -> tuple
    (** [find key] находит кортеж в PK по ключу [key]. Кидает
        исключение Not_found если не находит *)

    val insert : tuple -> unit
    (** [insert tuple] вставляет [tuple]. Если кортеж с таким же
        первичным ключом уже существует, то он заменяется.  *)

    val replace : tuple -> unit
    (** [replace tuple] заменяет [tuple]. Если кортежа с совпадающем
        ключом не существует, то кидает IProto_Failure *)

    val add : tuple -> unit
    (** [add obj_space tuple] добавляет [tuple]. Если кортеж с
        совпадающим ключом существует, то кидает IProto_Failure *)

    val delete : Descr.key -> unit
    (** [delete key] удаляет кортеж, соответсвующий [key] *)

    val update : Descr.key -> mop list -> unit
    (** [update key mops] последовательно выполняет [mops] над кортжем
        с первичным ключом [key] в [obj_space] *)
  end
end

val get_affected_tuple : unit -> tuple option
(** возвращает affected кортеж после [replace], [update],
    [delete]. Должна вызываться непосредственно после соотвествующей
    операции *)

val register_cb0 : string -> (unit -> tuple list) -> unit
(** [register_cb1 name cb] регистрирует коллбек без аргументов [cb] под именем [name].
    Если фактическое количество аргументов не совпадает, то вернет клиенту ошибку. *)

val register_cb1 : string -> (string -> tuple list) -> unit
(** [register_cb1 name cb] регистрирует 1-аргументный коллбек [cb] под именем [name].
    Если фактическое количество аргументов не совпадает, то вернет клиенту ошибку.  *)

val register_cb2 : string -> (string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cb3 : string -> (string -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cb4 : string -> (string -> string -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cb5 : string -> (string -> string -> string -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cbN : string -> (string array -> tuple list) -> unit
(** тоже что и [register_cb1], но все аргументы коллбека будет переданы в виде массива. *)
