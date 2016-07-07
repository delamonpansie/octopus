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
  (** [loop name cb] создает фибер и вызывает в бесконечном цикле cb
      (), при повторном вызове с тем же [name] заменяет [cb] в
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

type 'a obj_space
(** абстрактный тип obj_space, параметризированный типом первичного
    индекса.  Может использоваться только внутри коллбека, попытка
    сохранить его где-нибудь и использовать вне коллбека приведет к
    SEGV.  *)

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
  type 'a t
  (** абстрактный тип индекса, параметризированный типом ключа 'a
      Единственным конструктором таких объектов является
      [obj_space_index] *)

  type 'a iter_init = Iter_empty
                    | Iter_key of 'a
                    | Iter_partkey of (int * 'a)
                    | Iter_tuple of tuple
  (** алгебраический тип для инициализации итератора:
      Iter_empty для итерации с самого начала индекса,
      Iter_key 'a для произвольного ключа,
      Iter_partkey (int * 'a) для частичного ключа и
      Iter_tuple tuple для старта с [tuple] *)

  type iter_dir = Iter_forward | Iter_backward
  (** направление итератора. Iter_backward поддерживается только для
      деревьев. *)

  type _ field = NUM16 : int field
               | NUM32 : int field
               | NUM64 : Int64.t field
               | STRING : string field
  (** GADT тип, используемый для описания полей индекса *)

  val iterator_init : 'a t -> 'a iter_init -> iter_dir -> unit
  (** [iterator_init index init dir] инициализирует итератор по
      [index] используя [init] в качестве начального значения и [dir]
      как направление. Если индекс это хеш, то [dir] должен быть
      Iter_forward *)

  val iterator_next : 'a t -> tuple
  (** [iterator_next index] возвращает текущий кортеж; перемещает
      итератор на следующий *)

  val iterator_skip : 'a t -> unit
  (** [iterator_skip index] пропускает текущий кортеж; перемещает
      итератор на следующий *)

  val iterator_take : 'a t -> 'a iter_init -> iter_dir -> int -> tuple list
  (** [iterator_take index init dir count] возвращает список из
      [count] кортежей начания с [init] *)

  val index_find : 'a t -> 'a -> tuple
  (** [index_find index key] находит кортеж в [index] по ключу
      [key]. Кидает исключение Not_found если не находит *)
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

val obj_space_pk : 'a obj_space -> 'a Index.t
(** возвращает pk *)

val box_find : 'a obj_space -> 'a -> tuple
(** [box_find obj_space key] находит кортеж в [obj_space] по
    первичному ключу [key] *)

val box_insert : 'a obj_space -> tuple -> unit
(** [box_index obj_space yuple] вставляет [tuple] в [obj_space]. Если
    кортеж с таким же первичным ключом уже существует, то он
    заменяется.  *)

val box_replace : 'a obj_space -> tuple -> unit
(** [box_replace obj_space tuple] заменяет [tuple] в [obj_space]. Если
    кортежа с совпадающем ключом не существует, то кидает IProto_Failure *)

val box_add : 'a obj_space -> tuple -> unit
(** [box_replace obj_space tuple] заменяет [tuple] в [obj_space]. Если
    кортеж с совпадающим ключом существует, то кидает IProto_Failure *)

val box_delete : 'a obj_space -> 'a -> unit
(** [box_delete obj_space key] удаляет [key] из [obj_space]. *)

val box_update : 'a obj_space -> 'a -> mop list -> unit
(** [box_update obj_space key mops] последовательно выполняет [mops]
    над кортжем с первичным ключом [key] в [obj_space] *)

val box_get_affected_tuple : unit -> tuple option
(** возвращает affected кортеж после [box_replace], [box_update],
    [box_delete]. Должна вызываться непосредственно после
    соотвествующей операции *)

type 'a key_info
(** описание типа ключа индекса. Значение этого типа можно и нужно
    кешировать *)

val key_info1 : 'a Index.field -> 'a key_info
(** конструктор описания для 1-колоночного индекса *)

val key_info2 : 'a Index.field * 'b Index.field -> ('a * 'b) key_info
(** конструктор описания для 2-колоночного индекса *)

val key_info3 : 'a Index.field * 'b Index.field * 'c Index.field -> ('a * 'b * 'c) key_info
(** конструктор описания для 3-колоночного индекса *)

val key_info4 : 'a Index.field * 'b Index.field * 'c Index.field * 'd Index.field ->
                ('a * 'b * 'c * 'd) key_info
(** конструктор описания для 4-колоночного индекса *)

val obj_space : box -> int -> 'a key_info -> 'a obj_space
(** obj_space box no key_info] возвращает хендл обж_спейса? с номером
    [no] и описание типа первичного ключа [key_info].  Это значение
    нельзя кэшировать в глобальной переменной: оно валидно только в
    течении вызова коллбека *)

val obj_space_index : _ obj_space -> int -> 'a key_info -> 'a Index.t
(** [obj_space_index obj_space index_no key_info] возвращает хэндл
    индекса [index_no] в [obj_space] с описанием ключа [key_info]. Это
    значение нельзя кэшировать в глобальной переменной: оно валидно
    только в течении вызова коллбека *)

val register_cb0 : string -> (box -> 'a) -> ('a -> tuple list) -> unit
(** [register_cb1 name ctx cb] регистрирует коллбек без аргументов [cb] под именем [name].
    Если фактическое количество аргументов не совпадает, то вернет клиенту ошибку. *)

val register_cb1 : string -> (box -> 'a) -> ('a -> string -> tuple list) -> unit
(** [register_cb1 name ctx cb] регистрирует 1-аргументный коллбек [cb] под именем [name].
    Если фактическое количество аргументов не совпадает, то вернет клиенту ошибку.

    Конструктор контекста [ctx] предназначен для преобразования [box]
    в контекст.  Результат вызова [ctx box] будет передан как первый
    аргумент коллбека.  Данный механизм задуман для уменьшения
    количества boilerplate кода в коллбеках. Т.к. кешировать
    [obj_space] в глобальных переменных запрещено, то их приходится
    создавать при вызове любого коллбека. Вынос этого в конструктор
    контекста решает эту проблему. *)

val register_cb2 : string -> (box -> 'a) -> ('a -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cb3 : string -> (box -> 'a) -> ('a -> string -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cb4 : string -> (box -> 'a) -> ('a -> string -> string -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cb5 : string -> (box -> 'a) -> ('a -> string -> string -> string -> string -> string -> tuple list) -> unit
(** см. [register_cb1] *)

val register_cbN : string -> (box -> 'a) -> ('a -> string array -> tuple list) -> unit
(** тоже что и [register_cb1], но все аргументы коллбека будет переданы в виде массива. *)
