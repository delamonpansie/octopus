#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/fail.h>

#import <pickle.h>

value
stub_blit_i8(value str, value pos, value n)
{
	*(i8 *)(String_val(str) + Int_val(pos)) = Int_val(n);
	return Val_unit;
}

value
stub_blit_i16(value str, value pos, value n)
{
	*(i16 *)(String_val(str) + Int_val(pos)) = Int_val(n);
	return Val_unit;
}

value
stub_blit_i32(value str, value pos, value n)
{
	*(i32 *)(String_val(str) + Int_val(pos)) = Int_val(n);
	return Val_unit;
}

value
stub_blit_i64(value str, value pos, value n)
{
	*(i64 *)(String_val(str) + Int_val(pos)) = Int64_val(n);
	return Val_unit;
}

value
stub_blit_varint(value str, value pos, value n)
{
	u8 *ptr = (u8 *)(String_val(str) + Int_val(pos));
	u8 *end = save_varint32(ptr, Int_val(n));
	return Val_int(Int_val(pos) + end - ptr);
}

value
stub_blit_field_i8(value str, value pos, value n)
{
	*(i8 *)(String_val(str) + Int_val(pos)) = 1;
	*(i8 *)(String_val(str) + Int_val(pos) + 1) = Int_val(n);
	return Val_int(Int_val(pos) + 2);
}

value
stub_blit_field_i16(value str, value pos, value n)
{
	*(i8 *)(String_val(str) + Int_val(pos)) = 2;
	*(i16 *)(String_val(str) + Int_val(pos) + 1) = Int_val(n);
	return Val_int(Int_val(pos) + 3);
}

value
stub_blit_field_i32(value str, value pos, value n)
{
	*(i8 *)(String_val(str) + Int_val(pos)) = 4;
	*(i32 *)(String_val(str) + Int_val(pos) + 1) = Int_val(n);
	return Val_int(Int_val(pos) + 5);
}

value
stub_blit_field_i64(value str, value pos, value n)
{
	*(i8 *)(String_val(str) + Int_val(pos)) = 8;
	*(i64 *)(String_val(str) + Int_val(pos) + 1) = Int_val(n);
	return Val_int(Int_val(pos) + 9);
}

value
stub_blit_field_bytes(value str, value pos, value byt)
{
	int len = caml_string_length(byt);
	u8 *ptr = (u8 *)(String_val(str) + Int_val(pos));
 	u8 *end = save_varint32(ptr, len);
	memcpy(end, String_val(byt), len);
	return Val_int(Int_val(pos) + end - ptr + len);
}

value
stub_int_of_bits(value str, value pos, value size)
{
	const void *ptr = String_val(str) + Int_val(pos);
	switch (Int_val(size)) {
	case 1: return Val_int(*(i8 *)ptr);
	case 2: return Val_int(*(i16 *)ptr);
	case 4: return Val_int(*(i32 *)ptr);
	case 8: return Val_int(*(i64 *)ptr);
	default: assert(0);
	}
}

value
stub_int64_of_bits(value str, value pos)
{
	CAMLparam2(str, pos);
	if (caml_string_length(str) != 8)
		caml_invalid_argument("int64_of_bits");
	CAMLreturn(caml_copy_int64(*(i64 *)String_val(str)));
}

value
stub_bits_of_i16(value i)
{
	value str = caml_alloc_string(2);
	*(i16 *)String_val(str) = Int_val(i);
	return str;
}

value
stub_bits_of_i32(value i)
{
	value str = caml_alloc_string(4);
	*(i32 *)String_val(str) = Int_val(i);
	return str;
}

value
stub_bits_of_i64(value i)
{
	CAMLparam1(i);
	value str = caml_alloc_string(8);
	*(i64 *)String_val(str) = Int64_val(i);
	CAMLreturn(str);
}
