#include <index.h>
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/callback.h>
#include <caml/fail.h>

struct {
	struct index_node node;
	union index_field  padding_a[7];
} node_buf;

struct index_node *node = &node_buf.node;

extern void __attribute__((__noreturn__))release_and_failwith(Error *e);

value stub_index_node_set_cardinality(value arg)
{
	int n = Int_val(arg);
	if (n < 0 || n > (uintptr_t)node->obj)
		caml_invalid_argument("Index.index_pack_node");
	node->obj = (void *)(uintptr_t)n; /* cardinality */
	return(Val_unit);
}

value stub_index_node_pack_field(value index, value i, value ty, value arg)
{
	CAMLparam4(index, i, ty, arg);

	struct index_conf *ic = (struct index_conf *)((char *)index + sizeof(void *)); // Skip ISA
	const struct index_field_desc *fd = &ic->field[Int_val(i)];
	union index_field *f = (void *)&node->key + fd->offset;

	switch (fd->type) {
	case UNUM16:
	case SNUM16:
		if (Int_val(ty) != 0) caml_invalid_argument("Index.index_pack_node: field is NUM16");
		f->u16 = Int_val(arg);
		break;
	case UNUM32:
	case SNUM32:
		if (Int_val(ty) != 1) caml_invalid_argument("Index.index_pack_node: field is NUM32");
		f->u32 = Int_val(arg);
		break;
	case UNUM64:
	case SNUM64:
		if (Int_val(ty) != 2) caml_invalid_argument("Index.index_pack_node: field is NUM64");
		f->u64 = Int64_val(arg);
		break;
	case STRING:
		if (Int_val(ty) != 3) caml_invalid_argument("Index.index_pack_node: field is STRING");
		if (caml_string_length(arg) > 0xffff)
			caml_invalid_argument("Index.index_pack_node: string key too long");
		set_lstr_field(f, caml_string_length(arg), (u8 *)String_val(arg));
		break;
	case UNDEF:
		abort();
	}
	node->obj = (void *)(uintptr_t)(Int_val(i) + 1); /* cardinality */
	CAMLreturn(Val_unit);
}

value stub_index_find_node(value index)
{
	CAMLparam1(index);
	CAMLlocal1(oct_obj);
	@try {
		oct_obj = (uintptr_t)[(id<BasicIndex>)index find_node:node];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(oct_obj);
}

value stub_index_iterator_init_with_direction(value index, value direction)
{
	CAMLparam2(index, direction);
	@try {
		Index<BasicIndex> *obj = (Index<BasicIndex> *)index;
		if (Int_val(direction) == 1)
			[obj iterator_init];
		else {
			if (obj->conf.type < SPTREE)
				caml_invalid_argument("iterator_init");
			[(Tree *)obj iterator_init_with_direction:-1];
		}
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(Val_unit);
}

value stub_index_iterator_init_with_node_direction(value index, value direction)
{
	CAMLparam2(index, direction);
	@try {
		[(id<BasicIndex>)index iterator_init_with_node:node direction:Int_val(direction)];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(Val_unit);
}

value stub_index_iterator_init_with_object_direction(value index, value oct_object, value direction)
{
	CAMLparam2(index, oct_object);
	@try {
		[(id<BasicIndex>)index iterator_init_with_object:(void *)oct_object direction:Int_val(direction)];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(Val_unit);
}

value stub_index_iterator_next(value index)
{
	CAMLparam1(index);
	CAMLlocal1(obj);
	@try {
		obj = (uintptr_t)[(id<BasicIndex>)index iterator_next];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(obj);
}

value stub_index_position_with_node(value index)
{
	CAMLparam1(index);
	@try {
		[(id<BasicIndex>)index position_with_node:node];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(Val_unit);
}

value stub_index_position_with_object(value index, value oct_obj)
{
	CAMLparam1(index);
	@try {
		[(id<BasicIndex>)index position_with_object:(void *)oct_obj];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(Val_unit);
}

value stub_index_slots(value index)
{
	CAMLparam1(index);
	CAMLreturn(Val_int([(id<BasicIndex>)index slots]));
}

value stub_index_get(value index, value n)
{
	CAMLparam2(index, n);
	CAMLlocal1(obj);
	@try {
		obj = (uintptr_t)[(id<BasicIndex>)index get:Int_val(n)];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(obj);
}

value stub_index_type(value index)
{
	CAMLparam1(index);
	CAMLreturn(Val_int(((Index *)index)->conf.type));
}
