#include <index.h>
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/callback.h>
#include <caml/fail.h>

#include <mod/box/box.h>

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

value stub_index_node_pack_field(Index *index, value i, value ty, value arg)
{
	CAMLparam3(i, ty, arg);

	struct index_conf *ic = &index->conf;
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

typedef struct tnt_object *(*obj_visible)(struct tnt_object *obj);
static obj_visible txn_visibility()
{
	return tuple_visible_left;
}

value stub_index_find_node(id<BasicIndex> index)
{
	obj_visible visible = txn_visibility();
	struct tnt_object *obj = visible([(id<BasicIndex>)index find_node:node]);
	return (value)obj;
}

value stub_index_iterator_init_with_direction(Index<BasicIndex> *index, value direction)
{
	if (Int_val(direction) == 1)
		[index iterator_init];
	else {
		if (index->conf.type < SPTREE)
			caml_invalid_argument("iterator_init");
		[(Tree *)index iterator_init_with_direction:-1];
	}
	return Val_unit;
}

value stub_index_iterator_init_with_node_direction(Tree *index, value direction)
{
	if (index->conf.type < SPTREE)
		caml_invalid_argument("iterator_init");
	[index iterator_init_with_node:node direction:Int_val(direction)];
	return Val_unit;
}

value stub_index_iterator_init_with_object_direction(Tree *index, value oct_object, value direction)
{
	if (index->conf.type < SPTREE)
		caml_invalid_argument("iterator_init");

	CAMLparam1(oct_object);
	@try {
		[index iterator_init_with_object:(void *)oct_object direction:Int_val(direction)];
	}
	@catch (Error *e) {
		release_and_failwith(e);
	}
	CAMLreturn(Val_unit);
}

value stub_index_iterator_next(id<BasicIndex> index)
{
	struct tnt_object *obj;
	obj_visible visible = txn_visibility();
	do {
		obj = [(id<BasicIndex>)index iterator_next];
		if (!obj) break;
		obj = visible(obj);
	} while (!obj);
	if (!obj)
		caml_raise_not_found();
	return (value)obj;
}

value stub_index_slots(id<BasicIndex> index)
{
	return Val_int([index slots]);
}

value stub_index_get(Index<HashIndex> *index, value n)
{
	if (index->conf.type > NUMHASH)
		caml_invalid_argument("iterator_init");

	obj_visible visible = txn_visibility();
	struct tnt_object *obj = visible([index get:Int_val(n)]);
	return (value)obj;
}

value stub_index_type(const Index *index)
{
	return Val_int(index->conf.type);
}
