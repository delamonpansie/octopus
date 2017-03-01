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

static struct index_node *node = &node_buf.node;
static int node_i;

extern void __attribute__((__noreturn__))release_and_failwith(Error *e);

value stub_index_node_set_cardinality(value arg)
{
	int n = Int_val(arg);
	if (n < 0 || n > (uintptr_t)node->obj)
		caml_invalid_argument("Index.index_pack_node");
	node->obj = (void *)(uintptr_t)n; /* cardinality */
	return(Val_unit);
}

value stub_index_node_pack_begin(Index *index)
{
	(void)index;
	node_i = 0;
	return Val_unit;
}

value stub_index_node_pack_int(Index *index, value arg)
{
	struct index_conf *ic = &index->conf;
	if (node_i > ic->cardinality)
		caml_invalid_argument("Index.node_pack_int");
	const struct index_field_desc *fd = &ic->field[node_i];
	union index_field *f = (void *)&node->key + fd->offset;
	f->u32 = Int_val(arg);
	node->obj = (void *)(uintptr_t)(++node_i);
	return Val_unit;
}

value stub_index_node_pack_u64(Index *index, value arg)
{
	CAMLparam1(arg);
	struct index_conf *ic = &index->conf;
	if (node_i > ic->cardinality)
		caml_invalid_argument("Index.node_pack_u64");
	const struct index_field_desc *fd = &ic->field[node_i];
	union index_field *f = (void *)&node->key + fd->offset;
	f->u64 = Int64_val(arg);
	node->obj = (void *)(uintptr_t)(++node_i);
	CAMLreturn(Val_unit);
}

value stub_index_node_pack_string(Index *index, value arg)
{
	CAMLparam1(arg);
	struct index_conf *ic = &index->conf;
	if (node_i > ic->cardinality)
		caml_invalid_argument("Index.node_pack_string");
	const struct index_field_desc *fd = &ic->field[node_i];
	union index_field *f = (void *)&node->key + fd->offset;
	if (caml_string_length(arg) > 0xffff)
		caml_invalid_argument("Index.index_pack_node: string key too long");
	set_lstr_field(f, caml_string_length(arg), (u8 *)String_val(arg));

	node->obj = (void *)(uintptr_t)(++node_i);
	CAMLreturn(Val_unit);
}

typedef struct tnt_object *(*obj_visible)(struct tnt_object *obj);
static obj_visible txn_visibility()
{
	switch (((struct box_txn *)fiber->txn)->mode) {
	case RO: return tuple_visible_left;
	case RW: return tuple_visible_right;
	default: abort();
	}
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
		if (!index_is_tree(index))
			caml_invalid_argument("iterator_init");
		[(Tree *)index iterator_init_with_direction:-1];
	}
	return Val_unit;
}

value stub_index_iterator_init_with_position(Index<BasicIndex> *index, value direction)
{
	i64 pos = Int_val(direction);
	if (pos < 0 || pos > UINT32_MAX)
		caml_invalid_argument("negative position");
	if (!index_is_hash(index))
		caml_invalid_argument("iterator_init");
	[(id<HashIndex>)index iterator_init_pos: (u32)pos];
	return Val_unit;
}

value stub_index_iterator_init_with_node_direction(Tree *index, value direction)
{
	if (!index_is_tree(index))
		caml_invalid_argument("iterator_init");
	[index iterator_init_with_node:node direction:Int_val(direction)];
	return Val_unit;
}

struct tnt_object* value_to_tnt_object(value val); /* this function declared in box_tuple_stubs.m */
value stub_index_iterator_init_with_object_direction(Tree *index, value oct_object, value direction)
{
	if (!index_is_tree(index))
		caml_invalid_argument("iterator_init");

	CAMLparam1(oct_object);
	struct tnt_object* obj = value_to_tnt_object(oct_object);
	@try {
		[index iterator_init_with_object:obj direction:Int_val(direction)];
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
	if (index_is_tree(index))
		caml_invalid_argument("iterator_init");

	obj_visible visible = txn_visibility();
	struct tnt_object *obj = visible([index get:Int_val(n)]);
	return (value)obj;
}

value stub_index_type(const Index *index)
{
	return Val_int(index_is_hash(index) ? 0 : 1);
}
