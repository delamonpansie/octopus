#include <caml/mlvalues.h>
#include <caml/callback.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/threads.h>
#include <caml/fail.h>

#import <net_io.h>
#import <iproto.h>
#import <say.h>
#import <pickle.h>
#import <shard.h>

#import <mod/box/box.h>
#import <mod/box/src-ml/camlbox.h>

static value
parse_args(struct tbuf *req)
{
	CAMLparam0();
	CAMLlocal2(v, arr);

	static int arr_size;
	static void **adata;
	static int *alen;

	int count = read_u32(req);
	for (int i = 0; i < count; i++) {
		if (i >= arr_size) {
			if (arr_size == 0)
				arr_size = 16;
			else
				arr_size *= 2;
			adata = realloc(adata, arr_size * sizeof(*adata));
			alen = realloc(alen, arr_size * sizeof(*alen));
		}
		int flen = read_varint32(req);
		void *fdata = read_bytes(req, flen);

		adata[i] = fdata;
		alen[i] = flen;
	}

	if (count == 0)
		CAMLreturn(Atom(0));

	arr = caml_alloc (count, 0);
	for (int i = 0; i < count; i++) {
		v = caml_alloc_string(alen[i]);
		memcpy(String_val(v), adata[i], alen[i]);
		caml_modify(&Field(arr, i), v);
	}
	CAMLreturn(arr);
}

int
box_dispach_ocaml(struct netmsg_head *wbuf, struct iproto *request)
{
	static value *dispatch = NULL;
	int cb_not_found = 0;
	caml_leave_blocking_section();
	value state = Val_unit, cbname = Val_unit, cbarg = Val_unit, cbret = Val_unit;
	Begin_roots4(state, cbname, cbarg, cbret);

	if (dispatch == NULL)
		dispatch = caml_named_value("box_dispatch");

	struct tbuf req = TBUF(request->data, request->data_len, NULL);
	(void)read_u32(&req); /* ignore flags */
	int len = read_varint32(&req);
	cbname = caml_alloc_string(len);
	memcpy(String_val(cbname), read_bytes(&req, len), len);
	cbarg = parse_args(&req);

	state = caml_alloc_tuple(3);
	Field(state, 0) = (value)wbuf;
	Field(state, 1) = (value)request;

	cbret = caml_callback3_exn(*dispatch, state, cbname, cbarg);
	cb_not_found = Is_exception_result(cbret);
	End_roots();
	caml_enter_blocking_section();
	return cb_not_found;
}

// #define Val_none Val_int(0)
// #define Some_val(v) Field(v, 0)
// static value Val_some(value v)
// {
//     CAMLparam1(v);
//     CAMLlocal1(some);
//     some = caml_alloc(1, 0);
//     Store_field(some, 0, v);
//     CAMLreturn(some);
// }

static struct tnt_object *affected_obj;

value
stub_get_affected_obj()
{
	return (value)affected_obj;
}

extern void __attribute__((__noreturn__)) release_and_failwith(Error *e);


static struct tnt_object *
tuple_clone(struct tnt_object *obj)
{
	int smsize = sizeof(struct tnt_object) + sizeof(struct box_small_tuple) + tuple_bsize(obj);
	switch(obj->type) {
	case BOX_TUPLE:
		object_incr_ref(obj);
		return obj;
	case BOX_SMALL_TUPLE:
		return memcpy(object_alloc(BOX_SMALL_TUPLE, 0, smsize), obj, smsize);
	default: assert(false);
	}
}

value
stub_box_submit()
{
	caml_enter_blocking_section();
	int ret = box_submit(fiber->txn);
	caml_leave_blocking_section();
	return Val_int(ret);
}

value
stub_box_dispatch(struct box_txn *txn, value op, value packer)
{
	CAMLparam2(op, packer);
	CAMLlocal2(req, len);
	req = Field(packer, 0);
	len = Field(packer, 1);

	Error *error = nil;
	@try {
		struct box_op *bop = box_prepare(txn, Int_val(op), String_val(req), Int_val(len));

		if (affected_obj) {
			tuple_free(affected_obj);
			affected_obj = NULL;
		}
		if (bop->obj != NULL)
			affected_obj = bop->obj;
		else if (bop->op == DELETE && bop->old_obj != NULL)
			affected_obj = bop->old_obj;

		if (affected_obj)
			affected_obj = tuple_clone(affected_obj);
	}
	@catch (Error *e) {
		error = e;
	}
	if (error)
		release_and_failwith(error);
	CAMLreturn(Val_unit);
}

value
stub_txn(value unit)
{
	(void)unit;
	return (value)fiber->txn;
}

value
stub_obj_space_index(value val_oid, value val_iid)
{
	CAMLparam2(val_oid, val_iid);
	int oid = Int_val(val_oid),
	    iid = Int_val(val_iid);
	struct box_txn *txn = fiber->txn;

	if (oid < 0 || oid > nelem(txn->box->object_space_registry) - 1)
		caml_invalid_argument("obj_space_index");

	if (!txn->box->object_space_registry[oid])
		caml_failwith("object_space is not enabled");

	struct object_space *obj_spc = txn->box->object_space_registry[oid];

	if (iid < 0 || iid > MAX_IDX)
		caml_invalid_argument("obj_space_index");

	if (obj_spc->index[iid] == nil)
		caml_invalid_argument("obj_space_index");

	CAMLreturn((value)obj_spc->index[iid]);
}

register_source();
