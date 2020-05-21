#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/alloc.h>
#include <caml/fail.h>

#import <mod/box/box.h>
#import <util.h>

struct tuple_cache {
	union {
		struct tnt_object *obj;
		uintptr_t offset;
	} as;
	int cache[0];
};

#define Tuple_val(v) ((struct tuple_cache *)Data_custom_val(v))
static struct tnt_object*
tuple_obj(struct tuple_cache* tup)
{
	return tup->as.offset <= 256*2*sizeof(int) ?
		(struct tnt_object*)((char*)tup->cache + tup->as.offset) :
		tup->as.obj;
}

static void
box_tuple_finalize(value v)
{
	struct tuple_cache *tup = Tuple_val(v);
	object_decr_ref(tup->as.obj);
}

static const struct custom_operations box_tuple_ops = {
	.identifier = "octopus.box.tuple",
	.finalize = box_tuple_finalize,
	.compare =  custom_compare_default,
	.compare_ext = custom_compare_ext_default,
	.hash =  custom_hash_default,
	.serialize = custom_serialize_default,
	.deserialize = custom_deserialize_default
};

static const struct custom_operations box_small_tuple_ops = {
	.identifier = "octopus.box.small_tuple",
	.finalize = custom_finalize_default,
	.compare =  custom_compare_default,
	.compare_ext = custom_compare_ext_default,
	.hash =  custom_hash_default,
	.serialize = custom_serialize_default,
	.deserialize = custom_deserialize_default
};

static void
fill_cache(struct tuple_cache *tuple)
{
	struct tnt_object *obj = tuple_obj(tuple);
	int *cache = tuple->cache;
	int cardinality = tuple_cardinality(obj);
	const unsigned char *field = tuple_data(obj),
			     *data = field;
	for (int i = 0; i < cardinality; i++) {
		u32 len = LOAD_VARINT32(field);
		cache[i * 2] = len;
		cache[i * 2 + 1] = field - data;
		field = field + len;
	}
}

value
box_tuple_custom_alloc(struct tnt_object *obj)
{
	if (obj == NULL)
		caml_raise_not_found();

	int cardinality = tuple_cardinality(obj),
	     cache_size = sizeof(int) * cardinality * 2,
	 small_obj_size = sizeof(struct tnt_object) +
			  sizeof(struct box_small_tuple) +
			  tuple_bsize(obj),
	       tup_size = sizeof(struct tuple_cache) + cache_size,
	 small_tup_size = sizeof(struct tuple_cache) + cache_size + small_obj_size;

	value val = Val_unit;
	struct tuple_cache *tup;

	switch (obj->type) {
	case BOX_SMALL_TUPLE: {
		val = caml_alloc_custom((struct custom_operations *)&box_small_tuple_ops,
					small_tup_size, 0, 1);
		tup = Tuple_val(val);
		tup->as.offset = cache_size;
		memcpy(tuple_obj(tup), obj, small_obj_size);
		break;
	}
	case BOX_TUPLE: {
		val = caml_alloc_custom((struct custom_operations *)&box_tuple_ops,
					tup_size, 0, 1);
		tup = Tuple_val(val);
		tup->as.obj = obj;
		object_incr_ref(obj);
		break;
	}
	default:
		assert(false);
	}

	fill_cache(tup);
	return val;
}

value
stub_box_tuple_field(value val, value ftype, value valn)
{
	CAMLparam3(val, ftype, valn);
	CAMLlocal1(ret);

	struct tuple_cache *tup = Tuple_val(val);
	struct tnt_object *obj = tuple_obj(tup);
	int *cache = tup->cache;
	int n = Int_val(valn);

	if (n < 0 || n >= tuple_cardinality(obj))
		caml_failwith("invalid field number");

	int len = cache[n * 2];
	int offt = cache[n * 2 + 1];
	void *field = tuple_data(obj) + offt;
	switch (Int_val(ftype)) {
	case 0:
		if (len != 1)
			caml_failwith("invalid field length");
		ret = Val_int(*(u8 *)field);
		break;
	case 1:
		if (len != 2)
			caml_failwith("invalid field length");
		ret = Val_int(*(u16 *)field);
		break;
	case 2:
		if (len != 4)
			caml_failwith("invalid field length");
		ret = caml_copy_int32(*(u32 *)field);
		break;
	case 3:
		if (len != 8)
			caml_failwith("invalid field length");
		ret = caml_copy_int64(*(u64 *)field);
		break;
	case 4:
		switch (len) {
		case 1:
			ret = Val_int(*(u8 *)field);
			break;
		case 2:
			ret = Val_int(*(u16 *)field);
			break;
		case 4:
			ret = Val_int(*(u32 *)field);
			break;
		case 8:
			if (*(u64 *)field & (1ULL << 63))
				caml_failwith("int field overflow");
			ret = Val_int(*(u64 *)field);
			break;
		default:
			caml_failwith("invalid field length");
		}
		break;
	case 6:
		if (n != 0) {
			int prev_len = cache[(n - 1) * 2],
			   prev_offt = cache[(n - 1) * 2 + 1],
			    raw_offt = prev_len + prev_offt,
			     ber_len = offt - raw_offt;
			len += ber_len;
			field -= ber_len;
		} else {
			/* first field in tuple: ber len is offt */
			len += offt;
			field -= offt;
		}
		// fall through
	case 5:
		ret = caml_alloc_string(len);
		memcpy(String_val(ret), field, len);
		break;
	default:
		assert(false);
	}
	CAMLreturn(ret);
}

/* this function is used in box_index_stubs.m stub_index_iterator_init_with_object_direction */
struct tnt_object*
value_to_tnt_object(value val)
{
	struct tuple_cache *tup = Tuple_val(val);
	return tuple_obj(tup);
}

value
stub_box_tuple_cardinality(value val)
{
	struct tuple_cache *tup = Tuple_val(val);
	return Val_int(tuple_cardinality(tuple_obj(tup)));
}

value
stub_box_tuple_bsize(value val)
{
	struct tuple_cache *tup = Tuple_val(val);
	return Val_int(tuple_bsize(tuple_obj(tup)));
}

value
stub_box_tuple_raw_field_size(value val, value valn)
{
	struct tuple_cache *tup = Tuple_val(val);
	int n = Int_val(valn);
	int *cache = tup->cache;
	int len = cache[n * 2];
	int offt = cache[n * 2 + 1];
	if (n != 0) {
		int prev_len = cache[(n - 1) * 2],
		   prev_offt = cache[(n - 1) * 2 + 1],
		    raw_offt = prev_len + prev_offt,
		     ber_len = offt - raw_offt;
		len += ber_len;
	} else {
		/* first field in tuple: ber len is offt */
		len += offt;
	}
	return Val_int(len);
}

value
stub_box_tuple_blit_field(value str, value pos, value valtuple, value valn, value valcount)
{
	struct tuple_cache *tup = Tuple_val(valtuple);
	int n = Int_val(valn);
	int count = Int_val(valcount);
	int *cache = tup->cache;
	int len = cache[n * 2];
	int offt = cache[n * 2 + 1];
	void *field = tuple_data(tuple_obj(tup)) + offt;
	if (n != 0) {
		int prev_len = cache[(n - 1) * 2],
		   prev_offt = cache[(n - 1) * 2 + 1],
		    raw_offt = prev_len + prev_offt,
		     ber_len = offt - raw_offt;
		len += ber_len;
		field -= ber_len;
	} else {
		/* first field in tuple: ber len is offt */
		len += offt;
		field -= offt;
	}
	while (--count > 0) {
		n++;
		len += cache[n * 2 + 1] - offt;
		offt = cache[n * 2 + 1];
	}
	memcpy(String_val(str) + Int_val(pos), field, len);
	return Val_int(Int_val(pos) + len);
}


value
stub_net_tuple_add(struct netmsg_head *wbuf, value val)
{
	struct tuple_cache *tup = Tuple_val(val);
	net_tuple_add(wbuf, tuple_obj(tup));
	return Val_unit;
}
