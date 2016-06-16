#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <iproto.h>

value
stub_net_io_reply(struct netmsg_head *wbuf, struct iproto *request)
{
	return (value)iproto_reply(wbuf, request, 0);
}

value
stub_net_io_fixup(struct netmsg_head *wbuf, struct iproto_retcode *reply)
{
	iproto_reply_fixup(wbuf, reply);
	return Val_unit;
}

value
stub_net_io_error(struct netmsg_head *wbuf, struct iproto *request, value ret_code, value err)
{
	iproto_error_fmt(wbuf, request, Int_val(ret_code), "%s", String_val(err));
	return (Val_unit);
}

value
stub_net_io_add_i8(struct netmsg_head *wbuf, value n)
{
	u8 i = Int_val(n);
	net_add_iov_dup(wbuf, &i, sizeof(i));
	return Val_unit;
}

value
stub_net_io_add_i16(struct netmsg_head *wbuf, value n)
{
	u16 i = Int_val(n);
	net_add_iov_dup(wbuf, &i, sizeof(i));
	return Val_unit;
}

value
stub_net_io_add_i32(struct netmsg_head *wbuf, value n)
{
	u32 i = Int_val(n);
	net_add_iov_dup(wbuf, &i, sizeof(i));
	return Val_unit;
}

value
stub_net_io_add_i64(struct netmsg_head *wbuf, value n)
{
	int64_t i = Int64_val(n);
	net_add_iov_dup(wbuf, &i, sizeof(i));
	return Val_unit;
}

value
stub_net_io_blit_bytes(struct netmsg_head *wbuf, value buf, value offt, value len)
{
	net_add_iov_dup(wbuf, String_val(buf) + Int_val(offt), Int_val(len));
	return Val_unit;
}

value
stub_net_io_add_bytes(struct netmsg_head *wbuf, value buf)
{
	net_add_iov_dup(wbuf, String_val(buf), caml_string_length(buf));
	return Val_unit;
}
