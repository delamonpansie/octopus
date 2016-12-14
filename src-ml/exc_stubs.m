#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <objc.h>
#include <iproto.h>

static const value *exn_failure;
static const value *exn_iproto_failure;
static Class iproto_error_class;

void __attribute__((__noreturn__))
release_and_failwith(Error *e)
{
	if (exn_failure == NULL) {
		exn_failure = caml_named_value("exn_failure");
		exn_iproto_failure = caml_named_value("exn_iproto_failure");
		iproto_error_class = [IProtoError class];
	}
	const value *exn = exn_failure;
	value reason = caml_copy_string(e->reason);
	value args[2];
	if ([e class] == iproto_error_class) {
		exn = exn_iproto_failure;
		args[0] = Val_int(((IProtoError *)e)->code);
		args[1] = reason;
	}
	[e release];
	if (exn == exn_failure)
		caml_raise_with_arg(*exn, reason);
	else
		caml_raise_with_args(*exn, 2, args);
}
