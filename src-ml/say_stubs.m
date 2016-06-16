#include <caml/mlvalues.h>
#import <say.h>

#define STUB(x)						\
	value stub_say_##x(value string)		\
	{						\
		say_##x("%s", String_val(string));	\
		return Val_unit;			\
	}

STUB(error)
STUB(warn)
STUB(info)
STUB(debug)

