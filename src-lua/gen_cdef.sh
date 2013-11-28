cat <<EOF > $CDEF
local ffi = require 'ffi'
require('cdef_base')
module (...)
ffi.cdef [[
struct palloc_pool;
`$CPP -P $CPPFLAGS $srcdir/include/octopus.h | $SED -n '/^\(enum tnt_object_flags\|struct tnt_object\) \+{/,/^}/{s/\[\]/[?]/;p;} '`
`$CPP -P $CPPFLAGS $srcdir/include/octopus.h | $SED -n '/^void object_/p '`
`$CPP -P $CPPFLAGS $srcdir/include/index.h | $SED -n '/^\(struct\|union\) index_[a-z]\+ \+{/,/^}/{s/\[\]/[?]/;p;}'`
`$CPP -P $CPPFLAGS $srcdir/include/fiber.h | $SED -n '/^typedef struct coro_context/p;'`
`$CPP -P $CPPFLAGS $srcdir/include/fiber.h | $SED -n '/^struct \(fiber\|octopus_coro\|coro_context\) \+{/,/^}/{s/\[\]/[?]/;p;}'`
extern struct fiber *fiber;
`$CPP -P $CPPFLAGS $srcdir/include/octopus_ev.h | $SED -n '/^typedef [a-z]\+ ev_tstamp/p; /typedef struct ev_\(io\|timer\)/,/^}/p;'`
`$CPP -P $CPPFLAGS $srcdir/include/iproto_def.h | $SED -n '/^struct iproto\(_retcode\)\? \+{/,/^}/{s/\[\]/[?]/;p;}'`
`$CPP -P $CPPFLAGS $srcdir/include/net_io.h | $SED -n '/^struct netmsg\(_mark\|_head\|_tailq\)\? \+{/,/^}/p; /^enum conn_memory_ownership \+{/,/^}/p; /^struct conn \+{/,/^}/p'`
`$CPP -P $CPPFLAGS -DLUA_DEF='@@@' $srcdir/include/net_io.h | $SED -n '/@@@/!d; s/@@@//; p'`
`$CPP -P $CPPFLAGS $srcdir/include/tbuf.h | $SED -n '/^struct tbuf \+{/,/^}/{s/end/stop/;s/void/u8/;p}'`
`$CPP -P $CPPFLAGS $srcdir/include/tbuf.h | $SED -n '/^void tbuf_\(willneed\|append\|printf\).*/{s/void \*/u8 */;s/).*/);/;p}'`
`$CPP -P $CPPFLAGS $srcdir/include/pickle.h | $SED -n '/write.*(.*struct tbuf/p'`
]]
autoconf = {}
`cat $srcdir/include/config.h  | $SED '/^#define [^ ]* ["0-9]/!d; s/#define \([^ ]*\) \(.*\)/autoconf.\1 = \2/;'`
EOF
