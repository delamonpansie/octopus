#!/bin/sh
set -e

srcdir=${srcdir:-..}
CPP=${CPP:-cpp -I$srcdir -I$srcdir/include}
SED=${SED:-sed}

cat <<EOF
local ffi = require 'ffi'
require('cdef_base')
module (...)
ffi.cdef [[
struct palloc_pool;
struct tnt_object {
       char opaque;
       char data[0];
};
EOF
# $CPP $srcdir/include/octopus.h | $SED -n '/^\(struct tnt_object\) \+{/,/^}/p'
$CPP $srcdir/include/octopus.h | $SED '/^\(extern \)\?\(inline \)\?\(_Bool\|void\|int\) object_.*$/!d; s/^extern //; s/^inline //; /;$/!s/$/;/'
$CPP $srcdir/include/octopus.h | $SED -n '/^extern struct octopus_cfg/p;'
$CPP $srcdir/include/index.h | $SED -n '/struct field_desc \+{/,/^}/p'
$CPP $srcdir/include/index.h | $SED -n '/^\(struct\|union\|enum\) index_[a-z_]\+ \+{/,/^}/p'
$CPP $srcdir/include/index.h | $SED -n '/^enum iterator_direction\+ \+{/,/^}/p'
$CPP $srcdir/include/index.h | $SED -n '/set_lstr_field_noninline/p'
echo "typedef void* id;"
$CPP $srcdir/include/objc.h | $SED -n '/^struct autorelease_[a-z]\+ \+{/,/^}/p'
$CPP $srcdir/include/objc.h | $SED -n '/^\(struct autorelease.*\|void\|id\) autorelease[_a-z]*(.*);\s*$/p'
$CPP $srcdir/include/fiber.h | $SED -n '/fiber_wake\|fid2fiber\|fiber_write/p'
$CPP $srcdir/include/tbuf.h | $SED -n '/^struct tbuf \+{/,/^}/{s/end/stop/;s/void/u8/;p}'
$CPP $srcdir/include/tbuf.h | $SED -n '/^void tbuf_\(willneed\|append\|printf\).*/{s/void \*/u8 */;s/).*/);/;p}'
$CPP $srcdir/include/fiber.h | $SED -n '/^struct \(octopus_coro\|coro_context\) \+{/,/^}/p'
$CPP $srcdir/include/fiber.h | $SED -n '/^@interface Fiber/,/^}/p' | $SED '{s/@interface Fiber.*/typedef struct Fiber { void *isa;/ ; /^@public/ d}'
echo " Fiber;"
echo "struct Fiber *current_fiber();"
echo "int fiber_switch_cnt();"
$CPP $srcdir/include/octopus_ev.h | $SED -n '/^typedef [a-z]\+ ev_tstamp/p; /typedef struct ev_\(io\|timer\)/,/^}/p;'
$CPP $srcdir/include/iproto_def.h | $SED -n '/^struct iproto\(_retcode\)\? \+{/,/^}/{s/\[0\?\]/[?]/;p;}'
$CPP $srcdir/include/iproto_def.h | $SED -n '/^struct iproto\(_retcode\)\? \+{/,/^}/{s/\(iproto\w*\)/\1_0/;/\[0\?\]/ d;p;}'
$CPP $srcdir/include/net_io.h | $SED -n '/^struct netmsg\(_io\|_mark\|_head\|_tailq\)\? \+{/,/^}/p;'
$CPP -DLUA_DEF='@@@' $srcdir/include/net_io.h | $SED -n '/@@@/!d; s/@@@//; p'
$CPP $srcdir/include/pickle.h | $SED -n '/write.*(.*struct tbuf/p'
$CPP $srcdir/include/say.h | $SED -n '/^extern int.*max_level/p; /^enum say_level {/,/^}/p; /^void _say(/{s/$/;/;p;}'
$CPP $srcdir/include/palloc.h | $SED -n '/palloc(/p'
$CPP $srcdir/include/log_io.h | $SED -n '/^struct row_v12 {/,/^}/p; /^enum row_tag {/,/^}/p;'
$CPP cfg/octopus.h | $SED -n 's/u_int32_t/uint32_t/; /^typedef struct octopus_.* {/,/^}/p'
echo "]]"
echo "autoconf = {}"
cat include/config.h  | $SED '/^#define [^ ]* ["0-9]/!d; s/#define \([^ ]*\) \(.*\)/autoconf.\1 = \2/;'
