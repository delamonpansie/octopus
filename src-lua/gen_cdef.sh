#!/bin/sh
set -e

CPP=${CPP:-cpp -I. -Iinclude}
SED=${SED:-sed}
srcdir=${srcdir:-..}

cd "$srcdir"

cat <<EOF
local ffi = require 'ffi'
require('cdef_base')
module (...)
ffi.cdef [[
struct palloc_pool;
EOF
$CPP include/octopus.h | $SED -n '/^\(enum tnt_object_flags\|struct tnt_object\) \+{/,/^}/p'
$CPP include/octopus.h | $SED -n '/^void object_/p '
$CPP include/index.h | $SED -n '/^\(struct\|union\) index_[a-z]\+ \+{/,/^}/p'
$CPP include/fiber.h | $SED -n '/^typedef struct coro_context/p;'
$CPP include/fiber.h | $SED -n '/^struct \(fiber\|octopus_coro\|coro_context\) \+{/,/^}/p'
echo "extern struct fiber *fiber;"
$CPP include/octopus_ev.h | $SED -n '/^typedef [a-z]\+ ev_tstamp/p; /typedef struct ev_\(io\|timer\)/,/^}/p;'
$CPP include/iproto_def.h | $SED -n '/^struct iproto\(_retcode\)\? \+{/,/^}/{s/\[\]/[?]/;p;}'
$CPP include/iproto_def.h | $SED -n '/^struct iproto\(_retcode\)\? \+{/,/^}/{s/\(iproto\w*\)/\1_0/;/\[\]/ d;p;}'
$CPP include/net_io.h | $SED -n '/^struct netmsg\(_mark\|_head\|_tailq\)\? \+{/,/^}/p; /^enum conn_memory_ownership \+{/,/^}/p; /^struct conn \+{/,/^}/p'
$CPP -DLUA_DEF='@@@' include/net_io.h | $SED -n '/@@@/!d; s/@@@//; p'
$CPP include/tbuf.h | $SED -n '/^struct tbuf \+{/,/^}/{s/end/stop/;s/void/u8/;p}'
$CPP include/tbuf.h | $SED -n '/^void tbuf_\(willneed\|append\|printf\).*/{s/void \*/u8 */;s/).*/);/;p}'
$CPP include/pickle.h | $SED -n '/write.*(.*struct tbuf/p'
$CPP include/say.h | $SED -n '/^extern int.*max_level/p; /^enum say_level {/,/^}/p; /^void _say(/{s/$/;/;p;}'
$CPP include/palloc.h | $SED -n '/palloc(/p'
echo "]]"
echo "autoconf = {}"
cat include/config.h  | $SED '/^#define [^ ]* ["0-9]/!d; s/#define \([^ ]*\) \(.*\)/autoconf.\1 = \2/;'
