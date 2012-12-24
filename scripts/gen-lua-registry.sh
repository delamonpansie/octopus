#!/bin/sh

set -e

cname () {
    local name
    name=${1##*src-lua/}
    name=${name%%.*}
    echo $name | tr / _
}

cat <<EOF
#import <octopus.h>
#import <say.h>
#include <stdlib.h>

EOF

for i in $*; do
    # handle out of source build by
    ${0%%gen-lua-registry.sh}stringify lua_static_module_`cname $i` < $i
done

cat <<EOF

struct lua_src *lua_src;

static void __attribute__((constructor))
lua_src_init(void)
{
	lua_src = calloc($# + 1, sizeof(*lua_src));
	if (lua_src == NULL)
		panic("malloc failed");
	int i = 0;

EOF

for i in $*; do
    name=`cname $i`
    echo "lua_src[i++] = (struct lua_src){ \"$name\", lua_static_module_$name, sizeof(lua_static_module_$name) - 1 };"
done

cat <<EOF

}
EOF
