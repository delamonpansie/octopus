#!/bin/sh
set -e

# for repo in . client/* mod/*; do (cd $repo; git push); done

LANG=C

RDIR=$(pwd)
DESTDIR=$(mktemp -d)/octopus
mkdir "$DESTDIR"
test -n "$DESTDIR" 
modules=""
clients=""
conf_modules=""
conf_clients=""

clone() {
    local repo=$1
    local dep=$2
    echo -n "clone $repo ... "

    [ -d "$repo/.git" ] || { echo "skip"; return; }

    local branch=$(cd $repo && git name-rev HEAD | cut -f2 -d' ')
    git clone --quiet -b $branch $repo "$DESTDIR/$repo"
    echo "ok"

    case $repo in
      mod/*) modules="$modules ${repo#*/}" ;;
      client/*) clients="$clients ${repo#*/}";;
    esac
}

clone .
for repo in ${@:-client/* mod/*}; do
    case $repo in
      mod/*) conf_modules="$conf_modules ${repo#*/}" ;;
      client/*) conf_clients="$conf_clients ${repo#*/}";;
    esac

    clone $repo

    if [ -f "$repo/depend" ]; then
	for repo in $(cat "$repo/depend"); do
	    clone $repo
	done
    fi
done


bundle=$(date +%Y%m%d%H%M)
suffix=$(git name-rev HEAD | cut -f2 -d' ' | sed 's/^master$/experimental/'):$(git describe --always | sed 's/.*-//')
for mod in $modules; do
    mod_suffix="$mod_suffix+$mod:"$(cd mod/$mod; git describe --always | sed 's/.*-//')
done
name=octopus-${bundle}-${suffix}${mod_suffix}


(echo -n "configuring ... "
    cd "$DESTDIR"
    cat > modules.m4 <<EOF
AC_SUBST([BUNDLE], 1)
AC_ARG_ENABLE([modules],
              [AS_HELP_STRING([--enable-modules[[="$conf_modules"]]],
                              [this option is disabled in bundled builds])],
              [AC_MSG_ERROR([--enable-modules option is disabled in bundled builds])],	
	      [octopus_modules="$conf_modules"])
EOF

    autoconf
    ./configure >/dev/null
    echo "ok"
    make pre-dist BUNDLE="$bundle"
    cd ..
    mv octopus "$name"
)

tar zcf "../$name.tar.gz" --exclude-vcs -C "${DESTDIR%/*}" "$name"
echo "../$name.tar.gz"
