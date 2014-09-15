#!/bin/bash
# for some reason trapping EXIT doesn't work in /bin/sh

set -e

LANG=C

TMPDIR=$(mktemp -d)
DIR="$TMPDIR/octopus"
mkdir "$DIR"
test -n "$DIR"
function cleanup() { rm -rf "$TMPDIR";  }
trap cleanup EXIT

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
    git clone --quiet -b $branch $repo "$DIR/$repo"
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


last_commit=$(git log --date-order --format=format:%ct -n1)
suffix=$(git name-rev HEAD | cut -f2 -d' ' | sed 's/^master$/experimental/'):$(git describe --always | sed 's/.*-//')
for mod in $modules; do
    mod_suffix="$mod_suffix+$mod:"$(cd mod/$mod; git describe --always | sed 's/.*-//')
    mod_last_commit=$(cd mod/$mod; git log --date-order --format=format:%ct -n1)
    if [ $mod_last_commit -gt $last_commit ]; then
	last_commit=$mod_last_commit
    fi
done

bundle=$(date +%Y%m%d%H%M --date="@$last_commit")
name=octopus-${bundle}-${suffix}${mod_suffix}


(echo -n "configuring ... "
    cd "$DIR"
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

tar zcf "${DESTDIR:-..}/$name.tar.gz" --exclude-vcs -C "${DIR%/*}" "$name"
echo "${DESTDIR:-..}/$name.tar.gz"
