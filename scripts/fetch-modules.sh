#!/bin/sh

set -e
test -d .git
test -f include/config.h.in

mkdir -p mod client

while getopts ":r" opt; do
    case $opt in
      "r")
	    REF="--reference ."
	    ;;
      *) echo Unknown option: $opt; exit 1
	 ;;
    esac
done
shift $(($OPTIND - 1))
FETCH=${@:-ALL}

todir() { echo $1 | sed 's/_/\//'; }
tobranch() { echo $1 | sed 's/\//_/'; }

need_fetch() {
    local branch=$1

    [ "$FETCH" = ALL ] && return
    for m in $FETCH; do
	m=$(tobranch $m)
	[ "$m" = "$branch" ] && return
    done

    return 1
}

local_repo() {
    local repo="$1"
    git --git-dir="$repo/.git" remote show -n origin | grep -q 'Fetch URL: \(/\|file://\)'
}

git branch -a | sed -nEe 's/^..//; /^(mod|client)_/p' | while read branch_name; do
    dir=$(todir $branch)
    [ -e $dir ] || git clone -q --branch $branch . $dir
done

git remote show | while read remote_name; do
    remote_url=$(git remote show -n origin | sed '/Fetch URL:/!d; s/.*Fetch URL:[[:space:]]*//')

    git branch -a | sed -nEe "s/^..remotes[/]$remote_name[/]//; /^(mod|client)_/p" | while read branch_name; do
	branch_name=${branch_name#$remote_name/}
	dir=$(todir $branch_name)

	[ -e $dir ] && continue
	need_fetch $branch_name || continue

	git clone -q $REF --branch $branch_name $remote_url $dir
    done
done

for mod in mod/*; do
  for client in $mod/client/*; do
      if [ ! -e "${client##$mod/}" -a -e "$client" ]; then
          ln -s "../$client" "${client##$mod/}"
      fi
  done
done

for repo in mod/* client/*; do
    branch=$(echo $repo | tr / _)
    if test -d "$repo/.git" && ( need_fetch "$repo" || local_repo "$repo" ); then
	(cd "$repo" && echo -n "$repo ... " && git pull --quiet && echo "ok" || echo "fail")
    fi
done
