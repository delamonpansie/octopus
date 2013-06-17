#!/bin/sh

set -e
test -d .git
mkdir -p mod client
reference=${OCTOPUS_GIT_CLONE_SHARED:+--reference .}

git branch -a | sed 's/^..//' | grep '^mod_' | while read branch_name; do
    if [ ! -e mod/${branch_name##mod_} ]; then
        git clone -q --branch $branch_name . mod/${branch_name##mod_}
    fi
done

git branch -a | sed 's/^..//' | grep '^client_' | while read branch_name; do
    if [ ! -e client/${branch_name##client_} ]; then
        git clone -q --branch $branch_name . client/${branch_name##client_}
    fi
done

git remote show | while read remote_name; do
    remote_url=$(git remote show -n origin | sed '/Fetch URL:/!d; s/.*Fetch URL:[[:space:]]*//')

    git branch -a | sed "s/^..//; s/^remotes\///; s/^$remote_name\///;" | grep "^\(mod_\|client_\)" | while read branch_name; do
	branch_name=${branch_name#$remote_name/}
	dir=$(echo $branch_name | tr _ /)
	if [ ! -e $dir ]; then
	    git clone $reference -q --branch $branch_name $remote_url $dir
	fi
    done
done

for mod in mod/*; do
  for client in $mod/client/*; do
      if [ ! -e "${client##$mod/}" -a -e "$client" ]; then
          ln -s "../$client" "${client##$mod/}"
      fi
  done
done
