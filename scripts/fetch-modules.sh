#!/bin/sh

set -e
test -d .git
mkdir -p mod client

if test -z "`git branch --list mod_*`"; then
    git remote show | while read remote_name; do
        git branch --list --remote $remote_name/{mod,client}_* | while read branch_name; do
            git branch ${branch_name##$remote_name/} $branch_name
        done
    done
fi

git branch --list mod_* | while read branch_name; do
    if [ ! -e mod/${branch_name##mod_} ]; then
        git clone -q --branch $branch_name . mod/${branch_name##mod_}
    fi
done

git branch --list client_* | while read branch_name; do
    if [ ! -e mod/${branch_name##client_} ]; then
        git clone -q --branch $branch_name . client/${branch_name##client_}
    fi
done

for mod in mod/*; do
  for client in mod/$mod/client/*; do
      if [ ! -e "${client##mod/$mod}" -a -e "$client" ]; then
          ln -s "$client" "${client##mod/$mod}"
      fi
  done
done
