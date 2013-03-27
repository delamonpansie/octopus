#!/bin/sh

set -e
test -d .git
mkdir -p mod
cd mod

test -z "`ls`"

if test -z "`git branch --list mod_*`"; then
    git remote show | while read remote_name; do
        git branch --list --remote $remote_name/mod_* | while read branch_name; do
            git branch ${branch_name##$remote_name/} $branch_name
        done
    done
fi

git branch --list mod_* | while read branch_name; do
    git clone -q --branch $branch_name .. ${branch_name##mod_}
done
