#!/bin/bash

set -e
shopt -s extglob
read -p "Enter revision: " ver

git clone . ../octopus-$ver
rm -rf ../octopus-$ver/.git
echo HAVE_GIT=0 > ../octopus-$ver/config.mk
echo "const char octopus_version_string[] = \"$ver\";" > ../octopus-$ver/octopus_version.h
(cd ..; tar zcvf octopus-$ver.tar.gz octopus-$ver)
git tag $ver
