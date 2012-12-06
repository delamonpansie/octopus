#!/bin/sh
set -e

PORT=${1:-33015}

while true; do
    sleep 1
    /bin/echo -e 'show stat\nquit' | nc localhost $PORT | perl parse.pl
done | perl ./driveGnuPlotStreams.pl 3 1 100  0 2000000  1024x1024 'insert' 'select' 'delete' 0 0 0
