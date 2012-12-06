#!/bin/sh

PORT=$1

while (true) do sleep 1; echo 'show stat' | nc localhost $PORT | perl parse.pl ; done |\
	perl ./driveGnuPlotStreams.pl 3 1 100  0 500000  1024x1024 'insert' 'select' 'delete' 0 0 0

