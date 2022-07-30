#! /bin/bash

FILE_FULL=$1
REMOTE_HOST=$2

FILE_DIR=$(dirname $FILE_FULL)
FILE_NAME=$(basename $FILE_FULL)
LOCALHOST=$(hostname)

ZIP_TOOL=pigz
NC_PORT=2345

tar -cf - -C $FILE_DIR $FILE_NAME | pv -s `du -sb $FILE_FULL | awk '{s += $1} END {printf "%d", s}'` | $ZIP_TOOL | nc $REMOTE_HOST $NC_PORT &
mkdir -p /tmp/$FILE_NAME; nc -l -p $NC_PORT | $ZIP_TOOL -d | tar xf - -C /tmp/$FILE_NAME