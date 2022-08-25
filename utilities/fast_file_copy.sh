#! /bin/bash

FILE_FULL=$1
# REMOTE_HOST=$2

FILE_DIR=$(dirname $FILE_FULL)
FILE_NAME=$(basename $FILE_FULL)
LOCALHOST=$(hostname)

ZIP_TOOL=pigz
NC_PORT=2345
# change pigz compression from -0 to -6 (default compression)
# -p60 is the number of threads to use

# ------- origal
# tar -cf - -C $FILE_DIR $FILE_NAME | pv -s `du -sb $FILE_FULL | awk '{s += $1} END {printf "%d", s}'` | $ZIP_TOOL | nc $REMOTE_HOST $NC_PORT &   
# mkdir -p /tmp/$FILE_NAME; nc -l -p $NC_PORT | $ZIP_TOOL -d | tar xf - -C /tmp/$FILE_NAME
# -------

# new 
export FILE_FULL=/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub052/july_13_fifteennode/staged
export NC_PORT=23455
tar --use-compress-program="pigz -p60 -k " -cf kas_staged_dir.tar.gz $FILE_FULL | nc $(hostname) $NC_PORT

export FILE_FULL=/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub052/july_13_fifteennode/staged
export NC_PORT=23455
export FILE_NAME=$(basename $FILE_FULL)
mkdir -p /tmp/$FILE_NAME; nc -l -p $NC_PORT | pigz -d | tar xf - -C /tmp/$FILE_NAME   

## | nc -l $NC_PORT &
# mkdir -p /tmp/$FILE_NAME; nc -l -p $NC_PORT | 