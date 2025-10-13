#!/bin/bash

DALIES=$( echo /Volumes/bigbig/theaiguy/daily/*[0-9])

export EXEC=$PWD
#echo $DALIES
for WORKDIR in $DALIES; do
	~/bin/cron_train.sh $WORKDIR 
	sleep 100
done

