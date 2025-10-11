#!/bin/bash

DALIES=$( echo /Volumes/bigbig/theaiguy/daily/*[0-9])

export EXEC=$PWD
export STEP_NAME=examination
echo $DALIES
for WORKDIR in $DALIES; do
        cd $WORKDIR
        echo cd $WORKDIR
        echo python $EXEC/scripts/$1
        python $EXEC/scripts/$1
done

