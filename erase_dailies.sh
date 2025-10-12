#!/bin/bash

DALIES=$( echo /Volumes/bigbig/theaiguy/daily/*[0-9])

export EXEC=$PWD
#echo $DALIES
for WORKDIR in $DALIES; do
        rm -rf $WORKDIR/run
        rm -rf $WORKDIR/eval_out
        rm -rf $WORKDIR/logs
        rm  $WORKDIR/experiment.yaml $WORKDIR/evaluate.yaml
done

