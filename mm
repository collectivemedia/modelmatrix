#!/usr/bin/env bash

# Prepare Model Matrix cmd
cmd="$SPARK_HOME/bin/spark-submit --master spark://ezhulenev-MBPro.local:7077 --class com.collective.modelmatrix.cli.ModelMatrixCli ./modelmatrix-cli/target/scala-2.10/model-matrix-cli.jar"

# Run it
exec $cmd "$@"
