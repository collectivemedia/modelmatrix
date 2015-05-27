#!/usr/bin/env bash

# Prepare Model Matrix cmd
$SPARK_HOME/bin/spark-submit \
--master spark://ezhulenev-MBPro.local:7077 \
--driver-java-options "-Dlog4j.configuration=file:./modelmatrix-cli/conf/log4j.properties -Dspark.io.compression.codec=lzf" \
--class com.collective.modelmatrix.cli.ModelMatrixCli \
./modelmatrix-cli/target/scala-2.10/model-matrix-cli.jar $@
