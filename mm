#!/usr/bin/env bash

# Prepare Model Matrix cmd
cmd="java -jar ./modelmatrix-cli/target/scala-2.10/model-matrix-cli.jar"

# Run it
exec $cmd "$@"
