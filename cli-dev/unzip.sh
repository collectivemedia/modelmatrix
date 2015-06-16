#!/usr/bin/env bash

# Helper script that takes packaged CLI and uzip it into current direcrory
# Use it for dev testing packaged App

rm -R -f ./conf
rm -R -f ./bin
rm -R -f ./lib
rm -R -f ./*.zip
find ../modelmatrix-cli/target/universal/*.zip -exec unzip "{}" -d . \;
find ./modelmatrix-cli* -exec mv "{}" . \;
find ./modelmatrix-cli* -exec rm -R -f "{}" \;
cp /Users/eugenezhulenev/projects/snappy-java/target/snappy-1.1.1-Mac-x86_64/libsnappyjava.jnilib .

