#!/bin/bash

set -e
set -o pipefail
set -u
set -x

BASEDIR=$(dirname $0)
NUM_THREADS=$(nproc)
SOURCE_DIR=$BASEDIR
BUILD_DIR=$BASEDIR/build

cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -S "$SOURCE_DIR" -B "$BUILD_DIR"
cmake --build "$BUILD_DIR" --target velox4j -j "$NUM_THREADS"
