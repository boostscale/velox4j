#!/bin/bash

set -e
set -o pipefail
set -u
set -x

BASEDIR=$(dirname $0)
NUM_THREADS=$(nproc)
SOURCE_DIR=$BASEDIR
BUILD_DIR=$BASEDIR/build

cmake -DCMAKE_BUILD_TYPE=Debug -S "$SOURCE_DIR" -B "$BUILD_DIR"
cmake --build "$BUILD_DIR" --target velox4j-shared -j "$NUM_THREADS"
cmake --install "$BUILD_DIR" --component velox4j --prefix "$BUILD_DIR/install"
