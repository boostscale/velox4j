#!/bin/bash

set -e
set -o pipefail
set -u

BASE_DIR=$(dirname $0)
NUM_THREADS=$(nproc)
SOURCE_DIR=$BASE_DIR
BUILD_DIR=$BASE_DIR/build
INSTALL_DIR=$BUILD_DIR/dist
INSTALL_LIB_DIR=$INSTALL_DIR/lib

cmake -DCMAKE_BUILD_TYPE=Debug -S "$SOURCE_DIR" -B "$BUILD_DIR"
cmake --build "$BUILD_DIR" --target velox4j-shared -j "$NUM_THREADS"
cmake --install "$BUILD_DIR" --component velox4j --prefix "$INSTALL_DIR"

for file in "$INSTALL_LIB_DIR"/*
do
  if [ -L "$file" ]
  then
    target=$(readlink -f "$file")
    if [ "$(dirname "$target")" != "$(readlink -f "$INSTALL_LIB_DIR")" ]
    then
      echo "Target $target is not in the same directory as $file."
      exit 1
    fi
    mv -v "$target" "$file"
    chrpath -r '$ORIGIN' "$file"
  fi
done
