#!/bin/bash

set -e
set -o pipefail
set -u

BASE_DIR=$(dirname "$0")
NUM_THREADS=$(nproc)
SOURCE_DIR=$BASE_DIR
BUILD_DIR=$BASE_DIR/build
INSTALL_PREFIX=$BUILD_DIR/dist
INSTALL_DESTINATION=lib
INSTALL_LIB_DIR=$INSTALL_PREFIX/$INSTALL_DESTINATION
VELOX4J_LIB_NAME=libvelox4j.so

# Build C++ so libraries.
cmake -DCMAKE_BUILD_TYPE=Release -DVELOX4J_ENABLE_CCACHE=ON -DVELOX4J_BUILD_TESTING=OFF -DVELOX4J_INSTALL_DESTINATION="$INSTALL_DESTINATION" -S "$SOURCE_DIR" -B "$BUILD_DIR"
cmake --build "$BUILD_DIR" --target velox4j-shared -j "$NUM_THREADS"
cmake --install "$BUILD_DIR" --component velox4j --prefix "$INSTALL_PREFIX"

# Resolve symlinks in the library installation directory.
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
  fi
done

# Force '$ORIGIN' runpaths for all so libraries to make the build portable.
# 1. Remove any already set RUNPATH sections.
for file in "$INSTALL_LIB_DIR"/*
do
  echo "Removing RUNPATH on file: $file ..."
  patchelf --remove-rpath "$file"
done

# 2. Add new RUNPATH sections with '$ORIGIN'.
for file in "$INSTALL_LIB_DIR"/*
do
  echo "Adding RUNPATH on file: $file ..."
  patchelf --set-rpath '$ORIGIN' "$file"
done

# 3. Print new ELF headers.
for file in "$INSTALL_LIB_DIR"/*
do
  echo "Checking ELF header on file: $file ..."
  readelf -d "$file"
done

# Do final checks.
# 1. Check ldd result.
echo "Checking ldd result of libvelox4j.so: "
ldd "$INSTALL_LIB_DIR/$VELOX4J_LIB_NAME"

# 2. Check ld result.
echo "Checking ld result of libvelox4j.so: "
ld "$INSTALL_LIB_DIR/$VELOX4J_LIB_NAME"

# Finished.
echo "Successfully built velox4j-cpp."
