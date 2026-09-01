#!/bin/bash

set -e
set -o pipefail
set -u

# Detect architecture.
ARCH=$(uname -m)

case "$ARCH" in
  x86_64)
    PATCHELF_ARCH="x86_64"
    ;;
  aarch64|arm64)
    PATCHELF_ARCH="aarch64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Install essentials.
dnf install -y java-11-openjdk-devel patch

# Install patchelf.
cd /tmp
mkdir -p patchelf
cd patchelf

URL="https://github.com/NixOS/patchelf/releases/download/0.17.2/patchelf-0.17.2-${PATCHELF_ARCH}.tar.gz"

wget "$URL"
tar -xvf "patchelf-0.17.2-${PATCHELF_ARCH}.tar.gz"

ln -sf /tmp/patchelf/bin/patchelf /usr/local/bin/patchelf
patchelf --version
