#!/bin/bash

set -e
set -o pipefail
set -u

# Install essentials.
dnf install -y maven-openjdk11 patch

# Install patchelf.
cd /tmp
mkdir patchelf
cd patchelf
wget https://github.com/NixOS/patchelf/releases/download/0.17.2/patchelf-0.17.2-x86_64.tar.gz
tar -xvf patchelf-0.17.2-x86_64.tar.gz
ln -s /tmp/patchelf/bin/patchelf /usr/local/bin/patchelf
patchelf --version
