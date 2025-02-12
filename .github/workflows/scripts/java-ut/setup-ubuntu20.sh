#!/bin/bash

set -e
set -o pipefail
set -u

# Install essentials
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y sudo locales wget tar tzdata git ccache ninja-build build-essential llvm-11-dev clang-11 libiberty-dev libdwarf-dev libre2-dev libz-dev libssl-dev libboost-all-dev libcurl4-openssl-dev curl zip unzip tar pkg-config autoconf-archive bison flex
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y autoconf automake g++ libnuma-dev libtool numactl unzip libdaxctl-dev
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-8-jdk
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y maven


# Install CMake
cd /opt
wget https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.sh
mkdir cmake
bash cmake-3.28.3-linux-x86_64.sh --skip-license --prefix=/opt/cmake
ln -s /opt/cmake/bin/cmake /usr/bin/cmake

# Install GCC 11
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common
add-apt-repository ppa:ubuntu-toolchain-r/test
apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y gcc-11 g++-11
rm -f /usr/bin/gcc /usr/bin/g++
ln -s /usr/bin/gcc-11 /usr/bin/gcc
ln -s /usr/bin/g++-11 /usr/bin/g++
cc --version
c++ --version
