#!/bin/bash

set -e
set -o pipefail
set -u

# APT update.
sudo apt-get update

# Install essentials.
sudo apt-get install -y sudo locales wget tar tzdata git ccache ninja-build build-essential
sudo apt-get install -y llvm-11-dev clang-11 libiberty-dev libdwarf-dev libre2-dev libz-dev
sudo apt-get install -y liblzo2-dev libzstd-dev libsnappy-dev libdouble-conversion-dev libssl-dev
sudo apt-get install -y libboost-all-dev libcurl4-openssl-dev curl zip unzip tar pkg-config
sudo apt-get install -y autoconf-archive bison flex libfl-dev libc-ares-dev libicu-dev
sudo apt-get install -y libgoogle-glog-dev libbz2-dev libgflags-dev libgmock-dev libevent-dev
sudo apt-get install -y liblz4-dev libsodium-dev libelf-dev
sudo apt-get install -y autoconf automake g++ libnuma-dev libtool numactl unzip libdaxctl-dev
sudo apt-get install -y openjdk-8-jdk
sudo apt-get install -y maven
sudo apt-get install -y chrpath patchelf

# Install CMake.
cd /opt
wget https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.sh
mkdir cmake
bash cmake-3.28.3-linux-x86_64.sh --skip-license --prefix=/opt/cmake
sudo ln -s /opt/cmake/bin/cmake /usr/bin/cmake

# Install GCC 11.
sudo apt-get install -y software-properties-common
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get install -y gcc-11 g++-11
sudo rm -f /usr/bin/gcc /usr/bin/g++
sudo ln -s /usr/bin/gcc-11 /usr/bin/gcc
sudo ln -s /usr/bin/g++-11 /usr/bin/g++
cc --version
c++ --version
