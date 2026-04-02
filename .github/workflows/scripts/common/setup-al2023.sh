#!/bin/bash

set -e
set -o pipefail
set -u

# Install essentials.
# Use --allowerasing to replace curl-minimal (shipped in the base Docker image)
# with the full curl package required by other dependencies.
dnf -y install --allowerasing wget curl tar zip unzip which patch sudo git xz
dnf -y install ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool
dnf -y install gcc-c++ python3 python3-pip
dnf -y install bison flex
dnf -y install lz4-devel lzo-devel libzstd-devel snappy-devel
dnf -y install libevent-devel libatomic
dnf -y install openssl-devel libicu-devel
dnf -y install libdwarf-devel re2-devel zlib-devel
dnf -y install boost-devel libcurl-devel
dnf -y install gflags-devel bzip2-devel
dnf -y install libsodium-devel elfutils-libelf-devel
dnf -y install numactl-devel numactl
dnf -y install findutils

# Verify GCC version (AL2023 ships GCC 11).
cc --version
c++ --version

# Install CMake >= 3.28.3 (AL2023's default cmake is too old).
pip3 install cmake==3.28.3

# Locate the pip-installed cmake modules directory.
CMAKE_MODULE_DIR=$(python3 -c "import cmake; import os; print(os.path.join(os.path.dirname(cmake.__file__), 'data', 'share'))")/cmake-3.28/Modules

# Add FindSnappy.cmake (removed by Velox upstream).
if [ -d "$CMAKE_MODULE_DIR" ]; then
  curl -fsSL https://raw.githubusercontent.com/facebookincubator/velox/c43eab52c9f4cd7d24a729025266d77dc2e52ca0/CMake/FindSnappy.cmake \
    -o "$CMAKE_MODULE_DIR/FindSnappy.cmake"
fi

# Install glog from source (not available in AL2023 repos).
cd /tmp
git clone --depth 1 --branch v0.6.0 https://github.com/google/glog.git
cd glog
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DWITH_GFLAGS=ON -DWITH_GTEST=OFF -DWITH_UNWIND=OFF
cmake --build build -j$(nproc)
cmake --install build --prefix /usr/local
ldconfig

# Install double-conversion from source (not available in AL2023 repos).
cd /tmp
git clone --depth 1 --branch v3.3.0 https://github.com/google/double-conversion.git
cd double-conversion
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
cmake --build build -j$(nproc)
cmake --install build --prefix /usr/local
ldconfig

# Install Java 21 (Amazon Corretto).
# Both the full (non-headless) package and the devel package are needed.
# The full package provides libjawt.so required by CMake's FindJNI.
dnf -y install java-21-amazon-corretto java-21-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto.x86_64

# Install Maven.
if [ -z "$(which mvn 2>/dev/null)" ]; then
  MAVEN_VERSION=3.9.2
  MAVEN_INSTALL_DIR=/opt/maven-$MAVEN_VERSION
  cd /tmp
  wget https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz
  tar -xvf apache-maven-$MAVEN_VERSION-bin.tar.gz
  rm -f apache-maven-$MAVEN_VERSION-bin.tar.gz
  mv apache-maven-$MAVEN_VERSION "${MAVEN_INSTALL_DIR}"
  ln -s "${MAVEN_INSTALL_DIR}/bin/mvn" /usr/local/bin/mvn
fi

# Install ccache.
dnf -y install ccache || {
  cd /tmp
  wget https://github.com/ccache/ccache/releases/download/v4.9.1/ccache-4.9.1-linux-x86_64.tar.xz
  tar -xvf ccache-4.9.1-linux-x86_64.tar.xz
  cp ccache-4.9.1-linux-x86_64/ccache /usr/local/bin/ccache
}

# Install patchelf.
dnf -y install patchelf || {
  cd /tmp
  mkdir -p patchelf
  cd patchelf
  wget https://github.com/NixOS/patchelf/releases/download/0.17.2/patchelf-0.17.2-x86_64.tar.gz
  tar -xvf patchelf-0.17.2-x86_64.tar.gz
  ln -s /tmp/patchelf/bin/patchelf /usr/local/bin/patchelf
}
patchelf --version
