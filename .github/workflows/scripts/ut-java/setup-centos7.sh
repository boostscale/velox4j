#!/bin/bash

set -e
set -o pipefail
set -u

sed -i -e "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf
sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-*
sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*
yum install -y centos-release-scl
rm -f /etc/yum.repos.d/CentOS-SCLo-scl.repo
sed -i \
  -e 's/^mirrorlist/#mirrorlist/' \
  -e 's/^#baseurl/baseurl/' \
  -e 's/mirror\.centos\.org/vault.centos.org/' \
  /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

yum -y install epel-release
yum -y install wget curl tar zip unzip which patch sudo
yum -y install ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool
yum -y install devtoolset-11 python3 pip dnf
yum -y install bison java-1.8.0-openjdk java-1.8.0-openjdk-devel
yum -y install ccache patchelf
yum -y install lz4-devel lzo-devel libzstd-devel snappy-devel double-conversion-devel
yum -y install libevent-devel

# Link cc / c++ to the ones in devtoolset.
rm -f /usr/bin/cc /usr/bin/c++ /usr/bin/ld
ln -s /opt/rh/devtoolset-11/root/usr/bin/cc /usr/bin/cc
ln -s /opt/rh/devtoolset-11/root/usr/bin/c++ /usr/bin/c++
ln -s /opt/rh/devtoolset-11/root/usr/bin/ld /usr/bin/ld

pip3 install --upgrade pip

# Install cmake >= 3.28.3.
pip3 install cmake==3.28.3

# Install git >= 2.7.4
case "$(git --version)" in "git version 2."*)
  [ -f /etc/yum.repos.d/ius.repo ] || yum -y install https://repo.ius.io/ius-release-el7.rpm
  yum -y remove git
  yum -y install git236
  ;;
esac

# Install openssl >= 1.1.1.
cd /tmp
wget https://github.com/openssl/openssl/releases/download/OpenSSL_1_1_1o/openssl-1.1.1o.tar.gz
tar -xzvf openssl-1.1.1o.tar.gz
cd openssl-1.1.1o
./config --prefix=/usr --openssldir=/etc/ssl --libdir=lib no-shared zlib-dynamic
make
make install

# Install flex >= 2.6.0.
case "$(PATH="/usr/local/bin:$PATH" flex --version 2>&1)" in "flex 2.6."*)
cd /tmp
  yum -y install gettext-devel
  FLEX_URL="https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"
  mkdir -p flex
  wget -q --max-redirect 3 -O - "$FLEX_URL" | tar -xz -C flex --strip-components=1
  cd flex
  ./autogen.sh
  ./configure
  make install
  ;;
esac
