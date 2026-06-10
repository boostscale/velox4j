#!/bin/bash
set -euo pipefail

# --- Configuration ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"
DEPENDENCY_DIR="${DEPENDENCY_DIR:-$(pwd)/deps-download}"
NPROC="${NPROC:-$(nproc)}"

# Versions (from scripts/setup-versions.sh)
FB_OS_VERSION="v2026.01.05.00"
FMT_VERSION="11.2.0"
BOOST_VERSION="boost-1.84.0"
GFLAGS_VERSION="v2.2.2"
FAST_FLOAT_VERSION="v8.0.2"

mkdir -p "$DEPENDENCY_DIR" "$INSTALL_PREFIX"

# --- Helpers ---
wget_and_untar() {
    local url=$1 dir=$2
    cd "$DEPENDENCY_DIR"
    rm -rf "$dir"
    mkdir -p "$dir" && cd "$dir"
    curl -L "$url" -o archive.tar.gz
    tar -xz --strip-components=1 -f archive.tar.gz
    cd "$DEPENDENCY_DIR"
}

cmake_install_dir() {
    local dir=$1; shift
    cd "$DEPENDENCY_DIR/$dir"
    rm -rf _build && mkdir _build
    cmake -B_build -GNinja \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
        -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF \
        "$@"
    cmake --build _build -j "$NPROC"
    sudo cmake --install _build
}

# --- fmt (needed by folly) ---
install_fmt() {
    echo "=== fmt ==="
    wget_and_untar "https://github.com/fmtlib/fmt/archive/${FMT_VERSION}.tar.gz" fmt
    cmake_install_dir fmt -DFMT_TEST=OFF
}

# --- boost (b2 build, not cmake) ---
install_boost() {
    echo "=== boost ==="
    wget_and_untar \
        "https://github.com/boostorg/boost/releases/download/${BOOST_VERSION}/${BOOST_VERSION}.tar.gz" \
        boost
    cd "$DEPENDENCY_DIR/boost"
    ./bootstrap.sh --prefix="$INSTALL_PREFIX"
    sudo ./b2 -j "$NPROC" -d0 install threading=multi --without-python
}

# --- fast_float ---
install_fast_float() {
    echo "=== fast_float ==="
    wget_and_untar \
        "https://github.com/fastfloat/fast_float/archive/refs/tags/${FAST_FLOAT_VERSION}.tar.gz" \
        fast_float
    cmake_install_dir fast_float -DBUILD_TESTS=OFF
}

# --- gflags (needed by folly) ---
install_gflags() {
    echo "=== gflags ==="
    wget_and_untar \
        "https://github.com/gflags/gflags/archive/refs/tags/${GFLAGS_VERSION}.tar.gz" \
        gflags
    cmake_install_dir gflags \
        -DBUILD_SHARED_LIBS=OFF \
        -DGFLAGS_BUILD_STATIC_LIBS=ON \
        -DGFLAGS_BUILD_gflags_LIB=ON
}

# --- folly (needs fmt, gflags) ---
install_folly() {
    echo "=== folly ==="
    wget_and_untar \
        "https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        folly
    cmake_install_dir folly \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_TESTS=OFF \
        -DFOLLY_HAVE_INT128_T=ON \
        -DGFLAGS_SHARED=FALSE
}

# --- fizz (needs folly) ---
install_fizz() {
    echo "=== fizz ==="
    wget_and_untar \
        "https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        fizz
    cmake_install_dir fizz/fizz -DBUILD_TESTS=OFF
}

# --- wangle (needs folly) ---
install_wangle() {
    echo "=== wangle ==="
    wget_and_untar \
        "https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        wangle
    cmake_install_dir wangle/wangle -DBUILD_TESTS=OFF
}

# --- mvfst (needs folly) ---
install_mvfst() {
    echo "=== mvfst ==="
    wget_and_untar \
        "https://github.com/facebook/mvfst/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        mvfst
    cmake_install_dir mvfst -DBUILD_TESTS=OFF
}

# --- fbthrift (needs folly, fizz, wangle, mvfst) ---
install_fbthrift() {
    echo "=== fbthrift ==="
    wget_and_untar \
        "https://github.com/facebook/fbthrift/archive/refs/tags/${FB_OS_VERSION}.tar.gz" \
        fbthrift

    # Apply CompactV1 protocol refiller patch so derived classes (like Velox
    # CompactV1ProtocolReaderWithRefill) can read from the internal buffer
    # without a friend declaration in the upstream library.
    #
    # NOTE: This patch was merged upstream starting from fbthrift v2026.05.04.00.
    # If the version is bumped past that, the patch is a no-op (already applied).
    echo "Applying CompactV1 protocol refiller patch..."
    cd "$DEPENDENCY_DIR/fbthrift"
    patch -p1 < "$SCRIPT_DIR/fbthrift-compact-refiller.patch"
    cd "$DEPENDENCY_DIR"

    cmake_install_dir fbthrift \
        -Denable_tests=OFF \
        -DBUILD_TESTS=OFF \
        -DBUILD_SHARED_LIBS=OFF
}

# --- Main ---
install_fmt
install_boost
install_fast_float
install_gflags
install_folly
install_fizz
install_wangle
install_mvfst
install_fbthrift

echo "=== Done. Installed to $INSTALL_PREFIX ==="
