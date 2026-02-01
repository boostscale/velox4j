#!/bin/bash
# Patch to fix SO_INCOMING_NAPI_ID missing on older kernels (CentOS 7)
# This patches the AsyncSocketTransport.cpp file after it's downloaded by Velox

set -e

# Search in the typical FetchContent build directory structure
# Velox downloads dependencies to _deps subdirectories
ASYNC_SOCKET_FILE=$(find _deps -path "*/folly/io/async/AsyncSocketTransport.cpp" 2>/dev/null | head -1)

if [ -z "$ASYNC_SOCKET_FILE" ]; then
    # Fallback to broader search if not found in _deps
    ASYNC_SOCKET_FILE=$(find . -path "*/folly/io/async/AsyncSocketTransport.cpp" 2>/dev/null | head -1)
fi

if [ -n "$ASYNC_SOCKET_FILE" ]; then
    echo "Patching $ASYNC_SOCKET_FILE for SO_INCOMING_NAPI_ID compatibility"
    
    # Check if already patched
    if grep -q "Define SO_INCOMING_NAPI_ID if not available" "$ASYNC_SOCKET_FILE"; then
        echo "File already patched, skipping"
        exit 0
    fi
    
    # Create patch content
    cat > /tmp/folly_napi_patch.tmp << 'PATCH_EOF'
#include <folly/io/async/AsyncSocketTransport.h>

// Define SO_INCOMING_NAPI_ID if not available (e.g., on older kernels like CentOS 7)
#ifndef SO_INCOMING_NAPI_ID
#define SO_INCOMING_NAPI_ID 56
#endif
PATCH_EOF
    
    # Apply the patch: replace the include line with include + define
    sed -i '/#include <folly\/io\/async\/AsyncSocketTransport.h>/r /tmp/folly_napi_patch.tmp' "$ASYNC_SOCKET_FILE"
    sed -i '/#include <folly\/io\/async\/AsyncSocketTransport.h>/d' "$ASYNC_SOCKET_FILE"
    
    # Cleanup
    rm -f /tmp/folly_napi_patch.tmp
    
    echo "Patch applied successfully"
else
    echo "AsyncSocketTransport.cpp not found, skipping patch"
fi

exit 0
