#!/bin/bash
# Patch to fix SO_INCOMING_NAPI_ID missing on older kernels (CentOS 7)
# This patches the AsyncSocketTransport.cpp file after it's downloaded by Velox

set -e

# Find the AsyncSocketTransport.cpp file
ASYNC_SOCKET_FILE=$(find . -path "*/folly/io/async/AsyncSocketTransport.cpp" 2>/dev/null | head -1)

if [ -n "$ASYNC_SOCKET_FILE" ]; then
    echo "Patching $ASYNC_SOCKET_FILE for SO_INCOMING_NAPI_ID compatibility"
    
    # Check if already patched
    if grep -q "Define SO_INCOMING_NAPI_ID if not available" "$ASYNC_SOCKET_FILE"; then
        echo "File already patched, skipping"
        exit 0
    fi
    
    # Add the define after the include statement
    sed -i '/#include <folly\/io\/async\/AsyncSocketTransport.h>/a\
\
// Define SO_INCOMING_NAPI_ID if not available (e.g., on older kernels like CentOS 7)\
#ifndef SO_INCOMING_NAPI_ID\
#define SO_INCOMING_NAPI_ID 56\
#endif' "$ASYNC_SOCKET_FILE"
    
    echo "Patch applied successfully"
else
    echo "AsyncSocketTransport.cpp not found, skipping patch"
fi

exit 0
