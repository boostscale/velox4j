#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -o pipefail
set -u

# Parse action argument: must be -check or -fix
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 -check | -fix"
    exit 1
fi

ACTION="$1"

if [[ "$ACTION" != "-check" && "$ACTION" != "-fix" ]]; then
    echo "Invalid argument: $ACTION"
    echo "Usage: $0 -check | -fix"
    exit 1
fi

# Ensure Maven is installed.
mvn -version

# Directory containing the source code to check
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/../../../../"

# Set image name.
IMAGE_NAME=velox4j-format

# Set Ubuntu version.
OS_VERSION=24.04

# Build the Docker image, passing the OS_VERSION build argument.
docker build --build-arg "OS_VERSION=$OS_VERSION" -t "$IMAGE_NAME:$OS_VERSION" "$SCRIPT_DIR"

# CPP code paths.
CPP_MAIN_PATH="$SRC_DIR/src/main/cpp/main/velox4j/"
CPP_TEST_PATH="$SRC_DIR/src/main/cpp/test/velox4j/"

# Determine the clang-format command
if [[ "$ACTION" == "-check" ]]; then
    FORMAT_COMMAND="clang-format-18 --dry-run --Werror"
    MAVEN_COMMAND="spotless:check"
else
    FORMAT_COMMAND="clang-format-18 -i"
    MAVEN_COMMAND="spotless:apply"
fi

# 1. Run clang-format-18 on the CPP main code.
docker run --rm -v "$CPP_MAIN_PATH":/workspace -w /workspace "$IMAGE_NAME:$OS_VERSION" \
    sh -c "find . \( -name '*.h' -o -name '*.cc' -o -name '*.cpp' \) | xargs -r $FORMAT_COMMAND"

# 2. Run clang-format-18 on the CPP test code.
docker run --rm -v "$CPP_TEST_PATH":/workspace -w /workspace "$IMAGE_NAME:$OS_VERSION" \
    sh -c "find . \( -name '*.h' -o -name '*.cc' -o -name '*.cpp' \) | xargs -r $FORMAT_COMMAND"

# 3. Run Maven Spotless check or apply under the project root.
(
    cd "$SRC_DIR"
    mvn "$MAVEN_COMMAND"
)
