#!/usr/bin/env bash

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

set -euo pipefail

if [[ $# -ne 1 || "$1" != "-fix" ]]; then
    echo "Usage: $0 -fix"
    exit 1
fi

# Directory containing the source code to check
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/../../../../"

if ! command -v pre-commit >/dev/null 2>&1; then
    echo "Missing required command: pre-commit"
    exit 1
fi

PRE_COMMIT_BIN="$(command -v pre-commit)"

cd "$SRC_DIR"

"$PRE_COMMIT_BIN" run --all-files
