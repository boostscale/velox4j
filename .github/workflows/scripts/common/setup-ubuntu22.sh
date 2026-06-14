#!/bin/bash

set -e
set -o pipefail
set -u

# APT update.
apt-get update

# Install essentials.
apt-get install -y openjdk-11-jdk patchelf
