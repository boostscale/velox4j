#!/bin/bash

set -euo pipefail

echo "Disk usage BEFORE cleanup:"
df -h /

# capture available space in KB
before=$(df --output=avail / | tail -1)

echo "Removing unwanted software... "
sudo rm -rf /usr/share/dotnet
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/ghc
sudo rm -rf /opt/hostedtoolcache/CodeQL
sudo docker image prune --all --force > /dev/null

echo "Disk usage AFTER cleanup:"
df -h /
