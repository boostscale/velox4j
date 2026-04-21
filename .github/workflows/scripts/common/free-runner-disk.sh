#!/bin/bash

set -euo pipefail
echo "Removing unwanted software... "
sudo rm -rf /usr/share/dotnet
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/ghc
sudo rm -rf /opt/hostedtoolcache/CodeQL
sudo docker image prune --all --force > /dev/null
df -h
