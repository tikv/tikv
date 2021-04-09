#!/bin/bash

set -e

source env.sh

if [[ $(uname -s) == "Darwin" ]]; then
  echo "Kernel is Darwin, change build type to debug"
  echo ""
  target_path="target/release"
  mkdir -p "${target_path}"
  PROXY_LIB_TARGET_COPY_PATH="${target_path}/lib${ENGINE_LABEL_VALUE}_proxy.dylib" make debug
else
  export PROXY_BUILD_TYPE=release
  export PROXY_PROFILE=release
  make build
fi
