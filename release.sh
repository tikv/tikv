#!/bin/bash

set -e

source env.sh

if [[ $(uname -s) == "Darwin" ]]; then
  echo "Kernel is Darwin, change build type to debug"
  echo ""
  target_path="target/release"
  mkdir -p "${target_path}"
  OPENSSL_ROOT_DIR=$(brew --prefix openssl) OPENSSL_NO_VENDOR=1 OPENSSL_STATIC=1 PROXY_LIB_TARGET_COPY_PATH="${target_path}/lib${ENGINE_LABEL_VALUE}_proxy.dylib" make debug
else
  export PROXY_BUILD_TYPE=release
  export PROXY_PROFILE=release
  make build
fi
