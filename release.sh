#!/bin/bash

set -e

source env.sh

export PROXY_BUILD_TYPE=release
export PROXY_PROFILE=release
if [[ $(uname -s) == "Darwin" ]]; then
  echo "Kernel is Darwin, change build type to debug"
  unset PROXY_BUILD_TYPE
  export PROXY_PROFILE=debug
  echo ""
  echo "try to use openssl lib from system: "
  brew --prefix openssl
  echo ""
#  export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
#  export OPENSSL_LIB_DIR=$(brew --prefix openssl)"/lib"
#  export OPENSSL_INCLUDE_DIR=$(brew --prefix openssl)"/include"
#  export OPENSSL_NO_VENDOR=1
  export OPENSSL_ROOT_DIR="/usr/local/opt/openssl"
  export CARGO_PROFILE_DEV_DEBUG="true"
  export CARGO_PROFILE_RELEASE_DEBUG="true"
  mkdir -p target/release
  PROXY_LIB_TARGET_COPY_PATH="target/release/lib${ENGINE_LABEL_VALUE}_proxy.dylib" make build
else
  make build
fi
