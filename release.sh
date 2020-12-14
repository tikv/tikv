#!/bin/bash

set -e
export BUILD_TYPE=release
export PROXY_PROFILE=release
if [[ $(uname -s) == "Darwin" ]]; then
  echo "Kernel is Darwin, change build type to debug"
  unset BUILD_TYPE
  export PROXY_PROFILE=debug
  brew --prefix openssl
#  export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
#  export OPENSSL_LIB_DIR=$(brew --prefix openssl)"/lib"
#  export OPENSSL_INCLUDE_DIR=$(brew --prefix openssl)"/include"
#  export OPENSSL_NO_VENDOR=1
  make build_by_type
  cp target/debug/libtiflash_proxy.dylib target/release/libtiflash_proxy.dylib
else
  make build_by_type
fi
