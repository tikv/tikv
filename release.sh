#!/bin/bash

export BUILD_TYPE=release
export PROXY_PROFILE=release
if [[ $(uname -s) == "Darwin" ]]; then
  PROXY_GIT_HASH=$(git log -1 --format="%H")
  mkdir -p target/debug
  mkdir -p target/release
  curl -o "target/debug/.libtiflash_proxy.dylib" http://fileserver.pingcap.net/download/builds/pingcap/tiflash-proxy/${PROXY_GIT_HASH}/libtiflash_proxy.dylib
  PROXY_SIZE=$(stat -f "%z" target/debug/.libtiflash_proxy.dylib)
  if [[ ${PROXY_SIZE} -gt 102400 ]]; then
    echo "Use compiled libtiflash_proxy.dylib from remote"
    rm -rf target/debug/libtiflash_proxy.dylib
    cp target/debug/.libtiflash_proxy.dylib target/debug/libtiflash_proxy.dylib
    PROXY_TAR_REAL_PATH=$(pwd)/target/debug/libtiflash_proxy.dylib
    install_name_tool -id ${PROXY_TAR_REAL_PATH} ${PROXY_TAR_REAL_PATH}
  else
    echo "Kernel is Darwin, change build type to debug"
    unset BUILD_TYPE
    export PROXY_PROFILE=debug
    make build_by_type
  fi
  cp target/debug/libtiflash_proxy.dylib target/release/libtiflash_proxy.dylib
else
  make build_by_type
fi
