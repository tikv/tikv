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

  if [[ -n "${PROXY_BUILD_STD}" && "${PROXY_BUILD_STD}" != "0" ]]; then
      # When `-Z build-std` is enabled, `--target` must be specified explicitly,
      # and specifying `--target` will cause the generated binary to be located
      # in the `target/${TARGET}/release` directory instead of `target/release`,
      # so we need to explicitly specify `--out-dir` here, to avoid errors when
      # copying the output binary later.
      export PROXY_BUILD_STD_ARGS="-Z build-std=core,std,alloc,proc_macro,test --target=${PROXY_BUILD_RUSTC_TARGET}"
      export PROXY_BUILD_STD_ARGS="${PROXY_BUILD_STD_ARGS} -Z unstable-options --out-dir=target/release"
  fi

  make build
fi
