#!/bin/bash

set -e

export PROXY_PROFILE=${PROXY_PROFILE-debug}
export ENGINE_LABEL_VALUE=${ENGINE_LABEL_VALUE-tiflash}
export PROMETHEUS_METRIC_NAME_PREFIX=${PROMETHEUS_METRIC_NAME_PREFIX-"${ENGINE_LABEL_VALUE}_proxy_"}

echo "INFO: PROXY_PROFILE=${PROXY_PROFILE}"
echo "INFO: ENGINE_LABEL_VALUE=${ENGINE_LABEL_VALUE}"
echo "INFO: PROMETHEUS_METRIC_NAME_PREFIX=${PROMETHEUS_METRIC_NAME_PREFIX}"

if [[ -n "${PROXY_FRAME_POINTER}" && "${PROXY_FRAME_POINTER}" != 0 ]]; then
  echo "INFO: frame pointer is enabled"
else
  echo "INFO: frame pointer is disabled"
fi

if [[ $(uname -s) == "Darwin" ]]; then
  # use the openssl 1.1 lib from system
  export OPENSSL_DIR=${OPENSSL_DIR:-$(brew --prefix openssl@1.1)}  # for openssl-sys
  export OPENSSL_ROOT_DIR=${OPENSSL_DIR}  # for Cmake

  if [[ -z ${OPENSSL_DIR} ]]; then
    echo "Not found openssl installed by Homebrew or env 'OPENSSL_DIR', please install openssl by 'brew install openssl@1.1'"
    exit -1
  fi

  echo "INFO: OPENSSL_DIR=${OPENSSL_DIR}"
  export OPENSSL_NO_VENDOR=1  # for openssl-sys
  export OPENSSL_STATIC=1     # for openssl-sys
fi

CARGO_EXTRA_ARGS=""

if [[ "${PROXY_PROFILE}" == "release" ]]; then
  CARGO_EXTRA_ARGS="--release"
fi

if [[ -n "${PROXY_FRAME_POINTER}" && "${PROXY_FRAME_POINTER}" != "0" ]]; then
  CARGO_EXTRA_ARGS="${CARGO_EXTRA_ARGS} -Z build-std=core,std,alloc,proc_macro,test --target=$(rustc -vV | awk '/host/ { print $2 }')"
  CARGO_EXTRA_ARGS="${CARGO_EXTRA_ARGS} -Z unstable-options"

  # When `-Z build-std` is enabled, `--target` must be specified explicitly,
  # and specifying `--target` will cause the generated binary to be located
  # in the `target/${TARGET}/release|debug` directory instead of
  # `target/release|debug`, so we need to explicitly specify `--out-dir` here,
  # to avoid errors when copying the output binary later.
  # Note: CARGO_TARGET_DIR may be set in tiflash-proxy-cmake.
  CARGO_EXTRA_ARGS="${CARGO_EXTRA_ARGS} --out-dir=${CARGO_TARGET_DIR:-target}/${PROXY_PROFILE}"

  rustup component add rust-src
fi

echo "INFO: CARGO_EXTRA_ARGS=${CARGO_EXTRA_ARGS}"
echo "INFO: PROXY_ENABLE_FEATURES=${PROXY_ENABLE_FEATURES}"

cargo build --no-default-features --features "${PROXY_ENABLE_FEATURES}" ${CARGO_EXTRA_ARGS}

# The generated file is libraftstore_proxy.so
# Let's rename it according to the engine label.

LIB_SUFFIX="so"
if [[ $(uname -s) == "Darwin" ]]; then
  LIB_SUFFIX="dylib"
fi

OUT_DIR="${CARGO_TARGET_DIR:-target}/${PROXY_PROFILE}"
CURRENT_LIB_PATH="${OUT_DIR}/libraftstore_proxy.${LIB_SUFFIX}"
DEST_LIB_PATH="${OUT_DIR}/lib${ENGINE_LABEL_VALUE}_proxy.${LIB_SUFFIX}"

echo "Copy ${CURRENT_LIB_PATH} to ${DEST_LIB_PATH}"
rm -f "${DEST_LIB_PATH}"
cp "${CURRENT_LIB_PATH}" "${DEST_LIB_PATH}"

if [[ $(uname -s) == "Darwin" ]]; then
  install_name_tool -id "@rpath/lib${ENGINE_LABEL_VALUE}_proxy.dylib" "${DEST_LIB_PATH}"
fi
