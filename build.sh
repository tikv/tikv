#!/bin/bash

set -e

source env.sh

echo "profile is ${PROXY_PROFILE}"
echo "engine is ${ENGINE_LABEL_VALUE}"
echo "prometheus metric name prefix is ${PROMETHEUS_METRIC_NAME_PREFIX}"

lib_suffix="so"
if [[ $(uname -s) == "Darwin" ]]; then
  lib_suffix="dylib"
  # use the openssl 1.1 lib from system
  export OPENSSL_DIR=${OPENSSL_DIR:-$(brew --prefix openssl@1.1)}  # for openssl-sys
  export OPENSSL_ROOT_DIR=${OPENSSL_DIR}  # for Cmake

  if [[ -z ${OPENSSL_DIR} ]]; then
    echo "Not found openssl installed by Homebrew or env 'OPENSSL_DIR', please install openssl by 'brew install openssl@1.1'"
    exit -1
  fi

  echo "OPENSSL_DIR: ${OPENSSL_DIR}"
  export OPENSSL_NO_VENDOR=1  # for openssl-sys
  export OPENSSL_STATIC=1     # for openssl-sys
fi

PROXY_ENABLE_FEATURES=${PROXY_ENABLE_FEATURES} ./cargo-build.sh

target_name="lib${ENGINE_LABEL_VALUE}_proxy.${lib_suffix}"
ori_build_path="target/${PROXY_PROFILE}/libraftstore_proxy.${lib_suffix}"
target_path=${PROXY_LIB_TARGET_COPY_PATH-"target/${PROXY_PROFILE}/${target_name}"}

echo "forcibly remove ${target_path}"
rm -rf "${target_path}"
echo "copy ${ori_build_path} to ${target_path}"
cp "${ori_build_path}" "${target_path}"

if [[ $(uname -s) == "Darwin" ]]; then
  target_install_name="@rpath/${target_name}"
  install_name_tool -id "${target_install_name}" "${target_path}"
fi
