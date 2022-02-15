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
  export OPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR:-$(brew --prefix openssl@1.1)}

  if [[ -z ${OPENSSL_ROOT_DIR} ]]; then
    echo "Not found openssl installed by Homebrew or env 'OPENSSL_ROOT_DIR', please install openssl by 'brew install openssl@1.1'"
    exit -1
  fi
  
  echo "OPENSSL_ROOT_DIR: ${OPENSSL_ROOT_DIR}"
  export OPENSSL_NO_VENDOR=1
  export OPENSSL_STATIC=1
fi

PROXY_ENABLE_FEATURES=${PROXY_ENABLE_FEATURES} ./cargo-build.sh

target_name="lib${ENGINE_LABEL_VALUE}_proxy.${lib_suffix}"
ori_build_path="target/${PROXY_PROFILE}/libraftstore_proxy.${lib_suffix}"
target_path=${PROXY_LIB_TARGET_COPY_PATH-"target/${PROXY_PROFILE}/${target_name}"}

echo "forcibly remove ${target_path}"
rm -rf "${target_path}"
echo "copy ${ori_build_path} to ${target_path}"
cp "${ori_build_path}" "${target_path}"
