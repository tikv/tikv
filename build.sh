#!/bin/bash

set -e

source env.sh

echo "profile is ${PROXY_PROFILE}"
echo "engine is ${ENGINE_LABEL_VALUE}"
echo "prometheus metric name prefix is ${PROMETHEUS_METRIC_NAME_PREFIX}"

PROXY_ENABLE_FEATURES=${PROXY_ENABLE_FEATURES} ./cargo-build.sh

lib_suffix="so"
if [[ $(uname -s) == "Darwin" ]]; then
  lib_suffix="dylib"
fi

target_name="lib${ENGINE_LABEL_VALUE}_proxy.${lib_suffix}"
ori_build_path="target/${PROXY_PROFILE}/libraftstore_proxy.${lib_suffix}"
target_path=${PROXY_LIB_TARGET_COPY_PATH-"target/${PROXY_PROFILE}/${target_name}"}

echo "forcibly remove ${target_path}"
rm -rf "${target_path}"
echo "copy ${ori_build_path} to ${target_path}"
cp "${ori_build_path}" "${target_path}"
