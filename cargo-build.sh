#!/bin/bash

cargo_build_extra_parameter=""
if [[ ! -z ${PROXY_BUILD_TYPE} ]]; then
  cargo_build_extra_parameter="--${PROXY_BUILD_TYPE}"
fi

set -ex

if [[ -n "${PROXY_BUILD_STD}" && "${PROXY_BUILD_STD}" != "0" ]]; then
  rustup component add rust-src
  cargo build --no-default-features --features "${PROXY_ENABLE_FEATURES}" ${cargo_build_extra_parameter} ${PROXY_BUILD_STD_ARGS}
else
  cargo build --no-default-features --features "${PROXY_ENABLE_FEATURES}" ${cargo_build_extra_parameter}
fi