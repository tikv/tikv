#!/bin/bash

set -e

export CARGO_PROFILE_DEV_DEBUG="true"
export CARGO_PROFILE_RELEASE_DEBUG="true"

if [[ -n "${PROXY_BUILD_STD}" && "${PROXY_BUILD_STD}" != "0" ]]; then
    # When `-Z build-std` is enabled, `--target` must be specified explicitly,
    # and specifying `--target` will cause the generated binary to be located
    # in the `target/${TARGET}/release` directory instead of `target/release`,
    # so we need to explicitly specify `--out-dir` here, to avoid errors when
    # copying the output binary later.
    export PROXY_BUILD_STD_ARGS="-Z build-std=core,std,alloc,proc_macro,test --target=${PROXY_BUILD_RUSTC_TARGET}"
    export PROXY_BUILD_STD_ARGS="${PROXY_BUILD_STD_ARGS} -Z unstable-options --out-dir=target/debug"
fi

make build