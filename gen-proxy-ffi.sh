#!/bin/bash

ARGS=""

if [[ -n ${OVERWRITE_VERSION} ]]; then
    ARGS="${ARGS} --overwrite-version=${OVERWRITE_VERSION}"
fi

set -ex

ffi_path="raftstore-proxy/ffi"
./${ffi_path}/format.sh
cargo run --package gen-proxy-ffi --bin gen-proxy-ffi -- ${ARGS}
