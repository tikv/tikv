#!/bin/sh
if command -v objcopy >/dev/null 2>&1; then
    objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/tikv-server
    objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/tikv-ctl
else
    echo 'objcopy executable not found, skipping'
fi

