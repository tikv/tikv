#!/bin/sh
# This script exists to handle platforms where objcopy is not available.
# If a platform has some other tool that should be used, a special case should
# be added to this script.

if [ -z "$CARGO_TARGET_DIR" ]
then
    echo "This script expects CARGO_TARGET_DIR to be set in the environment. Aborting." >&2
    exit 1
fi

if command -v objcopy >/dev/null 2>&1
then
    for file in tikv-server tikv-ctl
    do
        objcopy --compress-debug-sections=zlib-gnu "${CARGO_TARGET_DIR}/release/$file"
    done
else
    echo 'objcopy executable not found, skipping'
fi
