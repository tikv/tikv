#!/bin/bash

# This is a script for running cargo with a variety of options. It is currently
# used by the 'x-' makefile targets, for experimenting with compile-time
# options.
#
# The most significant thing this script does is manage cargo config files,
# moving them into place from `etc/` to `.cargo/`, and cleaning up afterward.
#
# It takes its arguments as environment variables just because they are easier
# than parsing the command line in bash.
#
# The "X_" prefixes are to avoid polluting the "CARGO_*" namespace.

# Any error -> exit 1
set -e

if [[ -e .cargo/config ]]; then
   rm .cargo/config
fi

args=""

if [[ -n "$X_CARGO_CMD" ]]; then
    args="$args $X_CARGO_CMD"
else
    echo "X_CARGO_CMD must be set"
    exit 1
fi

# All features are enabled explicitly
args="$args --no-default-features"

features="default"

if [[ -n "$X_CARGO_FEATURES" ]]; then
    features="$X_CARGO_FEATURES"
fi

if [[ -n "$X_CARGO_RELEASE" && "$X_CARGO_RELEASE" != "0" ]]; then
    # TODO check that this works
    args="$args --release"
fi

if [[ -n "$X_CARGO_CONFIG_FILE" ]]; then
    echo "using $X_CARGO_CONFIG_FILE"
    args="$args -Zunstable-options -Zconfig-profile"
    mkdir .cargo 2> /dev/null || true
    set -x
    cp "$X_CARGO_CONFIG_FILE" .cargo/config
    set +x
fi

if [[ -n "$X_RUSTFLAGS" ]]; then
    export RUSTFLAGS="$RUSTFLAGS $X_RUSTFLAGS"
fi

if [[ -n "$X_DEBUG" ]]; then
    export CARGO_PROFILE_DEV_DEBUG="true"
    export CARGO_PROFILE_RELEASE_DEBUG="true"
fi

# Default package is tikv
package="tikv"

if [[ -n "$X_PACKAGE" ]]; then
    package=$X_PACKAGE
fi

# Turn off error -> exit
set +e

# Print commands
set -x
cargo $args --package="$package" --features="$features" $X_CARGO_ARGS

# Store the exit code
r=$?

# Stop printing commands
set +x

# Clean up
if [[ -e .cargo/config ]]; then
    rm .cargo/config 2> /dev/null
fi

if [[ -e .cargo ]]; then
    rmdir .cargo 2> /dev/null
fi

exit $r
