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

packages=""

for pkg in $X_PACKAGE; do
    packages="$packages --package=$pkg"
done

# Default package is tikv
if [[ -n "$X_RUSTFLAGS" ]]; then
    packages="--package=tikv"
fi

build_std_args=""
if [[ -n "$TIKV_FRAME_POINTER" && "$TIKV_FRAME_POINTER" != "0" ]]; then
    # When `-Z build-std` is enabled, `--target` must be specified explicitly,
    # and specifying `--target` will cause the generated binary to be located
    # in the `target/${TARGET}/release` directory instead of `target/release`,
    # so we need to explicitly specify `--out-dir` here, to avoid errors when
    # copying the output binary later.
    build_std_args="$build_std_args -Z build-std=core,std,alloc,proc_macro,test --target=$TIKV_BUILD_RUSTC_TARGET"
    if [[ -n "$X_CARGO_RELEASE" && "$X_CARGO_RELEASE" != "0" ]]; then
        build_std_args="$build_std_args -Z unstable-options --out-dir=$X_CARGO_TARGET_DIR/release"
    else
        build_std_args="$build_std_args -Z unstable-options --out-dir=$X_CARGO_TARGET_DIR/debug"
    fi
fi

# Turn off error -> exit
set +e

# Print commands
set -x

if [[ -n "$TIKV_FRAME_POINTER" && "$TIKV_FRAME_POINTER" != "0" ]]; then
    rustup component add rust-src
    cargo $args $packages --features="$features" $X_CARGO_ARGS $build_std_args
else
    cargo $args $packages --features="$features" $X_CARGO_ARGS
fi

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
