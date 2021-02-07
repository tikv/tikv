#!/bin/bash

set -ex

ffi_path="raftstore-proxy/ffi"
./${ffi_path}/format.sh
cargo run --package gen-proxy-ffi --bin gen-proxy-ffi
