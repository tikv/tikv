#!/bin/bash

base_path=$(cd $(dirname $0); pwd)

set -xe

clang-format -style google -i ${base_path}/src/RaftStoreProxyFFI/*.h
