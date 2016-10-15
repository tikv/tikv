#!/usr/bin/env bash

clean_target=clean

if [[ $1 == "--use-cache" ]]; then
    clean_target=
fi

if [[ "$(ls -A . 2>/dev/null)" = "" ]]; then
    echo please mount tikv source to /tikv first. >&2
    exit 1
fi

scl enable devtoolset-4 python27 "ROCKSDB_SYS_STATIC=1 ROCKSDB_SYS_PORTABLE=1 make $clean_target release" 

