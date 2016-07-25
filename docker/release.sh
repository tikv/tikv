#!/usr/bin/env bash

set -e

error() {
    echo $@ >&2
    return 1
}

if [[ $# -ne 1 ]]; then
    error $0 [hash\|tag\|branch]
fi

if [[ -d tikv ]]; then
    cd tikv
    if [[ ! -d .git ]]; then
        git init .
        git remote add origin https://github.com/pingcap/tikv.git
    fi
    git fetch origin
else
    git clone https://github.com/pingcap/tikv.git
    cd tikv
fi
git checkout $1
git merge origin/$1

scl enable devtoolset-4 python27 "ROCKSDB_SYS_STATIC=1 ROCKSDB_SYS_PORTABLE=1 make clean release" 
