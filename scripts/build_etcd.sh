#!/bin/bash

set -e

echo "building etcd..."
mkdir -p ${DEPS_PATH}
mkdir -p ${BIN_PATH}

cd ${DEPS_PATH}

case "$OSTYPE" in 
    linux*) 
        curl -L https://github.com/coreos/etcd/releases/download/v3.0.0/etcd-v3.0.0-linux-amd64.tar.gz -o etcd-v3.0.0-linux-amd64.tar.gz
        tar xzvf etcd-v3.0.0-linux-amd64.tar.gz
        cd etcd-v3.0.0-linux-amd64
        cp -f etcd ${BIN_PATH}
    ;;
    darwin*) 
        curl -L https://github.com/coreos/etcd/releases/download/v3.0.0/etcd-v3.0.0-darwin-amd64.zip -o etcd-v3.0.0-darwin-amd64.zip
        unzip etcd-v3.0.0-darwin-amd64.zip
        cd etcd-v3.0.0-darwin-amd64
        cp -f etcd ${BIN_PATH}
    ;;
    *) 
        echo "unsupported $OSTYPE"
        exit 1
    ;;
esac