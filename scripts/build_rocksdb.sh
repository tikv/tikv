#!/bin/bash

set -e

echo "building rocksdb..."
mkdir -p ${DEPS_PATH}
cd $DEPS_PATH

function get_linux_platform {
    if [ -f /etc/lsb-release ]; then 
        DIST=`cat /etc/lsb-release | grep '^DISTRIB_ID' | awk -F=  '{ print $2 }'`
        echo "$DIST"
    else
        echo "Unknown"
    fi 
}

function install_in_ubuntu {
    SUDO=
    if which sudo; then 
        SUDO=sudo
    fi

    if [ ! -d rocksdb-rocksdb-4.6.1 ]; then
        ${SUDO} apt-get update 
        ${SUDO} apt-get install -y --no-install-recommends zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev liblz4-dev 
        curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.6.1.tar.gz -o rocksdb.tar.gz 
        tar xf rocksdb.tar.gz 
    fi
    
    cd rocksdb-rocksdb-4.6.1 
    make shared_lib 
    ${SUDO} make install-shared 
    # guarantee tikv can find rocksdb.
    ${SUDO} ldconfig
}

case "$OSTYPE" in 
    linux*) 
        dist=$(get_linux_platform)
        case $dist in
            Ubuntu)
                install_in_ubuntu
            ;;
            *)
                echo "unsupported platform $dist, you may install RocksDB manually"
                exit 0
            ;;

        esac
    ;;
    darwin*) 
        brew update 
        brew rm rocksdb || true
        brew install rocksdb 
    ;;
    *) 
        echo "unsupported $OSTYPE"
        exit 1
    ;;
esac