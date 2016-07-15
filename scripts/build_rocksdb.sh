#!/bin/bash

set -e

echo "building rocksdb..."
mkdir -p ${DEPS_PATH}
cd $DEPS_PATH

ROCKSDB_VER=4.6.1

SUDO=
if which sudo; then 
    SUDO=sudo
fi

function get_linux_platform {
    if [ -f /etc/lsb-release ]; then 
        # For Ubuntu or debian, we can use apt-get.
        echo "Ubuntu"
    elif [ -f /etc/redhat-release ]; then
        # For CentOS or redhat, we can use yum.
        echo "CentOS"
    else
        echo "Unknown"
    fi 
}

function install_in_ubuntu {
    if [ ! -d rocksdb-rocksdb-${ROCKSDB_VER} ]; then
        ${SUDO} apt-get update 
        ${SUDO} apt-get install -y --no-install-recommends zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev liblz4-dev 
        curl -L https://github.com/facebook/rocksdb/archive/rocksdb-${ROCKSDB_VER}.tar.gz -o rocksdb.tar.gz 
        tar xf rocksdb.tar.gz 
    fi
    
    cd rocksdb-rocksdb-${ROCKSDB_VER} 
    make shared_lib 
    ${SUDO} make install-shared 
    # guarantee tikv can find rocksdb.
    ${SUDO} ldconfig
}

function install_in_centos {
    if [ ! -d rocksdb-rocksdb-${ROCKSDB_VER} ]; then
        ${SUDO} yum install -y epel-release
        ${SUDO} yum install -y snappy-devel zlib-devel bzip2-devel lz4-devel
        curl -L https://github.com/facebook/rocksdb/archive/rocksdb-${ROCKSDB_VER}.tar.gz -o rocksdb.tar.gz 
        tar xf rocksdb.tar.gz 
    fi
    
    cd rocksdb-rocksdb-${ROCKSDB_VER} 
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
            CentOS)
                install_in_centos
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