#!/bin/bash

echo "building rocksdb..."
cd $DEPS_PATH

case "$OSTYPE" in 
    linux*) 
        if [ ! -d "$DIRECTORY" ]; then
            sudo apt-get update 
            sudo apt-get install -y --no-install-recommends zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev liblz4-dev 
            curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.6.1.tar.gz -o rocksdb.tar.gz 
            tar xf rocksdb.tar.gz 
        fi
        
        cd rocksdb-rocksdb-4.6.1 
        make shared_lib 
        sudo make install-shared 
        # guarantee tikv can find rocksdb.
        sudo ldconfig
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