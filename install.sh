#!/bin/bash

CUR_PATH=`pwd`
DEPS_PATH=$CUR_PATH/tmp
rm -rf $DEPS_PATH
mkdir -p $DEPS_PATH

BIN_PATH=$CUR_PATH/bin
mkdir -p $BIN_PATH

export GOPATH=$DEPS_PATH

# Only support Ubuntu and Mac OS X here. 
# Rust nightly is required.
# Go 1.5+ is required.

PATH=$PATH:$GOROOT/bin:$GOPATH:/bin

echo "building rocksdb..."
cd $DEPS_PATH
case "$OSTYPE" in 
    linux*) 
        sudo apt-get update 
        sudo apt-get install -y --no-install-recommends zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev liblz4-dev 
        curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.6.1.tar.gz -o rocksdb.tar.gz 
        tar xf rocksdb.tar.gz 
        cd rocksdb-rocksdb-4.6.1 
        make shared_lib 
        sudo make install-shared 

        # guarantee tikv can find rocksdb.
        export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
    ;;
    darwin*) 
        brew update 
        brew rm rocksdb || true
        brew install rocksdb 
    ;;
    *) 
        echo "unsupported $OSTYPE"
        exit 0
    ;;
esac

echo "building etcd..."
cd $DEPS_PATH
git clone --depth=1 https://github.com/coreos/etcd.git ${GOPATH}/src/github.com/coreos/etcd
cd ${GOPATH}/src/github.com/coreos/etcd
./build
cp -f ./bin/etcd $BIN_PATH

echo "building pd..."
cd $DEPS_PATH
git clone --depth=1 https://github.com/pingcap/pd.git ${GOPATH}/src/github.com/pingcap/pd
cd ${GOPATH}/src/github.com/pingcap/pd
make build
cp -f ./bin/pd-server $BIN_PATH

echo "building tidb..."
cd $DEPS_PATH
git clone --depth=1 https://github.com/pingcap/tidb.git ${GOPATH}/src/github.com/pingcap/tidb
cd ${GOPATH}/src/github.com/pingcap/tidb
make server
cp -f ./tidb-server/tidb-server $BIN_PATH

echo "building tikv..."
cd $DEPS_PATH
git clone --depth=1 https://github.com/pingcap/tikv.git tikv
cd tikv
make release
cp -f ./target/release/tikv-server $BIN_PATH

echo "build finished."
