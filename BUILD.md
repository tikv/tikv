# Build

You can build all components manually. 

## System requirement

+ Linux or Mac OS X.
+ Rust, nightly version is required.
+ Go, 1.5+ is required.

## Components

+ **RocksDB**

    4.3.1+ is required.
    
    * **Linux - Ubuntu**
    
        GCC 4.7+ with C++11 support is required.

        ```sh
        sudo apt-get update
        sudo apt-get install -y --no-install-recommends zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev liblz4-dev
        curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.6.1.tar.gz -o rocksdb.tar.gz
        tar xf rocksdb.tar.gz 
        cd rocksdb-rocksdb-4.6.1 
        make shared_lib 
        sudo make install-shared
        ```
        
    * To install RocksDB on other Linux platforms, see [INSTALL.md](https://github.com/facebook/rocksdb/blob/master/INSTALL.md).

    * **Mac OS X**
        
        ```sh
        brew update
        brew install rocksdb
        ``` 

+ **Etcd**

    The latest version of etcd is required.
    
    ```sh
    git clone https://github.com/coreos/etcd.git ${GOPATH}/src/github.com/coreos/etcd
    cd ${GOPATH}/src/github.com/coreos/etcd
    ./build
    ```

    The binary of `etcd` is installed in `${GOPATH}/src/github.com/coreos/etcd/bin`.

+ **PD**

    ```sh
    git clone https://github.com/pingcap/pd.git ${GOPATH}/src/github.com/pingcap/pd
    cd ${GOPATH}/src/github.com/pingcap/pd
    make build
    ```
    
    The binary of `pd-server` is installed in `${GOPATH}/src/github.com/pingcap/pd/bin`.

+ **TiKV**

    ```sh
    git clone https://github.com/pingcap/tikv.git tikv
    cd tikv
    make release
    ```
    
    The binary of `tikv-server` is installed in `tikv/target/release/`.

+ **TiDB**

    ```sh
    git clone https://github.com/pingcap/tidb.git ${GOPATH}/src/github.com/pingcap/tidb
    cd ${GOPATH}/src/github.com/pingcap/tidb
    make server
    ```

    The binary of `tidb-server` is installed in `${GOPATH}/src/github.com/pingcap/tidb/tidb-server`
