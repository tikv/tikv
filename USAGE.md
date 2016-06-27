# How to use TiKV

## Getting started

### System requirement

+ Linux or Mac OS X.
+ Rust, nightly version is required.
+ Go, 1.5+ is required.

### Installing TiKV

+ **RocksDB**

    4.3.1+ is required.
    
    * **Linux - Ubuntu**
    
    GCC 4.7+ with C++11 support is required.

    ```sh
    apt-get install zlib1g-dev libbz2-dev libsnappy-dev libgflags-dev liblz4-dev
    curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.3.1.tar.gz -o rocksdb.tar.gz
    tar xf rocksdb.tar.gz 
    cd rocksdb-rocksdb-4.3.1 
    make shared_lib 
    make install-shared
    ```
    
    * **Mac OS X**
    
    ```sh
    brew update
    brew install rocksdb
    ``` 
    
    * To install RocksDB on other Linux platform, see [INSTALL.md](https://github.com/facebook/rocksdb/blob/master/INSTALL.md).

+ **Etcd**

    It is required to use the latest etcd version which supports gRPC from the master branch.
    
    ```sh
    git clone https://github.com/coreos/etcd.git ${GOPATH}/src/github.com/coreos/etcd
    cd ${GOPATH}/src/github.com/coreos/etcd
    ./build
    ```

    The binary of `etcd` will be installed in `${GOPATH}/src/github.com/coreos/etcd/bin`

+ **PD**

    ```sh
    git clone https://github.com/pingcap/pd.git ${GOPATH}/src/github.com/pingcap/pd
    cd ${GOPATH}/src/github.com/pingcap/pd
    make build
    ```
    
    The binary of `pd-server` will be installed in `${GOPATH}/src/github.com/pingcap/pd/bin`

+ **TiKV**

    ```sh
    git clone https://github.com/pingcap/tikv.git tikv
    cd tikv
    make release
    ```
    
    The binary of `tikv-server` will be installed in `tikv/target/release/`

+ **TiDB**

    ```sh
    git clone https://github.com/pingcap/tidb.git ${GOPATH}/src/github.com/pingcap/tidb
    cd ${GOPATH}/src/github.com/pingcap/tidb
    make server
    ```

    The binary of `tidb-server` will be installed in `${GOPATH}/src/github.com/pingcap/tidb/tidb-server`

### Running single node

1. Start Etcd with default listening port 2379.

    ```sh
    etcd 
    ```

2. Start PD with listening port 1234 and cluster ID 1.

    ```sh
    pd-server -addr 127.0.0.1:1234 --etcd 127.0.0.1:2379 --cluster-id 1 --root pd
    ```

    Cluster ID is to distinguish different TiKV cluster.
    The root `pd` is the root prefix path in Etcd. 

3. Start TiKV with listening port 5551, saving data path `data1` and cluster ID 1.

    ```sh
    tikv-server -S raftkv --addr 127.0.0.1:5551 --pd 127.0.0.1:1234 -s data1 --cluster-id 1
    ```

4. Start TiDB with listening port 5001. 

    ```sh
    tidb-server --store=tikv --path="127.0.0.1:2379/pd?cluster=1" -lease 1 -P 5001
    ```

5. Use official `mysql` client to connect TiDB and enjoy it. 

    ```sh
    mysql -h 127.0.0.1 -P 5001 -u root -D test
    ```

### Running cluster

In production, the cluster deployment is recommended. 

1. Start etcd cluster, see [multi-machine cluster](https://github.com/coreos/etcd/blob/master/Documentation/op-guide/clustering.md).

    Let's assume the etcd cluster endpoints are 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379.

2. Start PD cluster.

    ```sh
    pd-server -addr 127.0.0.1:1234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root pd
    pd-server -addr 127.0.0.1:2234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root pd
    pd-server -addr 127.0.0.1:3234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root pd
```

3. Start TiKV cluster.

    ```sh
    tikv-server -S raftkv --addr 127.0.0.1:5551 --pd 127.0.0.1:1234,127.0.0.1:2234,127.0.0.1:3234 -s data1 --cluster-id 1
    tikv-server -S raftkv --addr 127.0.0.1:5552 --pd 127.0.0.1:1234,127.0.0.1:2234,127.0.0.1:3234 -s data2 --cluster-id 1
    tikv-server -S raftkv --addr 127.0.0.1:5553 --pd 127.0.0.1:1234,127.0.0.1:2234,127.0.0.1:3234 -s data3 --cluster-id 1
    ```

4. Start TiDB and use `mysql`.

    ```sh
    tidb-server --store=tikv --path="127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379/pd?cluster=1" -lease 1 -P 5001
    
    mysql -h 127.0.0.1 -P 5001 -u root -D test
    ```