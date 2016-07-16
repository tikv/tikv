# How to use TiKV

## Getting started

### System requirement

+ Linux (Ubuntu 14.04+ or CentOS 7+) or Mac OS X.
+ Rust, [nightly version](https://www.rust-lang.org/downloads.html) is required.
+ Go, [1.5+](https://golang.org/doc/install) is required.
+ GCC 4.8+ is required.

### Installing TiKV

You can use `make install` to install all binary executions in `bin` directory.

Notice:

Now `make install` only supports Linux Ubuntu, CentOS and Mac OS X. If you use other Linux platforms, 
you should follow [INSTALL.md](https://github.com/facebook/rocksdb/blob/master/INSTALL.md) to
install RocksDB manually first. 

### Running in the standalone mode

1. Start etcd on the default listening port 2379.

    ```sh
    etcd 
    ```

2. Start PD on listening port 1234 and cluster ID is 1.

    ```sh
    pd-server -addr 127.0.0.1:1234 --etcd 127.0.0.1:2379 --cluster-id 1 --root /pd
    ```

    Cluster ID is used to distinguish different TiKV clusters.
    `/pd` is the root prefix path in etcd. 

3. Start TiKV on listening port 5551. The data is stored in directory `data1` and cluster ID is 1.

    ```sh
    tikv-server -S raftkv --addr 127.0.0.1:5551 --etcd 127.0.0.1:2379 -s data1 --cluster-id 1
    ```

4. Start TiDB on listening port 5001. 

    ```sh
    tidb-server --store=tikv --path="127.0.0.1:2379/pd?cluster=1" -lease 1 -P 5001
    ```

5. Use the official `mysql` client to connect to TiDB and enjoy it. 

    ```sh
    mysql -h 127.0.0.1 -P 5001 -u root -D test
    ```

### Running in the cluster mode

In production environment, it is strongly recommended to run TiKV in the cluster mode. 

1. Start etcd cluster, see [multi-machine cluster](https://github.com/coreos/etcd/blob/master/Documentation/op-guide/clustering.md).

    Let's assume that the etcd cluster endpoints are 127.0.0.1:2379, 127.0.0.1:3379, 127.0.0.1:4379.

2. Start PD cluster.

    Start three pd-server instances listening on different ports with the same etcd address and cluster id. They will form a PD cluster.
    
    ```sh
    pd-server -addr 127.0.0.1:1234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root /pd
    pd-server -addr 127.0.0.1:2234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root /pd
    pd-server -addr 127.0.0.1:3234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root /pd
    ```

3. Start TiKV cluster.

    Start three tikv-server instances listening on different ports with the same pd address and cluster id. They will form a TiKV cluster.
    
    ```sh
    tikv-server -S raftkv --addr 127.0.0.1:5551 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 -s data1 --cluster-id 1
    tikv-server -S raftkv --addr 127.0.0.1:5552 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 -s data2 --cluster-id 1
    tikv-server -S raftkv --addr 127.0.0.1:5553 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 -s data3 --cluster-id 1
    ```

4. Start TiDB.

    ```sh
    tidb-server --store=tikv --path="127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379/pd?cluster=1" -lease 1 -P 5001
    ```
    
5. Connect to TiDB.

    ```
    mysql -h 127.0.0.1 -P 5001 -u root -D test
    ```
