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

### Running in the local standalone mode


1. Start PD.

    ```sh
    pd-server --cluster-id=1 \
              --addr=127.0.0.1:1234 \
              --advertise-addr="127.0.0.1:1234" \
              --http-addr="127.0.0.1:9090" \
              --etcd-name="default" \
              --etcd-data-dir="default.pd" \
              --etcd-listen-peer-url="http://127.0.0.1:2380" \
              --etcd-advertise-peer-url="http://127.0.0.1:2380" \
              --etcd-listen-client-url="http://127.0.0.1:2379" \
              --etcd-advertise-client-url="http://127.0.0.1:2379" \
              --etcd-initial-cluster="default=http://127.0.0.1:2380" \
              --etcd-initial-cluster-state="new"
    ```
    

    + `cluster-id`: Unique ID to distinguish different PD clusters. It can't be changed after bootstrapping.  
    + `addr`: Listening address for client traffic. The default official address is `127.0.0.1:1234`.
    + `advertise-addr`: Advertise address for external client communication. It must be accessible to the PD node.
    + `http-addr`: HTTP listening address for client requests. 
    + `etcd-name`: etcd human readable name for this node. 
    + `etcd-data-dir`: etcd path to the data directory.
    + `etcd-listen-peer-url`: etcd listening address for peer traffic.
    + `etcd-advertise-peer-url`: etcd advertise peer url to the rest of the cluster.
    + `etcd-listen-client-url`: etcd listening address for client traffic.
    + `etcd-advertise-client-url`: etcd advertise url to the public, it must be accessible to PD machine.
    + `etcd-initial-cluster-state`: etcd initial cluster state. The value is either`new` or `existing`.
    + `etcd-initial-cluster`: etcd initail cluster configuration for bootstrapping. 
    

2. Start TiKV on listening port 5551. The data is stored in directory `data1` and cluster ID is 1.

    ```sh
    tikv-server -S raftkv --addr 127.0.0.1:5551 --etcd 127.0.0.1:2379 -s data1 --cluster-id 1
    ```

3. Start TiDB on listening port 5001. 

    ```sh
    tidb-server --store=tikv --path="127.0.0.1:2379/pd?cluster=1" -lease 1 -P 5001
    ```

4. Use the official `mysql` client to connect to TiDB and enjoy it. 

    ```sh
    mysql -h 127.0.0.1 -P 5001 -u root -D test
    ```

### Running in the local cluster mode

In production environment, it is strongly recommended to run TiKV in the cluster mode. 

1. Start PD cluster.

    Start three pd-server instances listening on different ports with the same cluster id. They will form a PD cluster.
    
    ```sh
    pd-server --cluster-id=1 \
          --addr=127.0.0.1:11234 \
          --advertise-addr="127.0.0.1:11234" \
          --http-addr="127.0.0.1:19090" \
          --cluster-id=1 \
          --etcd-name=pd1 \
          --etcd-data-dir="default.pd1" \
          --etcd-advertise-client-url="http://127.0.0.1:12379" \
          --etcd-advertise-peer-url="http://127.0.0.1:12380" \
          --etcd-initial-cluster="pd1=http://127.0.0.1:12380,pd2=http://127.0.0.1:22380,pd3=http://127.0.0.1:32380" \
          --etcd-listen-peer-url="http://127.0.0.1:12380" \
          --etcd-listen-client-url="http://127.0.0.1:12379"  
          
    pd-server --cluster-id=1 \
          --addr=127.0.0.1:21234 \
          --advertise-addr="127.0.0.1:21234" \
          --http-addr="127.0.0.1:29090" \
          --cluster-id=1 \
          --etcd-name=pd2 \
          --etcd-data-dir="default.pd2" \
          --etcd-advertise-client-url="http://127.0.0.1:22379" \
          --etcd-advertise-peer-url="http://127.0.0.1:22380" \
          --etcd-initial-cluster="pd1=http://127.0.0.1:12380,pd2=http://127.0.0.1:22380,pd3=http://127.0.0.1:32380" \
          --etcd-listen-peer-url="http://127.0.0.1:22380" \
          --etcd-listen-client-url="http://127.0.0.1:22379"  

    pd-server --cluster-id=1 \
          --addr=127.0.0.1:31234 \
          --advertise-addr="127.0.0.1:31234" \
          --http-addr="127.0.0.1:39090" \
          --cluster-id=1 \
          --etcd-name=pd3 \
          --etcd-data-dir="default.pd3" \
          --etcd-advertise-client-url="http://127.0.0.1:32379" \
          --etcd-advertise-peer-url="http://127.0.0.1:32380" \
          --etcd-initial-cluster="pd1=http://127.0.0.1:12380,pd2=http://127.0.0.1:22380,pd3=http://127.0.0.1:32380" \
          --etcd-listen-peer-url="http://127.0.0.1:32380" \
          --etcd-listen-client-url="http://127.0.0.1:32379"  
    ```

2. Start TiKV cluster.

    Start three tikv-server instances listening on different ports with the same pd address and cluster id. They will form a TiKV cluster.
    
    ```sh
    tikv-server -S raftkv --addr 127.0.0.1:5551 --etcd 127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 -s data1 --cluster-id 1
    tikv-server -S raftkv --addr 127.0.0.1:5552 --etcd 127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 -s data2 --cluster-id 1
    tikv-server -S raftkv --addr 127.0.0.1:5553 --etcd 127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 -s data3 --cluster-id 1
    ```

3. Start TiDB.

    ```sh
    tidb-server --store=tikv --path="127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379/pd?cluster=1" -lease 1 -P 5001
    ```
    
4. Connect to TiDB.

    ```
    mysql -h 127.0.0.1 -P 5001 -u root -D test
    ```
