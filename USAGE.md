# How to use TiKV

## Getting started

### System requirement

+ Linux (Ubuntu 14.04+ or CentOS 7+) or Mac OS X.
+ Rust, [nightly version](https://www.rust-lang.org/downloads.html) is required.
+ Go, [1.5+](https://golang.org/doc/install) is required.
+ GCC 4.8+ is required.

### Installing TiKV

You can use `make install` to install all binary executions in `bin` directory.

Note:

Now `make install` only supports Linux Ubuntu, CentOS and Mac OS X. If you use other Linux platforms, install RocksDB manually according to [INSTALL.md](https://github.com/facebook/rocksdb/blob/master/INSTALL.md). 

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
    
    
    + `cluster-id`: The unique ID to distinguish different PD clusters. It can't be changed after bootstrapping.  
    + `addr`: The listening address for client traffic. 
    + `advertise-addr`: The advertise address for external client communication. It must be accessible to the PD node.
    + `http-addr`: The HTTP listening address for client requests. 
    + `etcd-name`: The etcd human readable name for this node. 
    + `etcd-data-dir`: The etcd path to the data directory.
    + `etcd-listen-peer-url`: The etcd listening address for peer traffic.
    + `etcd-advertise-peer-url`: The etcd advertise peer url to the rest of the cluster.
    + `etcd-listen-client-url`: The etcd listening address for client traffic.
    + `etcd-advertise-client-url`: The etcd advertise url to the public. It must be accessible to the PD node.
    + `etcd-initial-cluster-state`: The etcd initial cluster state. The value is either`new` or `existing`.
    + `etcd-initial-cluster`: The etcd initail cluster configuration for bootstrapping.  
    

2. Start TiKV on listening port 5551. The data is stored in the `data1` directory and the cluster ID is 1.

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

In the production environment, it is strongly recommended to run TiKV in the cluster mode. 

1. Start PD cluster.

    Start three pd-server instances listening on different ports with the same cluster ID. They will form a PD cluster.
    
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

### Running in the local cluster mode with `docker-compose`

A simple `docker-compose.yml`:

```bash
version: '2'

services:
  pd1:
    image: pingcap/pd
    ports:
      - "1234"
      - "9090"
      - "2379"
      - "2380"

    command:
      - --addr=0.0.0.0:1234
      - --advertise-addr=pd1:1234
      - --http-addr=0.0.0.0:9090
      - --cluster-id=1
      - --etcd-name=pd1 
      - --etcd-advertise-client-url=http://pd1:2379 
      - --etcd-advertise-peer-url=http://pd1:2380
      - --etcd-initial-cluster=pd1=http://pd1:2380,pd2=http://pd2:2380,pd3=http://pd3:2380
      - --etcd-listen-peer-url=http://0.0.0.0:2380
      - --etcd-listen-client-url=http://0.0.0.0:2379 

    privileged: true

  pd2:
    image: pingcap/pd
    ports:
      - "1234"
      - "9090"
      - "2379"
      - "2380"

    command:
      - --addr=0.0.0.0:1234
      - --advertise-addr=pd2:1234
      - --http-addr=0.0.0.0:9090
      - --cluster-id=1
      - --etcd-name=pd2
      - --etcd-advertise-client-url=http://pd2:2379
      - --etcd-advertise-peer-url=http://pd2:2380
      - --etcd-initial-cluster=pd1=http://pd1:2380,pd2=http://pd2:2380,pd3=http://pd3:2380
      - --etcd-listen-peer-url=http://0.0.0.0:2380 
      - --etcd-listen-client-url=http://0.0.0.0:2379 

    privileged: true

  pd3:
    image: pingcap/pd
    ports:
      - "1234"
      - "9090"
      - "2379"
      - "2380"

    command:
      - --addr=0.0.0.0:1234
      - --advertise-addr=pd3:1234
      - --http-addr=0.0.0.0:9090
      - --cluster-id=1
      - --etcd-name=pd3
      - --etcd-advertise-client-url=http://pd3:2379 
      - --etcd-advertise-peer-url=http://pd3:2380 
      - --etcd-initial-cluster=pd1=http://pd1:2380,pd2=http://pd2:2380,pd3=http://pd3:2380 
      - --etcd-listen-peer-url=http://0.0.0.0:2380 
      - --etcd-listen-client-url=http://0.0.0.0:2379 

    privileged: true

  tikv1:
    image: pingcap/tikv
    ports:
      - "20160"

    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv1:20160
      - --cluster-id=1
      - --dsn=raftkv
      - --store=/var/tikv
      - --etcd=pd1:2379,pd2:2379,pd3:2379

    depends_on:
      - "pd1"
      - "pd2"
      - "pd3"

    entrypoint: /tikv-server

    privileged: true

  tikv2:
    image: pingcap/tikv
    ports:
      - "20160"

    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv2:20160
      - --cluster-id=1
      - --dsn=raftkv
      - --store=/var/tikv
      - --etcd=pd1:2379,pd2:2379,pd3:2379

    depends_on:
      - "pd1"
      - "pd2"
      - "pd3"

    entrypoint: /tikv-server

    privileged: true

  tikv3:
    image: pingcap/tikv
    ports:
      - "20160"

    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv3:20160
      - --cluster-id=1
      - --dsn=raftkv
      - --store=/var/tikv
      - --etcd=pd1:2379,pd2:2379,pd3:2379

    depends_on:
      - "pd1"
      - "pd2"
      - "pd3"

    entrypoint: /tikv-server

    privileged: true

  tidb:
    image: pingcap/tidb
    ports:
      - "4000"

    command:
      - --store=tikv 
      - --path=pd1:2379,pd2:2379,pd3:2379/pd?cluster=1
      - -L=info

    depends_on:
      - "tikv1"
      - "tikv2"
      - "tikv3"

    privileged: true
    
```

+ Use `docker-compose up -d` to create and start the cluster. 
+ Use `docker-compose port tidb 4000` to print the TiDB host port. For example, if the output is `0.0.0.0:32966`, the TiDB host port is `32966`.
+ Use `mysql -h 127.0.0.1 -P 32966 -u root -D test` to connect to TiDB and enjoy it. 
+ Use `docker-compose down` to stop and remove the cluster.

You can define multiple TiDBs and put a haproxy before them, for example:

```bash
  tidb1:
    image: pingcap/tidb
    ports:
      - "4000"

    command:
      - --store=tikv 
      - --path=pd1:2379,pd2:2379,pd3:2379/pd?cluster=1
      - -L=info

    depends_on:
      - "tikv1"
      - "tikv2"
      - "tikv3"

    privileged: true

  tidb2:
    image: pingcap/tidb
    ports:
      - "4000"

    command:
      - --store=tikv 
      - --path=pd1:2379,pd2:2379,pd3:2379/pd?cluster=1
      - -L=info

    depends_on:
      - "tikv1"
      - "tikv2"
      - "tikv3"

    privileged: true

  tidb3:
    image: pingcap/tidb
    ports:
      - "4000"

    command:
      - --store=tikv 
      - --path=pd1:2379,pd2:2379,pd3:2379/pd?cluster=1
      - -L=info

    depends_on:
      - "tikv1"
      - "tikv2"
      - "tikv3"

    privileged: true

  haproxy:
    image: pingcap/haproxy

    ports:
      - "4000"

    depends_on:
      - "tidb1"
      - "tidb2"
      - "tidb3"
```

You can use `docker-compose port haproxy 4000` to get haproxy host port and use `mysql` to connect to it. 