# How to use TiKV

## Getting started

### Installing TiKV

TiKV depends on Etcd and PD, which both need [*Go*](https://golang.org/) installed on your machine (version 1.5+ is required). 

It is required to use the latest etcd version which supports gRPC from the master branch.

Etcd:

```sh
git clone https://github.com/coreos/etcd.git ${GOPATH}/src/github.com/coreos/etcd
cd ${GOPATH}/src/github.com/coreos/etcd
./build
```

The `etcd` binary execution will be installed in `${GOPATH}/src/github.com/coreos/etcd/bin`

PD:

```sh
git clone https://github.com/pingcap/pd.git ${GOPATH}/src/github.com/pingcap/pd
cd ${GOPATH}/src/github.com/pingcap/pd
make build
```

The `pd-server` binary execution will be installed in `${GOPATH}/src/github.com/pingcap/pd/bin`

TiKV:

```sh
git clone https://github.com/pingcap/tikv.git tikv
cd tikv
make release
```

The `tikv-server` binary execution will be installed in `tikv/target/release/`

You should also install TiDB for using TiKV.

```sh
git clone https://github.com/pingcap/tidb.git ${GOPATH}/src/github.com/pingcap/tidb
cd ${GOPATH}/src/github.com/pingcap/tidb
make server
```

The `tidb-server` binary execution will be installed in `${GOPATH}/src/github.com/pingcap/tidb/tidb-server`

### Running single node

Start Etcd with default listening port 2379.

```sh
etcd 
```

Start PD with listening port 1234 and cluster ID 1.

```sh
pd-server -addr 127.0.0.1:1234 --etcd 127.0.0.1:2379 --cluster-id 1 --root pd
```

Cluster ID is to distinguish different TiKV cluster.
The root `pd` is the root prefix path in Etcd. 

Start TiKV with listening port 5551, saving data path `data1` and cluster ID 1.

```sh
tikv-server -S raftkv --addr 127.0.0.1:5551 --pd 127.0.0.1:1234 -s data1 --cluster-id 1
```

Start TiDB with listening port 5001. 

```sh
tidb-server --store=tikv --path="127.0.0.1:2379/pd?cluster=1" -lease 1 -P 5001
```

Use official `mysql` client to connect TiDB and enjoy it. 

```sh
mysql -h 127.0.0.1 -P 5001 -u root -D test
```

### Running cluster

In production, the cluster deployment is recommended. 

Start Etcd cluster, see [multi-machine cluster](https://github.com/coreos/etcd/blob/master/Documentation/op-guide/clustering.md).

Let's assume the Etcd cluster endpoints are 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379.

Start PD cluster.
```sh
pd-server -addr 127.0.0.1:1234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root pd
pd-server -addr 127.0.0.1:2234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root pd
pd-server -addr 127.0.0.1:3234 --etcd 127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379 --cluster-id 1 --root pd
```

Start TiKV cluster.
```sh
tikv-server -S raftkv --addr 127.0.0.1:5551 --pd 127.0.0.1:1234,127.0.0.1:2234,127.0.0.1:3234 -s data1 --cluster-id 1
tikv-server -S raftkv --addr 127.0.0.1:5552 --pd 127.0.0.1:1234,127.0.0.1:2234,127.0.0.1:3234 -s data2 --cluster-id 1
tikv-server -S raftkv --addr 127.0.0.1:5553 --pd 127.0.0.1:1234,127.0.0.1:2234,127.0.0.1:3234 -s data3 --cluster-id 1
```

Start TiDB and use `mysql`.
```sh
tidb-server --store=tikv --path="127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379/pd?cluster=1" -lease 1 -P 5001

mysql -h 127.0.0.1 -P 5001 -u root -D test
```