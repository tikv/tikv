---
title: Try Two Types of APIs
summary: Learn how to use the Raw Key-Value API and the Transactional Key-Value API in TiKV.
category: reference
---

# Try Two Types of APIs

To apply to different scenarios, TiKV provides [two types of APIs](../../overview.md#two-types-of-apis) for developers: the Raw Key-Value API and the Transactional Key-Value API. This document uses two examples to guide you through how to use the two APIs in TiKV. The usage examples are based on multiple nodes for testing. You can also quickly try the two types of APIs on a single machine.

> **Warning:** Do not use these two APIs together in the same cluster, otherwise they might corrupt each other's data.

## Try the Raw Key-Value API

To use the Raw Key-Value API in applications developed in the Go language, take the following steps:

1. Install the necessary packages.

    ```bash
    export GO111MODULE=on
    go mod init rawkv-demo
    go get github.com/pingcap/tidb@master
    ```

2. Import the dependency packages.

    ```go
    import (
        "fmt"
        "github.com/pingcap/tidb/config"
        "github.com/pingcap/tidb/store/tikv"
    )
    ```

3. Create a Raw Key-Value client.

    ```go
    cli, err := tikv.NewRawKVClient([]string{"192.168.199.113:2379"}, config.Security{})
    ```

    Description of two parameters in the above command:

    - `string`: a list of PD servers’ addresses
    - `config.Security`: used to establish TLS connections, usually left empty when you do not need TLS

4. Call the Raw Key-Value client methods to access the data on TiKV. The Raw Key-Value API contains the following methods, and you can also find them at [GoDoc](https://godoc.org/github.com/pingcap/tidb/store/tikv#RawKVClient).

    ```go
    type RawKVClient struct
    func (c *RawKVClient) Close() error
    func (c *RawKVClient) ClusterID() uint64
    func (c *RawKVClient) Delete(key []byte) error
    func (c *RawKVClient) Get(key []byte) ([]byte, error)
    func (c *RawKVClient) Put(key, value []byte) error
    func (c *RawKVClient) Scan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
    ```

### Usage example of the Raw Key-Value API

```go
package main

import (
    "fmt"

    "github.com/pingcap/tidb/config"
    "github.com/pingcap/tidb/store/tikv"
)

func main() {
    cli, err := tikv.NewRawKVClient([]string{"192.168.199.113:2379"}, config.Security{})
    if err != nil {
        panic(err)
    }
    defer cli.Close()

    fmt.Printf("cluster ID: %d\n", cli.ClusterID())

    key := []byte("Company")
    val := []byte("PingCAP")

    // put key into tikv
    err = cli.Put(key, val)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Successfully put %s:%s to tikv\n", key, val)

    // get key from tikv
    val, err = cli.Get(key)
    if err != nil {
        panic(err)
    }
    fmt.Printf("found val: %s for key: %s\n", val, key)

    // delete key from tikv
    err = cli.Delete(key)
    if err != nil {
        panic(err)
    }
    fmt.Printf("key: %s deleted\n", key)

    // get key again from tikv
    val, err = cli.Get(key)
    if err != nil {
        panic(err)
    }
    fmt.Printf("found val: %s for key: %s\n", val, key)
}
```

The result is like:

```bash
INFO[0000] [pd] create pd client with endpoints [192.168.199.113:2379]
INFO[0000] [pd] leader switches to: http://127.0.0.1:2379, previous:
INFO[0000] [pd] init cluster id 6554145799874853483
cluster ID: 6554145799874853483
Successfully put Company:PingCAP to tikv
found val: PingCAP for key: Company
key: Company deleted
found val:  for key: Company
```

RawKVClient is a client of the TiKV server and only supports the GET/PUT/DELETE/SCAN commands. The RawKVClient can be safely and concurrently accessed by multiple goroutines, as long as it is not closed. Therefore, for one process, one client is enough generally.

### Possible Error

- If you see this error:

    ```bash
    build rawkv-demo: cannot load github.com/pingcap/pd/pd-client: cannot find module providing package github.com/pingcap/pd/pd-client
    ```

    You can run `GO111MODULE=on go get -u github.com/pingcap/tidb@master` to fix it.

- If you got this error when you run `go get -u github.com/pingcap/tidb@master`:

    ```
    go: github.com/golang/lint@v0.0.0-20190409202823-959b441ac422: parsing go.mod: unexpected module path "golang.org/x/lint"
    ```

    You can run `go mod edit -replace github.com/golang/lint=golang.org/x/lint@latest` to fix it. [Refer Link](https://github.com/golang/lint/issues/446#issuecomment-483638233)

## Try the Transactional Key-Value API

The Transactional Key-Value API is more complicated than the Raw Key-Value API. Some transaction related concepts are listed as follows. For more details, see the [KV package](https://github.com/pingcap/tidb/tree/master/kv).

- Storage
    
    Like the RawKVClient, a Storage is an abstract TiKV cluster.

- Snapshot

    A Snapshot is the state of a Storage at a particular point of time, which provides some readonly methods. The multiple times read from a same Snapshot is guaranteed consistent.

- Transaction

    Like the transactions in SQL, a Transaction symbolizes a series of read and write operations performed within the Storage. Internally, a Transaction consists of a Snapshot for reads, and a MemBuffer for all writes. The default isolation level of a Transaction is Snapshot Isolation.

To use the Transactional Key-Value API in applications developed by golang, take the following steps:

1. Install the necessary packages.

    ```bash
    export GO111MODULE=on
    go mod init txnkv-demo
    go get github.com/pingcap/tidb@master
    ```

2. Import the dependency packages.

    ```go
    import (
        "flag"
        "fmt"
        "os"

        "github.com/juju/errors"
        "github.com/pingcap/tidb/kv"
        "github.com/pingcap/tidb/store/tikv"
        "github.com/pingcap/tidb/terror"

        goctx "golang.org/x/net/context"
    )
    ```

3. Create Storage using a URL scheme.

    ```go
    driver := tikv.Driver{}
    storage, err := driver.Open("tikv://192.168.199.113:2379")
    ```

4. (Optional) Modify the Storage using a Transaction.

    The lifecycle of a Transaction is: _begin → {get, set, delete, scan} → {commit, rollback}_.

5. Call the Transactional Key-Value API's methods to access the data on TiKV. The Transactional Key-Value API contains the following methods:

    ```go
    Begin() -> Txn
    Txn.Get(key []byte) -> (value []byte)
    Txn.Set(key []byte, value []byte)
    Txn.Iter(begin, end []byte) -> Iterator
    Txn.Delete(key []byte)
    Txn.Commit()
    ```

### Usage example of the Transactional Key-Value API

```go
package main

import (
    "flag"
    "fmt"
    "os"

    "github.com/juju/errors"
    "github.com/pingcap/tidb/kv"
    "github.com/pingcap/tidb/store/tikv"
    "github.com/pingcap/tidb/terror"

    goctx "golang.org/x/net/context"
)

type KV struct {
    K, V []byte
}

func (kv KV) String() string {
    return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

var (
    store  kv.Storage
    pdAddr = flag.String("pd", "192.168.199.113:2379", "pd address:192.168.199.113:2379")
)

// Init initializes information.
func initStore() {
    driver := tikv.Driver{}
    var err error
    store, err = driver.Open(fmt.Sprintf("tikv://%s", *pdAddr))
    terror.MustNil(err)
}

// key1 val1 key2 val2 ...
func puts(args ...[]byte) error {
    tx, err := store.Begin()
    if err != nil {
        return errors.Trace(err)
    }

    for i := 0; i < len(args); i += 2 {
        key, val := args[i], args[i+1]
        err := tx.Set(key, val)
        if err != nil {
            return errors.Trace(err)
        }
    }
    err = tx.Commit(goctx.Background())
    if err != nil {
        return errors.Trace(err)
    }

    return nil
}

func get(k []byte) (KV, error) {
    tx, err := store.Begin()
    if err != nil {
        return KV{}, errors.Trace(err)
    }
    v, err := tx.Get(k)
    if err != nil {
        return KV{}, errors.Trace(err)
    }
    return KV{K: k, V: v}, nil
}

func dels(keys ...[]byte) error {
    tx, err := store.Begin()
    if err != nil {
        return errors.Trace(err)
    }
    for _, key := range keys {
        err := tx.Delete(key)
        if err != nil {
            return errors.Trace(err)
        }
    }
    err = tx.Commit(goctx.Background())
    if err != nil {
        return errors.Trace(err)
    }
    return nil
}

func scan(keyPrefix []byte, limit int) ([]KV, error) {
    tx, err := store.Begin()
    if err != nil {
        return nil, errors.Trace(err)
    }
    it, err := tx.Iter(kv.Key(keyPrefix), nil)
    if err != nil {
        return nil, errors.Trace(err)
    }
    defer it.Close()
    var ret []KV
    for it.Valid() && limit > 0 {
        ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
        limit--
        it.Next()
    }
    return ret, nil
}

func main() {
    pdAddr := os.Getenv("PD_ADDR")
    if pdAddr != "" {
        os.Args = append(os.Args, "-pd", pdAddr)
    }
    flag.Parse()
    initStore()

    // set
    err := puts([]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2"))
    terror.MustNil(err)

    // get
    kv, err := get([]byte("key1"))
    terror.MustNil(err)
    fmt.Println(kv)

    // scan
    ret, err := scan([]byte("key"), 10)
    for _, kv := range ret {
        fmt.Println(kv)
    }

    // delete
    err = dels([]byte("key1"), []byte("key2"))
    terror.MustNil(err)
}
```

The result is like:

```bash
INFO[0000] [pd] create pd client with endpoints [192.168.199.113:2379]
INFO[0000] [pd] leader switches to: http://192.168.199.113:2379, previous:
INFO[0000] [pd] init cluster id 6563858376412119197
key1 => value1 ([118 97 108 117 101 49])
key1 => value1 ([118 97 108 117 101 49])
key2 => value2 ([118 97 108 117 101 50])
```
