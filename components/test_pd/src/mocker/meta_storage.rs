// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use super::etcd::Etcd;

struct MetaStorage {
    store: Etcd,
}
