// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use cluster::*;
use node::new_node_cluster;
use server::new_server_cluster;
use test::BenchSamples;
use test_util::*;

use rocksdb::{Writable, WriteBatch, DB};
use tikv::raftstore::store::*;

use super::print_result;

const DEFAULT_DATA_SIZE: usize = 100_000;

fn enc_write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(&keys::data_key(k), v).expect("");
    }
    db.write(wb).expect("");
}

fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>, initial_kvs: &[(Vec<u8>, Vec<u8>)]) {
    cluster.run();
    for engines in cluster.engines.values() {
        enc_write_kvs(&engines.kv_engine, initial_kvs);
    }
    cluster.leader_of_region(1).unwrap();
}

fn print_set_progress(tag: &str, ncnt: usize, vlen: usize) {
    printf!(
        "benching Set on {},\tnodes: {}, value len: {:4}\t...",
        tag,
        ncnt,
        vlen
    );
}

fn print_other_progress(tag: &str, action: &str, ncnt: usize) {
    printf!("benching {} on {},\tnodes: {}\t...", action, tag, ncnt);
}

fn bench_set<T: Simulator>(mut cluster: Cluster<T>, value_size: usize) -> BenchSamples {
    prepare_cluster(&mut cluster, &[]);

    let mut kvs = KvGenerator::new(100, value_size);

    bench!("bench_set", || {
        let (k, v) = kvs.next().unwrap();
        cluster.must_put(&k, &v)
    })
}

fn bench_get<T: Simulator>(mut cluster: Cluster<T>) -> BenchSamples {
    let mut kvs = generate_random_kvs(DEFAULT_DATA_SIZE, 100, 128);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs
        .drain(..)
        .take(DEFAULT_DATA_SIZE / 10)
        .map(|i| i.0)
        .chain(KvGenerator::new(100, 0).map(|i| i.0));

    bench!("bench_get", || {
        let k = keys.next().unwrap();
        cluster.get(&k)
    })
}

fn bench_delete<T: Simulator>(mut cluster: Cluster<T>) -> BenchSamples {
    let mut kvs = generate_random_kvs(DEFAULT_DATA_SIZE, 100, 128);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs
        .drain(..)
        .take(DEFAULT_DATA_SIZE / 10)
        .map(|i| i.0)
        .chain(KvGenerator::new(100, 0).map(|i| i.0));

    bench!("bench_delete", || {
        let k = keys.next().unwrap();
        cluster.must_delete(&k)
    })
}

fn bench_raft_cluster<T, F>(factory: F, tag: &'static str)
where
    T: Simulator,
    F: Fn(u64, usize) -> Cluster<T>,
{
    let node_cnts = vec![1, 3, 5];
    let bytes_lens = vec![8, 128, 1024, 4096];

    for ncnt in node_cnts {
        for &vlen in &bytes_lens {
            print_set_progress(tag, ncnt, vlen);
            print_result(bench_set(factory(1, ncnt), vlen));
        }
        print_other_progress(tag, "Get", ncnt);
        print_result(bench_get(factory(1, ncnt)));
        print_other_progress(tag, "Delete", ncnt);
        print_result(bench_delete(factory(1, ncnt)));
    }
}

pub fn bench_raftstore() {
    bench_raft_cluster(new_node_cluster, "raft cluster (channel)");
    bench_raft_cluster(new_server_cluster, "raft cluster (tcp)");
}
