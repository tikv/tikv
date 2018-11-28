// Copyright 2018 PingCAP, Inc.
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

/// This suite contains benchmarks for several different configurations generated
/// dynamically. Thus it has `harness = false`.
extern crate criterion;

extern crate rocksdb;
extern crate test_raftstore;
extern crate test_util;
extern crate tikv;

use std::fmt;

use criterion::{Bencher, Criterion};
use rocksdb::{Writable, WriteBatch, DB};

use test_raftstore::*;
use test_util::*;
use tikv::raftstore::store::keys;

const DEFAULT_DATA_SIZE: usize = 100_000;

fn enc_write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(&keys::data_key(k), v).unwrap();
    }
    db.write(wb).unwrap();
}

fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>, initial_kvs: &[(Vec<u8>, Vec<u8>)]) {
    cluster.run();
    for engines in cluster.engines.values() {
        enc_write_kvs(&engines.kv, initial_kvs);
    }
    cluster.leader_of_region(1).unwrap();
}

#[derive(Debug)]
struct SetConfig<F> {
    factory: F,
    nodes: usize,
    value_size: usize,
}

fn bench_set<T, F>(b: &mut Bencher, input: &SetConfig<F>)
where
    T: Simulator,
    F: ClusterFactory<T>,
{
    let mut cluster = input.factory.build(input.nodes);
    prepare_cluster(&mut cluster, &[]);

    let mut kvs = KvGenerator::new(100, input.value_size);

    b.iter(|| {
        let (k, v) = kvs.next().unwrap();
        cluster.must_put(&k, &v)
    });
}

#[derive(Debug)]
struct GetConfig<F> {
    factory: F,
    nodes: usize,
}

fn bench_get<T, F>(b: &mut Bencher, input: &GetConfig<F>)
where
    T: Simulator,
    F: ClusterFactory<T>,
{
    let mut cluster = input.factory.build(input.nodes);
    let mut kvs = KvGenerator::new(100, 128).generate(DEFAULT_DATA_SIZE);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs
        .drain(..)
        .take(DEFAULT_DATA_SIZE / 10)
        .map(|i| i.0)
        .chain(KvGenerator::new(100, 0).map(|i| i.0));

    b.iter(|| {
        let k = keys.next().unwrap();
        cluster.get(&k)
    });
}

#[derive(Debug)]
struct DeleteConfig<F> {
    factory: F,
    nodes: usize,
}

fn bench_delete<T, F>(b: &mut Bencher, input: &DeleteConfig<F>)
where
    T: Simulator,
    F: ClusterFactory<T>,
{
    let mut cluster = input.factory.build(input.nodes);
    let mut kvs = KvGenerator::new(100, 128).generate(DEFAULT_DATA_SIZE);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs
        .drain(..)
        .take(DEFAULT_DATA_SIZE / 10)
        .map(|i| i.0)
        .chain(KvGenerator::new(100, 0).map(|i| i.0));

    b.iter(|| {
        let k = keys.next().unwrap();
        cluster.must_delete(&k)
    });
}

fn bench_raft_cluster<T, F>(c: &mut Criterion, factory: F)
where
    T: Simulator + 'static,
    F: ClusterFactory<T>,
{
    let nodes_coll = vec![1, 3, 5];
    let value_size_coll = vec![8, 128, 1024, 4096];

    let mut set_inputs = vec![];
    let mut get_inputs = vec![];
    let mut delete_inputs = vec![];
    for nodes in nodes_coll {
        for &value_size in &value_size_coll {
            set_inputs.push(SetConfig {
                factory: factory.clone(),
                nodes,
                value_size,
            });
        }
        get_inputs.push(GetConfig {
            factory: factory.clone(),
            nodes,
        });
        delete_inputs.push(DeleteConfig {
            factory: factory.clone(),
            nodes,
        });
    }
    c.bench_function_over_inputs("bench_set", bench_set, set_inputs);
    c.bench_function_over_inputs("bench_get", bench_get, get_inputs);
    c.bench_function_over_inputs("bench_delete", bench_delete, delete_inputs);
}

trait ClusterFactory<T: Simulator>: Clone + fmt::Debug + 'static {
    fn build(&self, nodes: usize) -> Cluster<T>;
}

#[derive(Clone)]
struct NodeClusterFactory;

impl ClusterFactory<NodeCluster> for NodeClusterFactory {
    fn build(&self, nodes: usize) -> Cluster<NodeCluster> {
        new_node_cluster(1, nodes)
    }
}

impl fmt::Debug for NodeClusterFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Node")
    }
}

#[derive(Clone)]
struct ServerClusterFactory;

impl ClusterFactory<ServerCluster> for ServerClusterFactory {
    fn build(&self, nodes: usize) -> Cluster<ServerCluster> {
        new_server_cluster(1, nodes)
    }
}

impl fmt::Debug for ServerClusterFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Server")
    }
}

fn main() {
    tikv::util::config::check_max_open_fds(4096).unwrap();

    let mut criterion = Criterion::default().sample_size(10);
    bench_raft_cluster(&mut criterion, NodeClusterFactory {});
    bench_raft_cluster(&mut criterion, ServerClusterFactory {});

    criterion.final_summary();
}
