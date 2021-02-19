// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;

use criterion::{Bencher, Criterion};
use engine_rocks::raw::DB;
use engine_rocks::Compat;
use engine_traits::{Mutable, WriteBatch, WriteBatchExt};
use test_raftstore::*;
use test_util::*;

const DEFAULT_DATA_SIZE: usize = 100_000;

fn enc_write_kvs(db: &Arc<DB>, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let mut wb = db.c().write_batch();
    for &(ref k, ref v) in kvs {
        wb.put(&keys::data_key(k), v).unwrap();
    }
    wb.write().unwrap();
}

fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>, initial_kvs: &[(Vec<u8>, Vec<u8>)]) {
    cluster.run();
    for engines in cluster.engines.values() {
        enc_write_kvs(engines.kv.as_inner(), initial_kvs);
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

fn bench_raft_cluster<T, F>(c: &mut Criterion, factory: F, label: &str)
where
    T: Simulator + 'static,
    F: ClusterFactory<T>,
{
    let nodes_coll = vec![1, 3, 5];
    let value_size_coll = vec![8, 128, 1024, 4096];

    let mut group = c.benchmark_group(label);

    for nodes in nodes_coll {
        for &value_size in &value_size_coll {
            let config = SetConfig {
                factory: factory.clone(),
                nodes,
                value_size,
            };
            group.bench_with_input(format!("bench_set/{:?}", &config), &config, bench_set);
        }
        let config = GetConfig {
            factory: factory.clone(),
            nodes,
        };
        group.bench_with_input(format!("bench_get/{:?}", &config), &config, bench_get);
        let config = DeleteConfig {
            factory: factory.clone(),
            nodes,
        };
        group.bench_with_input(format!("bench_delete/{:?}", &config), &config, bench_delete);
    }
    group.finish();
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server")
    }
}

fn main() {
    tikv_util::config::check_max_open_fds(4096).unwrap();

    let mut criterion = Criterion::default().configure_from_args().sample_size(10);
    bench_raft_cluster(&mut criterion, NodeClusterFactory {}, "raftstore::node");
    bench_raft_cluster(&mut criterion, ServerClusterFactory {}, "raftstore::server");

    criterion.final_summary();
}
