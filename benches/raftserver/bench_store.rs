use test::Bencher;
use rand::{self, Rng};

use super::cluster::*;
use super::util::*;
use super::node::{NodeCluster, new_node_cluster};

use tikv::raftserver::store::*;

/// assume one action takes 2μs, then very iteration will try 1,000,000 / 2,000 = 500 times.
/// see also https://github.com/rust-lang/rust/blob/master/src/libtest/lib.rs#L1239
const DEFAULT_SAMPLE_SIZE: usize = 500;

fn generate_random_kvs(n: usize, value_length: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut kvs = Vec::with_capacity(n);
    let mut rng = rand::thread_rng();
    for i in 0..n {
        let k = i.to_string();
        let mut v = Vec::with_capacity(value_length);
        rng.fill_bytes(&mut v);
        kvs.push((k.into_bytes(), v));
    }
    kvs
}

#[bench]
fn bench_store_set_8_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, 5, 8)
}

#[bench]
fn bench_store_set_128_bytes_in_1_node(b: &mut Bencher) {
    bench_set(b, 1, 128)
}

#[bench]
fn bench_store_set_128_bytes_in_3_node(b: &mut Bencher) {
    bench_set(b, 3, 128)
}

#[bench]
fn bench_store_set_128_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, 5, 128)
}

#[bench]
fn bench_store_set_1024_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, 5, 1024)
}

#[bench]
fn bench_store_set_4096_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, 5, 4096)
}

fn prepare_cluster(node_count: usize) -> Cluster<NodeCluster> {
    let mut cluster = new_node_cluster(0, node_count);
    cluster.bootstrap_region().expect("");
    cluster.run_all_nodes();
    sleep_ms(400);
    assert!(cluster.leader_of_region(1).is_some());
    cluster
}

fn bench_set(b: &mut Bencher, node_count: usize, value_size: usize) {
    let mut cluster = prepare_cluster(node_count);

    // Because we can't get the iteration count from Bencher currently,
    // so we use the max possible iteration round count here.
    // See also [Issue 18043](https://github.com/rust-lang/rust/issues/18043).
    // TODO: use actual iteration count to initialize kvs.
    let mut kvs = generate_random_kvs(DEFAULT_SAMPLE_SIZE, value_size);
    let mut iter = kvs.drain(..);

    b.iter(|| {
        let (k, v) = iter.next().unwrap();
        cluster.put(&k, &v);
    });
}

#[bench]
fn bench_store_delete_in_1_node(b: &mut Bencher) {
    bench_delete(b, 1);
}

#[bench]
fn bench_store_delete_in_3_node(b: &mut Bencher) {
    bench_delete(b, 3);
}

#[bench]
fn bench_store_delete_in_5_node(b: &mut Bencher) {
    bench_delete(b, 5);
}

fn bench_delete(b: &mut Bencher, node_count: usize) {
    let mut cluster = prepare_cluster(node_count);

    // TODO: use actual iteration count to initialize kvs.
    let mut kvs = generate_random_kvs(DEFAULT_SAMPLE_SIZE, 128);
    for engine in cluster.engines.values() {
        write_kvs(engine, &kvs);
    }

    // make sure write_kvs actually work.
    let kv = cluster.get(b"1");
    assert!(kv.is_some());

    let mut iter = kvs.drain(..);
    b.iter(|| {
        let (k, _) = iter.next().unwrap();
        cluster.delete(&k);
    });
}

#[bench]
fn bench_seek(b: &mut Bencher) {
    let mut cluster = prepare_cluster(5);
    let kvs = generate_random_kvs(100_000, 128);
    for engine in cluster.engines.values() {
        write_kvs(engine, &kvs);
    }

    // make sure write_kvs actually work.
    let kv = cluster.seek(b"9999");
    assert!(kv.is_some());
    assert_eq!(kv.unwrap().0, b"z9999");

    let mut rng = rand::thread_rng();
    let mut keys = rand::sample(&mut rng,
                                (1..200_000).map(|i| i.to_string().into_bytes()),
                                DEFAULT_SAMPLE_SIZE);
    let mut iter = keys.drain(..);

    b.iter(|| {
        let k = iter.next().unwrap();
        cluster.seek(&k);
    });
}

#[bench]
fn bench_get(b: &mut Bencher) {
    let mut cluster = prepare_cluster(5);

    let kvs = generate_random_kvs(100_000, 128);
    for engine in cluster.engines.values() {
        write_kvs(engine, &kvs);
    }

    // make sure write_kvs actually work.
    let kv = cluster.get(b"9999");
    assert!(kv.is_some());

    let mut rng = rand::thread_rng();
    let mut keys = rand::sample(&mut rng,
                                (1..200_000).map(|i| i.to_string().into_bytes()),
                                DEFAULT_SAMPLE_SIZE);
    let mut iter = keys.drain(..);

    b.iter(|| {
        let k = iter.next().unwrap();
        cluster.get(&k);
    });
}
