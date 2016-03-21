use test::Bencher;
use rand;
use rocksdb::Writable;

use util;
use super::util as cluster_util;
use super::cluster::*;
use super::node::new_node_cluster;
use super::server::new_server_cluster;

/// assume one action takes 2μs, then very iteration will try 1,000,000 / 2,000 = 500 times.
/// see also https://github.com/rust-lang/rust/blob/master/src/libtest/lib.rs#L1239
const DEFAULT_SAMPLE_SIZE: usize = 500;

#[bench]
fn bench_node_set_8_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_node_cluster(0, 5), 8)
}

#[bench]
fn bench_node_set_128_bytes_in_1_node(b: &mut Bencher) {
    bench_set(b, new_node_cluster(0, 1), 128)
}

#[bench]
fn bench_node_set_128_bytes_in_3_node(b: &mut Bencher) {
    bench_set(b, new_node_cluster(0, 3), 128)
}

#[bench]
fn bench_node_set_128_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_node_cluster(0, 5), 128)
}

#[bench]
fn bench_node_set_1024_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_node_cluster(0, 5), 1024)
}

#[bench]
fn bench_node_set_4096_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_node_cluster(0, 5), 4096)
}

#[bench]
fn bench_server_set_8_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_server_cluster(0, 5), 8)
}

#[bench]
fn bench_server_set_128_bytes_in_1_node(b: &mut Bencher) {
    bench_set(b, new_server_cluster(0, 1), 128)
}

#[bench]
fn bench_server_set_128_bytes_in_3_node(b: &mut Bencher) {
    bench_set(b, new_server_cluster(0, 3), 128)
}

#[bench]
fn bench_server_set_128_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_server_cluster(0, 5), 128)
}

#[bench]
fn bench_server_set_1024_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_server_cluster(0, 5), 1024)
}

#[bench]
fn bench_server_set_4096_bytes_in_5_node(b: &mut Bencher) {
    bench_set(b, new_server_cluster(0, 5), 4096)
}

fn bench_set<T: Simulator>(b: &mut Bencher, mut cluster: Cluster<T>, value_size: usize) {
    cluster_util::prepare_cluster(&mut cluster, &[]);

    // Because we can't get the iteration count from Bencher currently,
    // so we use the max possible iteration round count here.
    // See also [Issue 18043](https://github.com/rust-lang/rust/issues/18043).
    // TODO: use actual iteration count to initialize kvs.
    let kvs = util::generate_random_kvs(DEFAULT_SAMPLE_SIZE, 100, value_size);
    let mut iter = kvs.iter();

    b.iter(|| {
        let &(ref k, ref v) = iter.next().unwrap();
        cluster.put(k, v);
    });
}

#[bench]
fn bench_node_delete_in_1_node(b: &mut Bencher) {
    bench_delete(b, new_node_cluster(0, 1));
}

#[bench]
fn bench_node_delete_in_3_node(b: &mut Bencher) {
    bench_delete(b, new_node_cluster(0, 3));
}

#[bench]
fn bench_node_delete_in_5_node(b: &mut Bencher) {
    bench_delete(b, new_node_cluster(0, 5));
}

#[bench]
fn bench_server_delete_in_1_node(b: &mut Bencher) {
    bench_delete(b, new_server_cluster(0, 1));
}

#[bench]
fn bench_server_delete_in_3_node(b: &mut Bencher) {
    bench_delete(b, new_server_cluster(0, 3));
}

#[bench]
fn bench_server_delete_in_5_node(b: &mut Bencher) {
    bench_delete(b, new_server_cluster(0, 5));
}

fn bench_delete<T: Simulator>(b: &mut Bencher, mut cluster: Cluster<T>) {
    // TODO: use actual iteration count to initialize kvs.
    let kvs = util::generate_random_kvs(DEFAULT_SAMPLE_SIZE, 100, 128);

    cluster_util::prepare_cluster(&mut cluster, &kvs);

    let mut iter = kvs.iter();
    b.iter(|| {
        let &(ref k, _) = iter.next().unwrap();
        cluster.delete(&k);
    });
}

#[bench]
fn bench_node_seek(b: &mut Bencher) {
    bench_seek(b, new_node_cluster(0, 5));
}

#[bench]
fn bench_server_seek(b: &mut Bencher) {
    bench_seek(b, new_server_cluster(0, 5));
}

fn bench_seek<T: Simulator>(b: &mut Bencher, mut cluster: Cluster<T>) {
    let kvs = util::generate_random_kvs(100_000, 100, 128);
    cluster_util::prepare_cluster(&mut cluster, &kvs);

    let mut rng = rand::thread_rng();
    let keys = rand::sample(&mut rng,
                            (1..200_000).map(|i| i.to_string().into_bytes()),
                            DEFAULT_SAMPLE_SIZE);
    let mut iter = keys.iter();

    b.iter(|| {
        let k = iter.next().unwrap();
        cluster.seek(&k);
    });
}

#[bench]
fn bench_node_get(b: &mut Bencher) {
    bench_get(b, new_node_cluster(0, 5));
}

#[bench]
fn bench_server_get(b: &mut Bencher) {
    bench_get(b, new_server_cluster(0, 5));
}

fn bench_get<T: Simulator>(b: &mut Bencher, mut cluster: Cluster<T>) {
    let kvs = util::generate_random_kvs(100_000, 100, 128);
    cluster_util::prepare_cluster(&mut cluster, &kvs);

    let mut rng = rand::thread_rng();
    let keys = rand::sample(&mut rng,
                            (1..200_000).map(|i| i.to_string().into_bytes()),
                            DEFAULT_SAMPLE_SIZE);
    let mut iter = keys.iter();

    b.iter(|| {
        let k = iter.next().unwrap();
        cluster.get(&k);
    });
}
