use test::Bencher;
use rand::{self, Rng};

use super::cluster::*;
use super::util::*;

/// Once [issue 18043](https://github.com/rust-lang/rust/issues/18043) is resolved,
/// use this function to generate test datas.
fn generate_random_kvs(n: usize, value_length: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut kvs = Vec::with_capacity(n);
    let mut rng = rand::thread_rng();
    for i in 0..n {
        let k = format!("{:010}", i);
        let mut v = Vec::with_capacity(value_length);
        rng.fill_bytes(&mut v);
        kvs.push((k.into_bytes(), v));
    }
    kvs
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

fn bench_set(b: &mut Bencher, node_count: usize, value_size: usize) {
    let mut cluster = Cluster::new(0, node_count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();
    
    sleep_ms(100);
    
    assert!(cluster.leader_of_region(1).is_some());
    
    // Because we can't get the iteration count from Bencher currently,
    // so we use the max possible iteration round count here.
    // See also [Issue 18043](https://github.com/rust-lang/rust/issues/18043).
    // And https://github.com/rust-lang/rust/blob/master/src/libtest/lib.rs#L1239
    // TODO: use actual iteration count to initialize kvs.
    let mut kvs = generate_random_kvs(1_000_000, value_size);
    let mut iter = kvs.drain(..);
    
    b.iter(|| {
        let (k, v) = iter.next().unwrap();
        cluster.put(&k, &v);
    });
}

fn bench_delete(b: &mut Bencher, node_count: usize, value_size: usize) {
    let mut cluster = Cluster::new(0, node_count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();
    
    sleep_ms(300);
    
}