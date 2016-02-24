use test::Bencher;
use rand::{self, Rng};

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
fn bench_get(b: &mut Bencher) {
    b.iter(|| {
       1 + 1; 
    });
}