// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use rand::{thread_rng, RngCore};
use test::Bencher;
use tikv_util::keybuilder::KeyBuilder;

#[inline]
fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut rand_str = vec![0; len];
    thread_rng().fill_bytes(&mut rand_str);
    rand_str
}

#[bench]
fn bench_key_builder_data_key(b: &mut Bencher) {
    let k = gen_rand_str(64);
    let ks = k.as_slice();
    b.iter(|| {
        let key = ks.to_vec();
        let _data_key = keys::data_key(key.as_slice());
    })
}

#[bench]
fn bench_key_builder_from_slice(b: &mut Bencher) {
    let k = gen_rand_str(64);
    b.iter(|| {
        let mut builder = KeyBuilder::from_slice(k.as_slice(), 1, 0);
        builder.set_prefix(b"z");
        let _res = builder.build();
    })
}
