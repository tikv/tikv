// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use rand::{self, Rng, RngCore};
use tidb_query_datatype::{
    codec::{datum, table, Datum},
    expr::EvalContext,
};
use txn_types::Key;

#[inline]
fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut rand_str = vec![0; len];
    rand::thread_rng().fill_bytes(&mut rand_str);
    rand_str
}

#[bench]
fn bench_row_key_gen_hash(b: &mut test::Bencher) {
    let id: i64 = rand::thread_rng().gen();
    let row_key = Key::from_raw(&table::encode_row_key(id, id));
    b.iter(|| {
        test::black_box(row_key.gen_hash());
    });
}

#[bench]
fn bench_index_key_gen_hash(b: &mut test::Bencher) {
    let id: i64 = rand::thread_rng().gen();
    let encoded_index_val = datum::encode_key(
        &mut EvalContext::default(),
        &[Datum::Bytes(gen_rand_str(64))],
    )
    .unwrap();
    let index_key = Key::from_raw(&table::encode_index_seek_key(id, id, &encoded_index_val));
    b.iter(|| {
        test::black_box(index_key.gen_hash());
    });
}
