extern crate rand;

use std::sync::atomic::{ATOMIC_U64_INIT, AtomicU64, Ordering};
use tikv::coprocessor::codec::table::encode_row_key;
use tikv::util::codec::number::NumberEncoder;

use rand::Rng;

#[inline]
pub fn next_ts() -> u64 {
    static CURRENT: AtomicU64 = ATOMIC_U64_INIT;
    CURRENT.fetch_add(1, Ordering::SeqCst)
}

pub fn generate_row_keys(table_id: i64, start_id: i64, count: usize) -> Vec<Vec<u8>> {
    let mut result = Vec::with_capacity(count);
    for i in (start_id)..(start_id + count as i64) {
        let mut handle = Vec::with_capacity(8);
        handle.encode_i64(i as i64).unwrap();
        let key = encode_row_key(table_id, &handle);
        result.push(key);
    }
    result
}


pub fn generate_unique_index_keys(table_id: i64, value_len: usize, count: usize) -> Vec<Vec<u8>> {
    panic!("Not implemented");
}

pub fn shuffle<T>(data: &mut [T]) {
    let mut rng = rand::thread_rng();
    for i in 0..(data.len()) {
        let j = rng.gen_range(i, data.len());
        data.swap(i, j);
    }
}

//#[inline]
//pub fn next_ts() -> u64 {
//    static CURRENT: AtomicU64 = ATOMIC_U64_INIT;
//    CURRENT.fetch_add(1, Ordering::SeqCst)
//}
//
//#[inline]
//fn create_row_key(tableid: i64, rowid: i64) -> Vec<u8> {
//    let mut handle = Vec::with_capacity(8);
//    handle.encode_i64(rowid).unwrap();
//    encode_row_key(tableid, &handle)
//}
//
////#[inline]
////fn create_unique_index_key(tableid: i64, rowid: i64) -> Vec<u8> {
////
////}
//
//pub fn generate_row_keys(count: usize) -> Vec<Vec<u8>> {
//    let mut result = Vec::with_capacity(count);
//    for i in 0..count {
//        result.push(create_row_key(1, i as i64));
//    }
//    result
//}
