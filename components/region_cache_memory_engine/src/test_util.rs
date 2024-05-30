// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crossbeam::epoch;
use txn_types::{Key, TimeStamp, Write, WriteType};

use crate::{
    engine::SkiplistHandle,
    keys::{encode_key, InternalBytes, ValueType},
    memory_controller::MemoryController,
    write_batch::RangeCacheWriteBatchEntry,
};

// Put data with write cf and related start cf
pub fn put_data(
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    commit_ts: u64,
    seq_num: u64,
    short_value: bool,
    default_cf: &SkiplistHandle,
    write_cf: &SkiplistHandle,
    mem_controller: Arc<MemoryController>,
) {
    put_data_impl(
        key,
        value,
        start_ts,
        commit_ts,
        seq_num,
        None,
        short_value,
        default_cf,
        write_cf,
        mem_controller,
    )
}

// Put data with write cf and related start cf and overwrite the write cf with
// sequence number `seq_num2` which also points to the prevous default cf
pub fn put_data_with_overwrite(
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    commit_ts: u64,
    seq_num: u64,
    seq_num2: u64,
    short_value: bool,
    default_cf: &SkiplistHandle,
    write_cf: &SkiplistHandle,
    mem_controller: Arc<MemoryController>,
) {
    put_data_impl(
        key,
        value,
        start_ts,
        commit_ts,
        seq_num,
        Some(seq_num2),
        short_value,
        default_cf,
        write_cf,
        mem_controller,
    )
}

fn put_data_impl(
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    commit_ts: u64,
    seq_num: u64,
    overwrite_seq_num: Option<u64>,
    short_value: bool,
    default_cf: &SkiplistHandle,
    write_cf: &SkiplistHandle,
    mem_controller: Arc<MemoryController>,
) {
    let raw_write_k = Key::from_raw(key)
        .append_ts(TimeStamp::new(commit_ts))
        .into_encoded();
    let mut write_k = encode_key(&raw_write_k, seq_num, ValueType::Value);
    write_k.set_memory_controller(mem_controller.clone());
    let write_v = Write::new(
        WriteType::Put,
        TimeStamp::new(start_ts),
        if short_value {
            Some(value.to_vec())
        } else {
            None
        },
    );
    let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
    val.set_memory_controller(mem_controller.clone());
    let guard = &epoch::pin();
    let _ = mem_controller.acquire(RangeCacheWriteBatchEntry::calc_put_entry_size(
        &raw_write_k,
        val.as_bytes(),
    ));
    write_cf.insert(write_k, val, guard);

    if let Some(seq) = overwrite_seq_num {
        let mut write_k = encode_key(&raw_write_k, seq, ValueType::Value);
        write_k.set_memory_controller(mem_controller.clone());
        let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
        val.set_memory_controller(mem_controller.clone());
        write_cf.insert(write_k, val, guard);
    }

    if !short_value {
        let raw_default_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(start_ts))
            .into_encoded();
        let mut default_k = encode_key(&raw_default_k, seq_num + 1, ValueType::Value);
        default_k.set_memory_controller(mem_controller.clone());
        let mut val = InternalBytes::from_vec(value.to_vec());
        val.set_memory_controller(mem_controller.clone());
        let _ = mem_controller.acquire(RangeCacheWriteBatchEntry::calc_put_entry_size(
            &raw_default_k,
            val.as_bytes(),
        ));
        default_cf.insert(default_k, val, guard);
    }
}
