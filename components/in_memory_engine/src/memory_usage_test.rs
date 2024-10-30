// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

/// These tests are heavy so we shouldn't run them daily.
/// Run them with the following command (recommending release mode) and see the
/// printed stats:
///
/// ```text
/// RUST_TEST_THREADS=1
/// cargo test --package in_memory_engine --lib --release \
///   -- memory_usage_test::test_memory_usage --nocapture --ignored
/// ```
use std::fmt::Write as _;

use engine_traits::CF_WRITE;
use hex::FromHex;
use tikv_util::config::ReadableSize;
use txn_types::{Key, Write, WriteType};

use crate::prop_test::new_skiplist_engine_for_test;

struct Case {
    name: &'static str,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    logical_size_pre_key: u64,
    logical_size_in_total: u64,
}

#[test]
#[ignore]
fn test_memory_usage_two_fields_secondary_index_two_fields_clustered_index() {
    // Say there is a secondary index composed of 2 fields, a 5-bytes string,
    // and an 8-bytes integer, and it maps to a clustered index with two
    // 8-byes integers.
    //
    // ```text
    // A raw key looks like:
    //  t{table_id}_i{index_id}\001{string}\003{integer}
    // For example:
    //  t\200\000\000\000\000\000\000l_i\200\000\000\000\000\000\000\017\001Error\000\000\000\374\003\200\000\000\000\000\000\000\005\003\200\017\220\331\313 \210h\003\200\000\0001G\221\245\331
    //
    // An encoded key looks like:
    //  BytesEncoder::encode_bytes(raw_key)
    // For example:
    //  t\200\000\000\000\000\000\000\377l_i\200\000\000\000\000\377\000\000\017\001Erro\377r\000\000\000\374\003\200\000\377\000\000\000\000\000\005\003\200\377\017\220\331\313 \210h\003\377\200\000\0001G\221\245\331\377\000\000\000\000\000\000\000\000\367
    //
    // A data key in the write CF is prefixed with 'z' and suffix a 8 bytes integer.
    //  z{encoded_key}{timestamp}
    // For example:
    //  zt\200\000\000\000\000\000\000\377l_i\200\000\000\000\000\377\000\000\017\001Erro\377r\000\000\000\374\003\200\000\377\000\000\000\000\000\005\003\200\377\017\220\331\313 \210h\003\377\200\000\0001G\221\245\331\377\000\000\000\000\000\000\000\000\367\371\306\021\275x\177\377\377
    // ```
    //
    // An encoded key (without timestamp) in hex format.
    let key = Vec::from_hex(
        "7480000000000000FF6C5F698000000000FF00000F014572726FFF72000000FC038000\
        FF0000000000050380FF0F90D9CB20886803FF800000314791A5D9FF00000000000000\
        00F7",
    )
    .unwrap();
    // For a secondary index record, it doesn't have a value.
    let value = None;

    // Let's define the logical size of a secondary index record is 29 bytes
    // which is the sum of the size of the two fields and the clustered index.
    let logical_size_pre_key = 5 + 8 + 8 + 8;
    let logical_size_in_total = ReadableSize::mb(200).0;

    let case = Case {
        name: "two_fields_secondary_index_two_fields_clustered_index",
        key,
        value,
        logical_size_pre_key,
        logical_size_in_total,
    };
    evaluate_memory_usage(case)
}

#[test]
#[ignore]
fn test_memory_usage_two_fields_unique_index_two_fields_clustered_index() {
    // Say there is a unique index composed of 2 fields, a 5-bytes string,
    // and an 8-bytes integer, and it maps to a clustered index with two
    // 8-byes integers.
    //
    // ```text
    // A raw key looks like:
    //  t{table_id}_i{index_id}\001{string}\003{integer}
    // For example:
    //  t\200\000\000\000\000\000\000l_i\200\000\000\000\000\000\000\017\001Error\000\000\000\374\003\200\000\000\000\000\000\000\005
    //
    // An encoded key looks like:
    //  BytesEncoder::encode_bytes(raw_key)
    // For example:
    //  t\200\000\000\000\000\000\000\377l_i\200\000\000\000\000\377\000\000\017\001Erro\377r\000\000\000\374\003\200\000\377\000\000\000\000\000\005\000\000\375
    //
    // A data key in the write CF is prefixed with 'z' and suffix a 8 bytes integer.
    //  z{encoded_key}{timestamp}
    // For example:
    //  zt\200\000\000\000\000\000\000\377l_i\200\000\000\000\000\377\000\000\017\001Erro\377r\000\000\000\374\003\200\000\377\000\000\000\000\000\005\000\000\375\371\306\021\275x\177\377\377
    // ```
    //
    // An encoded key (without timestamp) in hex format.
    let key = Vec::from_hex(
        "7A7480000000000000FF6C5F698000000000FF00000F014572726FFF72000000FC0380\
        00FF0000000000050000FDF9C611BD787FFFFF",
    )
    .unwrap();
    // For a unique index record, it has a value in the format:
    //
    // ```text
    // Layout: TailLen | VersionFlag | Version | Options      | [UntouchedFlag]
    // Length:   1     |      1      |    1    | len(options) |   1
    //
    // Where Options for common handle (aka clustered index) is:
    //
    // Layout: CHandle flag | CHandle Len | CHandle      |
    // Length:      1       |      2      | len(CHandle) |
    //
    // For example:
    //  007D017F001203800162F749A43FAE038000000727DB8931 can be interpreted as:
    //
    //  TailLen | VersionFlag | Version |                            Options                                | [UntouchedFlag]
    //                                  | CHandle flag | CHandle Len |                CHandle               |
    //     00   |     7D      |   01    |      7F      |    00 12    | 03800162F749A43FAE 038000000727DB8931
    // ```
    // See: https://github.com/pingcap/tidb/blob/c201eb7335/table/tables/index.go#L134
    let value = Some(Vec::from_hex("007D017F001203800162F749A43FAE038000000727DB8931").unwrap());

    // Let's define the logical size of a unique index record is 29 bytes
    // which is the sum of the size of the two fields and the clustered index.
    let logical_size_pre_key = 5 + 8 + 8 + 8;
    let logical_size_in_total = ReadableSize::mb(200).0;

    let case = Case {
        name: "two_fields_unique_index_two_fields_clustered_index",
        key,
        value,
        logical_size_pre_key,
        logical_size_in_total,
    };
    evaluate_memory_usage(case)
}

fn evaluate_memory_usage(case: Case) {
    let Case {
        name,
        key,
        value,
        logical_size_pre_key,
        logical_size_in_total,
    } = case;
    // 2024-03-27 00:21:00.348 +0800 CST
    let commit_ts = 448651607500000000u64;

    // Preallocate 8KB buffer to avoid reallocation.
    let mut log_buf = String::from_utf8(vec![b'0'; ReadableSize::kb(8).0 as _]).unwrap();
    log_buf.clear();
    writeln!(log_buf, "\nCase: {}", name).unwrap();

    let (skiplist, skiplist_args) = new_skiplist_engine_for_test();
    let start = tikv_alloc::fetch_stats().unwrap().unwrap();
    for i in 0..=logical_size_in_total / logical_size_pre_key {
        // Append a timestamp to the key to make it unique.
        let key = Key::from_encoded(key.clone()).append_ts(i.into());
        let value = Write::new(WriteType::Put, commit_ts.into(), value.clone())
            .as_ref()
            .to_bytes();
        if i == 0 {
            writeln!(
                log_buf,
                "  Pre key amplification: {:.2}",
                key.len() as f64 / logical_size_pre_key as f64,
            )
            .unwrap();
            writeln!(
                log_buf,
                "  Pre key value amplification: {:.2}",
                (key.len() + value.len()) as f64 / logical_size_pre_key as f64,
            )
            .unwrap();
        }

        let handle = skiplist.cf_handle(CF_WRITE);
        let (key, value, guard) = skiplist_args(key.into_encoded(), Some(value));
        handle.insert(key, value.unwrap(), &guard)
    }
    let end = tikv_alloc::fetch_stats().unwrap().unwrap();

    let resident_start = start.iter().find(|(k, _)| *k == "resident").unwrap();
    let resident_end = end.iter().find(|(k, _)| *k == "resident").unwrap();
    writeln!(
        log_buf,
        "  SkiplistEngine amplification: {:.2}",
        (resident_end.1 - resident_start.1) as f64 / logical_size_in_total as f64,
    )
    .unwrap();
    for (i, (k, v)) in end.into_iter().enumerate() {
        if k == "resident" {}
        writeln!(log_buf, "    {}: {}", k, v.saturating_sub(start[i].1)).unwrap();
    }
    println!("{}", log_buf);
    drop(skiplist);
}
