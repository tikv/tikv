// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::u64;

use kvproto::coprocessor::{KeyRange, Request};
use kvproto::kvrpcpb::{Context, IsolationLevel};
use protobuf::Message;
use tipb::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse, ChecksumScanOn};

use keys::TimeStamp;
use test_coprocessor::*;
use tidb_query::storage::scanner::{RangesScanner, RangesScannerOptions};
use tidb_query::storage::Range;
use tikv::coprocessor::dag::TiKVStorage;
use tikv::coprocessor::*;
use tikv::storage::{Engine, SnapshotStore};

fn new_checksum_request(range: KeyRange, scan_on: ChecksumScanOn) -> Request {
    let mut ctx = Context::default();
    ctx.set_isolation_level(IsolationLevel::Si);

    let mut checksum = ChecksumRequest::default();
    checksum.set_scan_on(scan_on);
    checksum.set_algorithm(ChecksumAlgorithm::Crc64Xor);

    let mut req = Request::default();
    req.set_start_ts(u64::MAX);
    req.set_context(ctx);
    req.set_tp(REQ_TYPE_CHECKSUM);
    req.set_data(checksum.write_to_bytes().unwrap());
    req.mut_ranges().push(range);
    req
}

#[test]
fn test_checksum() {
    let data = vec![
        (1, Some("name:1"), 1),
        (2, Some("name:2"), 2),
        (3, Some("name:3"), 3),
        (4, Some("name:4"), 4),
    ];

    let product = ProductTable::new();
    let (store, endpoint) = init_data_with_commit(&product, &data, true);

    for column in &[&product["id"], &product["name"], &product["count"]] {
        assert!(column.index >= 0);
        let (range, scan_on) = if column.index == 0 {
            let range = product.get_record_range_all();
            (range, ChecksumScanOn::Table)
        } else {
            let range = product.get_index_range_all(column.index);
            (range, ChecksumScanOn::Index)
        };
        let request = new_checksum_request(range.clone(), scan_on);
        let expected = reversed_checksum_crc64_xor(&store, range);

        let response = handle_request(&endpoint, request);
        let mut resp = ChecksumResponse::default();
        resp.merge_from_bytes(response.get_data()).unwrap();
        assert_eq!(resp.get_checksum(), expected);
        assert_eq!(resp.get_total_kvs(), data.len() as u64);
    }
}

fn reversed_checksum_crc64_xor<E: Engine>(store: &Store<E>, range: KeyRange) -> u64 {
    let ctx = Context::default();
    let store = SnapshotStore::new(
        store.get_engine().snapshot(&ctx).unwrap(),
        TimeStamp::max(),
        IsolationLevel::Si,
        true,
        Default::default(),
    );
    let mut scanner = RangesScanner::new(RangesScannerOptions {
        storage: TiKVStorage::from(store),
        ranges: vec![Range::from_pb_range(range, false)],
        scan_backward_in_range: true,
        is_key_only: false,
        is_scanned_range_aware: false,
    });

    let mut checksum = 0;
    let digest = crc64fast::Digest::new();
    while let Some((k, v)) = scanner.next().unwrap() {
        let mut digest = digest.clone();
        digest.write(&k);
        digest.write(&v);
        checksum ^= digest.sum64();
    }
    checksum
}
