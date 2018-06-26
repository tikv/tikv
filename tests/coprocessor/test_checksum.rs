// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::u64;

use kvproto::coprocessor::{KeyRange, Request};
use kvproto::kvrpcpb::{Context, IsolationLevel};
use protobuf::Message;
use tipb::checksum::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse, ChecksumScanOn};

use tikv::coprocessor::*;
use tikv::storage::SnapshotStore;

use super::test_select::*;

fn new_checksum_request(range: KeyRange, scan_on: ChecksumScanOn) -> Request {
    let mut ctx = Context::new();
    ctx.set_isolation_level(IsolationLevel::SI);

    let mut checksum = ChecksumRequest::new();
    checksum.set_start_ts(u64::MAX);
    checksum.set_scan_on(scan_on);
    checksum.set_algorithm(ChecksumAlgorithm::Crc64_Xor);

    let mut req = Request::new();
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
    let (store, end_point) = init_data_with_commit(&product, &data, true);

    for column in &[product.id, product.name, product.count] {
        assert!(column.index >= 0);
        let (range, scan_on) = if column.index == 0 {
            let range = product.table.get_select_range();
            (range, ChecksumScanOn::Table)
        } else {
            let range = product.table.get_index_range(column.index);
            (range, ChecksumScanOn::Index)
        };
        let request = new_checksum_request(range.clone(), scan_on);
        let expected = reversed_checksum_crc64_xor(&store, range, scan_on);

        let response = handle_request(&end_point, request);
        let mut resp = ChecksumResponse::new();
        resp.merge_from_bytes(response.get_data()).unwrap();
        assert_eq!(resp.get_checksum(), expected);
        assert_eq!(resp.get_total_kvs(), data.len() as u64);
    }
}

fn reversed_checksum_crc64_xor(store: &Store, range: KeyRange, scan_on: ChecksumScanOn) -> u64 {
    use crc::crc64::{self, Digest, Hasher64};

    let ctx = Context::new();
    let snap = SnapshotStore::new(
        store.get_engine().snapshot(&ctx).unwrap(),
        u64::MAX,
        IsolationLevel::SI,
        true,
    );
    let scan_on = match scan_on {
        ChecksumScanOn::Table => ScanOn::Table,
        ChecksumScanOn::Index => ScanOn::Index,
    };
    let mut scanner = Scanner::new(
        &snap, scan_on, true, // Scan in reversed order.
        false, range,
    ).unwrap();

    let mut checksum = 0;
    while let Some((k, v)) = scanner.next_row().unwrap() {
        let mut digest = Digest::new(crc64::ECMA);
        digest.write(&k);
        digest.write(&v);
        checksum ^= digest.sum64();
    }
    checksum
}
