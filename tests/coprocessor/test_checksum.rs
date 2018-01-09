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

use protobuf::Message;
use kvproto::coprocessor::{KeyRange, Request};
use kvproto::kvrpcpb::{Context, IsolationLevel};
use tipb::checksum::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse, ChecksumScanOn};

use tikv::coprocessor::*;

use super::test_select::*;

fn new_checksum_request(scan_on: ChecksumScanOn, range: KeyRange) -> Request {
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

    // Pre-calculated checksums.
    let expected = vec![
        16755715561722909511,
        8265834051855199110,
        8265834051855199110,
    ];

    let product = ProductTable::new();
    let (_, end_point) = init_data_with_commit(&product, &data, true);

    let columns = &[product.id, product.name, product.count];
    for (column, checksum) in columns.iter().zip(expected) {
        assert!(column.index >= 0);
        let request = if column.index == 0 {
            let range = product.table.get_select_range();
            new_checksum_request(ChecksumScanOn::Table, range)
        } else {
            let range = product.table.get_index_range(column.index);
            new_checksum_request(ChecksumScanOn::Index, range)
        };

        let response = handle_request(&end_point, request);
        let mut resp = ChecksumResponse::new();
        resp.merge_from_bytes(response.get_data()).unwrap();
        assert_eq!(resp.get_checksum(), checksum);
    }
}
