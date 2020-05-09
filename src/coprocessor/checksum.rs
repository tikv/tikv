// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query_common::storage::scanner::{RangesScanner, RangesScannerOptions};
use tidb_query_common::storage::Range;
use tipb::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse};

use crate::coprocessor::dag::TiKVStorage;
use crate::coprocessor::*;
use crate::storage::{Snapshot, SnapshotStore, Statistics};

// `ChecksumContext` is used to handle `ChecksumRequest`
pub struct ChecksumContext<S: Snapshot> {
    req: ChecksumRequest,
    scanner: RangesScanner<TiKVStorage<SnapshotStore<S>>>,
}

impl<S: Snapshot> ChecksumContext<S> {
    pub fn new(
        req: ChecksumRequest,
        ranges: Vec<KeyRange>,
        start_ts: u64,
        snap: S,
        req_ctx: &ReqContext,
    ) -> Result<Self> {
        let store = SnapshotStore::new(
            snap,
            start_ts.into(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
            req_ctx.bypass_locks.clone(),
            false,
        );
        let scanner = RangesScanner::new(RangesScannerOptions {
            storage: TiKVStorage::new(store, false),
            ranges: ranges
                .into_iter()
                .map(|r| Range::from_pb_range(r, false))
                .collect(),
            scan_backward_in_range: false,
            is_key_only: false,
            is_scanned_range_aware: false,
        });
        Ok(Self { req, scanner })
    }
}

#[async_trait]
impl<S: Snapshot> RequestHandler for ChecksumContext<S> {
    async fn handle_request(&mut self) -> Result<Response> {
        let algorithm = self.req.get_algorithm();
        if algorithm != ChecksumAlgorithm::Crc64Xor {
            return Err(box_err!("unknown checksum algorithm {:?}", algorithm));
        }

        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let (old_prefix, new_prefix) = if self.req.has_rule() {
            let mut rule = self.req.get_rule().clone();
            (rule.take_old_prefix(), rule.take_new_prefix())
        } else {
            (vec![], vec![])
        };

        let mut prefix_digest = crc64fast::Digest::new();
        prefix_digest.write(&old_prefix);

        while let Some((k, v)) = self.scanner.next()? {
            if !k.starts_with(&new_prefix) {
                return Err(box_err!("Wrong prefix expect: {:?}", new_prefix));
            }
            checksum =
                checksum_crc64_xor(checksum, prefix_digest.clone(), &k[new_prefix.len()..], &v);
            total_kvs += 1;
            total_bytes += k.len() + v.len() + old_prefix.len() - new_prefix.len();
        }

        let mut resp = ChecksumResponse::default();
        resp.set_checksum(checksum);
        resp.set_total_kvs(total_kvs);
        resp.set_total_bytes(total_bytes as u64);
        let data = box_try!(resp.write_to_bytes());

        let mut resp = Response::default();
        resp.set_data(data);
        Ok(resp)
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.scanner.collect_storage_stats(dest)
    }
}

pub fn checksum_crc64_xor(
    checksum: u64,
    mut digest: crc64fast::Digest,
    k_suffix: &[u8],
    v: &[u8],
) -> u64 {
    digest.write(k_suffix);
    digest.write(v);
    checksum ^ digest.sum64()
}
