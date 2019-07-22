// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crc::crc64::{self, Digest, Hasher64};
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tipb::checksum::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse};

use crate::storage::{Snapshot, SnapshotStore, Statistics};

use crate::coprocessor::dag::storage::scanner::RangesScanner;
use crate::coprocessor::dag::storage::Range;
use crate::coprocessor::dag::storage_impl::TiKVStorage;
use crate::coprocessor::*;

// `ChecksumContext` is used to handle `ChecksumRequest`
pub struct ChecksumContext<S: Snapshot> {
    req: ChecksumRequest,
    scanner: RangesScanner<TiKVStorage<SnapshotStore<S>>>,
}

impl<S: Snapshot> ChecksumContext<S> {
    pub fn new(
        req: ChecksumRequest,
        ranges: Vec<KeyRange>,
        snap: S,
        req_ctx: &ReqContext,
    ) -> Result<Self> {
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
        );
        let scanner = RangesScanner::new(
            store.into(),
            ranges
                .into_iter()
                .map(|r| Range::from_pb_range(r, false))
                .collect(),
            false,
            false,
            false,
        );
        Ok(Self { req, scanner })
    }
}

impl<S: Snapshot> RequestHandler for ChecksumContext<S> {
    fn handle_request(&mut self) -> Result<Response> {
        let algorithm = self.req.get_algorithm();
        if algorithm != ChecksumAlgorithm::Crc64_Xor {
            return Err(box_err!("unknown checksum algorithm {:?}", algorithm));
        }

        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        while let Some((k, v)) = self.scanner.next()? {
            checksum = checksum_crc64_xor(checksum, &k, &v);
            total_kvs += 1;
            total_bytes += k.len() + v.len();
        }

        let mut resp = ChecksumResponse::new();
        resp.set_checksum(checksum);
        resp.set_total_kvs(total_kvs);
        resp.set_total_bytes(total_bytes as u64);
        let data = box_try!(resp.write_to_bytes());

        let mut resp = Response::new();
        resp.set_data(data);
        Ok(resp)
    }

    fn collect_scan_statistics(&mut self, stats: &mut Statistics) {
        self.scanner.collect_storage_stats(stats)
    }
}

fn checksum_crc64_xor(checksum: u64, k: &[u8], v: &[u8]) -> u64 {
    let mut digest = Digest::new(crc64::ECMA);
    digest.write(k);
    digest.write(v);
    checksum ^ digest.sum64()
}
