// Copyright 2016 PingCAP, Inc.
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

use super::{Coprocessor, RegionObserver, ObserverContext};

use kvproto::raft_cmdpb::{SplitRequest, AdminRequest, Request, AdminResponse, Response,
                          AdminCmdType};
use protobuf::RepeatedField;
use util::codec::bytes;
use std::result;

/// SplitObserver adjusts the split key so that it won't seperate
/// the data of a row into two region.
pub struct SplitObserver;

pub const TABLE_PREFIX: &'static [u8] = b"t";
pub const TABLE_ROW_MARK: &'static [u8] = b"_r";

type Result<T> = result::Result<T, String>;

impl SplitObserver {
    fn extract_sql_key(&self, split: &SplitRequest) -> Result<Vec<u8>> {
        if !split.has_split_key() {
            return Err("split key is expected!".to_owned());
        }
        let split_key = split.get_split_key();
        bytes::decode_bytes(split_key)
            .map_err(|e| format!("invalid key {:?}: {}", split_key, e))
            .map(|r| r.0)
    }

    fn on_split(&mut self, _: &mut ObserverContext, split: &mut SplitRequest) -> Result<()> {
        let key = try!(self.extract_sql_key(split));

        // format of a key is TABLE_PREFIX + table_id + TABLE_ROW_MARK + handle or
        // TABLE_PREFIX + table_id + TABLE_INDEX_MARK

        if !key.starts_with(TABLE_PREFIX) {
            // if it's not a data key, just split.
            return Ok(());
        }
        // table id is a u64 and we only care about row data.
        let mark_index = TABLE_PREFIX.len() + 8;
        let new_key: &[u8];
        if key.len() > mark_index && key[mark_index..].starts_with(TABLE_ROW_MARK) {
            // handle is a u64 too.
            let expected_len = TABLE_PREFIX.len() + 8 + TABLE_ROW_MARK.len() + 8;
            if key.len() < expected_len {
                return Err(format!("invalid key {:?}; len should be at least {}",
                                   key,
                                   expected_len));
            }
            new_key = &key[..expected_len];
        } else {
            new_key = &key;
        }
        split.set_split_key(bytes::encode_bytes(new_key));
        Ok(())
    }
}

impl Coprocessor for SplitObserver {
    fn start(&mut self) {}
    fn stop(&mut self) {}
}

impl RegionObserver for SplitObserver {
    fn pre_admin(&mut self, ctx: &mut ObserverContext, req: &mut AdminRequest) {
        if req.get_cmd_type() != AdminCmdType::Split {
            return;
        }
        if !req.has_split() {
            error!("cmd_type is Split but it doesn't have split request, message maybe corrupted!");
            return;
        }
        if let Err(e) = self.on_split(ctx, req.mut_split()) {
            error!("failed to check split request: {}", e);
            // TODO add error to coprocessor
            req.take_split();
        }
    }

    fn post_admin(&mut self, _: &mut ObserverContext, _: &AdminRequest, _: &mut AdminResponse) {}

    /// Hook to call before execute read/write request.
    fn pre_query(&mut self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) -> () {}

    /// Hook to call after read/write request being executed.
    fn post_query(&mut self,
                  _: &mut ObserverContext,
                  _: &[Request],
                  _: &mut RepeatedField<Response>)
                  -> () {
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use tempdir::TempDir;
    use raftstore::store::engine::*;
    use raftstore::store::PeerStorage;
    use raftstore::coprocessor::ObserverContext;
    use raftstore::coprocessor::RegionObserver;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{SplitRequest, AdminRequest, AdminCmdType};
    use util::codec::bytes;
    use byteorder::{ByteOrder, BigEndian, WriteBytesExt};
    use std::io::Write;

    fn new_peer_storage(path: &TempDir) -> PeerStorage {
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        PeerStorage::new(Arc::new(engine), &Region::new()).unwrap()
    }

    fn new_split_request(key: &[u8]) -> AdminRequest {
        let mut req = AdminRequest::new();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::new();
        split_req.set_split_key(key.to_vec());
        req.set_split(split_req);
        req
    }

    fn new_row_key(table_id: u64, row_id: u64, column_id: u64, version_id: u64) -> Vec<u8> {
        new_key(table_id, row_id, TABLE_ROW_MARK, column_id, version_id)
    }

    fn new_index_key(table_id: u64, row_id: u64, column_id: u64, version_id: u64) -> Vec<u8> {
        new_key(table_id, row_id, b"_i", column_id, version_id)
    }

    fn new_key(table_id: u64,
               row_id: u64,
               mark: &[u8],
               column_id: u64,
               version_id: u64)
               -> Vec<u8> {
        let mut key = Vec::with_capacity(100);
        key.write(TABLE_PREFIX).unwrap();
        key.write_u64::<BigEndian>(table_id).unwrap();
        key.write(mark).unwrap();
        key.write_u64::<BigEndian>(row_id).unwrap();
        if column_id > 0 {
            key.write_u64::<BigEndian>(column_id).unwrap();
        }
        key = bytes::encode_bytes(&key);
        if version_id > 0 {
            key.write_u64::<BigEndian>(version_id).unwrap();
        }
        key
    }

    #[test]
    fn test_split() {
        let path = TempDir::new("test-raftstore").unwrap();
        let storage = new_peer_storage(&path);
        let mut ctx = ObserverContext::new(&storage);
        let mut req = AdminRequest::new();

        let mut observer = SplitObserver;

        observer.pre_admin(&mut ctx, &mut req);
        assert!(!req.has_split(),
                "only when split request should be handle.");

        req = new_split_request(b"test");
        observer.pre_admin(&mut ctx, &mut req);
        assert!(!req.has_split(), "invalid split should be prevented");

        let mut key = Vec::with_capacity(100);
        key.write(TABLE_PREFIX).unwrap();
        key = bytes::encode_bytes(&key);
        req = new_split_request(&key);
        observer.pre_admin(&mut ctx, &mut req);
        assert_eq!(req.get_split().get_split_key(), &*key);

        key = new_row_key(1, 2, 0, 0);
        req = new_split_request(&key);
        let mut expect_key = key;
        observer.pre_admin(&mut ctx, &mut req);
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 1, 0);
        req = new_split_request(&key);
        observer.pre_admin(&mut ctx, &mut req);
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 1, 1);
        req = new_split_request(&key);
        observer.pre_admin(&mut ctx, &mut req);
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_index_key(1, 2, 0, 0);
        req = new_split_request(&key);
        expect_key = key;
        observer.pre_admin(&mut ctx, &mut req);
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_index_key(1, 2, 1, 5);
        req = new_split_request(&key);
        let expect_key = new_index_key(1, 2, 1, 0);
        observer.pre_admin(&mut ctx, &mut req);
        assert_eq!(req.get_split().get_split_key(), &*expect_key);
    }
}
