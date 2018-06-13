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

use super::{AdminObserver, Coprocessor, ObserverContext, Result as CopResult};
use coprocessor::codec::table;
use util::codec::bytes::{self, encode_bytes};

use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
use std::result::Result as StdResult;

/// `SplitObserver` adjusts the split key so that it won't separate
/// the data of a row into two region. It adjusts the key according
/// to the key format of `TiDB`.
pub struct SplitObserver;

type Result<T> = StdResult<T, String>;

impl SplitObserver {
    fn on_split(&self, ctx: &mut ObserverContext, split: &mut SplitRequest) -> Result<()> {
        if split.get_split_key().is_empty() {
            return Err("split key is expected!".to_owned());
        }

        let mut key = match bytes::decode_bytes(&mut split.get_split_key(), false) {
            Ok(x) => x,
            Err(_) => return Ok(()),
        };

        // format of a key is TABLE_PREFIX + table_id + RECORD_PREFIX_SEP + handle + column_id
        // + version or TABLE_PREFIX + table_id + INDEX_PREFIX_SEP + index_id + values + version
        // or meta_key + version
        // The length of TABLE_PREFIX + table_id is TABLE_PREFIX_KEY_LEN.
        if key.starts_with(table::TABLE_PREFIX)
            && key.len() > table::TABLE_PREFIX_KEY_LEN
            && key[table::TABLE_PREFIX_KEY_LEN..].starts_with(table::RECORD_PREFIX_SEP)
        {
            // row key, truncate to handle
            key.truncate(table::PREFIX_LEN + table::ID_LEN);
        }

        let region_start_key = ctx.region().get_start_key();

        let key = encode_bytes(&key);
        if *key <= *region_start_key {
            return Err("no need to split".to_owned());
        }

        split.set_split_key(key);
        Ok(())
    }
}

impl Coprocessor for SplitObserver {}

impl AdminObserver for SplitObserver {
    fn pre_propose_admin(
        &self,
        ctx: &mut ObserverContext,
        req: &mut AdminRequest,
    ) -> CopResult<()> {
        if req.get_cmd_type() != AdminCmdType::Split {
            return Ok(());
        }
        if !req.has_split() {
            box_try!(Err(
                "cmd_type is Split but it doesn't have split request, message maybe \
                 corrupted!"
                    .to_owned()
            ));
        }
        if let Err(e) = self.on_split(ctx, req.mut_split()) {
            error!("failed to handle split req: {:?}", e);
            return Err(box_err!(e));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::{BigEndian, WriteBytesExt};
    use coprocessor::codec::{datum, table, Datum};
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
    use raftstore::coprocessor::AdminObserver;
    use raftstore::coprocessor::ObserverContext;
    use util::codec::bytes::encode_bytes;

    fn new_split_request(key: &[u8]) -> AdminRequest {
        let mut req = AdminRequest::new();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::new();
        split_req.set_split_key(key.to_vec());
        req.set_split(split_req);
        req
    }

    fn new_row_key(table_id: i64, row_id: i64, column_id: u64, version_id: u64) -> Vec<u8> {
        let mut key = table::encode_row_key(table_id, row_id);
        if column_id > 0 {
            key.write_u64::<BigEndian>(column_id).unwrap();
        }
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_index_key(table_id: i64, idx_id: i64, datums: &[Datum], version_id: u64) -> Vec<u8> {
        let mut key =
            table::encode_index_seek_key(table_id, idx_id, &datum::encode_key(datums).unwrap());
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    #[test]
    fn test_forget_encode() {
        let region_start_key = new_row_key(256, 1, 0, 0);
        let key = new_row_key(256, 2, 1, 0);
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(region_start_key);

        let mut ctx = ObserverContext::new(&r);
        let observer = SplitObserver;

        let mut req = new_split_request(&key);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        let expect_key = new_row_key(256, 2, 0, 0);
        let len = expect_key.len();
        assert_eq!(req.get_split().get_split_key(), &expect_key[..len - 8]);
    }

    #[test]
    fn test_split() {
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        let mut req = AdminRequest::new();

        let observer = SplitObserver;

        let resp = observer.pre_propose_admin(&mut ctx, &mut req);
        // since no split is defined, actual coprocessor won't be invoke.
        assert!(resp.is_ok());
        assert!(!req.has_split(), "only split req should be handle.");

        req = new_split_request(b"test");
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), b"test");

        let mut key = encode_bytes(b"db:1");
        key.write_u64::<BigEndian>(0).unwrap();
        let mut expect_key = encode_bytes(b"db:1");
        req = new_split_request(&key);
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 0, 0);
        req = new_split_request(&key);
        expect_key = key[..key.len() - 8].to_vec();
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 1, 0);
        req = new_split_request(&key);
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 1, 1);
        req = new_split_request(&key);
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_index_key(1, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 0);
        req = new_split_request(&key);
        expect_key = key[..key.len() - 8].to_vec();
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_index_key(1, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 5);
        req = new_split_request(&key);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        expect_key =
            encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\x82\x7f");
        key = expect_key.clone();
        key.extend_from_slice(b"\x80\x00\x00\x00\x00\x00\x00\xd3");
        req = new_split_request(&key);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        // Split at table prefix.
        expect_key = encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea");
        req = new_split_request(&expect_key);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        assert_eq!(req.get_split().get_split_key(), &*expect_key);
    }
}
