// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;
use kvproto::{
    metapb::Region,
    raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest},
};
use tikv_util::{box_err, box_try, codec::bytes, error, warn};

use super::{AdminObserver, Coprocessor, ObserverContext, Result as CopResult};
use crate::{store::util, Error};

fn strip_timestamp_if_exists(mut key: Vec<u8>) -> Vec<u8> {
    let mut slice = key.as_slice();
    let strip_len = match bytes::decode_bytes(&mut slice, false) {
        // It is an encoded key and the slice points to the remaining unparsable
        // part which most likely is timestamp. Note that the key can be a raw key
        // in valid encoded form, but treat it as a encoded key anyway.
        Ok(_) => slice.len(),
        // It must be a raw key so no need to strip.
        Err(_) => 0,
    };
    key.truncate(key.len() - strip_len);
    key
}

fn is_valid_split_key(key: &[u8], index: usize, region: &Region) -> bool {
    if key.is_empty() {
        warn!(
            "skip invalid split key: key is empty";
            "region_id" => region.get_id(),
            "index" => index,
        );
        return false;
    }

    if let Err(Error::KeyNotInRegion(..)) = util::check_key_in_region_exclusive(key, region) {
        warn!(
            "skip invalid split key: key is not in region";
            "key" => log_wrappers::Value::key(key),
            "region_id" => region.get_id(),
            "start_key" => log_wrappers::Value::key(region.get_start_key()),
            "end_key" => log_wrappers::Value::key(region.get_end_key()),
            "index" => index,
        );
        return false;
    }

    true
}

/// `SplitObserver` adjusts the split key so that it won't separate
/// multiple MVCC versions of a key into two regions.
#[derive(Clone)]
pub struct SplitObserver;

impl SplitObserver {
    fn on_split(
        &self,
        ctx: &mut ObserverContext<'_>,
        splits: &mut Vec<SplitRequest>,
    ) -> Result<(), String> {
        let ajusted_splits = std::mem::take(splits)
            .into_iter()
            .enumerate()
            .filter_map(|(i, mut split)| {
                let key = split.take_split_key();
                let key = strip_timestamp_if_exists(key);
                if is_valid_split_key(&key, i, ctx.region) {
                    split.split_key = key;
                    Some(split)
                } else {
                    None
                }
            })
            .coalesce(|prev, curr| {
                // Make sure that the split keys are sorted and unique.
                if prev.split_key < curr.split_key {
                    Err((prev, curr))
                } else {
                    warn!(
                        "skip invalid split key: key should not be larger than the previous.";
                        "region_id" => ctx.region.id,
                        "key" => log_wrappers::Value::key(&curr.split_key),
                        "previous" => log_wrappers::Value::key(&prev.split_key),
                    );
                    Ok(prev)
                }
            })
            .collect::<Vec<_>>();

        if ajusted_splits.is_empty() {
            Err("no valid key found for split.".to_owned())
        } else {
            // Rewrite the splits.
            *splits = ajusted_splits;
            Ok(())
        }
    }
}

impl Coprocessor for SplitObserver {}

impl AdminObserver for SplitObserver {
    fn pre_propose_admin(
        &self,
        ctx: &mut ObserverContext<'_>,
        req: &mut AdminRequest,
    ) -> CopResult<()> {
        match req.get_cmd_type() {
            AdminCmdType::Split => {
                if !req.has_split() {
                    box_try!(Err(
                        "cmd_type is Split but it doesn't have split request, message maybe \
                         corrupted!"
                            .to_owned()
                    ));
                }
                let mut request = vec![req.take_split()];
                if let Err(e) = self.on_split(ctx, &mut request) {
                    error!(
                        "failed to handle split req";
                        "region_id" => ctx.region().get_id(),
                        "err" => ?e,
                    );
                    return Err(box_err!(e));
                }
                // self.on_split() makes sure request is not empty, or it will return error.
                // so directly unwrap here.
                req.set_split(request.pop().unwrap());
            }
            AdminCmdType::BatchSplit => {
                if !req.has_splits() {
                    return Err(box_err!(
                        "cmd_type is BatchSplit but it doesn't have splits request, message maybe \
                         corrupted!"
                            .to_owned()
                    ));
                }
                let mut requests = req.mut_splits().take_requests().into();
                if let Err(e) = self.on_split(ctx, &mut requests) {
                    error!(
                        "failed to handle split req";
                        "region_id" => ctx.region().get_id(),
                        "err" => ?e,
                    );
                    return Err(box_err!(e));
                }
                req.mut_splits().set_requests(requests.into());
            }
            _ => return Ok(()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, WriteBytesExt};
    use kvproto::{
        metapb::Region,
        raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest},
    };
    use tidb_query_datatype::{
        codec::{datum, table, Datum},
        expr::EvalContext,
    };
    use tikv_util::codec::bytes::encode_bytes;

    use super::*;
    use crate::coprocessor::{AdminObserver, ObserverContext};

    fn new_split_request(key: Vec<u8>) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::default();
        split_req.set_split_key(key);
        req.set_split(split_req);
        req
    }

    fn new_batch_split_request(keys: Vec<Vec<u8>>) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        for key in keys {
            let mut split_req = SplitRequest::default();
            split_req.set_split_key(key);
            req.mut_splits().mut_requests().push(split_req);
        }
        req
    }

    fn new_row_key(table_id: i64, row_id: i64, version_id: u64) -> Vec<u8> {
        let mut key = table::encode_row_key(table_id, row_id);
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_index_key(table_id: i64, idx_id: i64, datums: &[Datum], version_id: u64) -> Vec<u8> {
        let mut key = table::encode_index_seek_key(
            table_id,
            idx_id,
            &datum::encode_key(&mut EvalContext::default(), datums).unwrap(),
        );
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    #[test]
    fn test_forget_encode() {
        let region_start_key = new_row_key(256, 1, 0);
        let key = new_row_key(256, 2, 0);
        let mut r = Region::default();
        r.set_id(10);
        r.set_start_key(region_start_key);

        let mut ctx = ObserverContext::new(&r);
        let observer = SplitObserver;

        let mut req = new_batch_split_request(vec![key]);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        let expect_key = new_row_key(256, 2, 0);
        let len = expect_key.len();
        assert_eq!(req.get_splits().get_requests().len(), 1);
        assert_eq!(
            req.get_splits().get_requests()[0].get_split_key(),
            &expect_key[..len - 8]
        );
    }

    #[test]
    fn test_split() {
        let mut region = Region::default();
        let start_key = new_row_key(1, 1, 1);
        region.set_start_key(start_key.clone());
        let mut ctx = ObserverContext::new(&region);
        let mut req = AdminRequest::default();

        let observer = SplitObserver;

        let resp = observer.pre_propose_admin(&mut ctx, &mut req);
        // since no split is defined, actual coprocessor won't be invoke.
        assert!(resp.is_ok());
        assert!(!req.has_split(), "only split req should be handle.");

        req = new_split_request(new_row_key(1, 2, 0));
        // For compatible reason, split should supported too.
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_ok());

        // Empty key should be skipped.
        let mut split_keys = vec![vec![]];
        // Start key should be skipped.
        split_keys.push(start_key);

        req = new_batch_split_request(split_keys.clone());
        // Although invalid keys should be skipped, but if all keys are
        // invalid, errors should be reported.
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_err());

        let mut key = new_row_key(1, 2, 0);
        let mut expected_key = key[..key.len() - 8].to_vec();
        split_keys.push(key);
        let mut expected_keys = vec![expected_key.clone()];

        // Extra version of same key will be ignored.
        key = new_row_key(1, 2, 1);
        split_keys.push(key);

        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 0);
        expected_key = key[..key.len() - 8].to_vec();
        split_keys.push(key);
        expected_keys.push(expected_key.clone());

        // Extra version of same key will be ignored.
        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 5);
        split_keys.push(key);

        expected_key =
            encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\x82\x7f");
        key = expected_key.clone();
        key.extend_from_slice(b"\x80\x00\x00\x00\x00\x00\x00\xd3");
        split_keys.push(key);
        expected_keys.push(expected_key.clone());

        // Split at table prefix.
        key = encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xee");
        split_keys.push(key.clone());
        expected_keys.push(key);

        // Raw key should be preserved.
        split_keys.push(b"xyz".to_vec());
        expected_keys.push(b"xyz".to_vec());

        key = encode_bytes(b"xyz:1");
        key.write_u64::<BigEndian>(0).unwrap();
        split_keys.push(key);
        expected_key = encode_bytes(b"xyz:1");
        expected_keys.push(expected_key);

        req = new_batch_split_request(split_keys);
        req.mut_splits().set_right_derive(true);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        assert!(req.get_splits().get_right_derive());
        assert_eq!(req.get_splits().get_requests().len(), expected_keys.len());
        for (i, (req, expected_key)) in req
            .get_splits()
            .get_requests()
            .iter()
            .zip(expected_keys)
            .enumerate()
        {
            assert_eq!(
                req.get_split_key(),
                expected_key.as_slice(),
                "case {}",
                i + 1
            );
        }
    }
}
