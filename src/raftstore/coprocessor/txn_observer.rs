// Copyright 2017 PingCAP, Inc.
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
use kvproto::raft_cmdpb::{CmdType, Request, PutRequest, DeleteRequest};
use storage::types::Key;
use storage::{CF_LOCK, CF_WRITE};

use protobuf::RepeatedField;

pub struct TxnObserver;

impl Coprocessor for TxnObserver {}

impl RegionObserver for TxnObserver {
    fn pre_apply_query(&self, _: &mut ObserverContext, reqs: &mut RepeatedField<Request>) {
        for i in 0..reqs.len() {
            match reqs[i].get_cmd_type() {
                CmdType::Prewrite => {
                    let mut prewrite = reqs[i].take_prewrite();
                    let key = prewrite.take_key();
                    let lock_key = Key::from_encoded(key.clone())
                        .truncate_ts()
                        .unwrap()
                        .into_encoded();

                    let mut put = PutRequest::new();
                    put.set_cf(CF_LOCK.to_owned());
                    put.set_key(lock_key);
                    put.set_value(prewrite.take_lock());
                    let mut new_req = Request::new();
                    new_req.set_cmd_type(CmdType::Put);
                    new_req.set_put(put);
                    reqs[i] = new_req;

                    let mut put = PutRequest::new();
                    put.set_key(key);
                    put.set_value(prewrite.take_value());
                    let mut new_req = Request::new();
                    new_req.set_cmd_type(CmdType::Put);
                    new_req.set_put(put);
                    reqs.push(new_req);
                }
                CmdType::Commit => {
                    let mut commit = reqs[i].take_commit();
                    let key = commit.take_key();
                    let lock_key = Key::from_encoded(key.clone())
                        .truncate_ts()
                        .unwrap()
                        .into_encoded();

                    let mut put = PutRequest::new();
                    put.set_cf(CF_WRITE.to_owned());
                    put.set_key(key);
                    put.set_value(commit.take_value());
                    let mut new_req = Request::new();
                    new_req.set_cmd_type(CmdType::Put);
                    new_req.set_put(put);
                    reqs[i] = new_req;

                    let mut del = DeleteRequest::new();
                    del.set_cf(CF_LOCK.to_owned());
                    del.set_key(lock_key);
                    let mut new_req = Request::new();
                    new_req.set_cmd_type(CmdType::Delete);
                    new_req.set_delete(del);
                    reqs.push(new_req);
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::Modify;
    use storage::types::make_key;
    use storage::{CF_DEFAULT, CF_LOCK, Options};
    use kvproto::raft_cmdpb::{CmdType, Request, PutRequest, DeleteRequest, PrewriteRequest};
    use kvproto::metapb::Region;
    use storage::mvcc::{LockType, Lock};

    fn gen_request(modifies: Vec<Modify>) -> Vec<Request> {
        let mut reqs = Vec::with_capacity(modifies.len());
        for m in modifies {
            let mut req = Request::new();
            match m {
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::new();
                    delete.set_key(k.encoded().to_owned());
                    if cf != CF_DEFAULT {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.encoded().to_owned());
                    put.set_value(v);
                    if cf != CF_DEFAULT {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
                Modify::Prewrite(k, v, l) => {
                    let mut prewrite = PrewriteRequest::new();
                    prewrite.set_key(k.encoded().to_owned());
                    prewrite.set_value(v);
                    prewrite.set_lock(l.to_bytes());
                    req.set_cmd_type(CmdType::Prewrite);
                    req.set_prewrite(prewrite);
                }
            }
            reqs.push(req);
        }
        reqs
    }

    fn gen_put_req(key: &[u8], value: &[u8], pk: &[u8], ts: u64) -> Vec<Request> {
        let mut modifies = Vec::new();
        let opt = Options::default();
        let key = make_key(key);
        let lock = Lock::new(LockType::Put, pk.to_vec(), ts, opt.lock_ttl, None);
        modifies.push(Modify::Put(CF_LOCK, key.clone(), lock.to_bytes()));
        modifies.push(Modify::Put(CF_DEFAULT, key.append_ts(ts), value.to_vec()));
        gen_request(modifies)
    }

    fn gen_prewrite_req(key: &[u8], value: &[u8], pk: &[u8], ts: u64) -> Vec<Request> {
        let mut modifies = Vec::new();
        let opt = Options::default();
        let key = make_key(key);
        let lock = Lock::new(LockType::Put, pk.to_vec(), ts, opt.lock_ttl, None);
        modifies.push(Modify::Prewrite(key.append_ts(ts), value.to_vec(), lock));
        gen_request(modifies)
    }

    fn test_pre_apply_query_impl(key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let put_req = RepeatedField::from_vec(gen_put_req(key, value, pk, ts));
        let mut prewrite_req = RepeatedField::from_vec(gen_prewrite_req(key, value, pk, ts));
        let observer = TxnObserver;
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        observer.pre_apply_query(&mut ctx, &mut prewrite_req);
        assert_eq!(prewrite_req, put_req);
    }

    #[test]
    fn test_pre_apply_query() {
        test_pre_apply_query_impl(b"k1", b"v1", b"k1", 5);
    }

    use test::Bencher;
    use storage::SHORT_VALUE_MAX_LEN;

    fn gen_value(v: u8, len: usize) -> Vec<u8> {
        let mut value = Vec::with_capacity(len);
        for _ in 0..len {
            value.push(v);
        }

        value
    }

    fn gen_prewrite_reqs(cnt: u32, key: &[u8], value: &[u8], pk: &[u8], ts: u64) -> Vec<Request> {
        let mut modifies = Vec::new();
        let opt = Options::default();
        let key = make_key(key);
        let lock = Lock::new(LockType::Put, pk.to_vec(), ts, opt.lock_ttl, None);
        for _ in 0..cnt {
            modifies.push(Modify::Prewrite(key.append_ts(ts), value.to_vec(), lock.clone()));
        }
        gen_request(modifies)
    }

    #[bench]
    fn bench_gen_prewrite_reqs(b: &mut Bencher) {
        let value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        let (key, pk, ts) = (b"k1", b"k1", 5);
        b.iter(|| {
            RepeatedField::from_vec(gen_prewrite_reqs(4, key, &value, pk, ts));
        });
    }

    #[bench]
    fn bench_pre_apply_query(b: &mut Bencher) {
        let value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        let (key, pk, ts) = (b"k1", b"k1", 5);
        let observer = TxnObserver;
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        b.iter(|| {
            let mut prewrite_reqs =
                RepeatedField::from_vec(gen_prewrite_reqs(4, key, &value, pk, ts));
            observer.pre_apply_query(&mut ctx, &mut prewrite_reqs);
        });
    }
}
