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
                    let unlock_key = Key::from_encoded(key.clone())
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
                    del.set_key(unlock_key);
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
    use kvproto::raft_cmdpb::{CmdType, Request, PutRequest, DeleteRequest, PrewriteRequest,
                              CommitRequest};
    use kvproto::metapb::Region;
    use storage::mvcc::{LockType, Lock};
    use storage::mvcc::{Write, WriteType};

    fn gen_requests(modifies: Vec<Modify>) -> Vec<Request> {
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
                Modify::Commit(k, v) => {
                    let mut commit = CommitRequest::new();
                    commit.set_key(k.into_encoded());
                    commit.set_value(v);
                    req.set_cmd_type(CmdType::Commit);
                    req.set_commit(commit);
                }
            }
            reqs.push(req);
        }
        reqs
    }

    fn gen_prewrite_req(key: &[u8],
                        value: &[u8],
                        pk: &[u8],
                        ts: u64)
                        -> (Vec<Request>, Vec<Request>) {
        let opt = Options::default();
        let key = make_key(key);
        let lock = Lock::new(LockType::Put, pk.to_vec(), ts, opt.lock_ttl, None);

        let mut org_pw_modifies = Vec::new();
        org_pw_modifies.push(Modify::Put(CF_LOCK, key.clone(), lock.to_bytes()));
        org_pw_modifies.push(Modify::Put(CF_DEFAULT, key.append_ts(ts), value.to_vec()));
        let mut new_pw_modifies = Vec::new();
        new_pw_modifies.push(Modify::Prewrite(key.append_ts(ts), value.to_vec(), lock));
        (gen_requests(org_pw_modifies), gen_requests(new_pw_modifies))
    }

    fn gen_commit_req(key: &[u8], ts: u64) -> (Vec<Request>, Vec<Request>) {
        let key = make_key(key);
        let write = Write::new(WriteType::Put, ts, None);

        let mut org_commit_modifies = Vec::new();
        org_commit_modifies.push(Modify::Put(CF_WRITE, key.append_ts(ts), write.to_bytes()));
        org_commit_modifies.push(Modify::Delete(CF_LOCK, key.clone()));
        let mut new_commit_modifies = Vec::new();
        new_commit_modifies.push(Modify::Commit(key.append_ts(ts), write.to_bytes()));
        (gen_requests(org_commit_modifies), gen_requests(new_commit_modifies))
    }

    fn test_resolve_prewrite_request(key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let (org_pw_req, new_pw_req) = gen_prewrite_req(key, value, pk, ts);
        let observer = TxnObserver;
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        let mut new_pw_req = RepeatedField::from_vec(new_pw_req);
        observer.pre_apply_query(&mut ctx, &mut new_pw_req);
        assert_eq!(new_pw_req.into_vec(), org_pw_req);
    }

    fn test_resolve_commit_request(key: &[u8], ts: u64) {
        let (org_commit_req, new_commit_req) = gen_commit_req(key, ts);
        let observer = TxnObserver;
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        let mut new_commit_req = RepeatedField::from_vec(new_commit_req);
        observer.pre_apply_query(&mut ctx, &mut new_commit_req);
        assert_eq!(new_commit_req.into_vec(), org_commit_req);
    }

    #[test]
    fn test_pre_apply_query() {
        test_resolve_prewrite_request(b"k1", b"v1", b"k1", 5);
        test_resolve_commit_request(b"k1", 5);
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

    #[bench]
    fn bench_gen_prewrite_req(b: &mut Bencher) {
        let value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        let (key, pk, ts) = (b"k1", b"k1", 5);
        b.iter(|| {
            RepeatedField::from_vec(gen_prewrite_req(key, &value, pk, ts).1);
        });
    }

    #[bench]
    fn bench_resolve_prewrite(b: &mut Bencher) {
        let value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        let (key, pk, ts) = (b"k1", b"k1", 5);
        let observer = TxnObserver;
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        b.iter(|| {
            let mut prewrite_reqs = RepeatedField::from_vec(gen_prewrite_req(key, &value, pk, ts).1);
            observer.pre_apply_query(&mut ctx, &mut prewrite_reqs);
        });
    }

    #[bench]
    fn bench_gen_commit_req(b: &mut Bencher) {
        let (key, ts) = (b"k1", 5);
        b.iter(|| {
            RepeatedField::from_vec(gen_commit_req(key, ts).1);
        });
    }

    #[bench]
    fn bench_resolve_commit(b: &mut Bencher) {
        let (key, ts) = (b"k1", 5);
        let observer = TxnObserver;
        let region = Region::new();
        let mut ctx = ObserverContext::new(&region);
        b.iter(|| {
            let mut commit_req = RepeatedField::from_vec(gen_commit_req(key, ts).1);
            observer.pre_apply_query(&mut ctx, &mut commit_req);
        });
    }
}
