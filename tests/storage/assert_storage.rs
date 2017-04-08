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

use super::sync_storage::SyncStorage;
use kvproto::kvrpcpb::{Context, LockInfo};
use tikv::storage::{self, Key, KvPair, Mutation, make_key};
use tikv::storage::mvcc::{self, MAX_TXN_WRITE_SIZE};
use tikv::storage::txn;
use raftstore::cluster::Cluster;
use raftstore::server::ServerCluster;
use tikv::util::HandyRwLock;
use super::util::new_raft_storage_with_store_count;
use tikv::storage::config::Config;
use tikv::storage::engine;


#[derive(Clone)]
pub struct AssertionStorage {
    pub store: SyncStorage,
    pub ctx: Context,
}

impl Default for AssertionStorage {
    fn default() -> AssertionStorage {
        AssertionStorage {
            ctx: Context::new(),
            store: SyncStorage::new(&Config::default()),
        }
    }
}

impl AssertionStorage {
    pub fn new_raft_storage_with_store_count(count: usize,
                                             key: &str)
                                             -> (Cluster<ServerCluster>, AssertionStorage) {
        let (cluster, store, ctx) = new_raft_storage_with_store_count(count, key);
        let storage = AssertionStorage {
            ctx: ctx,
            store: store,
        };
        (cluster, storage)
    }

    pub fn update_with_key(&mut self, cluster: &mut Cluster<ServerCluster>, key: &str) {
        self.update_with_key_byte(cluster, key.as_bytes());
    }

    pub fn update_with_key_byte(&mut self, cluster: &mut Cluster<ServerCluster>, key: &[u8]) {
        // ensure the leader of range which contains current key has been elected
        cluster.must_get(key);
        let region = cluster.get_region(key);
        let leader = cluster.leader_of_region(region.get_id()).unwrap();
        if leader.get_store_id() == self.ctx.get_peer().get_store_id() {
            return;
        }
        let engine = cluster.sim.rl().storages[&leader.get_id()].clone();
        self.ctx.set_region_id(region.get_id());
        self.ctx.set_region_epoch(region.get_region_epoch().clone());
        self.ctx.set_peer(leader.clone());
        self.store = SyncStorage::from_engine(engine, &Config::default());
    }

    pub fn get_none(&self, key: &[u8], ts: u64) {
        let key = make_key(key);
        assert_eq!(self.store.get(self.ctx.clone(), &key, ts).unwrap(), None);
    }

    pub fn get_err(&self, key: &[u8], ts: u64) {
        let key = make_key(key);
        assert!(self.store.get(self.ctx.clone(), &key, ts).is_err());
    }

    pub fn get_ok(&self, key: &[u8], ts: u64, expect: &[u8]) {
        let key = make_key(key);
        assert_eq!(self.store.get(self.ctx.clone(), &key, ts).unwrap().unwrap(),
                   expect);
    }

    pub fn batch_get_ok(&self, keys: &[&[u8]], ts: u64, expect: Vec<&[u8]>) {
        let keys: Vec<Key> = keys.into_iter().map(|x| make_key(x)).collect();
        let result: Vec<Vec<u8>> = self.store
            .batch_get(self.ctx.clone(), &keys, ts)
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap().1)
            .collect();
        let expect: Vec<Vec<u8>> = expect.into_iter().map(|x| x.to_vec()).collect();
        assert_eq!(result, expect);
    }

    fn expect_not_leader_or_stale_command(&self, err: storage::Error) {
        match err {
            storage::Error::Txn(
                txn::Error::Mvcc(mvcc::Error::Engine(engine::Error::Request(ref e)))) |
            storage::Error::Txn(txn::Error::Engine(engine::Error::Request(ref e))) |
            storage::Error::Engine(engine::Error::Request(ref e)) => {
                assert!(e.has_not_leader() | e.has_stale_command(), "invalid error {:?}", e);
            }
            _ => {
                panic!("expect not leader error or stale command, but got {:?}",
                       err);
            }
        }
    }

    fn expect_invalid_tso_err(&self, resp: Result<(), storage::Error>, sts: u64, cmt_ts: u64) {
        assert!(resp.is_err());
        let err = resp.unwrap_err();
        match err {
            storage::Error::Txn(txn::Error::InvalidTxnTso {
                                    start_ts,
                                    commit_ts,
                                }) => {
                assert_eq!(sts, start_ts);
                assert_eq!(cmt_ts, commit_ts);
            }
            _ => {
                panic!("expect invalid tso error, but got {:?}", err);
            }
        }
    }

    pub fn get_none_from_cluster(&mut self,
                                 cluster: &mut Cluster<ServerCluster>,
                                 key: &[u8],
                                 ts: u64) {
        let item = make_key(key);
        for _ in 0..3 {
            let ret = self.store.get(self.ctx.clone(), &item, ts);
            if ret.is_ok() {
                assert_eq!(ret.unwrap(), None);
                return;
            }
            self.expect_not_leader_or_stale_command(ret.unwrap_err());
            self.update_with_key_byte(cluster, key);
        }
        panic!("failed with 3 retry!");
    }

    pub fn put_ok_for_cluster(&mut self,
                              cluster: &mut Cluster<ServerCluster>,
                              key: &[u8],
                              value: &[u8],
                              start_ts: u64,
                              commit_ts: u64) {
        let mut success = false;
        for _ in 0..3 {
            let res = self.store
                .prewrite(self.ctx.clone(),
                          vec![Mutation::Put((make_key(key), value.to_vec()))],
                          key.to_vec(),
                          start_ts);
            if res.is_ok() {
                success = true;
                break;
            }
            self.expect_not_leader_or_stale_command(res.unwrap_err());
            self.update_with_key_byte(cluster, key)
        }
        assert!(success);

        success = false;
        for _ in 0..3 {
            let res = self.store.commit(self.ctx.clone(), vec![make_key(key)], start_ts, commit_ts);
            if res.is_ok() {
                success = true;
                break;
            }
            self.expect_not_leader_or_stale_command(res.unwrap_err());
            self.update_with_key_byte(cluster, key)
        }
        assert!(success);
    }


    pub fn put_ok(&self, key: &[u8], value: &[u8], start_ts: u64, commit_ts: u64) {
        self.store
            .prewrite(self.ctx.clone(),
                      vec![Mutation::Put((make_key(key), value.to_vec()))],
                      key.to_vec(),
                      start_ts)
            .unwrap();
        self.store
            .commit(self.ctx.clone(), vec![make_key(key)], start_ts, commit_ts)
            .unwrap();
    }

    pub fn delete_ok(&self, key: &[u8], start_ts: u64, commit_ts: u64) {
        self.store
            .prewrite(self.ctx.clone(),
                      vec![Mutation::Delete(make_key(key))],
                      key.to_vec(),
                      start_ts)
            .unwrap();
        self.store
            .commit(self.ctx.clone(), vec![make_key(key)], start_ts, commit_ts)
            .unwrap();
    }

    pub fn scan_ok(&self,
                   start_key: &[u8],
                   limit: usize,
                   ts: u64,
                   expect: Vec<Option<(&[u8], &[u8])>>) {
        let key_address = make_key(start_key);
        let result = self.store.scan(self.ctx.clone(), key_address, limit, false, ts).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> = expect
            .into_iter()
            .map(|x| x.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect();
        assert_eq!(result, expect);
    }

    pub fn scan_key_only_ok(&self,
                            start_key: &[u8],
                            limit: usize,
                            ts: u64,
                            expect: Vec<Option<&[u8]>>) {
        let key_address = make_key(start_key);
        let result = self.store.scan(self.ctx.clone(), key_address, limit, true, ts).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> =
            expect.into_iter().map(|x| x.map(|k| (k.to_vec(), vec![]))).collect();
        assert_eq!(result, expect);
    }

    pub fn prewrite_ok(&self, mutations: Vec<Mutation>, primary: &[u8], start_ts: u64) {
        self.store
            .prewrite(self.ctx.clone(), mutations, primary.to_vec(), start_ts)
            .unwrap();
    }

    pub fn prewrite_locked(&self,
                           mutations: Vec<Mutation>,
                           primary: &[u8],
                           start_ts: u64,
                           expect_locks: Vec<(&[u8], &[u8], u64)>) {
        let res = self.store
            .prewrite(self.ctx.clone(), mutations, primary.to_vec(), start_ts)
            .unwrap();
        let locks: Vec<(&[u8], &[u8], u64)> = res.iter()
            .filter_map(|x| {
                if let Err(storage::Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked {
                                                                    ref key,
                                                                    ref primary,
                                                                    ts,
                                                                    ..
                                                                }))) = *x {
                    Some((key.as_ref(), primary.as_ref(), ts))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(expect_locks, locks);
    }

    pub fn commit_ok(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64) {
        let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
        self.store.commit(self.ctx.clone(), keys, start_ts, commit_ts).unwrap();
    }

    pub fn commit_with_illegal_tso(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64) {
        let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
        let resp = self.store.commit(self.ctx.clone(), keys, start_ts, commit_ts);
        self.expect_invalid_tso_err(resp, start_ts, commit_ts);
    }

    pub fn cleanup_ok(&self, key: &[u8], start_ts: u64) {
        self.store.cleanup(self.ctx.clone(), make_key(key), start_ts).unwrap();
    }

    pub fn cleanup_err(&self, key: &[u8], start_ts: u64) {
        assert!(self.store.cleanup(self.ctx.clone(), make_key(key), start_ts).is_err());
    }

    pub fn rollback_ok(&self, keys: Vec<&[u8]>, start_ts: u64) {
        let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
        self.store.rollback(self.ctx.clone(), keys, start_ts).unwrap();
    }

    pub fn rollback_err(&self, keys: Vec<&[u8]>, start_ts: u64) {
        let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
        assert!(self.store.rollback(self.ctx.clone(), keys, start_ts).is_err());
    }

    pub fn scan_lock_ok(&self, max_ts: u64, expect: Vec<LockInfo>) {
        assert_eq!(self.store.scan_lock(self.ctx.clone(), max_ts).unwrap(),
                   expect);
    }

    pub fn resolve_lock_ok(&self, start_ts: u64, commit_ts: Option<u64>) {
        self.store.resolve_lock(self.ctx.clone(), start_ts, commit_ts).unwrap();
    }

    pub fn resolve_lock_with_illegal_tso(&self, start_ts: u64, commit_ts: Option<u64>) {
        let resp = self.store.resolve_lock(self.ctx.clone(), start_ts, commit_ts);
        self.expect_invalid_tso_err(resp, start_ts, commit_ts.unwrap())
    }


    pub fn gc_ok(&self, safe_point: u64) {
        self.store.gc(self.ctx.clone(), safe_point).unwrap();
    }

    pub fn raw_get_ok(&self, key: Vec<u8>, value: Option<Vec<u8>>) {
        assert_eq!(self.store.raw_get(self.ctx.clone(), key).unwrap(), value);
    }

    pub fn raw_put_ok(&self, key: Vec<u8>, value: Vec<u8>) {
        self.store.raw_put(self.ctx.clone(), key, value).unwrap();
    }

    pub fn raw_delete_ok(&self, key: Vec<u8>) {
        self.store.raw_delete(self.ctx.clone(), key).unwrap()
    }

    pub fn test_txn_store_gc(&self, key: &str) {
        let key_bytes = key.as_bytes();
        self.put_ok(key_bytes, b"v1", 5, 10);
        self.put_ok(key_bytes, b"v2", 15, 20);
        self.gc_ok(30);
        self.get_none(key_bytes, 15);
        self.get_ok(key_bytes, 25, b"v2");
    }

    pub fn test_txn_store_gc3(&self, key_prefix: u8) {
        let key_len = 10_000;
        let key = vec![key_prefix; 10_000];
        for k in 1u64..(MAX_TXN_WRITE_SIZE / key_len * 2) as u64 {
            self.put_ok(&key, b"", k * 10, k * 10 + 5);
        }
        self.delete_ok(&key, 1000, 1050);
        self.get_none(&key, 2000);
        self.gc_ok(2000);
        self.get_none(&key, 3000);
    }
}
