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

use std::sync::Arc;
use std::collections::HashMap;
use std::fmt::{self, Formatter, Display};

use rocksdb::DB;
use sha1::{Sha1, Digest};

use kvproto::metapb::{Region, RegionEpoch};
use raftstore::store::{PeerStorage, keys, Msg};
use raftstore::store::engine::{Snapshot, Iterable, Peekable};
use storage::CF_RAFT;
use util::escape;
use util::codec::number::NumberEncoder;
use util::worker::Runnable;

use super::metrics::*;
use super::MsgSender;

/// Split checking task.
pub enum Task {
    SplitCheck {
        region_id: u64,
        epoch: RegionEpoch,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
    ComputeHash {
        index: u64,
        region: Region,
        snap: Snapshot,
    },
    VerifyHash {
        region_id: u64,
        index: u64,
        hash: Vec<u8>,
    },
}

impl Task {
    pub fn split_check(ps: &PeerStorage) -> Task {
        Task::SplitCheck {
            region_id: ps.get_region_id(),
            epoch: ps.get_region().get_region_epoch().clone(),
            start_key: keys::enc_start_key(&ps.region),
            end_key: keys::enc_end_key(&ps.region),
        }
    }

    pub fn compute_hash(region: Region, index: u64, snap: Snapshot) -> Task {
        Task::ComputeHash {
            region: region,
            index: index,
            snap: snap,
        }
    }

    pub fn verify_hash(region_id: u64, index: u64, hash: Vec<u8>) -> Task {
        Task::VerifyHash {
            region_id: region_id,
            index: index,
            hash: hash,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::SplitCheck { region_id, .. } => write!(f, "Split Check Task for {}", region_id),
            Task::ComputeHash { ref region, index, .. } => {
                write!(f, "Compute Hash Task for {:?} at {}", region, index)
            }
            Task::VerifyHash { region_id, index, ref hash } => {
                write!(f,
                       "Verify Hash Task for {} at {} [hash: {}]",
                       region_id,
                       index,
                       escape(hash))
            }
        }
    }
}

pub struct Runner<C: MsgSender> {
    db: Arc<DB>,
    ch: C,
    region_max_size: u64,
    split_size: u64,

    // region_id -> (index, hash)
    checksum: HashMap<u64, (u64, Digest)>,
}

#[inline]
fn update(sha1: &mut Sha1, v: &[u8]) {
    let size = v.len();
    let mut size_arr = [0u8; 8];
    size_arr.as_mut().encode_u64(size as u64).unwrap();
    sha1.update(&size_arr);
    sha1.update(v);
}

impl<C: MsgSender> Runner<C> {
    pub fn new(db: Arc<DB>, ch: C, region_max_size: u64, split_size: u64) -> Runner<C> {
        Runner {
            db: db,
            ch: ch,
            region_max_size: region_max_size,
            split_size: split_size,
            checksum: HashMap::new(),
        }
    }

    fn compute_hash(&mut self, region: Region, index: u64, snap: Snapshot) {
        let region_id = region.get_id();
        info!("[region {}] computing hash at {}", region_id, index);
        REGION_HASH_COUNTER_VEC.with_label_values(&["compute", "all"]).inc();

        let timer = REGION_HASH_HISTOGRAM.start_timer();
        let mut sha1 = Sha1::new();
        let mut cf_names = snap.cf_names();
        cf_names.sort();
        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        for cf in cf_names {
            let res = snap.scan_cf(cf,
                                   &start_key,
                                   &end_key,
                                   false,
                                   &mut |k, v| {
                                       update(&mut sha1, k);
                                       update(&mut sha1, v);
                                       Ok(true)
                                   });
            if let Err(e) = res {
                REGION_HASH_COUNTER_VEC.with_label_values(&["compute", "failed"]).inc();
                error!("[region {}] failed to calculate hash: {:?}", region_id, e);
                return;
            }
        }
        let region_state_key = keys::region_state_key(region_id);
        update(&mut sha1, &region_state_key);
        match snap.get_value_cf(CF_RAFT, &region_state_key) {
            Err(e) => {
                REGION_HASH_COUNTER_VEC.with_label_values(&["compute", "failed"]).inc();
                error!("[region {}] failed to get region state: {:?}", region_id, e);
                return;
            }
            Ok(Some(v)) => update(&mut sha1, &v),
            Ok(None) => update(&mut sha1, b""),
        }
        let digest = sha1.digest();
        timer.observe_duration();

        let msg = Msg::ComputeHashResult {
            region_id: region_id,
            index: index,
            hash: digest.bytes().to_vec(),
        };
        self.checksum.insert(region_id, (index, digest));
        if let Err(e) = self.ch.try_send(msg) {
            warn!("[region {}] failed to send hash compute result, err {:?}",
                  region_id,
                  e);
        }
    }

    fn verify_hash(&mut self, region_id: u64, expect_index: u64, expect_hash: Vec<u8>) {
        if let Some(&(index, ref digest)) = self.checksum.get(&region_id) {
            if index != expect_index {
                REGION_HASH_COUNTER_VEC.with_label_values(&["verify", "miss"]).inc();
                warn!("[region {}] computed hash belongs to index {}, but we want {}, skip.",
                      region_id,
                      index,
                      expect_index);
                return;
            }
            let actual_hash = digest.bytes();
            if actual_hash != expect_hash.as_slice() {
                REGION_HASH_COUNTER_VEC.with_label_values(&["verify", "failed"]).inc();
                panic!("[region {}] hash not correct, want {}, got {}!!!",
                       region_id,
                       escape(&expect_hash),
                       escape(&actual_hash));
            }
        }
        REGION_HASH_COUNTER_VEC.with_label_values(&["verify", "matched"]).inc();
        self.checksum.remove(&region_id);
    }

    fn check_split(&self,
                   region_id: u64,
                   epoch: RegionEpoch,
                   start_key: Vec<u8>,
                   end_key: Vec<u8>) {
        debug!("[region {}] executing split check task {} {}",
               region_id,
               escape(&start_key),
               escape(&end_key));
        CHECK_SPILT_COUNTER_VEC.with_label_values(&["all"]).inc();

        let mut size = 0;
        let mut split_key = vec![];
        let timer = CHECK_SPILT_HISTOGRAM.start_timer();

        let res = self.db.scan(&start_key,
                               &end_key,
                               false,
                               &mut |k, v| {
            size += k.len() as u64;
            size += v.len() as u64;
            if split_key.is_empty() && size > self.split_size {
                split_key = k.to_vec();
            }
            Ok(size < self.region_max_size)
        });
        if let Err(e) = res {
            error!("failed to scan split key of region {}: {:?}", region_id, e);
            return;
        }

        timer.observe_duration();

        if size < self.region_max_size {
            debug!("[region {}] no need to send for {} < {}",
                   region_id,
                   size,
                   self.region_max_size);

            CHECK_SPILT_COUNTER_VEC.with_label_values(&["ignore"]).inc();
            return;
        }
        let res = self.ch.try_send(new_split_check_result(region_id, epoch, split_key));
        if let Err(e) = res {
            warn!("[region {}] failed to send check result, err {:?}",
                  region_id,
                  e);
        }

        CHECK_SPILT_COUNTER_VEC.with_label_values(&["success"]).inc();
    }
}

impl<C: MsgSender> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        match task {
            Task::SplitCheck { region_id, epoch, start_key, end_key } => {
                self.check_split(region_id, epoch, start_key, end_key)
            }
            Task::ComputeHash { region, index, snap } => self.compute_hash(region, index, snap),
            Task::VerifyHash { region_id, index, hash } => self.verify_hash(region_id, index, hash),
        }
    }
}

fn new_split_check_result(region_id: u64, epoch: RegionEpoch, split_key: Vec<u8>) -> Msg {
    Msg::SplitCheckResult {
        region_id: region_id,
        epoch: epoch,
        split_key: split_key,
    }
}

#[cfg(test)]
mod test {
    use rocksdb::Writable;
    use tempdir::TempDir;
    use storage::{CF_DEFAULT, CF_RAFT};
    use sha1::Sha1;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use kvproto::metapb::*;
    use util::rocksdb::new_engine;
    use util::worker::Runnable;
    use raftstore::store::engine::Snapshot;
    use raftstore::store::{keys, Msg};
    use super::*;

    #[test]
    fn test_consistency_check() {
        let path = TempDir::new("tikv-store-test").unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, CF_RAFT]).unwrap();
        let db = Arc::new(db);

        let mut region = Region::new();
        region.mut_peers().push(Peer::new());

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(db.clone(), tx, 10, 10);
        let mut sha1 = Sha1::new();
        let kvs = vec![
            (b"k1", b"v1"),
            (b"k2", b"v2"),
        ];
        for (k, v) in kvs {
            let key = keys::data_key(k);
            db.put(&key, v).unwrap();
            // hash should contain all kvs
            super::update(&mut sha1, &key);
            super::update(&mut sha1, v);
        }

        // hash should also contains region state key.
        super::update(&mut sha1, &keys::region_state_key(region.get_id()));
        super::update(&mut sha1, b"");
        let digest = sha1.digest();
        runner.run(Task::ComputeHash {
            index: 10,
            region: region.clone(),
            snap: Snapshot::new(db.clone()),
        });

        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        match res {
            Msg::ComputeHashResult { region_id, index, hash } => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(index, 10);
                assert_eq!(hash, digest.bytes().to_vec());
            }
            e => panic!("unexpected {:?}", e),
        }

        {
            let checksum = runner.checksum.get(&region.get_id()).unwrap();
            assert_eq!(checksum.0, 10);
            assert_eq!(checksum.1.bytes(), digest.bytes());
        }

        runner.run(Task::verify_hash(region.get_id(), 5, vec![1, 2]));
        // invalid index should be skipped.
        assert!(runner.checksum.contains_key(&region.get_id()));

        runner.run(Task::verify_hash(region.get_id(), 10, digest.bytes().to_vec()));
        // successful check should remove related entry.
        assert!(runner.checksum.is_empty());

        runner.run(Task::compute_hash(region.clone(), 15, Snapshot::new(db.clone())));
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let res = recover_safe!(|| runner.run(Task::verify_hash(region.get_id(), 15, vec![])));
        assert!(res.is_err(), "invalid hash should lead to panic");
    }
}
