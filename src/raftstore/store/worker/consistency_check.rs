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


use std::collections::HashMap;
use std::fmt::{self, Formatter, Display};

use crc::crc32::{self, Digest, Hasher32};
use byteorder::{BigEndian, WriteBytesExt};

use kvproto::metapb::Region;
use raftstore::store::{keys, Msg};
use raftstore::store::engine::{Snapshot, Iterable, Peekable};
use storage::CF_RAFT;
use util::escape;
use util::worker::Runnable;

use super::metrics::*;
use super::MsgSender;

/// Consistency checking task.
pub enum Task {
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
    ch: C,
    // region_id -> (index, checksum)
    checksum: HashMap<u64, (u64, u32)>,
}

impl<C: MsgSender> Runner<C> {
    pub fn new(ch: C) -> Runner<C> {
        Runner {
            ch: ch,
            checksum: HashMap::new(),
        }
    }

    fn compute_hash(&mut self, region: Region, index: u64, snap: Snapshot) {
        let region_id = region.get_id();
        info!("[region {}] computing hash at {}", region_id, index);
        REGION_HASH_COUNTER_VEC.with_label_values(&["compute", "all"]).inc();

        let timer = REGION_HASH_HISTOGRAM.start_timer();
        let mut digest = Digest::new(crc32::IEEE);
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
                                       digest.write(k);
                                       digest.write(v);
                                       Ok(true)
                                   });
            if let Err(e) = res {
                REGION_HASH_COUNTER_VEC.with_label_values(&["compute", "failed"]).inc();
                error!("[region {}] failed to calculate hash: {:?}", region_id, e);
                return;
            }
        }
        let region_state_key = keys::region_state_key(region_id);
        digest.write(&region_state_key);
        match snap.get_value_cf(CF_RAFT, &region_state_key) {
            Err(e) => {
                REGION_HASH_COUNTER_VEC.with_label_values(&["compute", "failed"]).inc();
                error!("[region {}] failed to get region state: {:?}", region_id, e);
                return;
            }
            Ok(Some(v)) => digest.write(&v),
            Ok(None) => digest.write(b""),
        }
        let sum = digest.sum32();
        timer.observe_duration();

        let mut checksum = Vec::with_capacity(4);
        checksum.write_u32::<BigEndian>(sum).unwrap();
        let msg = Msg::ComputeHashResult {
            region_id: region_id,
            index: index,
            hash: checksum,
        };
        self.checksum.insert(region_id, (index, sum));
        if let Err(e) = self.ch.try_send(msg) {
            warn!("[region {}] failed to send hash compute result, err {:?}",
                  region_id,
                  e);
        }
    }

    fn verify_hash(&mut self, region_id: u64, expect_index: u64, expect_hash: Vec<u8>) {
        if let Some(&(index, checksum)) = self.checksum.get(&region_id) {
            if index != expect_index {
                REGION_HASH_COUNTER_VEC.with_label_values(&["verify", "miss"]).inc();
                warn!("[region {}] computed hash belongs to index {}, but we want {}, skip.",
                      region_id,
                      index,
                      expect_index);
                return;
            }
            let mut actual_hash = [0; 4];
            actual_hash.as_mut().write_u32::<BigEndian>(checksum).unwrap();
            if expect_hash != actual_hash {
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
}

impl<C: MsgSender> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        match task {
            Task::ComputeHash { region, index, snap } => self.compute_hash(region, index, snap),
            Task::VerifyHash { region_id, index, hash } => self.verify_hash(region_id, index, hash),
        }
    }
}

#[cfg(test)]
mod test {
    use rocksdb::Writable;
    use tempdir::TempDir;
    use storage::{CF_DEFAULT, CF_RAFT};
    use crc::crc32::{self, Digest, Hasher32};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use byteorder::{BigEndian, WriteBytesExt};
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
        let mut runner = Runner::new(tx);
        let mut digest = Digest::new(crc32::IEEE);
        let kvs = vec![
            (b"k1", b"v1"),
            (b"k2", b"v2"),
        ];
        for (k, v) in kvs {
            let key = keys::data_key(k);
            db.put(&key, v).unwrap();
            // hash should contain all kvs
            digest.write(&key);
            digest.write(v);
        }

        // hash should also contains region state key.
        digest.write(&keys::region_state_key(region.get_id()));
        digest.write(b"");
        let sum = digest.sum32();
        runner.run(Task::ComputeHash {
            index: 10,
            region: region.clone(),
            snap: Snapshot::new(db.clone()),
        });
        let mut checksum_bytes = vec![];
        checksum_bytes.write_u32::<BigEndian>(sum).unwrap();

        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        match res {
            Msg::ComputeHashResult { region_id, index, hash } => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(index, 10);
                assert_eq!(hash, checksum_bytes);
            }
            e => panic!("unexpected {:?}", e),
        }

        {
            let checksum = runner.checksum.get(&region.get_id()).unwrap();
            assert_eq!(checksum.0, 10);
            assert_eq!(checksum.1, sum);
        }

        runner.run(Task::verify_hash(region.get_id(), 5, vec![1, 2]));
        // invalid index should be skipped.
        assert!(runner.checksum.contains_key(&region.get_id()));

        runner.run(Task::verify_hash(region.get_id(), 10, checksum_bytes));
        // successful check should remove related entry.
        assert!(runner.checksum.is_empty());

        runner.run(Task::compute_hash(region.clone(), 15, Snapshot::new(db.clone())));
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let res = recover_safe!(|| runner.run(Task::verify_hash(region.get_id(), 15, vec![])));
        assert!(res.is_err(), "invalid hash should lead to panic");
    }
}
