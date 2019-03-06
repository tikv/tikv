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

use std::fmt::{self, Display, Formatter};

use byteorder::{BigEndian, WriteBytesExt};
use crc::crc32::{self, Digest, Hasher32};

use crate::raftstore::store::engine::{Iterable, Peekable, Snapshot};
use crate::raftstore::store::{keys, CasualMessage, CasualRouter};
use crate::util::worker::Runnable;
use kvproto::metapb::Region;

use super::metrics::*;
use crate::raftstore::store::metrics::*;

/// Consistency checking task.
pub enum Task {
    ComputeHash {
        index: u64,
        region: Region,
        raft_snap: Snapshot,
        kv_snap: Snapshot,
    },
}

impl Task {
    pub fn compute_hash(
        region: Region,
        index: u64,
        raft_snap: Snapshot,
        kv_snap: Snapshot,
    ) -> Task {
        Task::ComputeHash {
            region,
            index,
            raft_snap,
            kv_snap,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::ComputeHash {
                ref region, index, ..
            } => write!(f, "Compute Hash Task for {:?} at {}", region, index),
        }
    }
}

pub struct Runner<C: CasualRouter> {
    router: C,
}

impl<C: CasualRouter> Runner<C> {
    pub fn new(router: C) -> Runner<C> {
        Runner { router }
    }

    /// Computes the hash of the Region.
    fn compute_hash(&mut self, region: Region, index: u64, raft_snap: Snapshot, kv_snap: Snapshot) {
        let region_id = region.get_id();
        info!(
            "computing hash";
            "region_id" => region_id,
            "index" => index,
        );
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["compute", "all"])
            .inc();

        let timer = REGION_HASH_HISTOGRAM.start_coarse_timer();
        let mut digest = Digest::new(crc32::IEEE);
        let mut cf_names = kv_snap.cf_names();
        cf_names.sort();

        // Computes the hash from all the keys and values in the range of the Region.
        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        for cf in cf_names {
            let res = kv_snap.scan_cf(cf, &start_key, &end_key, false, |k, v| {
                digest.write(k);
                digest.write(v);
                Ok(true)
            });
            if let Err(e) = res {
                REGION_HASH_COUNTER_VEC
                    .with_label_values(&["compute", "failed"])
                    .inc();
                error!(
                    "failed to calculate hash";
                    "region_id" => region_id,
                    "err" => %e,
                );
                return;
            }
        }

        // Computes the hash from the Region state too.
        let region_state_key = keys::region_state_key(region_id);
        digest.write(&region_state_key);
        match raft_snap.get_value(&region_state_key) {
            Err(e) => {
                REGION_HASH_COUNTER_VEC
                    .with_label_values(&["compute", "failed"])
                    .inc();
                error!(
                    "failed to get region state";
                    "region_id" => region_id,
                    "err" => %e,
                );
                return;
            }
            Ok(Some(v)) => digest.write(&v),
            Ok(None) => digest.write(b""),
        }
        let sum = digest.sum32();
        timer.observe_duration();

        let mut checksum = Vec::with_capacity(4);
        checksum.write_u32::<BigEndian>(sum).unwrap();
        let msg = CasualMessage::ComputeHashResult {
            index,
            hash: checksum,
        };
        if let Err(e) = self.router.send(region_id, msg) {
            warn!(
                "failed to send hash compute result";
                "region_id" => region_id,
                "err" => %e,
            );
        }
    }
}

impl<C: CasualRouter> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        match task {
            Task::ComputeHash {
                region,
                index,
                raft_snap,
                kv_snap,
            } => {
                self.compute_hash(region, index, raft_snap, kv_snap);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raftstore::store::keys;
    use crate::storage::CF_DEFAULT;
    use crate::util::rocksdb_util::new_engine;
    use byteorder::{BigEndian, WriteBytesExt};
    use crc::crc32::{self, Digest, Hasher32};
    use kvproto::metapb::*;
    use kvproto::raft_serverpb::RegionLocalState;
    use protobuf::Message;
    use rocksdb::Writable;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tempdir::TempDir;

    #[test]
    fn test_consistency_check() {
        let path = TempDir::new("tikv-store-test").unwrap();

        let kv_db = new_engine(
            path.path().join("kv").as_path().to_str().unwrap(),
            None,
            &[CF_DEFAULT],
            None,
        )
        .unwrap();

        let raft_db = new_engine(
            path.path().join("raft").as_path().to_str().unwrap(),
            None,
            &[CF_DEFAULT],
            None,
        )
        .unwrap();
        let kv_engine = Arc::new(kv_db);
        let raft_engine = Arc::new(raft_db);

        let mut peer = Peer::new();
        peer.set_id(777);
        let mut region = Region::new();
        region.mut_peers().push(peer);

        let (tx, rx) = mpsc::sync_channel(100);
        let mut runner = Runner::new(tx);
        let mut digest = Digest::new(crc32::IEEE);
        let kvs = vec![(b"k1", b"v1"), (b"k2", b"v2")];
        for (k, v) in kvs {
            let key = keys::data_key(k);
            kv_engine.put(&key, v).unwrap();
            // hash should contain all kvs
            digest.write(&key);
            digest.write(v);
        }

        // hash should also contains region state key.
        let mut region_state = RegionLocalState::new();
        region_state.set_region(region.clone());
        let region_state_bytes = region_state.write_to_bytes().unwrap();
        let region_state_key = keys::region_state_key(region.get_id());
        raft_engine
            .put(&region_state_key, &region_state_bytes)
            .unwrap();
        digest.write(&region_state_key);
        digest.write(&region_state_bytes);
        let sum = digest.sum32();
        runner.run(Task::compute_hash(
            region.clone(),
            10,
            Snapshot::new(raft_engine.clone()),
            Snapshot::new(kv_engine.clone()),
        ));
        let mut checksum_bytes = vec![];
        checksum_bytes.write_u32::<BigEndian>(sum).unwrap();

        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        match res {
            (region_id, CasualMessage::ComputeHashResult { index, hash }) => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(index, 10);
                assert_eq!(hash, checksum_bytes);
            }
            e => panic!("unexpected {:?}", e),
        }
    }
}
