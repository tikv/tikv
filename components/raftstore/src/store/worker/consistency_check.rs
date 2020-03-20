// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use byteorder::{BigEndian, WriteBytesExt};
use kvproto::metapb::Region;

use crate::store::{CasualMessage, CasualRouter};
use engine_rocks::RocksEngine;
use engine_traits::CF_RAFT;
use engine_traits::{Iterable, KvEngine, Peekable, Snapshot};
use tikv_util::worker::Runnable;

use super::metrics::*;
use crate::store::metrics::*;

/// Consistency checking task.
pub enum Task<E>
where
    E: KvEngine,
{
    ComputeHash {
        index: u64,
        region: Region,
        snap: E::Snapshot,
    },
}

impl<E> Task<E>
where
    E: KvEngine,
{
    pub fn compute_hash(region: Region, index: u64, snap: E::Snapshot) -> Task<E> {
        Task::ComputeHash {
            region,
            index,
            snap,
        }
    }
}

impl<E> Display for Task<E>
where
    E: KvEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::ComputeHash {
                ref region, index, ..
            } => write!(f, "Compute Hash Task for {:?} at {}", region, index),
        }
    }
}

pub struct Runner<C: CasualRouter<RocksEngine>> {
    router: C,
}

impl<C: CasualRouter<RocksEngine>> Runner<C> {
    pub fn new(router: C) -> Runner<C> {
        Runner { router }
    }

    /// Computes the hash of the Region.
    fn compute_hash<E>(&mut self, region: Region, index: u64, snap: E::Snapshot)
    where
        E: KvEngine,
    {
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
        let mut digest = crc32fast::Hasher::new();
        let mut cf_names = snap.cf_names();
        cf_names.sort();

        // Computes the hash from all the keys and values in the range of the Region.
        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        for cf in cf_names {
            let res = snap.scan_cf(cf, &start_key, &end_key, false, |k, v| {
                digest.update(k);
                digest.update(v);
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
        digest.update(&region_state_key);
        match snap.get_value_cf(CF_RAFT, &region_state_key) {
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
            Ok(Some(v)) => digest.update(&v),
            Ok(None) => {}
        }
        let sum = digest.finalize();
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

impl<C, E> Runnable<Task<E>> for Runner<C>
where
    C: CasualRouter<RocksEngine>,
    E: KvEngine,
{
    fn run(&mut self, task: Task<E>) {
        match task {
            Task::ComputeHash {
                region,
                index,
                snap,
            } => self.compute_hash::<E>(region, index, snap),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, WriteBytesExt};
    use engine::rocks::util::new_engine;
    use engine::rocks::Writable;
    use engine_rocks::{RocksEngine, RocksSnapshot};
    use engine_traits::{CF_DEFAULT, CF_RAFT};
    use kvproto::metapb::*;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tempfile::Builder;
    use tikv_util::worker::Runnable;

    #[test]
    fn test_consistency_check() {
        let path = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let db = new_engine(
            path.path().to_str().unwrap(),
            None,
            &[CF_DEFAULT, CF_RAFT],
            None,
        )
        .unwrap();
        let db = Arc::new(db);

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());

        let (tx, rx) = mpsc::sync_channel(100);
        let mut runner = Runner::new(tx);
        let mut digest = crc32fast::Hasher::new();
        let kvs = vec![(b"k1", b"v1"), (b"k2", b"v2")];
        for (k, v) in kvs {
            let key = keys::data_key(k);
            db.put(&key, v).unwrap();
            // hash should contain all kvs
            digest.update(&key);
            digest.update(v);
        }

        // hash should also contains region state key.
        digest.update(&keys::region_state_key(region.get_id()));
        let sum = digest.finalize();
        runner.run(Task::<RocksEngine>::ComputeHash {
            index: 10,
            region: region.clone(),
            snap: RocksSnapshot::new(Arc::clone(&db)),
        });
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
