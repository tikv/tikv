// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::Bound::{Excluded, Unbounded},
    fmt,
    sync::{Arc, Mutex},
};

use fail::fail_point;
use keys::{data_end_key, data_key, enc_start_key};
use kvproto::kvrpcpb::{KeyRange, LeaderInfo};
use tikv_util::worker::Runnable;

use crate::store::{fsm::store::StoreMeta, util::RegionReadProgressRegistry};

pub struct Runner {
    store_meta: Arc<Mutex<StoreMeta>>,
    region_read_progress: RegionReadProgressRegistry,
}

pub enum Task {
    // Check if the provided `LeaderInfo`s are same as ours local `LeaderInfo`
    CheckLeader {
        leaders: Vec<LeaderInfo>,
        cb: Box<dyn FnOnce(Vec<u64>) + Send>,
    },
    // Get the minimal `safe_ts` from regions overlap with the key range [`start_key`, `end_key`)
    GetStoreTs {
        key_range: KeyRange,
        cb: Box<dyn FnOnce(u64) + Send>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CheckLeaderTask");
        match self {
            Task::CheckLeader { ref leaders, .. } => de
                .field("name", &"check_leader")
                .field("leader_num", &leaders.len())
                .finish(),
            Task::GetStoreTs { ref key_range, .. } => de
                .field("name", &"fet_store_ts")
                .field("key_range", &key_range)
                .finish(),
        }
    }
}

impl Runner {
    pub fn new(store_meta: Arc<Mutex<StoreMeta>>) -> Runner {
        let region_read_progress = store_meta.lock().unwrap().region_read_progress.clone();
        Runner {
            region_read_progress,
            store_meta,
        }
    }

    // Get the minimal `safe_ts` from regions overlap with the key range [`start_key`, `end_key`)
    fn get_range_safe_ts(&self, key_range: KeyRange) -> u64 {
        if key_range.get_start_key().is_empty() && key_range.get_end_key().is_empty() {
            // Fast path to get the min `safe_ts` of all regions in this store
            self.region_read_progress.with(|registry| {
                registry
                .iter()
                .map(|(_, rrp)| rrp.safe_ts())
                .filter(|ts| *ts != 0) // ts == 0 means the peer is uninitialized
                .min()
                .unwrap_or(0)
            })
        } else {
            let (start_key, end_key) = (
                data_key(key_range.get_start_key()),
                data_end_key(key_range.get_end_key()),
            );
            // `store_safe_ts` won't be accessed frequently (like per-request or per-transaction),
            // also this branch won't entry because the request key range is empty currently (in v5.1)
            // keep this branch for robustness and future use, so it is okay getting `store_safe_ts`
            // from `store_meta` (behide a mutex)
            let meta = self.store_meta.lock().unwrap();
            meta.region_read_progress.with(|registry| {
                meta.region_ranges
                // get overlapped regions
                .range((Excluded(start_key), Unbounded))
                .take_while(|(_, id)| end_key > enc_start_key(&meta.regions[id]))
                // get the min `safe_ts`
                .map(|(_, id)| {
                    registry.get(id).unwrap().safe_ts()
                })
                .filter(|ts| *ts != 0) // ts == 0 means the peer is uninitialized
                .min()
                .unwrap_or(0)
            })
        }
    }
}

impl Runnable for Runner {
    type Task = Task;
    fn run(&mut self, task: Task) {
        match task {
            Task::CheckLeader { leaders, cb } => {
                fail_point!(
                    "before_check_leader_store_2",
                    self.store_meta.lock().unwrap().store_id == Some(2),
                    |_| {}
                );
                fail_point!(
                    "before_check_leader_store_3",
                    self.store_meta.lock().unwrap().store_id == Some(3),
                    |_| {}
                );
                let regions = self.region_read_progress.handle_check_leaders(leaders);
                cb(regions);
            }
            Task::GetStoreTs { key_range, cb } => {
                let ts = self.get_range_safe_ts(key_range);
                cb(ts);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use keys::enc_end_key;
    use kvproto::metapb::Region;

    use super::*;
    use crate::store::util::RegionReadProgress;

    #[test]
    fn test_get_range_min_safe_ts() {
        fn add_region(meta: &Arc<Mutex<StoreMeta>>, id: u64, kr: KeyRange, safe_ts: u64) {
            let mut meta = meta.lock().unwrap();
            let mut region = Region::default();
            region.set_id(id);
            region.set_start_key(kr.get_start_key().to_vec());
            region.set_end_key(kr.get_end_key().to_vec());
            region.set_peers(vec![kvproto::metapb::Peer::default()].into());
            let rrp = RegionReadProgress::new(&region, 1, 1, "".to_owned());
            rrp.update_safe_ts(1, safe_ts);
            assert_eq!(rrp.safe_ts(), safe_ts);
            meta.region_ranges.insert(enc_end_key(&region), id);
            meta.regions.insert(id, region);
            meta.region_read_progress.insert(id, Arc::new(rrp));
        }

        fn key_range(start_key: &[u8], end_key: &[u8]) -> KeyRange {
            let mut kr = KeyRange::default();
            kr.set_start_key(start_key.to_vec());
            kr.set_end_key(end_key.to_vec());
            kr
        }

        let meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let runner = Runner::new(meta.clone());
        assert_eq!(0, runner.get_range_safe_ts(key_range(b"", b"")));
        add_region(&meta, 1, key_range(b"", b"k1"), 100);
        assert_eq!(100, runner.get_range_safe_ts(key_range(b"", b"")));
        assert_eq!(0, runner.get_range_safe_ts(key_range(b"k1", b"")));

        add_region(&meta, 2, key_range(b"k5", b"k6"), 80);
        add_region(&meta, 3, key_range(b"k6", b"k8"), 70);
        // The zero ts will be ignore
        add_region(&meta, 5, key_range(b"k8", b"k9"), 0);
        add_region(&meta, 4, key_range(b"k9", b""), 90);
        assert_eq!(70, runner.get_range_safe_ts(key_range(b"", b"")));
        assert_eq!(80, runner.get_range_safe_ts(key_range(b"", b"k6")));
        assert_eq!(90, runner.get_range_safe_ts(key_range(b"k99", b"")));
        assert_eq!(70, runner.get_range_safe_ts(key_range(b"k5", b"k99")));
        assert_eq!(70, runner.get_range_safe_ts(key_range(b"k", b"k9")));
        assert_eq!(80, runner.get_range_safe_ts(key_range(b"k4", b"k6")));
        assert_eq!(100, runner.get_range_safe_ts(key_range(b"", b"k1")));
        assert_eq!(90, runner.get_range_safe_ts(key_range(b"k9", b"")));
        assert_eq!(80, runner.get_range_safe_ts(key_range(b"k5", b"k6")));
        assert_eq!(0, runner.get_range_safe_ts(key_range(b"k1", b"k4")));
        assert_eq!(0, runner.get_range_safe_ts(key_range(b"k2", b"k3")));
    }
}
