// Copyright 2018 PingCAP, Inc.
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
#![allow(dead_code)] // TODO: remove this.
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{error, fmt, io};

use kvproto::raft_serverpb::{RegionLocalState, SnapshotMeta};
use rocksdb::DB;

use storage::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use util::codec;

pub const SNAPSHOT_VERSION: u64 = 2;
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const SNAP_GEN_PREFIX: &str = "gen";
pub const SNAP_REV_PREFIX: &str = "rev";
pub const META_FILE_NAME: &str = "meta";
pub const SST_FILE_SUFFIX: &str = ".sst";
pub const TMP_FILE_SUFFIX: &str = ".tmp";
pub const CLONE_FILE_SUFFIX: &str = ".clone";

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Abort {
            description("abort")
            display("abort")
        }
        Stale {
            description("stale snapshot")
            display("stale snapshot")
        }
        Unavaliable {
            description("snapshot unavaliable")
            display("snapshot unavaliable")
        }
        Conflict {
            description("snapshot conflict")
            display("snapshot conflict")
        }
        NoSpace(size: u64, limit: u64) {
            description("disk space exceed")
            display("NoSpace, size: {}, limit: {}", size, limit)
        }

        // Following is for From other errors.
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
            display("io {}", err)
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
            display("codec {}", err)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,
}

impl SnapKey {
    pub fn new(region_id: u64, term: u64, idx: u64) -> SnapKey {
        SnapKey {
            region_id,
            term,
            idx,
        }
    }
}

impl fmt::Display for SnapKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}_{}_{}", self.region_id, self.term, self.idx)
    }
}

/// Get the total size of all cf files of the snapshot.
pub fn get_size_from_snapshot_meta(meta: &SnapshotMeta) -> u64 {
    meta.get_cf_files().iter().fold(0, |mut acc, cf| {
        acc += cf.get_size() as u64;
        acc
    })
}

/// `SnapStaleChecker` is used for checking a snapshot is stale or not. Normally the `PeerStorage`
/// respectively holds a reference of this, and update the fields which are changed by raftstore.
#[derive(Debug)]
pub struct SnapStaleChecker {
    pub compacted_term: AtomicU64,
    pub compacted_idx: AtomicU64,
    pub apply_canceled: AtomicBool,
}

/// `ApplyOptions` is used for applying snapshots.
pub struct ApplyOptions {
    pub db: Arc<DB>,
    pub region_state: RegionLocalState,
    pub write_batch_size: usize,
}

/// Return true if the snapshot is stale when generating or sending.
pub fn stale_for_generate(key: SnapKey, snap_stale_checker: &SnapStaleChecker) -> bool {
    let compacted_term = snap_stale_checker.compacted_term.load(Ordering::SeqCst);
    let compacted_idx = snap_stale_checker.compacted_idx.load(Ordering::SeqCst);
    key.term < compacted_term || key.idx < compacted_idx
}

/// Return true if the snapshot is stale when receiving or applying.
pub fn stale_for_apply(key: SnapKey, snap_stale_checker: &SnapStaleChecker) -> bool {
    let compacted_term = snap_stale_checker.compacted_term.load(Ordering::SeqCst);
    let compacted_idx = snap_stale_checker.compacted_idx.load(Ordering::SeqCst);
    let apply_canceled = snap_stale_checker.apply_canceled.load(Ordering::SeqCst);
    key.term < compacted_term || key.idx < compacted_idx || apply_canceled
}

/// Generate a directory path. Snapshot files after building will be stored in the path.
pub fn gen_snap_dir(dir: &str, for_send: bool, key: SnapKey) -> PathBuf {
    let dir_path = PathBuf::from(dir);
    let snap_dir_name = if for_send {
        format!("{}_{}", SNAP_GEN_PREFIX, key)
    } else {
        format!("{}_{}", SNAP_REV_PREFIX, key)
    };
    dir_path.join(&snap_dir_name)
}

/// Generate a directory path. Snapshot files in building will be stored in the path.
pub fn gen_tmp_snap_dir(dir: &str, for_send: bool, key: SnapKey) -> PathBuf {
    let dir_path = PathBuf::from(dir);
    let snap_dir_name = if for_send {
        format!("{}_{}{}", SNAP_GEN_PREFIX, key, TMP_FILE_SUFFIX)
    } else {
        format!("{}_{}{}", SNAP_REV_PREFIX, key, TMP_FILE_SUFFIX)
    };
    dir_path.join(&snap_dir_name)
}

/// Generate a snapshot meta file path from given entries.
pub fn gen_meta_file_path(dir: &str, for_send: bool, key: SnapKey) -> PathBuf {
    let snap_dir = gen_snap_dir(dir, for_send, key);
    snap_dir.join(META_FILE_NAME)
}

/// Generate a snapshot cf file path from given entries.
pub fn gen_cf_file_path(dir: &str, for_send: bool, key: SnapKey, cf: &str) -> PathBuf {
    let snap_dir = gen_snap_dir(dir, for_send, key);
    let file_name = format!("{}{}", cf, SST_FILE_SUFFIX);
    snap_dir.join(&file_name)
}

pub fn plain_file_used(cf: &str) -> bool {
    cf == CF_LOCK
}

/// Return an error indicates snapshot size corrupt.
pub fn snapshot_size_corrupt(expected: u64, got: u64) -> Error {
    Error::from(io::Error::new(
        io::ErrorKind::Other,
        format!(
            "snapshot size corrupted, expected: {}, got: {}",
            expected, got
        ),
    ))
}

/// Return an error indicates snapshot checksum corrupt.
pub fn snapshot_checksum_corrupt(expected: u32, got: u32) -> Error {
    Error::from(io::Error::new(
        io::ErrorKind::Other,
        format!(
            "snapshot checksum corrupted, expected: {}, got: {}",
            expected, got
        ),
    ))
}

#[cfg(test)]
pub mod tests {
    use kvproto::metapb::{Peer, Region};
    use tempdir::TempDir;
    use test::Bencher;

    use super::*;
    use storage::ALL_CFS;
    use util::rocksdb;

    pub fn get_test_stale_checker() -> SnapStaleChecker {
        SnapStaleChecker {
            compacted_term: AtomicU64::new(0),
            compacted_idx: AtomicU64::new(0),
            apply_canceled: AtomicBool::new(false),
        }
    }

    pub fn get_test_region(region_id: u64, store_id: u64, peer_id: u64) -> Region {
        let mut peer = Peer::new();
        peer.set_store_id(store_id);
        peer.set_id(peer_id);
        let mut region = Region::new();
        region.set_id(region_id);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"z".to_vec());
        region.mut_region_epoch().set_version(1);
        region.mut_region_epoch().set_conf_ver(1);
        region.mut_peers().push(peer.clone());
        region
    }

    pub fn get_test_empty_db(path: &TempDir) -> Arc<DB> {
        let p = path.path().to_str().unwrap();
        let db = rocksdb::new_engine(p, ALL_CFS, None).unwrap();
        Arc::new(db)
    }

    #[test]
    fn test_gen_meta_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, expected) in &[
            ("abc", true, key, "abc/gen_1_2_3/meta"),
            ("abc/", false, key, "abc/rev_1_2_3/meta"),
            ("ab/c", false, key, "ab/c/rev_1_2_3/meta"),
            ("", false, key, "rev_1_2_3/meta"),
        ] {
            let path = gen_meta_file_path(dir, for_send, key);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[test]
    fn test_gen_cf_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, cf, expected) in &[
            ("abc", true, key, CF_LOCK, "abc/gen_1_2_3/lock.sst"),
            ("abc/", false, key, CF_WRITE, "abc/rev_1_2_3/write.sst"),
            ("ab/c", false, key, CF_DEFAULT, "ab/c/rev_1_2_3/default.sst"),
            ("", false, key, CF_LOCK, "rev_1_2_3/lock.sst"),
        ] {
            let path = gen_cf_file_path(dir, for_send, key, cf);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }

    #[bench]
    fn bench_stale_for_generate(b: &mut Bencher) {
        let notifier = SnapStaleChecker {
            compacted_term: AtomicU64::new(0),
            compacted_idx: AtomicU64::new(0),
            apply_canceled: AtomicBool::new(false),
        };
        let key = SnapKey::new(10, 10, 10);
        b.iter(|| stale_for_generate(key, &notifier));
    }

    #[bench]
    fn bench_stale_for_apply(b: &mut Bencher) {
        let notifier = Arc::new(SnapStaleChecker {
            compacted_term: AtomicU64::new(0),
            compacted_idx: AtomicU64::new(0),
            apply_canceled: AtomicBool::new(false),
        });
        let key = SnapKey::new(10, 10, 10);
        b.iter(|| stale_for_apply(key, notifier.as_ref()));
    }
}
