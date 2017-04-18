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

use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;
use std::io;

use tempdir::TempDir;
use test::Bencher;
use rocksdb::{DB, Writable};
use tikv::util::rocksdb::new_engine;
use tikv::storage::ALL_CFS;
use tikv::raftstore::store::engine::{Snapshot as DbSnapshot};
use tikv::raftstore::store::peer_storage::JOB_STATUS_RUNNING;
use tikv::raftstore::store::snap::{SnapKey, SnapshotStatistics, ApplyOptions, Snapshot};
use kvproto::metapb::{Region, Peer};
use kvproto::raft_serverpb::RaftSnapshotData;
use tikv::raftstore::store::snap::v1::{Snap as SnapV1};
use tikv::raftstore::store::snap::v2::{Snap as SnapV2};

const MB: usize = 1024 * 1024;

fn init_db(path: &str, target_size: usize) -> (TempDir, Arc<DB>) {
    let path = TempDir::new(path).unwrap();
    let db = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());
    let v = b"this is the content of value";
    let mut current_size = 0;
    for i in 0.. {
        let k = format!("key_a_{}", i);
        db.put(k.as_bytes(), v);
        current_size += k.len() + v.len();
        if current_size >= target_size {
            break;
        }
    }
    (path, db)
}

fn gen_region(store_id: u64, peer_id: u64, region_id: u64, start_key: &[u8], end_key: &[u8]) -> Region {
    let mut peer = Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    let mut region = Region::new();
    region.set_id(region_id);
    region.set_start_key(start_key.to_vec());
    region.set_end_key(end_key.to_vec());
    region.mut_region_epoch().set_version(1);
    region.mut_region_epoch().set_conf_ver(1);
    region.mut_peers().push(peer.clone());
    region
}

#[bench]
fn bench_gen_snap_v1(b: &mut Bencher) {
    let (path, db) = init_db("bench_gen_snapshot", 10 * MB);
    let snapshot = DbSnapshot::new(db.clone());

    b.iter(|| {
        let key = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let mut snap = SnapV1::new_for_writing(snap_dir.path(), size_track.clone(), true, &key).unwrap();
        let region = gen_region(1, 1, 1, b"key_a", b"key_b");

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        snap.build(&snapshot, &region, &mut snap_data, &mut stat).unwrap();
    });
}

#[bench]
fn bench_gen_snap_v2(b: &mut Bencher) {
    let (path, db) = init_db("bench_gen_snapshot", 10 * MB);
    let snapshot = DbSnapshot::new(db.clone());

    b.iter(|| {
        let key = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let mut snap = SnapV2::new_for_building(snap_dir.path(), &key, &snapshot, size_track.clone()).unwrap();
        let region = gen_region(1, 1, 1, b"key_a", b"key_b");

        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        snap.build(&snapshot, &region, &mut snap_data, &mut stat).unwrap();
    });
}

#[bench]
fn bench_validate_snap_v1(b: &mut Bencher) {
    let (path, db) = init_db("bench_validate_snapshot", 10 * MB);
    let snapshot = DbSnapshot::new(db.clone());
    let key = SnapKey::new(1, 1, 1);
    let size_track = Arc::new(RwLock::new(0));
    let snap_dir = TempDir::new("snap_dir").unwrap();
    let mut snap = SnapV1::new_for_writing(snap_dir.path(), size_track.clone(), true, &key).unwrap();
    let region = gen_region(1, 1, 1, b"key_a", b"key_b");

    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(region.clone());
    let mut stat = SnapshotStatistics::new();
        snap.build(&snapshot, &region, &mut snap_data, &mut stat).unwrap();

    b.iter(|| {
        snap.get_validation_reader().and_then(|r| r.validate()).unwrap();
    });
}

#[bench]
fn bench_validate_snap_v2(b: &mut Bencher) {
    let (path, db) = init_db("bench_validate_snapshot", 10 * MB);
    let snapshot = DbSnapshot::new(db.clone());
    let key = SnapKey::new(1, 1, 1);
    let size_track = Arc::new(RwLock::new(0));
    let snap_dir = TempDir::new("snap_dir").unwrap();
    let mut snap = SnapV2::new_for_building(snap_dir.path(), &key, &snapshot, size_track.clone()).unwrap();
    let region = gen_region(1, 1, 1, b"key_a", b"key_b");

    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(region.clone());
    let mut stat = SnapshotStatistics::new();
    snap.build(&snapshot, &region, &mut snap_data, &mut stat).unwrap();

    b.iter(|| {
            snap.validate().unwrap();
    });
}
