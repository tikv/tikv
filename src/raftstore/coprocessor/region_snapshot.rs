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

use rocksdb::{DB, IteratorMode, Direction, DBVector};
use rocksdb::rocksdb::Snapshot;
use raftstore::store::engine::{Iterable, Peekable};
use raftstore::store::keys::{self, enc_end_key};
use raftstore::store::{util, PeerStorage};
use raftstore::Result;
use kvproto::metapb;


type Kv<'a> = (&'a [u8], &'a [u8]);

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
pub struct RegionSnapshot<'a> {
    snap: Snapshot<'a>,
    region: metapb::Region,
}

impl<'a> RegionSnapshot<'a> {
    pub fn new(ps: &'a PeerStorage) -> RegionSnapshot<'a> {
        RegionSnapshot {
            snap: ps.raw_snapshot(),
            region: ps.get_region().clone(),
        }
    }

    pub fn from_raw(db: &'a DB, region: metapb::Region) -> RegionSnapshot<'a> {
        RegionSnapshot {
            snap: db.snapshot(),
            region: region,
        }
    }

    fn new_iterator(&'a self, start_key: &[u8]) -> Box<Iterator<Item = Kv> + 'a> {
        let scan_start_key = if start_key < self.region.get_start_key() {
            keys::data_key(self.region.get_start_key())
        } else {
            keys::data_key(start_key)
        };
        let scan_end_key = enc_end_key(&self.region);
        box self.snap
            .new_iterator(&scan_start_key)
            .take_while(move |&(k, _)| k < &scan_end_key)
            .map(|(k, v)| (keys::origin_key(k), v))
    }

    // Seek the first key >= given key, if no found, return None.
    pub fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let pair = self.new_iterator(key)
            .next()
            .map(|(k, v)| (k.to_vec(), v.to_vec()));
        Ok(pair)
    }

    fn new_reverse_iterator(&'a self, start_key: &[u8]) -> Box<Iterator<Item = Kv> + 'a> {
        let scan_start_key = if start_key > self.region.get_end_key() &&
                                !self.region.get_end_key().is_empty() {
            enc_end_key(&self.region)
        } else {
            keys::data_key(start_key)
        };
        let mut iter = self.snap.iterator(IteratorMode::From(&scan_start_key, Direction::Reverse));
        if !iter.valid() {
            iter = self.snap.iterator(IteratorMode::End);
        }
        let scan_end_key = keys::data_key(self.region.get_start_key());
        box iter.skip_while(move |&(k, _)| k >= &scan_start_key)
            .take_while(move |&(k, _)| k >= &scan_end_key)
            .map(|(k, v)| (keys::origin_key(k), v))
    }

    // Seek the first key < given key, if no found, return None.
    pub fn reverse_seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let pair = self.new_reverse_iterator(key).next().map(|(k, v)| (k.to_vec(), v.to_vec()));
        Ok(pair)
    }

    pub fn get_region(&self) -> &metapb::Region {
        &self.region
    }

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
        where F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        let it = self.new_iterator(start_key);

        for (key, value) in it {
            if !end_key.is_empty() && key >= end_key {
                break;
            }

            let r = try!(f(key, value));
            if !r {
                break;
            }
        }

        Ok(())
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl<'a> Peekable for RegionSnapshot<'a> {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        try!(util::check_key_in_region(key, &self.region));
        let data_key = keys::data_key(key);
        self.snap.get_value(&data_key)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use rocksdb::{Writable, DB};
    use raftstore::store::engine::*;
    use raftstore::store::keys::*;
    use raftstore::store::PeerStorage;

    use super::*;
    use std::sync::Arc;
    use kvproto::metapb::Region;

    type DataSet = Vec<(Vec<u8>, Vec<u8>)>;

    fn new_temp_engine(path: &TempDir) -> Arc<DB> {
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        Arc::new(engine)
    }

    fn new_peer_storage(engine: Arc<DB>, r: &Region) -> PeerStorage {
        PeerStorage::new(engine, r).unwrap()
    }

    fn new_snapshot(peer_storage: &PeerStorage) -> RegionSnapshot {
        RegionSnapshot::new(peer_storage)
    }

    fn load_default_dataset(engine: Arc<DB>) -> (PeerStorage, DataSet) {
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(b"a2".to_vec());
        r.set_end_key(b"a7".to_vec());

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engine.put(&data_key(k), v).expect("");
        }
        let store = new_peer_storage(engine.clone(), &r);
        (store, base_data)
    }

    #[test]
    fn test_peekable() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = new_temp_engine(&path);
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(b"key0".to_vec());
        r.set_end_key(b"key4".to_vec());
        let store = new_peer_storage(engine.clone(), &r);

        let (key1, value1) = (b"key1", 2u64);
        engine.put_u64(&data_key(key1), value1).expect("");
        let (key2, value2) = (b"key2", 2i64);
        engine.put_i64(&data_key(key2), value2).expect("");
        let key3 = b"key3";
        engine.put_msg(&data_key(key3), &r).expect("");

        let snap = RegionSnapshot::new(&store);
        let v1 = snap.get_u64(key1).expect("");
        assert_eq!(v1, Some(value1));
        let v2 = snap.get_i64(key2).expect("");
        assert_eq!(v2, Some(value2));
        let v3 = snap.get_msg(key3).expect("");
        assert_eq!(v3, Some(r));

        let v0 = snap.get_value(b"key0").expect("");
        assert!(v0.is_none());

        let v4 = snap.get_value(b"key5");
        assert!(v4.is_err());
    }

    #[test]
    fn test_iterate() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = new_temp_engine(&path);
        let (store, base_data) = load_default_dataset(engine.clone());

        let snap = RegionSnapshot::new(&store);
        let mut data = vec![];
        snap.scan(b"",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data, &base_data[1..3]);

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a3".to_vec(), b"v3".to_vec()));
        assert!(snap.seek(b"a6").unwrap().is_none());

        data.clear();
        snap.scan(b"a2",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(false)
                  })
            .unwrap();

        assert_eq!(data.len(), 1);

        // test last region
        let store = new_peer_storage(engine.clone(), &Region::new());
        let snap = RegionSnapshot::new(&store);
        data.clear();
        snap.scan(b"",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 4);
        assert_eq!(data, base_data);

        assert!(snap.seek(b"a1").unwrap().is_some());
    }

    #[allow(needless_range_loop)]
    #[test]
    fn test_reverse_iterate() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = new_temp_engine(&path);
        let (store, test_data) = load_default_dataset(engine.clone());

        let snap = RegionSnapshot::new(&store);
        assert!(snap.reverse_seek(b"a2").unwrap().is_none());
        let mut pair = snap.reverse_seek(b"a7").unwrap().unwrap();
        assert_eq!(pair, (b"a5".to_vec(), b"v5".to_vec()));
        pair = snap.reverse_seek(b"a5").unwrap().unwrap();
        assert_eq!(pair, (b"a3".to_vec(), b"v3".to_vec()));
        assert!(snap.reverse_seek(b"a3").unwrap().is_none());

        // test last region
        let store = new_peer_storage(engine.clone(), &Region::new());
        let snap = RegionSnapshot::new(&store);
        assert!(snap.reverse_seek(b"a1").unwrap().is_none());
        let pair = snap.reverse_seek(b"a2").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        for i in 0..test_data.len() - 1 {
            let seek_key = &test_data[i + 1].0;
            let pair = snap.reverse_seek(seek_key).unwrap().unwrap();
            assert_eq!(pair, test_data[i]);
        }
    }
}
