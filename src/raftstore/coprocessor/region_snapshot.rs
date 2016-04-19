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

use rocksdb::rocksdb::{Snapshot, DBIterator, DB};
use raftstore::store::engine::{Iterable, Peekable, DBValue};
use raftstore::store::keys::{self, enc_end_key};
use raftstore::store::{util, PeerStorage};
use raftstore::Result;
use kvproto::metapb;

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

    // TODO: return TakeWhile instead.
    fn new_iterator(&self, start_key: &[u8]) -> DBIterator {
        let scan_start_key = if start_key < self.region.get_start_key() {
            keys::data_key(self.region.get_start_key())
        } else {
            keys::data_key(start_key)
        };
        self.snap.new_iterator(&scan_start_key)
    }

    // Seek the first key >= given key, if no found, return None.
    pub fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let region_end_key = enc_end_key(&self.region);
        let pair = self.new_iterator(key)
                       .take_while(|&(ref k, _)| k.as_ref() < &region_end_key)
                       .next()
                       .map(|(k, v)| (keys::origin_key(&k).to_vec(), v.into_vec()));
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
        let region_end_key = enc_end_key(&self.region);
        let data_end_key = if end_key.is_empty() {
            region_end_key.clone()
        } else {
            keys::data_key(end_key)
        };
        let it = self.new_iterator(start_key);

        for (key, value) in it {
            if key.as_ref() >= &data_end_key || key.as_ref() >= &region_end_key {
                break;
            }

            let r = try!(f(keys::origin_key(&key), value.as_ref()));
            if !r {
                break;
            }
        }

        Ok(())
    }

    /// Return next kv whose key is greater than the specified.
    pub fn next(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut next = None;
        try!(self.scan(key,
                       self.region.get_end_key(),
                       &mut |k, v| {
                           if k == key {
                               Ok(true)
                           } else {
                               next = Some((k.to_vec(), v.to_vec()));
                               Ok(false)
                           }
                       }));
        Ok(next)
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl<'a> Peekable for RegionSnapshot<'a> {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBValue>> {
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
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(b"a2".to_vec());
        r.set_end_key(b"a4".to_vec());
        let store = new_peer_storage(engine.clone(), &r);

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a2".to_vec(), b"v2".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a4".to_vec(), b"v4".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engine.put(&data_key(k), v).expect("");
        }

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
        assert_eq!(pair, (b"a2".to_vec(), b"v2".to_vec()));
        assert!(snap.seek(b"a4").unwrap().is_none());

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

    #[test]
    fn test_next() {
        let path = TempDir::new("test-raftstore").unwrap();
        let engine = new_temp_engine(&path);
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(b"a1".to_vec());
        r.set_end_key(b"a5".to_vec());
        let store = new_peer_storage(engine.clone(), &r);

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engine.put(&data_key(k), v).expect("");
        }

        let snap = RegionSnapshot::new(&store);
        assert_eq!(snap.next(b"a0").unwrap(),
                   Some((b"a1".to_vec(), b"v1".to_vec())));
        assert_eq!(snap.next(b"a1").unwrap(),
                   Some((b"a3".to_vec(), b"v3".to_vec())));
        assert_eq!(snap.next(b"a5").unwrap(), None);
    }
}
