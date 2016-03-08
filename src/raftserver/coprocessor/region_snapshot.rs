use rocksdb::rocksdb::{Snapshot, DBIterator};
use raftserver::store::engine::{Iterable, Peekable, DBValue};
use raftserver::store::{keys, PeerStorage};
use raftserver::store::util;
use raftserver::Result;
use proto::metapb;

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
pub struct RegionSnapshot<'a> {
    snap: Snapshot<'a>,
    region: &'a metapb::Region,
}

impl<'a> RegionSnapshot<'a> {
    pub fn new(peer: &'a PeerStorage) -> RegionSnapshot<'a> {
        RegionSnapshot {
            snap: peer.raw_snapshot(),
            region: peer.get_region(),
        }
    }

    // TODO: return TakeWhile instead.
    fn new_iterator(&self, start_key: &[u8]) -> DBIterator {
        let region_start_key = self.region.get_start_key();
        let data_key = keys::data_key(start_key);
        let scan_start_key = if &*data_key < region_start_key {
            region_start_key
        } else {
            &*data_key
        };
        self.snap.new_iterator(scan_start_key)
    }

    // Seek the first key >= given key, if no found, return None.
    pub fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let region_end_key = self.region.get_end_key();
        let pair = self.new_iterator(key)
                       .take_while(|&(ref k, _)| k.as_ref() < region_end_key)
                       .next()
                       .map(|(k, v)| (keys::origin_key(&k).to_vec(), v.into_vec()));
        Ok(pair)
    }

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
        where F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        let region_end_key = self.region.get_end_key();
        let it = self.new_iterator(start_key);

        for (key, value) in it {
            if key.as_ref() >= end_key || key.as_ref() >= region_end_key {
                break;
            }

            let r = try!(f(keys::origin_key(&key), value.as_ref()));
            if !r {
                break;
            }
        }

        Ok(())
    }
}

impl<'a> Peekable for RegionSnapshot<'a> {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBValue>> {
        let data_key = keys::data_key(key);
        try!(util::check_key_in_region(&data_key, self.region));
        self.snap.get_value(&data_key)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use rocksdb::{Writable, DB};
    use raftserver::store::engine::*;
    use raftserver::store::keys::*;
    use raftserver::store::PeerStorage;

    use super::*;
    use raftserver::Result;
    use std::sync::Arc;
    use proto::metapb::Region;

    fn new_temp_engine() -> Arc<DB> {
        let path = TempDir::new("test-raftserver").unwrap();
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
        let engine = new_temp_engine();
        let mut r = Region::new();
        r.set_region_id(10);
        r.set_start_key(data_key(b"key0"));
        r.set_end_key(data_key(b"key4"));
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
        let engine = new_temp_engine();
        let mut r = Region::new();
        r.set_region_id(10);
        r.set_start_key(data_key(b"a0"));
        r.set_end_key(data_key(b"a4"));
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
                  &mut |key, value| -> Result<bool> {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 3);
        assert_eq!(data, &base_data[..3]);

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a4").unwrap().is_none());

        data.clear();
        snap.scan(b"a2",
                  &[0xFF, 0xFF],
                  &mut |key, value| -> Result<bool> {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(false)
                  })
            .unwrap();

        assert_eq!(data.len(), 1);
    }
}
