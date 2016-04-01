use std::option::Option;
use std::ops::Deref;

use rocksdb::{DB, Writable, DBIterator, Direction, IteratorMode, DBVector, WriteBatch};
use rocksdb::rocksdb::Snapshot;
use protobuf;
use byteorder::{ByteOrder, BigEndian};

use raftstore::Result;

pub fn new_engine(path: &str) -> Result<DB> {
    // TODO: set proper options here,
    let db = try!(DB::open_default(path));
    Ok(db)
}

pub enum DBValue {
    DBVector(DBVector),
    Box(Box<[u8]>),
}

impl Deref for DBValue {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match *self {
            DBValue::DBVector(ref v) => v,
            DBValue::Box(ref v) => v,
        }
    }
}

pub trait Peekable {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBValue>>;

    fn get_msg<M>(&self, key: &[u8]) -> Result<Option<M>>
        where M: protobuf::Message + protobuf::MessageStatic
    {
        let value = try!(self.get_value(key));

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        try!(m.merge_from_bytes(&value.unwrap()));
        Ok(Some(m))
    }

    fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        let value = try!(self.get_value(key));

        if value.is_none() {
            return Ok(None);
        }

        let value = value.unwrap();
        if value.len() != 8 {
            return Err(box_err!("need 8 bytes, but only got {}", value.len()));
        }

        let n = BigEndian::read_u64(&value);
        Ok(Some(n))
    }

    fn get_i64(&self, key: &[u8]) -> Result<Option<i64>> {
        let r = try!(self.get_u64(key));
        match r {
            None => Ok(None),
            Some(n) => Ok(Some(n as i64)),
        }
    }
}

pub trait Iterable {
    fn new_iterator(&self, start_key: &[u8]) -> DBIterator;

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
        where F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        let it = self.new_iterator(start_key);

        for (key, value) in it {
            if key.as_ref() >= end_key {
                break;
            }

            let r = try!(f(key.as_ref(), value.as_ref()));
            if !r {
                break;
            }
        }

        Ok(())
    }

    // Seek the first key >= given key, if no found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let pair = self.new_iterator(key).next().map(|(k, v)| (k.into_vec(), v.into_vec()));
        Ok(pair)
    }
}

impl Peekable for DB {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBValue>> {
        let v = try!(self.get(key));
        Ok(v.map(DBValue::DBVector))
    }
}

impl Iterable for DB {
    fn new_iterator(&self, start_key: &[u8]) -> DBIterator {
        self.iterator(IteratorMode::From(start_key, Direction::Forward))
    }
}

impl<'a> Peekable for Snapshot<'a> {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBValue>> {
        let v = try!(self.get(key));
        Ok(v.map(DBValue::DBVector))
    }
}

impl<'a> Iterable for Snapshot<'a> {
    fn new_iterator(&self, start_key: &[u8]) -> DBIterator {
        self.iterator(IteratorMode::From(start_key, Direction::Forward))
    }
}

pub trait Mutable : Writable{
    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        let value = try!(m.write_to_bytes());
        try!(self.put(key, &value));
        Ok(())
    }

    fn put_u64(&self, key: &[u8], n: u64) -> Result<()> {
        let mut value = vec![0;8];
        BigEndian::write_u64(&mut value, n);
        try!(self.put(key, &value));
        Ok(())
    }

    fn put_i64(&self, key: &[u8], n: i64) -> Result<()> {
        self.put_u64(key, n as u64)
    }

    fn del(&self, key: &[u8]) -> Result<()> {
        try!(self.delete(key));
        Ok(())
    }
}

impl Mutable for DB {}
impl Mutable for WriteBatch {}


#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use rocksdb::Writable;

    use super::*;
    use kvproto::metapb::Region;

    #[test]
    fn test_base() {
        let path = TempDir::new("var").unwrap();
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();

        let mut r = Region::new();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert!(r1 != r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());

        engine.put_i64(key, -1).unwrap();
        assert_eq!(engine.get_i64(key).unwrap(), Some(-1));
        assert!(engine.get_i64(b"missing_key").unwrap().is_none());

        let snap = engine.snapshot();
        assert_eq!(snap.get_i64(key).unwrap(), Some(-1));
        assert!(snap.get_i64(b"missing_key").unwrap().is_none());

        engine.put_u64(key, 1).unwrap();
        assert_eq!(engine.get_u64(key).unwrap(), Some(1));
        assert_eq!(snap.get_i64(key).unwrap(), Some(-1));
    }

    #[test]
    fn test_scan() {
        let path = TempDir::new("var").unwrap();
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();

        let mut data = vec![];
        engine.scan(b"",
                    &[0xFF, 0xFF],
                    &mut |key, value| {
                        data.push((key.to_vec(), value.to_vec()));
                        Ok(true)
                    })
              .unwrap();

        assert_eq!(data.len(), 2);
        let pair = engine.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(b"a3").unwrap().is_none());

        data.clear();
        let mut index = 0;
        engine.scan(b"",
                    &[0xFF, 0xFF],
                    &mut |key, value| {
                        data.push((key.to_vec(), value.to_vec()));
                        index += 1;
                        Ok(index != 1)
                    })
              .unwrap();

        assert_eq!(data.len(), 1);

        let snap = engine.snapshot();

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(b"a3").unwrap().is_some());

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a3").unwrap().is_none());

        data.clear();

        snap.scan(b"",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 2);
    }
}
