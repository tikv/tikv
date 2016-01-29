use std::option::Option;

use rocksdb::{DB, Writable, DBIterator, Direction, IteratorMode};
use rocksdb::rocksdb::Snapshot;
use protobuf;
use byteorder::{ByteOrder, BigEndian};

use raftserver::{Result, other};

pub fn new_engine(path: &str) -> Result<DB> {
    // TODO: set proper options here,
    let db = try!(DB::open_default(path));
    Ok(db)
}

pub fn get_msg<M>(db: &DB, key: &[u8]) -> Result<Option<M>>
    where M: protobuf::Message + protobuf::MessageStatic
{
    let value = try!(db.get(key));

    if value.is_none() {
        return Ok(None);
    }

    let mut m = M::new();
    try!(m.merge_from_bytes(&value.unwrap()));
    Ok(Some(m))
}

pub fn put_msg<T: Writable, M: protobuf::Message>(w: &T, key: &[u8], m: &M) -> Result<()> {
    let value = try!(m.write_to_bytes());
    try!(w.put(key, &value));
    Ok(())
}

pub fn get_u64(db: &DB, key: &[u8]) -> Result<Option<u64>> {
    let value = try!(db.get(key));

    if value.is_none() {
        return Ok(None);
    }

    let value = value.unwrap();
    if value.len() != 8 {
        return Err(other(format!("need 8 bytes, but only got {}", value.len())));
    }

    let n = BigEndian::read_u64(&value);
    Ok(Some(n))
}

pub fn get_i64(db: &DB, key: &[u8]) -> Result<Option<i64>> {
    let r = try!(get_u64(db, key));
    match r {
        None => Ok(None),
        Some(n) => Ok(Some(n as i64)),
    }
}

pub fn put_u64<T: Writable>(w: &T, key: &[u8], n: u64) -> Result<()> {
    let mut value = vec![0;8];
    BigEndian::write_u64(&mut value, n);
    try!(w.put(key, &value));
    Ok(())
}

pub fn put_i64<T: Writable>(w: &T, key: &[u8], n: i64) -> Result<()> {
    put_u64(w, key, n as u64)
}

fn snap_get(snap: &Snapshot, key: &[u8]) -> Result<Option<Box<[u8]>>> {
    // Snapshot now doesn't have get method, we use iterator instead.
    let mut it = snap.iterator(IteratorMode::From(key, Direction::forward));
    if let Some((seek_key, value)) = it.next() {
        if seek_key.as_ref() == key {
            return Ok(Some(value));
        }
    }

    Ok(None)
}

// Now snapshot doesn't support get function, so it is not easy to combine DB and Snapshot operations.
// If origin rust rocksdb supports snapshot get, we will discard these functions.
pub fn snap_get_msg<M>(snap: &Snapshot, key: &[u8]) -> Result<Option<M>>
    where M: protobuf::Message + protobuf::MessageStatic
{
    let value = try!(snap_get(snap, key));

    if value.is_none() {
        return Ok(None);
    }

    let mut m = M::new();
    try!(m.merge_from_bytes(&value.unwrap()));
    Ok(Some(m))
}

pub fn snap_get_u64(snap: &Snapshot, key: &[u8]) -> Result<Option<u64>> {
    let value = try!(snap_get(snap, key));

    if value.is_none() {
        return Ok(None);
    }

    let value = value.unwrap();
    if value.len() != 8 {
        return Err(other(format!("need 8 bytes, but only got {}", value.len())));
    }

    let n = BigEndian::read_u64(&value);
    Ok(Some(n))
}

pub fn snap_get_i64(snap: &Snapshot, key: &[u8]) -> Result<Option<i64>> {
    let r = try!(snap_get_u64(snap, key));
    match r {
        None => Ok(None),
        Some(n) => Ok(Some(n as i64)),
    }
}

// scan scans database using an iterator in range [start_key, end_key), calls function f for
// each iteration, if f returns false, terminates this scan.
pub fn scan<F>(db: &DB, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
    where F: FnMut(&[u8], &[u8]) -> Result<bool>
{
    let mut it = db.iterator(IteratorMode::From(start_key, Direction::forward));

    scan_inner(&mut it, end_key, f)
}

// snap_scan like above scan, scans the data in a snapshot.
pub fn snap_scan<F>(snap: &Snapshot, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
    where F: FnMut(&[u8], &[u8]) -> Result<bool>
{
    let mut it = snap.iterator(IteratorMode::From(start_key, Direction::forward));

    scan_inner(&mut it, end_key, f)
}

fn scan_inner<F>(it: &mut DBIterator, end_key: &[u8], f: &mut F) -> Result<()>
    where F: FnMut(&[u8], &[u8]) -> Result<bool>
{
    while let Some((key, value)) = it.next() {
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

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use rocksdb::Writable;

    use super::*;
    use raftserver::Result;
    use proto::metapb::Region;

    #[test]
    fn test_base() {
        let path = TempDir::new("var").unwrap();
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();

        let mut r = Region::new();
        r.set_region_id(10);

        let key = b"key";
        put_msg(&engine, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1 = get_msg::<Region>(&engine, key).unwrap().unwrap();
        assert_eq!(r, r1);

        let mut r2 = snap_get_msg::<Region>(&snap, key).unwrap().unwrap();
        assert_eq!(r, r2);

        r.set_region_id(11);
        put_msg(&engine, key, &r).unwrap();
        r1 = get_msg::<Region>(&engine, key).unwrap().unwrap();
        r2 = snap_get_msg::<Region>(&snap, key).unwrap().unwrap();
        assert!(r1 != r2);

        let b = get_msg::<Region>(&engine, b"missing_key").unwrap();
        assert!(b.is_none());

        put_i64(&engine, key, -1).unwrap();
        assert_eq!(get_i64(&engine, key).unwrap(), Some(-1));
        assert!(get_i64(&engine, b"missing_key").unwrap().is_none());

        put_u64(&engine, key, 1).unwrap();
        assert_eq!(get_u64(&engine, key).unwrap(), Some(1));
    }

    #[test]
    fn test_scan() {
        let path = TempDir::new("var").unwrap();
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();


        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();

        let mut data = vec![];
        scan(&engine,
             b"",
             &[0xFF, 0xFF],
             &mut |key, value| -> Result<bool> {
                 data.push((key.to_vec(), value.to_vec()));
                 Ok(true)
             })
            .unwrap();

        assert_eq!(data.len(), 2);

        data.clear();
        let mut index = 0;
        scan(&engine,
             b"",
             &[0xFF, 0xFF],
             &mut |key, value| -> Result<bool> {
                 data.push((key.to_vec(), value.to_vec()));
                 index += 1;
                 Ok(index != 1)
             })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = engine.snapshot();

        engine.put(b"a3", b"v3").unwrap();

        data.clear();

        snap_scan(&snap,
                  b"",
                  &[0xFF, 0xFF],
                  &mut |key, value| -> Result<bool> {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 2);
    }
}
