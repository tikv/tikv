use std::cmp::{Ord, Ordering};
use std::io::{Cursor, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use storage::{Error, Result};

#[derive(Debug, Copy, Clone)]
enum MetaItem {
    WithValue(u64),
    Deleted(u64),
}

impl MetaItem {
    fn new(data: &[u8]) -> Result<MetaItem> {
        match data.split_first() {
            Some((flag, rest)) => {
                let mut rdr = Cursor::new(rest);
                // TODO(disksing): wrap returned error
                let ver = rdr.read_u64::<BigEndian>().unwrap();
                match *flag {
                    b'v' => Ok(MetaItem::WithValue(ver)),
                    b'd' => Ok(MetaItem::Deleted(ver)),
                    _ => Err(Error::Mvcc("bad format meta".to_string())),
                }
            }
            None => Err(Error::Mvcc("bad format meta".to_string())),
        }
    }

    fn into_bytes(&self) -> Vec<u8> {
        let mut v = vec![];
        match *self {
            MetaItem::WithValue(ver) => {
                v.write(b"v").unwrap();
                v.write_u64::<BigEndian>(ver).unwrap();
            }
            MetaItem::Deleted(ver) => {
                v.write(b"d").unwrap();
                v.write_u64::<BigEndian>(ver).unwrap();
            }
        }
        v
    }

    fn version(self) -> u64 {
        match self {
            MetaItem::WithValue(v) => v,
            MetaItem::Deleted(v) => v,
        }
    }
}

impl PartialEq for MetaItem {
    fn eq(&self, other: &Self) -> bool {
        self.version() == other.version()
    }
}

impl Eq for MetaItem {}

impl Ord for MetaItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version().cmp(&other.version())
    }
}

impl PartialOrd for MetaItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.version().partial_cmp(&other.version())
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Meta {
    items: Vec<MetaItem>,
}

impl Meta {
    #[allow(dead_code)]
    pub fn new() -> Meta {
        Meta { items: vec![] }
    }

    #[allow(dead_code)]
    pub fn parse(data: &[u8]) -> Result<Meta> {
        let mut v = vec![];
        for chunk in data.chunks(9) {
            let item = try!(MetaItem::new(chunk));
            v.push(item);
        }
        v.sort();
        Ok(Meta { items: v })
    }

    #[allow(dead_code)]
    pub fn into_bytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        for item in &self.items {
            v.append(&mut item.into_bytes());
        }
        v
    }

    #[allow(dead_code)]
    pub fn latest(&self, ver: u64) -> Option<u64> {
        let mut index = self.items.binary_search(&MetaItem::WithValue(ver));
        match index {
            Err(0) => {
                return None;
            }
            Err(i) => index = Ok(i - 1),
            Ok(..) => {}
        }
        match self.items[index.unwrap()] {
            MetaItem::WithValue(ver) => Some(ver),
            MetaItem::Deleted(_) => None,
        }
    }

    #[allow(dead_code)]
    pub fn add(&mut self, ver: u64) {
        self.push(&MetaItem::WithValue(ver))
    }

    #[allow(dead_code)]
    pub fn delete(&mut self, ver: u64) {
        self.push(&MetaItem::Deleted(ver))
    }

    fn push(&mut self, item: &MetaItem) {
        let index = self.items.binary_search(item);
        match index {
            Ok(i) => self.items[i] = *item,            
            Err(i) => self.items.insert(i, *item),
        }
    }

    // TODO(diskiing)
    // remove all metaItems that version <= ver
    #[allow(dead_code)]
    #[allow(unused_variables)]
    fn clean(&mut self, ver: u64) {}
}

#[cfg(test)]
mod tests {
    use super::{Meta, MetaItem};

    #[test]
    fn test_meta_item() {
        let v100 = MetaItem::WithValue(100);
        let v100b = v100.into_bytes();
        let d99 = MetaItem::Deleted(99);
        let d99b = d99.into_bytes();

        assert_eq!(MetaItem::new(&v100b).unwrap(), v100);
        assert_eq!(MetaItem::new(&d99b).unwrap(), d99);
        assert_eq!(v100.version(), 100);
        assert_eq!(d99.version(), 99);
        assert!(d99 < v100);
    }

    #[test]
    fn test_latest() {
        let mut meta = Meta::new();
        assert_eq!(meta.latest(1), None);
        meta.add(10);
        assert_eq!(meta.latest(1), None);
        assert_eq!(meta.latest(10), Some(10));
        assert_eq!(meta.latest(11), Some(10));
        meta.delete(20);
        assert_eq!(meta.latest(11), Some(10));
        assert_eq!(meta.latest(20), None);
        assert_eq!(meta.latest(21), None);
    }

    #[test]
    fn test_parse() {
        let mut meta = Meta::new();
        meta.add(10);
        meta.delete(20);

        let bytes = meta.into_bytes();
        let meta2 = Meta::parse(&bytes).unwrap();
        assert_eq!(bytes, meta2.into_bytes());
    }
}
