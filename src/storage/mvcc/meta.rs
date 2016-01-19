use std::cmp::{Ord, Ordering};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use super::{Result, MvccErrorKind};

#[derive(Debug, Copy, Clone)]
enum MetaItem {
    WithValue(u64),
    Deleted(u64),
}

const FLAG_WITH_VALUE: u8 = b'v';
const FLAG_DELETED: u8 = b'd';

// 1 byte for flag + 8 bytes for u64
const META_ITEM_ENCODE_SIZE: usize = 9;

impl MetaItem {
    fn new(data: &[u8]) -> Result<MetaItem> {
        let (flag, rest) = match data.split_first() {
            Some(x) => x,
            None => return MvccErrorKind::MetaDataLength.as_result(),
        };
        let ver = match rest.as_ref().read_u64::<BigEndian>() {
            Ok(x) => x,
            Err(..) => return MvccErrorKind::MetaDataVersion.as_result(),
        };
        match *flag {
            FLAG_WITH_VALUE => Ok(MetaItem::WithValue(ver)),
            FLAG_DELETED => Ok(MetaItem::Deleted(ver)),
            _ => MvccErrorKind::MetaDataFlag.as_result(),
        }
    }

    fn write(&self, buf: &mut Vec<u8>) {
        match *self {
            MetaItem::WithValue(ver) => {
                buf.push(FLAG_WITH_VALUE);
                buf.write_u64::<BigEndian>(ver).unwrap();
            }
            MetaItem::Deleted(ver) => {
                buf.push(FLAG_DELETED);
                buf.write_u64::<BigEndian>(ver).unwrap();
            }
        }
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
pub struct Meta {
    items: Vec<MetaItem>,
}

impl Meta {
    pub fn new() -> Meta {
        Meta { items: vec![] }
    }

    pub fn parse(data: &[u8]) -> Result<Meta> {
        let mut v = Vec::<MetaItem>::with_capacity(data.len() / META_ITEM_ENCODE_SIZE);
        for chunk in data.chunks(META_ITEM_ENCODE_SIZE) {
            let item = try!(MetaItem::new(chunk));
            v.push(item);
        }
        Ok(Meta { items: v })
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        let mut v = Vec::<u8>::with_capacity(self.items.len() * META_ITEM_ENCODE_SIZE);
        for item in &self.items {
            item.write(&mut v);
        }
        v
    }

    pub fn latest(&self, ver: u64) -> Option<u64> {
        let index = self.items.binary_search(&MetaItem::WithValue(ver));
        let i = match index {
            Err(0) => return None,
            Err(x) => x - 1,
            Ok(x) => x,
        };
        match self.items[i] {
            MetaItem::WithValue(ver) => Some(ver),
            MetaItem::Deleted(_) => None,
        }
    }

    pub fn has_version(&self, ver: u64) -> bool {
        match self.items.binary_search(&MetaItem::WithValue(ver)) {
            Ok(..) => true,
            Err(..) => false,
        }
    }

    pub fn add(&mut self, ver: u64) {
        self.push(&MetaItem::WithValue(ver))
    }

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

    // remove all metaItems that version <= ver (for GC)
    // TODO(diskiing)
    #[allow(dead_code)]
    #[allow(unused_variables)]
    fn clean(&mut self, ver: u64) {}
}

#[cfg(test)]
mod tests {
    use super::{Meta, MetaItem};

    #[test]
    fn test_meta_item_new() {
        assert!(MetaItem::new(b"").is_err());
        MetaItem::new(b"v\x00\x00\x00\x00\x00\x00\x00\x00").unwrap();
        MetaItem::new(b"d\x00\x00\x00\x00\x00\x00\x00\xff").unwrap();
        assert!(MetaItem::new(b"x\x00\x00\x00\x00").is_err());
        assert!(MetaItem::new(b"v\x00\x00\x00\x00\x00\x00\x00").is_err());
    }

    #[test]
    fn test_meta_item_write() {
        let v100 = MetaItem::WithValue(100);
        let mut v100b: Vec<u8> = vec![];
        v100.write(&mut v100b);

        let d99 = MetaItem::Deleted(99);
        let mut d99b: Vec<u8> = vec![];
        d99.write(&mut d99b);

        assert_eq!(MetaItem::new(&v100b).unwrap(), v100);
        assert_eq!(MetaItem::new(&d99b).unwrap(), d99);
        assert_eq!(v100.version(), 100);
        assert_eq!(d99.version(), 99);
        assert!(d99 < v100);
    }

    #[test]
    fn test_meta() {
        let mut meta = Meta::new();
        assert_eq!(meta.latest(1), None);

        meta.add(10);
        assert!(!meta.has_version(9));
        assert!(meta.has_version(10));
        assert!(!meta.has_version(11));
        assert_eq!(meta.latest(9), None);
        assert_eq!(meta.latest(10), Some(10));
        assert_eq!(meta.latest(11), Some(10));

        meta.delete(30);
        assert!(meta.has_version(10));
        assert!(meta.has_version(30));
        assert_eq!(meta.latest(9), None);
        assert_eq!(meta.latest(29), Some(10));
        assert_eq!(meta.latest(30), None);
        assert_eq!(meta.latest(31), None);

        meta.add(20);
        assert_eq!(meta.latest(9), None);
        assert_eq!(meta.latest(19), Some(10));
        assert_eq!(meta.latest(20), Some(20));
        assert_eq!(meta.latest(21), Some(20));
        assert_eq!(meta.latest(29), Some(20));
        assert_eq!(meta.latest(30), None);
        assert_eq!(meta.latest(31), None);
    }

    #[test]
    fn test_meta_parse() {
        let mut meta = Meta::new();
        meta.add(10);
        meta.delete(20);

        let bytes = meta.into_bytes();
        let meta2 = Meta::parse(&bytes).unwrap();
        assert_eq!(bytes, meta2.into_bytes());
    }

    use test::Bencher;

    #[bench]
    fn bench_into_bytes(b: &mut Bencher) {
        let mut meta = Meta::new();
        for v in 1..10 {
            meta.add(v);
        }
        b.iter(|| meta.into_bytes());
    }

    #[bench]
    fn bench_from_bytes(b: &mut Bencher) {
        let mut meta = Meta::new();
        for v in 1..10 {
            meta.add(v);
        }
        let bytes = meta.into_bytes();
        b.iter(|| Meta::parse(&bytes));
    }
}
