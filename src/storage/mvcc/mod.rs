use std::{error, fmt, result};
use storage::engine::{self, Engine, Modify};
use self::meta::Meta;
use self::codec::{encode_key, decode_key};

mod meta;
mod codec;

pub trait MvccEngine : Engine {
    fn mvcc_get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
        let mkey = encode_key(key, 0u64);
        let mval = match try!(self.get(&mkey)) {
            Some(x) => x,
            None => return Ok(None),
        };
        let meta = try!(Meta::parse(&mval));
        let ver = match meta.latest(version) {
            Some(x) => x,
            None => return Ok(None),
        };
        let dkey = encode_key(key, ver);
        match try!(self.get(&dkey)) {
            Some(x) => Ok(Some(x)),
            None => MvccErrorKind::DataMissing.as_result(),
        }
    }

    fn mvcc_put(&mut self, key: &[u8], value: &[u8], version: u64) -> Result<()> {
        let mkey = encode_key(key, 0u64);
        let dkey = encode_key(key, version);
        let mval = try!(self.get(&mkey));
        let mut meta = match mval {
            Some(x) => try!(Meta::parse(&x)),
            None => Meta::new(),
        };
        meta.add(version);
        let mval = meta.into_bytes();
        let batch = vec![Modify::Put((&mkey, &mval)), Modify::Put((&dkey, value))];
        self.write(batch).map_err(|e| Error::from(e))
    }

    fn mvcc_delete(&mut self, key: &[u8], version: u64) -> Result<()> {
        let mkey = encode_key(key, 0u64);
        let dkey = encode_key(key, version);
        let mval = try!(self.get(&mkey));
        let mut meta = match mval {
            Some(x) => try!(Meta::parse(&x)),
            None => Meta::new(),
        };
        let has_old_ver = meta.has_version(version);
        meta.delete(version);
        let mval = meta.into_bytes();
        let mut batch = vec![Modify::Put((&mkey, &mval))];
        if has_old_ver {
            batch.push(Modify::Delete(&dkey));
        }
        self.write(batch).map_err(|e| Error::from(e))
    }

    fn mvcc_scan(&self,
                 start_key: &[u8],
                 limit: usize,
                 version: u64)
                 -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut pairs = vec![];
        let mut seek_key = encode_key(start_key, 0u64);
        loop {
            if pairs.len() >= limit {
                break;
            }
            let (mkey, mval) = match try!(self.seek(&seek_key)) {
                Some(x) => x,
                None => break,
            };
            let (mut key, _) = try!(decode_key(&mkey));
            let meta = try!(Meta::parse(&mval));
            let ver = match meta.latest(version) {
                Some(x) => x,
                None => {
                    key.push(0);
                    seek_key = encode_key(&key, 0u64);
                    continue;
                }
            };
            let dkey = encode_key(&key, ver);
            match try!(self.get(&dkey)) {
                Some(x) => pairs.push((key.clone(), x)),
                None => return MvccErrorKind::DataMissing.as_result(),
            }
            key.push(0);
            seek_key = encode_key(&key, 0u64);
        }
        Ok(pairs)
    }
}

impl<T: Engine + ?Sized> MvccEngine for T {}

#[derive(Debug)]
pub enum Error {
    Engine(engine::Error),
    Mvcc(MvccErrorKind),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MvccErrorKind {
    MetaDataLength,
    MetaDataFlag,
    MetaDataVersion,
    KeyLength,
    KeyVersion,
    DataMissing,
}

impl MvccErrorKind {
    fn description(self) -> &'static str {
        match self {
            MvccErrorKind::MetaDataLength => "bad format meta data(length)",
            MvccErrorKind::MetaDataFlag => "bad format meta data(flag)",
            MvccErrorKind::MetaDataVersion => "bad format meta data(version)",
            MvccErrorKind::KeyLength => "bad format key(length)",
            MvccErrorKind::KeyVersion => "bad format key(version)",
            MvccErrorKind::DataMissing => "version data missing",
        }
    }

    fn as_result<T>(self) -> Result<T> {
        Err(Error::Mvcc(self))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Engine(ref err) => err.fmt(f),
            Error::Mvcc(kind) => kind.description().fmt(f),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Engine(ref err) => err.description(),
            Error::Mvcc(kind) => kind.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Engine(ref err) => Some(err),
            Error::Mvcc(..) => None,
        }
    }
}

impl From<engine::Error> for Error {
    fn from(err: engine::Error) -> Error {
        Error::Engine(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use storage::engine::{self, Dsn};
    use super::MvccEngine;

    #[test]
    fn test_mvcc() {
        let mut eng = engine::new_engine(Dsn::Memory).unwrap();
        assert_eq!(eng.mvcc_get(b"x", 1).unwrap(), None);
        eng.mvcc_put(b"x", b"x10", 10).unwrap();
        assert_eq!(eng.mvcc_get(b"x", 10).unwrap().unwrap(), b"x10");
        assert_eq!(eng.mvcc_get(b"x", 11).unwrap().unwrap(), b"x10");
        eng.mvcc_delete(b"x", 20).unwrap();
        assert_eq!(eng.mvcc_get(b"x", 15).unwrap().unwrap(), b"x10");
        assert_eq!(eng.mvcc_get(b"x", 20).unwrap(), None);
        assert_eq!(eng.mvcc_get(b"x", 22).unwrap(), None);
    }

    #[test]
    fn test_scan() {
        let mut eng = engine::new_engine(Dsn::Memory).unwrap();
        eng.mvcc_put(b"aa", b"11", 10).unwrap();
        eng.mvcc_put(b"bb", b"22", 20).unwrap();
        eng.mvcc_put(b"cc", b"33", 15).unwrap();

        let vec = eng.mvcc_scan(b"a", 4, 19).unwrap();
        assert_eq!(vec.len(), 2);
        assert_eq!(vec[0].0, b"aa");
        assert_eq!(vec[0].1, b"11");
        assert_eq!(vec[1].0, b"cc");
        assert_eq!(vec[1].1, b"33");
    }
}
