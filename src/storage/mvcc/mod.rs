use std::{error, fmt, result};
use storage::engine::{self, Engine, Modify};
use self::meta::Meta;
use self::codec::encode_key;

mod meta;
mod codec;

pub fn get(eng: &Engine, key: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
    let mkey = encode_key(key, 0u64);
    let mval = match try!(eng.get(&mkey)) {
        Some(x) => x,
        None => return Ok(None),
    };
    let meta = try!(Meta::parse(&mval));
    let ver = match meta.latest(version) {
        Some(x) => x,
        None => return Ok(None),
    };
    let dkey = encode_key(key, ver);
    match try!(eng.get(&dkey)) {
        Some(x) => Ok(Some(x)),
        None => MvccErrorKind::DataMissing.as_result(),
    }
}

pub fn put(eng: &mut Engine, key: &[u8], value: &[u8], version: u64) -> Result<()> {
    let mkey = encode_key(key, 0u64);
    let dkey = encode_key(key, version);
    let mval = try!(eng.get(&mkey));
    let mut meta = match mval {
        Some(x) => try!(Meta::parse(&x)),
        None => Meta::new(),
    };
    meta.add(version);
    let mval = meta.into_bytes();
    let batch = vec![Modify::Put((&mkey, &mval)), Modify::Put((&dkey, value))];
    eng.write(batch).map_err(|e| Error::from(e))
}

pub fn delete(eng: &mut Engine, key: &[u8], version: u64) -> Result<()> {
    let mkey = encode_key(key, 0u64);
    let dkey = encode_key(key, version);
    let mval = try!(eng.get(&mkey));
    let mut meta = match mval {
        Some(x) => try!(Meta::parse(&x)),
        None => Meta::new(),
    };
    let has_old_ver = meta.has_version(version);
    meta.delete(version);
    let mval = meta.into_bytes();
    let mut batch = vec![Modify::Put((&mkey, &mval))];
    if has_old_ver {
        batch.push(Modify::Delete((&dkey)));
    }
    eng.write(batch).map_err(|e| Error::from(e))
}

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
