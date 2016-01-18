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
    KeyPadding,
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
            MvccErrorKind::KeyPadding => "bad format key(padding)",
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
    use storage::engine::{self, Dsn, Engine};
    use super::codec::encode_key;
    use super::MvccEngine;

    #[test]
    fn test_mvcc_get() {
        let mut eng = engine::new_engine(Dsn::Memory).unwrap();
        // not exist
        must_none(eng.as_ref(), b"x", 10);
        // after put
        must_put(eng.as_mut(), b"x", b"x", 10);
        must_none(eng.as_mut(), b"x", 9);
        must_get(eng.as_ref(), b"x", 10, b"x");
        must_get(eng.as_ref(), b"x", 11, b"x");
        // delete meta
        eng.delete(&encode_key(b"x", 0u64)).unwrap();
        must_none(eng.as_ref(), b"x", 10);
        // data missing
        must_put(eng.as_mut(), b"y", b"y", 10);
        eng.delete(&encode_key(b"y", 10)).unwrap();
        assert!(eng.mvcc_get(b"y", 10).is_err());
    }

    #[test]
    fn test_mvcc_put_delete() {
        let mut eng = engine::new_engine(Dsn::Memory).unwrap();
        must_delete(eng.as_mut(), b"x", 10);
        must_none(eng.as_ref(), b"x", 9);
        must_none(eng.as_ref(), b"x", 10);
        must_none(eng.as_ref(), b"x", 11);
        must_put(eng.as_mut(), b"x", b"x5", 5);
        must_get(eng.as_ref(), b"x", 9, b"x5");
        must_none(eng.as_ref(), b"x", 10);
        must_none(eng.as_ref(), b"x", 11);
        must_delete(eng.as_mut(), b"x", 5);
        must_none(eng.as_ref(), b"x", 9);
    }

    fn must_get<T: Engine + ?Sized>(eng: &T, key: &[u8], version: u64, expect: &[u8]) {
        assert_eq!(eng.mvcc_get(key, version).unwrap().unwrap(), expect);
    }

    fn must_none<T: Engine + ?Sized>(eng: &T, key: &[u8], version: u64) {
        assert_eq!(eng.mvcc_get(key, version).unwrap(), None);
    }

    fn must_put<T: Engine + ?Sized>(eng: &mut T, key: &[u8], value: &[u8], version: u64) {
        eng.mvcc_put(key, value, version).unwrap();
    }

    fn must_delete<T: Engine + ?Sized>(eng: &mut T, key: &[u8], version: u64) {
        eng.mvcc_delete(key, version).unwrap();
    }

    #[test]
    fn test_scan() {
        let mut eng = engine::new_engine(Dsn::Memory).unwrap();
        // ver1: A(1) - B(_) - C(1) - D(_) - E(1)
        eng.mvcc_put(b"A", b"A1", 1).unwrap();
        eng.mvcc_put(b"C", b"C1", 1).unwrap();
        eng.mvcc_put(b"E", b"E1", 1).unwrap();
        check_scan_ver1(eng.as_ref());

        // ver2: A(1) - B(2) - C(1) - D(2) - E(1)
        eng.mvcc_put(b"B", b"B2", 2).unwrap();
        eng.mvcc_put(b"D", b"D2", 2).unwrap();
        check_scan_ver1(eng.as_ref());
        check_scan_ver2(eng.as_ref());

        // ver3: A(_) - B(2) - C(1) - D(_) - E(1)
        eng.mvcc_delete(b"A", 3).unwrap();
        eng.mvcc_delete(b"D", 3).unwrap();
        check_scan_ver1(eng.as_ref());
        check_scan_ver2(eng.as_ref());
        check_scan_ver3(eng.as_ref());

        // ver4: A(_) - B(_) - C(4) - D(4) - E(1)
        eng.mvcc_delete(b"B", 4).unwrap();
        eng.mvcc_put(b"C", b"C4", 4).unwrap();
        eng.mvcc_put(b"D", b"D4", 4).unwrap();
        check_scan_ver1(eng.as_ref());
        check_scan_ver2(eng.as_ref());
        check_scan_ver3(eng.as_ref());
        check_scan_ver4(eng.as_ref());
    }

    fn check_scan_ver1<T: Engine + ?Sized>(eng: &T) {
        assert_scan_eq(eng.mvcc_scan(b"", 0, 1).unwrap(), vec![]);
        assert_scan_eq(eng.mvcc_scan(b"", 1, 1).unwrap(), vec![(b"A", b"A1")]);
        assert_scan_eq(eng.mvcc_scan(b"", 2, 1).unwrap(),
                       vec![(b"A", b"A1"), (b"C", b"C1")]);
        assert_scan_eq(eng.mvcc_scan(b"", 3, 1).unwrap(),
                       vec![(b"A", b"A1"), (b"C", b"C1"), (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"", 4, 1).unwrap(),
                       vec![(b"A", b"A1"), (b"C", b"C1"), (b"E", b"E1")]);

        assert_scan_eq(eng.mvcc_scan(b"A", 3, 1).unwrap(),
                       vec![(b"A", b"A1"), (b"C", b"C1"), (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"A\x00", 3, 1).unwrap(),
                       vec![(b"C", b"C1"), (b"E", b"E1")]);

        assert_scan_eq(eng.mvcc_scan(b"C", 4, 1).unwrap(),
                       vec![(b"C", b"C1"), (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"F", 1, 1).unwrap(), vec![]);
    }

    fn check_scan_ver2<T: Engine + ?Sized>(eng: &T) {
        assert_scan_eq(eng.mvcc_scan(b"", 5, 2).unwrap(),
                       vec![(b"A", b"A1"),
                            (b"B", b"B2"),
                            (b"C", b"C1"),
                            (b"D", b"D2"),
                            (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"C", 5, 2).unwrap(),
                       vec![(b"C", b"C1"), (b"D", b"D2"), (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"D\x00", 1, 2).unwrap(), vec![(b"E", b"E1")]);
    }

    fn check_scan_ver3<T: Engine + ?Sized>(eng: &T) {
        assert_scan_eq(eng.mvcc_scan(b"", 5, 3).unwrap(),
                       vec![(b"B", b"B2"), (b"C", b"C1"), (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"A", 1, 3).unwrap(), vec![(b"B", b"B2")]);
        assert_scan_eq(eng.mvcc_scan(b"C\x00", 5, 3).unwrap(), vec![(b"E", b"E1")]);
    }

    fn check_scan_ver4<T: Engine + ?Sized>(eng: &T) {
        assert_scan_eq(eng.mvcc_scan(b"", 5, 4).unwrap(),
                       vec![(b"C", b"C4"), (b"D", b"D4"), (b"E", b"E1")]);
        assert_scan_eq(eng.mvcc_scan(b"", 5, 20).unwrap(),
                       vec![(b"C", b"C4"), (b"D", b"D4"), (b"E", b"E1")]);
    }

    fn assert_scan_eq(result: Vec<(Vec<u8>, Vec<u8>)>,
                      expect: Vec<(&'static [u8], &'static [u8])>) {
        assert_eq!(result.len(), expect.len());
        for ((ref k1, ref v1), (ref k2, ref v2)) in result.into_iter().zip(expect.into_iter()) {
            assert_eq!(k1, k2);
            assert_eq!(v1, v2);
        }
    }

    use test::Bencher;
    use std::fs;
    use std::sync::{Once, ONCE_INIT};
    use rand::{self, thread_rng, Rng};

    const NUM_OF_KEYS: usize = 100000;
    const NUM_OF_VERSIONS: u64 = 10;
    const VAL_LEN: usize = 10;
    const SCAN_LEN: usize = 100;
    const BENCH_DATA_PATH: &'static str = "/tmp/rocks-scan-bench-data";

    #[bench]
    #[ignore]
    fn bench_scan(b: &mut Bencher) {
        prepare();

        let e = engine::new_engine(Dsn::RocksDBPath(BENCH_DATA_PATH)).unwrap();
        b.iter(|| {
            let key = format!("row_{}", rand::random::<usize>() % (NUM_OF_KEYS - SCAN_LEN));
            e.mvcc_scan(key.as_bytes(), SCAN_LEN, 11).unwrap();
        })
    }

    static PREPARE_ONCE: Once = ONCE_INIT;
    fn prepare() {
        PREPARE_ONCE.call_once(|| {
            fs::remove_dir_all(BENCH_DATA_PATH).ok();
            let mut e = engine::new_engine(Dsn::RocksDBPath(BENCH_DATA_PATH)).unwrap();

            let mut val = [0u8; VAL_LEN];
            for i in 0..NUM_OF_KEYS {
                let key = format!("row_{}", i);
                for j in 1..NUM_OF_VERSIONS + 1 {
                    let ver = j * 5;
                    thread_rng().fill_bytes(&mut val);
                    e.mvcc_put(key.as_bytes(), &val, ver).unwrap();
                }
            }
        })
    }
}
