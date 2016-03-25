use std::{error, result};
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use self::memory::EngineBtree;
use self::rocksdb::EngineRocksdb;
use self::raftkv::{Config, RaftKv};
use storage::{Key, Value, KvPair, KvOpt};
use pd;
use kvproto::errorpb::Error as ErrorHeader;

mod memory;
mod rocksdb;
pub mod raftkv;

#[derive(Debug)]
pub enum Modify {
    Delete(Key),
    Put((Key, Value)),
}

pub trait Engine : Send + Sync + Debug {
    fn get(&self, key: &Key, opt: &KvOpt) -> Result<Option<Value>>;
    fn seek(&self, key: &Key, opt: &KvOpt) -> Result<Option<KvPair>>;
    fn write(&self, batch: Vec<Modify>, opt: &KvOpt) -> Result<()>;

    fn put(&self, key: Key, value: Value, opt: &KvOpt) -> Result<()> {
        self.write(vec![Modify::Put((key, value))], opt)
    }

    fn delete(&self, key: Key, opt: &KvOpt) -> Result<()> {
        self.write(vec![Modify::Delete(key)], opt)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Dsn<'a> {
    Memory,
    RocksDBPath(&'a str),
    RaftKv(&'a Config, &'a str),
}

pub fn new_engine(dsn: Dsn) -> Result<Box<Engine>> {
    match dsn {
        Dsn::Memory => Ok(Box::new(EngineBtree::new())),
        Dsn::RocksDBPath(path) => {
            EngineRocksdb::new(path).map(|engine| -> Box<Engine> { Box::new(engine) })
        }
        Dsn::RaftKv(cfg, addr) => {
            let client = match pd::new_rpc_client(addr) {
                Err(e) => return Err(Error::Other(box e)),
                Ok(c) => c,
            };
            let client = Arc::new(RwLock::new(client));
            RaftKv::new(cfg, client).map(|e| -> Box<Engine> { box e }).map_err(From::from)
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Request(err: ErrorHeader) {
            from()
            description("request to underhook engine failed")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use storage::{make_key, KvOpt};
    use util::codec::bytes;

    #[test]
    fn memory() {
        let e = new_engine(Dsn::Memory).unwrap();
        get_put(e.as_ref());
        batch(e.as_ref());
        seek(e.as_ref());
    }

    #[test]
    fn rocksdb() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap())).unwrap();

        get_put(e.as_ref());
        batch(e.as_ref());
        seek(e.as_ref());
    }

    fn must_put<T: Engine + ?Sized>(engine: &T, key: &[u8], value: &[u8]) {
        engine.put(make_key(key), value.to_vec(), &KvOpt::none()).unwrap();
    }

    fn must_delete<T: Engine + ?Sized>(engine: &T, key: &[u8]) {
        engine.delete(make_key(key), &KvOpt::none()).unwrap();
    }

    fn assert_has<T: Engine + ?Sized>(engine: &T, key: &[u8], value: &[u8]) {
        assert_eq!(engine.get(&make_key(key), &KvOpt::none()).unwrap().unwrap(),
                   value);
    }

    fn assert_none<T: Engine + ?Sized>(engine: &T, key: &[u8]) {
        assert_eq!(engine.get(&make_key(key), &KvOpt::none()).unwrap(), None);
    }

    fn assert_seek<T: Engine + ?Sized>(engine: &T, key: &[u8], pair: (&[u8], &[u8])) {
        let (k, v) = engine.seek(&make_key(key), &KvOpt::none()).unwrap().unwrap();
        assert_eq!((k, &v as &[u8]), (bytes::encode_bytes(pair.0), pair.1));
    }

    fn get_put<T: Engine + ?Sized>(engine: &T) {
        assert_none(engine, b"x");
        must_put(engine, b"x", b"1");
        assert_has(engine, b"x", b"1");
        must_put(engine, b"x", b"2");
        assert_has(engine, b"x", b"2");
    }

    fn batch<T: Engine + ?Sized>(engine: &T) {
        engine.write(vec![Modify::Put((make_key(b"x"), b"1".to_vec())),
                          Modify::Put((make_key(b"y"), b"2".to_vec()))],
                     &KvOpt::none())
              .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine.write(vec![Modify::Delete(make_key(b"x")), Modify::Delete(make_key(b"y"))],
                     &KvOpt::none())
              .unwrap();
        assert_none(engine, b"y");
        assert_none(engine, b"y");
    }

    fn seek<T: Engine + ?Sized>(engine: &T) {
        must_put(engine, b"x", b"1");
        assert_seek(engine, b"x", (b"x", b"1"));
        assert_seek(engine, b"a", (b"x", b"1"));
        must_put(engine, b"z", b"2");
        assert_seek(engine, b"y", (b"z", b"2"));
        assert_seek(engine, b"x\x00", (b"z", b"2"));
        assert_eq!(engine.seek(&make_key(b"z\x00"), &KvOpt::none()).unwrap(),
                   None);
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }
}
