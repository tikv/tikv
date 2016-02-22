use std::{error, result};
use std::fmt::Debug;
use self::memory::EngineBtree;
use self::rocksdb::EngineRocksdb;
use storage::{Key, RefKey, Value, KvPair};

mod memory;
mod rocksdb;

#[derive(Debug)]
pub enum Modify {
    Delete(Key),
    Put(KvPair),
}

pub trait Engine : Send + Sync + Debug {
    fn get(&self, key: RefKey) -> Result<Option<Value>>;
    fn seek(&self, key: RefKey) -> Result<Option<KvPair>>;
    fn write(&self, batch: Vec<Modify>) -> Result<()>;

    fn put(&self, key: Key, value: Value) -> Result<()> {
        self.write(vec![Modify::Put((key, value))])
    }

    fn delete(&self, key: Key) -> Result<()> {
        self.write(vec![Modify::Delete(key)])
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Dsn<'a> {
    Memory,
    RocksDBPath(&'a str),
}

pub fn new_engine(dsn: Dsn) -> Result<Box<Engine>> {
    match dsn {
        Dsn::Memory => Ok(Box::new(EngineBtree::new())),
        Dsn::RocksDBPath(path) => {
            EngineRocksdb::new(path).map(|engine| -> Box<Engine> { Box::new(engine) })
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
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
    use super::{Dsn, Engine, Modify};
    use tempdir::TempDir;

    #[test]
    fn memory() {
        let e = super::new_engine(Dsn::Memory).unwrap();
        get_put(e.as_ref());
        batch(e.as_ref());
        seek(e.as_ref());
    }

    #[test]
    fn rocksdb() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = super::new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap())).unwrap();
        get_put(e.as_ref());
        batch(e.as_ref());
        seek(e.as_ref());
    }

    fn must_put<T: Engine + ?Sized>(engine: &T, key: &[u8], value: &[u8]) {
        engine.put(key.to_vec(), value.to_vec()).unwrap();
    }

    fn must_delete<T: Engine + ?Sized>(engine: &T, key: &[u8]) {
        engine.delete(key.to_vec()).unwrap();
    }

    fn assert_has<T: Engine + ?Sized>(engine: &T, key: &[u8], value: &[u8]) {
        assert_eq!(engine.get(key).unwrap().unwrap(), value);
    }

    fn assert_none<T: Engine + ?Sized>(engine: &T, key: &[u8]) {
        assert_eq!(engine.get(key).unwrap(), None);
    }

    fn assert_seek<T: Engine + ?Sized>(engine: &T, key: &[u8], pair: (&[u8], &[u8])) {
        let (k, v) = engine.seek(key).unwrap().unwrap();
        assert_eq!((&k as &[u8], &v as &[u8]), pair);
    }

    fn get_put<T: Engine + ?Sized>(engine: &T) {
        assert_none(engine, b"x");
        must_put(engine, b"x", b"1");
        assert_has(engine, b"x", b"1");
        must_put(engine, b"x", b"2");
        assert_has(engine, b"x", b"2");
    }

    fn batch<T: Engine + ?Sized>(engine: &T) {
        engine.write(vec![Modify::Put((b"x".to_vec(), b"1".to_vec())),
                          Modify::Put((b"y".to_vec(), b"2".to_vec()))])
              .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine.write(vec![Modify::Delete(b"x".to_vec()), Modify::Delete(b"y".to_vec())])
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
        assert_eq!(engine.seek(b"z\x00").unwrap(), None);
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }
}
