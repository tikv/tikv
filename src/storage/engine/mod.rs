use std::{error, result};
use std::fmt::Debug;
use self::memory::EngineBtree;
use self::rocksdb::EngineRocksdb;
use storage::{Key, Value, KvPair};
use kvproto::kvrpcpb::Context;
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
    fn get(&self, ctx: &Context, key: &Key) -> Result<Option<Value>>;
    fn seek(&self, ctx: &Context, key: &Key) -> Result<Option<KvPair>>;
    fn write(&self, ctx: &Context, batch: Vec<Modify>) -> Result<()>;

    fn put(&self, ctx: &Context, key: Key, value: Value) -> Result<()> {
        self.write(ctx, vec![Modify::Put((key, value))])
    }

    fn delete(&self, ctx: &Context, key: Key) -> Result<()> {
        self.write(ctx, vec![Modify::Delete(key)])
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Dsn<'a> {
    Memory,
    RocksDBPath(&'a str),
    RaftKv,
}

// Now we only support Memory and RocksDB.
pub fn new_engine(dsn: Dsn) -> Result<Box<Engine>> {
    match dsn {
        Dsn::Memory => Ok(Box::new(EngineBtree::new())),
        Dsn::RocksDBPath(path) => {
            EngineRocksdb::new(path).map(|engine| -> Box<Engine> { Box::new(engine) })
        }
        Dsn::RaftKv => unimplemented!(),
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
    use storage::make_key;
    use util::codec::bytes;
    use kvproto::kvrpcpb::Context;

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
        engine.put(&Context::new(), make_key(key), value.to_vec()).unwrap();
    }

    fn must_delete<T: Engine + ?Sized>(engine: &T, key: &[u8]) {
        engine.delete(&Context::new(), make_key(key)).unwrap();
    }

    fn assert_has<T: Engine + ?Sized>(engine: &T, key: &[u8], value: &[u8]) {
        assert_eq!(engine.get(&Context::new(), &make_key(key)).unwrap().unwrap(),
                   value);
    }

    fn assert_none<T: Engine + ?Sized>(engine: &T, key: &[u8]) {
        assert_eq!(engine.get(&Context::new(), &make_key(key)).unwrap(), None);
    }

    fn assert_seek<T: Engine + ?Sized>(engine: &T, key: &[u8], pair: (&[u8], &[u8])) {
        let (k, v) = engine.seek(&Context::new(), &make_key(key)).unwrap().unwrap();
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
        engine.write(&Context::new(),
                     vec![Modify::Put((make_key(b"x"), b"1".to_vec())),
                          Modify::Put((make_key(b"y"), b"2".to_vec()))])
              .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine.write(&Context::new(),
                     vec![Modify::Delete(make_key(b"x")), Modify::Delete(make_key(b"y"))])
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
        assert_eq!(engine.seek(&Context::new(), &make_key(b"z\x00")).unwrap(),
                   None);
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }
}
