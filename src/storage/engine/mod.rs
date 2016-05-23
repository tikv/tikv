// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{error, result};
use std::fmt::Debug;
use self::rocksdb::EngineRocksdb;
use storage::{Key, Value};
use kvproto::kvrpcpb::Context;
use kvproto::errorpb::Error as ErrorHeader;

mod rocksdb;
pub mod raftkv;

// only used for rocksdb without persistent.
pub const TEMP_DIR: &'static str = "";

#[derive(Debug)]
pub enum Modify {
    Delete(Key),
    Put((Key, Value)),
}

pub trait Engine: Send + Sync + Debug {
    fn get(&self, ctx: &Context, key: &Key) -> Result<Option<Value>>;
    fn write(&self, ctx: &Context, batch: Vec<Modify>) -> Result<()>;
    fn snapshot<'a>(&'a self, ctx: &Context) -> Result<Box<Snapshot + 'a>>;

    fn put(&self, ctx: &Context, key: Key, value: Value) -> Result<()> {
        self.write(ctx, vec![Modify::Put((key, value))])
    }

    fn delete(&self, ctx: &Context, key: Key) -> Result<()> {
        self.write(ctx, vec![Modify::Delete(key)])
    }

    fn iter<'a>(&'a self, ctx: &Context, start_key: &Key) -> Result<Box<Cursor + 'a>>;
}

pub trait Snapshot {
    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn iter<'a>(&'a self, start_key: &Key) -> Result<Box<Cursor + 'a>>;
}

pub trait Cursor {
    fn next(&mut self) -> bool;
    fn prev(&mut self) -> bool;
    fn seek(&mut self, key: &Key) -> Result<bool>;
    fn seek_to_first(&mut self) -> bool;
    fn seek_to_last(&mut self) -> bool;
    fn valid(&self) -> bool;

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];

    fn reverse_seek(&mut self, key: &Key) -> Result<bool> {
        if !try!(self.seek(key)) && !self.seek_to_last() {
            return Ok(false);
        }

        while self.key() >= key.encoded().as_slice() && self.prev() {
        }

        Ok(self.valid())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Dsn<'a> {
    RocksDBPath(&'a str),
    RaftKv,
}

// Now we only support RocksDB.
pub fn new_engine(dsn: Dsn) -> Result<Box<Engine>> {
    match dsn {
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
            display("{:?}", err)
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
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
    fn rocksdb() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap())).unwrap();

        get_put(e.as_ref());
        batch(e.as_ref());
        seek(e.as_ref());
    }

    fn must_put(engine: &Engine, key: &[u8], value: &[u8]) {
        engine.put(&Context::new(), make_key(key), value.to_vec()).unwrap();
    }

    fn must_delete(engine: &Engine, key: &[u8]) {
        engine.delete(&Context::new(), make_key(key)).unwrap();
    }

    fn assert_has(engine: &Engine, key: &[u8], value: &[u8]) {
        assert_eq!(engine.get(&Context::new(), &make_key(key)).unwrap().unwrap(),
                   value);
    }

    fn assert_none(engine: &Engine, key: &[u8]) {
        assert_eq!(engine.get(&Context::new(), &make_key(key)).unwrap(), None);
    }

    fn assert_seek(engine: &Engine, key: &[u8], pair: (&[u8], &[u8])) {
        let iter = engine.iter(&Context::new(), &make_key(key)).unwrap();
        assert_eq!((iter.key(), iter.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn get_put(engine: &Engine) {
        assert_none(engine, b"x");
        must_put(engine, b"x", b"1");
        assert_has(engine, b"x", b"1");
        must_put(engine, b"x", b"2");
        assert_has(engine, b"x", b"2");
    }

    fn batch(engine: &Engine) {
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

    fn seek(engine: &Engine) {
        must_put(engine, b"x", b"1");
        assert_seek(engine, b"x", (b"x", b"1"));
        assert_seek(engine, b"a", (b"x", b"1"));
        must_put(engine, b"z", b"2");
        assert_seek(engine, b"y", (b"z", b"2"));
        assert_seek(engine, b"x\x00", (b"z", b"2"));
        assert!(!engine.iter(&Context::new(), &make_key(b"z\x00")).unwrap().valid());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }
}
