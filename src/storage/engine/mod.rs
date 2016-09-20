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
use std::cmp::Ordering;
use std::boxed::FnBox;
use std::time::Duration;

use self::rocksdb::EngineRocksdb;
use storage::{Key, Value, CfName, CF_DEFAULT};
use kvproto::kvrpcpb::Context;
use kvproto::errorpb::Error as ErrorHeader;

mod rocksdb;
pub mod raftkv;
mod metrics;
use self::metrics::*;

// only used for rocksdb without persistent.
pub const TEMP_DIR: &'static str = "";

const SEEK_BOUND: usize = 30;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

#[derive(Debug)]
pub enum Modify {
    Delete(CfName, Key),
    Put(CfName, Key, Value),
}

pub trait Engine: Send + Debug {
    fn async_write(&self, ctx: &Context, batch: Vec<Modify>, callback: Callback<()>) -> Result<()>;
    fn async_snapshot(&self, ctx: &Context, callback: Callback<Box<Snapshot>>) -> Result<()>;

    fn write(&self, ctx: &Context, batch: Vec<Modify>) -> Result<()> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        wait_event!(|cb| self.async_write(ctx, batch, cb).unwrap(), timeout)
            .unwrap_or_else(|| Err(Error::Timeout(timeout)))
    }

    fn snapshot(&self, ctx: &Context) -> Result<Box<Snapshot>> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        wait_event!(|cb| self.async_snapshot(ctx, cb).unwrap(), timeout)
            .unwrap_or_else(|| Err(Error::Timeout(timeout)))
    }

    fn put(&self, ctx: &Context, key: Key, value: Value) -> Result<()> {
        self.put_cf(ctx, CF_DEFAULT, key, value)
    }

    fn put_cf(&self, ctx: &Context, cf: CfName, key: Key, value: Value) -> Result<()> {
        self.write(ctx, vec![Modify::Put(cf, key, value)])
    }

    fn delete(&self, ctx: &Context, key: Key) -> Result<()> {
        self.delete_cf(ctx, CF_DEFAULT, key)
    }

    fn delete_cf(&self, ctx: &Context, cf: CfName, key: Key) -> Result<()> {
        self.write(ctx, vec![Modify::Delete(cf, key)])
    }

    /// Create a share Engine pointer.
    fn clone(&self) -> Box<Engine + 'static>;
}

pub trait Snapshot: Send {
    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>>;
    #[allow(needless_lifetimes)]
    fn iter<'a>(&'a self, upper_bound: Option<&[u8]>) -> Result<Box<Cursor + 'a>>;
    #[allow(needless_lifetimes)]
    fn iter_cf<'a>(&'a self, cf: CfName, upper_bound: Option<&[u8]>) -> Result<Box<Cursor + 'a>>;
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

    /// Seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `seek` instead.
    fn near_seek(&mut self, key: &Key) -> Result<bool> {
        if !self.valid() {
            return self.seek(key);
        }
        if self.key() == &**key.encoded() {
            return Ok(true);
        }
        let (nav, ord): (fn(&mut _) -> bool, Ordering) = if self.key() > &**key.encoded() {
            (Cursor::prev, Ordering::Greater)
        } else {
            (Cursor::next, Ordering::Less)
        };
        let mut cnt = 0;
        while self.key().cmp(key.encoded()) == ord && nav(self) {
            cnt += 1;
            if cnt >= SEEK_BOUND {
                CURSOR_OVER_SEEK_BOUND_COUNTER.inc();

                return self.seek(key);
            }
        }
        if ord == Ordering::Greater {
            if self.valid() {
                if self.key() < &**key.encoded() {
                    self.next();
                }
            } else {
                self.seek_to_first();
            }
        }
        // TODO: check region range.
        Ok(self.valid())
    }

    /// Get the value of specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should `seek` first.
    fn get(&mut self, key: &Key) -> Result<Option<&[u8]>> {
        if try!(self.near_seek(key)) && self.key() == &**key.encoded() {
            Ok(Some(self.value()))
        } else {
            Ok(None)
        }
    }

    fn reverse_seek(&mut self, key: &Key) -> Result<bool> {
        if !try!(self.seek(key)) && !self.seek_to_last() {
            return Ok(false);
        }

        while self.key() >= key.encoded().as_slice() && self.prev() {
        }

        Ok(self.valid())
    }

    /// Reverse seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `reverse_seek` instead.
    fn near_reverse_seek(&mut self, key: &Key) -> Result<bool> {
        if !try!(self.near_seek(key)) && !self.seek_to_last() {
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
pub fn new_engine(dsn: Dsn, cfs: &[CfName]) -> Result<Box<Engine>> {
    match dsn {
        Dsn::RocksDBPath(path) => {
            EngineRocksdb::new(path, cfs).map(|engine| -> Box<Engine> { Box::new(engine) })
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
        RocksDb(msg: String) {
            from()
            description("RocksDb error")
            display("RocksDb {}", msg)
        }
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
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
    use storage::{CfName, CF_DEFAULT, make_key};
    use util::codec::bytes;
    use util::escape;
    use kvproto::kvrpcpb::Context;

    const TEST_ENGINE_CFS: &'static [CfName] = &["cf"];

    #[test]
    fn rocksdb() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap()),
                           TEST_ENGINE_CFS)
            .unwrap();

        test_get_put(e.as_ref());
        test_batch(e.as_ref());
        test_seek(e.as_ref());
        test_near_seek(e.as_ref());
        test_cf(e.as_ref());
        test_empty_write(e.as_ref());
    }

    #[test]
    fn rocksdb_reopen() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        {
            let e = new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap()),
                               TEST_ENGINE_CFS)
                .unwrap();
            must_put_cf(e.as_ref(), "cf", b"k", b"v1");
        }
        {
            let e = new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap()),
                               TEST_ENGINE_CFS)
                .unwrap();
            assert_has_cf(e.as_ref(), "cf", b"k", b"v1");
        }
    }

    fn must_put(engine: &Engine, key: &[u8], value: &[u8]) {
        engine.put(&Context::new(), make_key(key), value.to_vec()).unwrap();
    }

    fn must_put_cf(engine: &Engine, cf: CfName, key: &[u8], value: &[u8]) {
        engine.put_cf(&Context::new(), cf, make_key(key), value.to_vec()).unwrap();
    }

    fn must_delete(engine: &Engine, key: &[u8]) {
        engine.delete(&Context::new(), make_key(key)).unwrap();
    }

    fn muest_delete_cf(engine: &Engine, cf: CfName, key: &[u8]) {
        engine.delete_cf(&Context::new(), cf, make_key(key)).unwrap();
    }

    fn assert_has(engine: &Engine, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        assert_eq!(snapshot.get(&make_key(key)).unwrap().unwrap(), value);
    }

    fn assert_has_cf(engine: &Engine, cf: CfName, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        assert_eq!(snapshot.get_cf(cf, &make_key(key)).unwrap().unwrap(), value);
    }

    fn assert_none(engine: &Engine, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        assert_eq!(snapshot.get(&make_key(key)).unwrap(), None);
    }

    fn assert_none_cf(engine: &Engine, cf: CfName, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        assert_eq!(snapshot.get_cf(cf, &make_key(key)).unwrap(), None);
    }

    fn assert_seek(engine: &Engine, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut iter = snapshot.iter(None).unwrap();
        iter.seek(&make_key(key)).unwrap();
        assert_eq!((iter.key(), iter.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn assert_near_seek(cursor: &mut Cursor, key: &[u8], pair: (&[u8], &[u8])) {
        assert!(cursor.near_seek(&make_key(key)).unwrap(), escape(key));
        assert_eq!((cursor.key(), cursor.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn assert_near_reverse_seek(cursor: &mut Cursor, key: &[u8], pair: (&[u8], &[u8])) {
        assert!(cursor.near_reverse_seek(&make_key(key)).unwrap(),
                escape(key));
        assert_eq!((cursor.key(), cursor.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn test_get_put(engine: &Engine) {
        assert_none(engine, b"x");
        must_put(engine, b"x", b"1");
        assert_has(engine, b"x", b"1");
        must_put(engine, b"x", b"2");
        assert_has(engine, b"x", b"2");
    }

    fn test_batch(engine: &Engine) {
        engine.write(&Context::new(),
                   vec![Modify::Put(CF_DEFAULT, make_key(b"x"), b"1".to_vec()),
                        Modify::Put(CF_DEFAULT, make_key(b"y"), b"2".to_vec())])
            .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine.write(&Context::new(),
                   vec![Modify::Delete(CF_DEFAULT, make_key(b"x")),
                        Modify::Delete(CF_DEFAULT, make_key(b"y"))])
            .unwrap();
        assert_none(engine, b"y");
        assert_none(engine, b"y");
    }

    fn test_seek(engine: &Engine) {
        must_put(engine, b"x", b"1");
        assert_seek(engine, b"x", (b"x", b"1"));
        assert_seek(engine, b"a", (b"x", b"1"));
        must_put(engine, b"z", b"2");
        assert_seek(engine, b"y", (b"z", b"2"));
        assert_seek(engine, b"x\x00", (b"z", b"2"));
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut iter = snapshot.iter(None).unwrap();
        assert!(!iter.seek(&make_key(b"z\x00")).unwrap());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_near_seek(engine: &Engine) {
        must_put(engine, b"x", b"1");
        must_put(engine, b"z", b"2");
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut cursor = snapshot.iter(None).unwrap();
        let cursor_mut = cursor.as_mut();
        assert_near_seek(cursor_mut, b"x", (b"x", b"1"));
        assert_near_seek(cursor_mut, b"a", (b"x", b"1"));
        assert_near_reverse_seek(cursor_mut, b"z1", (b"z", b"2"));
        assert_near_reverse_seek(cursor_mut, b"x1", (b"x", b"1"));
        assert_near_seek(cursor_mut, b"y", (b"z", b"2"));
        assert_near_seek(cursor_mut, b"x\x00", (b"z", b"2"));
        assert!(!cursor_mut.near_seek(&make_key(b"z\x00")).unwrap());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_cf(engine: &Engine) {
        assert_none_cf(engine, "cf", b"key");
        must_put_cf(engine, "cf", b"key", b"value");
        assert_has_cf(engine, "cf", b"key", b"value");
        muest_delete_cf(engine, "cf", b"key");
        assert_none_cf(engine, "cf", b"key");
    }

    fn test_empty_write(engine: &Engine) {
        engine.write(&Context::new(), vec![]).unwrap();
    }
}
