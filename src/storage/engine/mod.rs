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
    fn iter<'a>(&'a self, upper_bound: Option<&[u8]>, mode: ScanMode) -> Result<Cursor<'a>>;
    #[allow(needless_lifetimes)]
    fn iter_cf<'a>(&'a self,
                   cf: CfName,
                   upper_bound: Option<&[u8]>,
                   mode: ScanMode)
                   -> Result<Cursor<'a>>;
}

pub trait Iterator {
    fn next(&mut self) -> bool;
    fn prev(&mut self) -> bool;
    fn seek(&mut self, key: &Key) -> Result<bool>;
    fn seek_to_first(&mut self) -> bool;
    fn seek_to_last(&mut self) -> bool;
    fn valid(&self) -> bool;

    fn validate_key(&self, _: &Key) -> Result<()> {
        Ok(())
    }

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

macro_rules! near_loop {
    ($cond:expr, $fallback:expr) => ({
        let mut cnt = 0;
        while $cond {
            cnt += 1;
            if cnt >= SEEK_BOUND {
                CURSOR_OVER_SEEK_BOUND_COUNTER.inc();
                return $fallback;
            }
        }
    })
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ScanMode {
    Forward,
    // No Backward here, because we won't just use pure Backward.
    Mixed,
}

pub struct Cursor<'a> {
    iter: Box<Iterator + 'a>,
    scan_mode: ScanMode,
    // the data cursor can be seen will be
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
}

impl<'a> Cursor<'a> {
    pub fn new<T: Iterator + 'a>(iter: T, mode: ScanMode) -> Cursor<'a> {
        Cursor {
            iter: Box::new(iter),
            scan_mode: mode,
            min_key: None,
            max_key: None,
        }
    }

    pub fn seek(&mut self, key: &Key) -> Result<bool> {
        if self.max_key.as_ref().map_or(false, |k| k <= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }

        if self.scan_mode == ScanMode::Forward && self.valid() && self.iter.key() >= key.encoded() {
            return Ok(true);
        }

        if !try!(self.iter.seek(key)) {
            self.max_key = Some(key.encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `seek` instead.
    pub fn near_seek(&mut self, key: &Key) -> Result<bool> {
        if !self.iter.valid() {
            return self.seek(key);
        }
        let ord = self.iter.key().cmp(key.encoded());
        if ord == Ordering::Equal ||
           (self.scan_mode == ScanMode::Forward && ord == Ordering::Greater) {
            return Ok(true);
        }
        if self.max_key.as_ref().map_or(false, |k| k <= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }
        if ord == Ordering::Greater {
            near_loop!(self.iter.prev() && self.iter.key() > key.encoded(),
                       self.seek(key));
            if self.iter.valid() {
                if self.iter.key() < key.encoded() {
                    self.iter.next();
                }
            } else {
                assert!(self.iter.seek_to_first());
                return Ok(true);
            }
        } else {
            // ord == Less
            near_loop!(self.iter.next() && self.iter.key() < key.encoded(),
                       self.seek(key));
        }
        if !self.iter.valid() {
            self.max_key = Some(key.encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Get the value of specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should `seek` first.
    pub fn get(&mut self, key: &Key) -> Result<Option<&[u8]>> {
        if try!(self.near_seek(key)) && self.iter.key() == &**key.encoded() {
            Ok(Some(self.iter.value()))
        } else {
            Ok(None)
        }
    }

    pub fn reverse_seek(&mut self, key: &Key) -> Result<bool> {
        assert!(self.scan_mode != ScanMode::Forward);
        if self.min_key.as_ref().map_or(false, |k| k >= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }
        if !try!(self.iter.seek(key)) && !self.iter.seek_to_last() {
            self.min_key = Some(key.encoded().to_owned());
            if self.max_key.as_ref().map_or(true, |k| k > key.encoded()) {
                self.max_key = Some(key.encoded().to_owned());
            }
            return Ok(false);
        }

        while self.iter.key() >= key.encoded() && self.iter.prev() {
        }

        if !self.iter.valid() {
            self.min_key = Some(key.encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Reverse seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `reverse_seek` instead.
    pub fn near_reverse_seek(&mut self, key: &Key) -> Result<bool> {
        assert!(self.scan_mode != ScanMode::Forward);
        if !self.iter.valid() {
            return self.reverse_seek(key);
        }
        if self.min_key.as_ref().map_or(false, |k| k >= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }

        let ord = self.iter.key().cmp(key.encoded());

        if ord == Ordering::Less {
            near_loop!(self.iter.next() && self.iter.key() < key.encoded(),
                       self.reverse_seek(key));
            if self.iter.valid() {
                self.iter.prev();
            } else {
                assert!(self.iter.seek_to_last());
                return Ok(true);
            }
        } else {
            near_loop!(self.iter.prev() && self.iter.key() >= key.encoded(),
                       self.reverse_seek(key));
        }

        if !self.iter.valid() {
            self.min_key = Some(key.encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        self.iter.key()
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        self.iter.value()
    }

    #[inline]
    pub fn seek_to_first(&mut self) -> bool {
        self.iter.seek_to_first()
    }

    #[inline]
    pub fn seek_to_last(&mut self) -> bool {
        self.iter.seek_to_last()
    }

    #[inline]
    #[allow(should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.iter.next()
    }

    #[inline]
    pub fn prev(&mut self) -> bool {
        self.iter.prev()
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.iter.valid()
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
    use super::SEEK_BOUND;
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
        test_empty_seek(e.as_ref());
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
        let mut iter = snapshot.iter(None, ScanMode::Mixed).unwrap();
        iter.seek(&make_key(key)).unwrap();
        assert_eq!((iter.key(), iter.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn assert_reverse_seek(engine: &Engine, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut iter = snapshot.iter(None, ScanMode::Mixed).unwrap();
        iter.reverse_seek(&make_key(key)).unwrap();
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
        assert_reverse_seek(engine, b"x1", (b"x", b"1"));
        must_put(engine, b"z", b"2");
        assert_seek(engine, b"y", (b"z", b"2"));
        assert_seek(engine, b"x\x00", (b"z", b"2"));
        assert_reverse_seek(engine, b"y", (b"x", b"1"));
        assert_reverse_seek(engine, b"z", (b"x", b"1"));
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut iter = snapshot.iter(None, ScanMode::Mixed).unwrap();
        assert!(!iter.seek(&make_key(b"z\x00")).unwrap());
        assert!(!iter.reverse_seek(&make_key(b"x")).unwrap());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_near_seek(engine: &Engine) {
        must_put(engine, b"x", b"1");
        must_put(engine, b"z", b"2");
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut cursor = snapshot.iter(None, ScanMode::Mixed).unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
        assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
        assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
        assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
        assert!(!cursor.near_seek(&make_key(b"z\x00")).unwrap());
        // Insert many key-values between 'x' and 'z' then near_seek will fallback to seek.
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_put(engine, key.as_bytes(), b"3");
        }
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut cursor = snapshot.iter(None, ScanMode::Mixed).unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"z", (b"z", b"2"));

        must_delete(engine, b"x");
        must_delete(engine, b"z");
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_delete(engine, key.as_bytes());
        }
    }

    fn test_empty_seek(engine: &Engine) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut cursor = snapshot.iter(None, ScanMode::Mixed).unwrap();
        assert!(!cursor.near_reverse_seek(&make_key(b"x")).unwrap());
        assert!(!cursor.near_reverse_seek(&make_key(b"z")).unwrap());
        assert!(!cursor.near_reverse_seek(&make_key(b"w")).unwrap());
        assert!(!cursor.near_seek(&make_key(b"x")).unwrap());
        assert!(!cursor.near_seek(&make_key(b"z")).unwrap());
        assert!(!cursor.near_seek(&make_key(b"w")).unwrap());
    }

    macro_rules! assert_seek {
        ($cursor:ident, $func:ident, $k:expr, $res:ident) => ({
            let msg = format!("assert_seek {} failed exp {:?}", $k, $res);
            assert!($cursor.$func(&$k).unwrap() == $res.is_some(), msg);
            if let Some((ref k, ref v)) = $res {
                assert_eq!($cursor.key(), bytes::encode_bytes(k.as_bytes()).as_slice());
                assert_eq!($cursor.value(), v.as_bytes());
            }
        })
    }

    // use step to controll the distance between target key and current key in cursor.
    fn test_linear_seek(snapshot: &Snapshot, mode: ScanMode, reverse: bool, step: usize) {
        let mut cursor = snapshot.iter(None, mode).unwrap();
        let mut near_cursor = snapshot.iter(None, mode).unwrap();
        let limit = (SEEK_BOUND * 10 + 50 - 1) * 2;

        for (_, mut i) in (0..SEEK_BOUND * 30).enumerate().filter(|&(i, _)| i % step == 0) {
            if reverse {
                i = SEEK_BOUND * 30 - 1 - i;
            }
            let key = format!("key_{:03}", i);
            let seek_key = make_key(key.as_bytes());
            let exp_kv = if i <= 100 {
                if reverse {
                    None
                } else {
                    Some(("key_100".to_owned(), "value_50".to_owned()))
                }
            } else if i <= limit {
                if reverse {
                    Some((format!("key_{}", (i - 1) / 2 * 2), format!("value_{}", (i - 1) / 2)))
                } else {
                    Some((format!("key_{}", (i + 1) / 2 * 2), format!("value_{}", (i + 1) / 2)))
                }
            } else if reverse {
                Some((format!("key_{:03}", limit), format!("value_{:03}", limit / 2)))
            } else {
                None
            };

            if reverse {
                assert_seek!(cursor, reverse_seek, seek_key, exp_kv);
                assert_seek!(near_cursor, near_reverse_seek, seek_key, exp_kv);
            } else {
                assert_seek!(cursor, seek, seek_key, exp_kv);
                assert_seek!(near_cursor, near_seek, seek_key, exp_kv);
            }
        }
    }

    // TODO: refactor engine tests
    #[test]
    fn test_linear() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap()),
                           TEST_ENGINE_CFS)
            .unwrap();
        for i in 50..50 + SEEK_BOUND * 10 {
            let key = format!("key_{}", i * 2);
            let value = format!("value_{}", i);
            must_put(e.as_ref(), key.as_bytes(), value.as_bytes());
        }
        let snapshot = e.snapshot(&Context::new()).unwrap();

        for step in 1..SEEK_BOUND * 3 {
            test_linear_seek(snapshot.as_ref(), ScanMode::Forward, false, step);
        }
        for &reverse in &[true, false] {
            for step in 1..SEEK_BOUND * 3 {
                test_linear_seek(snapshot.as_ref(), ScanMode::Mixed, reverse, step);
            }
        }
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
