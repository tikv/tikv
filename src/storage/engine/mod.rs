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
use storage::{Key, Value, CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::Context;
use kvproto::errorpb::Error as ErrorHeader;

mod rocksdb;
pub mod raftkv;
mod metrics;
use self::metrics::*;
use super::super::raftstore::store::engine::IterOption;

// only used for rocksdb without persistent.
pub const TEMP_DIR: &'static str = "";

const SEEK_BOUND: usize = 30;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

pub type Callback<T> = Box<FnBox((CbContext, Result<T>)) + Send>;

pub struct CbContext {
    pub term: Option<u64>,
}

impl CbContext {
    fn new() -> CbContext {
        CbContext { term: None }
    }
}

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
        match wait_op!(|cb| self.async_write(ctx, batch, cb).unwrap(), timeout) {
            Some((_, res)) => res,
            None => Err(Error::Timeout(timeout)),
        }
    }

    fn snapshot(&self, ctx: &Context) -> Result<Box<Snapshot>> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        match wait_op!(|cb| self.async_snapshot(ctx, cb).unwrap(), timeout) {
            Some((_, res)) => res,
            None => Err(Error::Timeout(timeout)),
        }
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
    fn iter<'a>(&'a self, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor<'a>>;
    #[allow(needless_lifetimes)]
    fn iter_cf<'a>(&'a self,
                   cf: CfName,
                   iter_opt: IterOption,
                   mode: ScanMode)
                   -> Result<Cursor<'a>>;
    fn clone(&self) -> Box<Snapshot>;
}

pub trait Iterator {
    fn next(&mut self) -> bool;
    fn prev(&mut self) -> bool;
    fn seek(&mut self, key: &Key) -> Result<bool>;
    fn seek_for_prev(&mut self, key: &Key) -> Result<bool>;
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
    Backward,
    Mixed,
}

/// Statistics collects the ops taken when fetching data.
#[derive(Default)]
pub struct CFStatistics {
    // How many keys that's effective to user. This counter should be increased
    // by the caller.
    pub processed: usize,
    pub get: usize,
    pub next: usize,
    pub prev: usize,
    pub seek: usize,
    pub seek_for_prev: usize,
}

impl CFStatistics {
    #[inline]
    pub fn total_op_count(&self) -> usize {
        self.get + self.next + self.prev + self.seek + self.seek_for_prev
    }

    // Calculate how the operation performs.
    #[inline]
    pub fn inefficiency(&self) -> f64 {
        let total_op_cnt = self.total_op_count();
        if total_op_cnt == 0 {
            0f64
        } else {
            (total_op_cnt as f64 - self.processed as f64) / total_op_cnt as f64
        }
    }
}

#[derive(Default)]
pub struct Statistics {
    pub lock: CFStatistics,
    pub write: CFStatistics,
    pub data: CFStatistics,
}

impl Statistics {
    pub fn total_op_count(&self) -> Vec<(&str, usize)> {
        vec![(CF_DEFAULT, self.data.total_op_count()),
             (CF_LOCK, self.lock.total_op_count()),
             (CF_WRITE, self.write.total_op_count())]
    }

    pub fn inefficiency(&self) -> Vec<(&str, f64)> {
        vec![(CF_DEFAULT, self.data.inefficiency()),
             (CF_LOCK, self.lock.inefficiency()),
             (CF_WRITE, self.write.inefficiency())]
    }
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

    pub fn seek(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Backward);
        if self.max_key.as_ref().map_or(false, |k| k <= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }

        if self.scan_mode == ScanMode::Forward && self.valid() &&
           self.iter.key() >= key.encoded().as_slice() {
            return Ok(true);
        }

        statistics.seek += 1;

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
    pub fn near_seek(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Backward);
        if !self.iter.valid() {
            return self.seek(key, statistics);
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
            near_loop!(self.prev(statistics) && self.iter.key() > key.encoded().as_slice(),
                       self.seek(key, statistics));
            if self.iter.valid() {
                if self.iter.key() < key.encoded().as_slice() {
                    self.next(statistics);
                }
            } else {
                assert!(self.seek_to_first(statistics));
                return Ok(true);
            }
        } else {
            // ord == Less
            near_loop!(self.next(statistics) && self.iter.key() < key.encoded().as_slice(),
                       self.seek(key, statistics));
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
    pub fn get(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<Option<&[u8]>> {
        if self.scan_mode != ScanMode::Backward {
            if try!(self.near_seek(key, statistics)) && self.iter.key() == &**key.encoded() {
                return Ok(Some(self.iter.value()));
            }
            return Ok(None);
        }
        if try!(self.near_seek_for_prev(key, statistics)) && self.iter.key() == &**key.encoded() {
            return Ok(Some(self.iter.value()));
        }
        Ok(None)
    }

    fn seek_for_prev(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Forward);
        if self.min_key.as_ref().map_or(false, |k| k >= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }

        if self.scan_mode == ScanMode::Backward && self.valid() &&
           self.iter.key() <= key.encoded().as_slice() {
            return Ok(true);
        }

        statistics.seek_for_prev += 1;
        if !try!(self.iter.seek_for_prev(key)) {
            self.min_key = Some(key.encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    /// Find the largest key that is not greater than the specific key.
    pub fn near_seek_for_prev(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<bool> {
        assert_ne!(self.scan_mode, ScanMode::Forward);
        if !self.iter.valid() {
            return self.seek_for_prev(key, statistics);
        }
        let ord = self.iter.key().cmp(key.encoded());
        if ord == Ordering::Equal ||
           (self.scan_mode == ScanMode::Backward && ord == Ordering::Less) {
            return Ok(true);
        }

        if self.min_key.as_ref().map_or(false, |k| k >= key.encoded()) {
            try!(self.iter.validate_key(key));
            return Ok(false);
        }

        if ord == Ordering::Less {
            near_loop!(self.next(statistics) && self.iter.key() < key.encoded().as_slice(),
                       self.seek_for_prev(key, statistics));
            if self.iter.valid() {
                if self.iter.key() > key.encoded().as_slice() {
                    self.prev(statistics);
                }
            } else {
                assert!(self.seek_to_last(statistics));
                return Ok(true);
            }
        } else {
            near_loop!(self.prev(statistics) && self.iter.key() > key.encoded().as_slice(),
                       self.seek_for_prev(key, statistics));
        }

        if !self.iter.valid() {
            self.min_key = Some(key.encoded().to_owned());
            return Ok(false);
        }
        Ok(true)
    }

    pub fn reverse_seek(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<bool> {
        if !try!(self.seek_for_prev(key, statistics)) {
            return Ok(false);
        }

        if self.iter.key() == &**key.encoded() {
            // should not update min_key here. otherwise reverse_seek_le may not
            // work as expected.
            return Ok(self.prev(statistics));
        }

        Ok(true)
    }

    /// Reverse seek the specified key.
    ///
    /// This method assume the current position of cursor is
    /// around `key`, otherwise you should use `reverse_seek` instead.
    pub fn near_reverse_seek(&mut self, key: &Key, statistics: &mut CFStatistics) -> Result<bool> {
        if !try!(self.near_seek_for_prev(key, statistics)) {
            return Ok(false);
        }

        if self.iter.key() == &**key.encoded() {
            return Ok(self.prev(statistics));
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
    pub fn seek_to_first(&mut self, statistics: &mut CFStatistics) -> bool {
        statistics.seek += 1;
        self.iter.seek_to_first()
    }

    #[inline]
    pub fn seek_to_last(&mut self, statistics: &mut CFStatistics) -> bool {
        statistics.seek += 1;
        self.iter.seek_to_last()
    }

    #[inline]
    pub fn next(&mut self, statistics: &mut CFStatistics) -> bool {
        statistics.next += 1;
        self.iter.next()
    }

    #[inline]
    pub fn prev(&mut self, statistics: &mut CFStatistics) -> bool {
        statistics.prev += 1;
        self.iter.prev()
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.iter.valid()
    }
}

/// Create a local Rocskdb engine. (Whihout raft, mainly for tests).
pub fn new_local_engine(path: &str, cfs: &[CfName]) -> Result<Box<Engine>> {
    EngineRocksdb::new(path, cfs).map(|engine| -> Box<Engine> { Box::new(engine) })
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
    use super::super::super::raftstore::store::engine::IterOption;

    const TEST_ENGINE_CFS: &'static [CfName] = &["cf"];

    #[test]
    fn rocksdb() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = new_local_engine(dir.path().to_str().unwrap(), TEST_ENGINE_CFS).unwrap();

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
            let e = new_local_engine(dir.path().to_str().unwrap(), TEST_ENGINE_CFS).unwrap();
            must_put_cf(e.as_ref(), "cf", b"k", b"v1");
        }
        {
            let e = new_local_engine(dir.path().to_str().unwrap(), TEST_ENGINE_CFS).unwrap();
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
        let mut iter = snapshot.iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        iter.seek(&make_key(key), &mut statistics).unwrap();
        assert_eq!((iter.key(), iter.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn assert_reverse_seek(engine: &Engine, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut iter = snapshot.iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        iter.reverse_seek(&make_key(key), &mut statistics).unwrap();
        assert_eq!((iter.key(), iter.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn assert_near_seek(cursor: &mut Cursor, key: &[u8], pair: (&[u8], &[u8])) {
        let mut statistics = CFStatistics::default();
        assert!(cursor.near_seek(&make_key(key), &mut statistics).unwrap(),
                escape(key));
        assert_eq!((cursor.key(), cursor.value()),
                   (&*bytes::encode_bytes(pair.0), pair.1));
    }

    fn assert_near_reverse_seek(cursor: &mut Cursor, key: &[u8], pair: (&[u8], &[u8])) {
        let mut statistics = CFStatistics::default();
        assert!(cursor.near_reverse_seek(&make_key(key), &mut statistics).unwrap(),
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
        let mut iter = snapshot.iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        assert!(!iter.seek(&make_key(b"z\x00"), &mut statistics).unwrap());
        assert!(!iter.reverse_seek(&make_key(b"x"), &mut statistics).unwrap());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_near_seek(engine: &Engine) {
        must_put(engine, b"x", b"1");
        must_put(engine, b"z", b"2");
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut cursor = snapshot.iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
        assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
        assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
        assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
        let mut statistics = CFStatistics::default();
        assert!(!cursor.near_seek(&make_key(b"z\x00"), &mut statistics).unwrap());
        // Insert many key-values between 'x' and 'z' then near_seek will fallback to seek.
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_put(engine, key.as_bytes(), b"3");
        }
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut cursor = snapshot.iter(IterOption::default(), ScanMode::Mixed).unwrap();
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
        let mut cursor = snapshot.iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        assert!(!cursor.near_reverse_seek(&make_key(b"x"), &mut statistics).unwrap());
        assert!(!cursor.near_reverse_seek(&make_key(b"z"), &mut statistics).unwrap());
        assert!(!cursor.near_reverse_seek(&make_key(b"w"), &mut statistics).unwrap());
        assert!(!cursor.near_seek(&make_key(b"x"), &mut statistics).unwrap());
        assert!(!cursor.near_seek(&make_key(b"z"), &mut statistics).unwrap());
        assert!(!cursor.near_seek(&make_key(b"w"), &mut statistics).unwrap());
    }

    macro_rules! assert_seek {
        ($cursor:ident, $func:ident, $k:expr, $res:ident) => ({
            let mut statistics = CFStatistics::default();
            assert_eq!($cursor.$func(&$k, &mut statistics).unwrap(), $res.is_some(),
                       "assert_seek {} failed exp {:?}", $k, $res);
            if let Some((ref k, ref v)) = $res {
                assert_eq!($cursor.key(), bytes::encode_bytes(k.as_bytes()).as_slice());
                assert_eq!($cursor.value(), v.as_bytes());
            }
        })
    }

    #[derive(PartialEq, Eq, Clone, Copy)]
    enum SeekMode {
        Normal,
        Reverse,
        ForPrev,
    }

    #[allow(cyclomatic_complexity)]
    // use step to controll the distance between target key and current key in cursor.
    fn test_linear_seek(snapshot: &Snapshot,
                        mode: ScanMode,
                        seek_mode: SeekMode,
                        start: usize,
                        step: usize) {
        let mut cursor = snapshot.iter(IterOption::default(), mode)
            .unwrap();
        let mut near_cursor = snapshot.iter(IterOption::default(), mode).unwrap();
        let limit = (SEEK_BOUND * 10 + 50 - 1) * 2;

        for (_, mut i) in (start..SEEK_BOUND * 30).enumerate().filter(|&(i, _)| i % step == 0) {
            if seek_mode != SeekMode::Normal {
                i = SEEK_BOUND * 30 - 1 - i;
            }
            let key = format!("key_{:03}", i);
            let seek_key = make_key(key.as_bytes());
            let exp_kv = if i <= 100 {
                match seek_mode {
                    SeekMode::Reverse => None,
                    SeekMode::ForPrev if i < 100 => None,
                    SeekMode::Normal | SeekMode::ForPrev => {
                        Some(("key_100".to_owned(), "value_50".to_owned()))
                    }
                }
            } else if i <= limit {
                if seek_mode == SeekMode::Reverse {
                    Some((format!("key_{}", (i - 1) / 2 * 2), format!("value_{}", (i - 1) / 2)))
                } else if seek_mode == SeekMode::ForPrev {
                    Some((format!("key_{}", i / 2 * 2), format!("value_{}", i / 2)))
                } else {
                    Some((format!("key_{}", (i + 1) / 2 * 2), format!("value_{}", (i + 1) / 2)))
                }
            } else if seek_mode != SeekMode::Normal {
                Some((format!("key_{:03}", limit), format!("value_{:03}", limit / 2)))
            } else {
                None
            };

            match seek_mode {
                SeekMode::Reverse => {
                    assert_seek!(cursor, reverse_seek, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_reverse_seek, seek_key, exp_kv);
                }
                SeekMode::Normal => {
                    assert_seek!(cursor, seek, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_seek, seek_key, exp_kv);
                }
                SeekMode::ForPrev => {
                    assert_seek!(cursor, seek_for_prev, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_seek_for_prev, seek_key, exp_kv);
                }
            }
        }
    }

    // TODO: refactor engine tests
    #[test]
    fn test_linear() {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let e = new_local_engine(dir.path().to_str().unwrap(), TEST_ENGINE_CFS).unwrap();
        for i in 50..50 + SEEK_BOUND * 10 {
            let key = format!("key_{}", i * 2);
            let value = format!("value_{}", i);
            must_put(e.as_ref(), key.as_bytes(), value.as_bytes());
        }
        let snapshot = e.snapshot(&Context::new()).unwrap();

        for step in 1..SEEK_BOUND * 3 {
            for start in 0..10 {
                test_linear_seek(snapshot.as_ref(),
                                 ScanMode::Forward,
                                 SeekMode::Normal,
                                 start * SEEK_BOUND,
                                 step);
                test_linear_seek(snapshot.as_ref(),
                                 ScanMode::Backward,
                                 SeekMode::Reverse,
                                 start * SEEK_BOUND,
                                 step);
                test_linear_seek(snapshot.as_ref(),
                                 ScanMode::Backward,
                                 SeekMode::ForPrev,
                                 start * SEEK_BOUND,
                                 step);
            }
        }
        for &seek_mode in &[SeekMode::Reverse, SeekMode::Normal, SeekMode::ForPrev] {
            for step in 1..SEEK_BOUND * 3 {
                for start in 0..10 {
                    test_linear_seek(snapshot.as_ref(),
                                     ScanMode::Mixed,
                                     seek_mode,
                                     start * SEEK_BOUND,
                                     step);
                }
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
