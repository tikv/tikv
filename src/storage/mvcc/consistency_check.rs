// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::storage::mvcc::{Lock, LockType, WriteRef, WriteType};
use engine_traits::{
    IterOptions, Iterable, Iterator as EngineIterator, KvEngine, Peekable, SeekKey,
};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use kvproto::kvrpcpb::{MvccInfo, MvccLock, MvccValue, MvccWrite, Op};
use raftstore::coprocessor::{ConsistencyCheckMethod, ConsistencyCheckObserver, Coprocessor};
use raftstore::Result;
use tikv_util::keybuilder::KeyBuilder;
use txn_types::Key;

const PHYSICAL_SHIFT_BITS: usize = 18;
const SAFE_POINT_WINDOW: usize = 120;

// When leader broadcasts a ComputeHash command to followers, it's possible that the safe point
// becomes stale when the command reaches followers. So use a 2 minutes window to reduce this.
fn get_safe_point_for_check(mut safe_point: u64) -> u64 {
    safe_point >>= PHYSICAL_SHIFT_BITS;
    safe_point += (SAFE_POINT_WINDOW * 1000) as u64; // 120s * 1000ms/s.
    safe_point << PHYSICAL_SHIFT_BITS
}

const fn zero_safe_point_for_check() -> u64 {
    let mut safe_point = 0;
    safe_point >>= PHYSICAL_SHIFT_BITS;
    safe_point += (SAFE_POINT_WINDOW * 1000) as u64; // 120s * 1000ms/s.
    safe_point << PHYSICAL_SHIFT_BITS
}

#[derive(Clone)]
pub struct Mvcc<E: KvEngine> {
    _engine: PhantomData<E>,
    local_safe_point: Arc<AtomicU64>,
}

impl<E: KvEngine> Coprocessor for Mvcc<E> {}

impl<E: KvEngine> Mvcc<E> {
    pub fn new(safe_point: Arc<AtomicU64>) -> Self {
        Mvcc {
            _engine: Default::default(),
            local_safe_point: safe_point,
        }
    }
}

impl<E: KvEngine> ConsistencyCheckObserver<E> for Mvcc<E> {
    fn update_context(&self, context: &mut Vec<u8>) -> bool {
        context.push(ConsistencyCheckMethod::Mvcc as u8);
        context.reserve(8);
        let len = context.len();

        let mut safe_point = self.local_safe_point.load(AtomicOrdering::Acquire);
        safe_point = get_safe_point_for_check(safe_point);
        unsafe {
            context.set_len(len + 8);
            std::ptr::copy_nonoverlapping(
                safe_point.to_le_bytes().as_ptr(),
                &mut context[len] as _,
                8,
            );
        }
        // Skiped all other observers.
        true
    }

    fn compute_hash(
        &self,
        region: &kvproto::metapb::Region,
        context: &mut &[u8],
        snap: &E::Snapshot,
    ) -> Result<Option<u32>> {
        if context.is_empty() {
            return Ok(None);
        }
        assert_eq!(context[0], ConsistencyCheckMethod::Mvcc as u8);
        let safe_point = u64::from_le_bytes(context[1..9].try_into().unwrap());
        *context = &context[9..];

        let local_safe_point = self.local_safe_point.load(AtomicOrdering::Acquire);
        if safe_point < local_safe_point || safe_point <= zero_safe_point_for_check() {
            warn!(
                "skip consistency check"; "region_id" => region.get_id(),
                "safe_ponit" => safe_point,
                "local_safe_point" => local_safe_point,
                "zero" => zero_safe_point_for_check(),
            );
            return Ok(None);
        }

        let mut scanner = MvccInfoScanner::new(
            |cf, opts| snap.iterator_cf_opt(cf, opts).map_err(|e| box_err!(e)),
            Some(&keys::data_key(region.get_start_key())),
            Some(&keys::data_end_key(region.get_end_key())),
            MvccChecksum::new(safe_point),
        )?;
        while scanner.next_item()?.is_some() {}

        // Computes the hash from the Region state too.
        let mut digest = scanner.observer.digest;
        let region_state_key = keys::region_state_key(region.get_id());
        digest.update(&region_state_key);
        match snap.get_value_cf(CF_RAFT, &region_state_key) {
            Err(e) => return Err(e.into()),
            Ok(Some(v)) => digest.update(&v),
            Ok(None) => {}
        }
        Ok(Some(digest.finalize()))
    }
}

pub trait MvccInfoObserver {
    type Target;

    // Meet a new mvcc record prefixed `key`.
    fn on_new_item(&mut self, key: &[u8]);
    // Emit a complete mvcc record.
    fn emit(&mut self) -> Self::Target;

    fn on_write(&mut self, key: &[u8], value: &[u8]) -> Result<bool>;
    fn on_lock(&mut self, key: &[u8], value: &[u8]) -> Result<bool>;
    fn on_default(&mut self, key: &[u8], value: &[u8]) -> Result<bool>;
}

pub struct MvccInfoScanner<Iter: EngineIterator, Ob: MvccInfoObserver> {
    lock_iter: Iter,
    default_iter: Iter,
    write_iter: Iter,
    observer: Ob,
}

impl<Iter: EngineIterator, Ob: MvccInfoObserver> MvccInfoScanner<Iter, Ob> {
    pub fn new<F>(f: F, from: Option<&[u8]>, to: Option<&[u8]>, ob: Ob) -> Result<Self>
    where
        F: Fn(&str, IterOptions) -> Result<Iter>,
    {
        let from = from.unwrap_or(keys::DATA_MIN_KEY);
        let to = to.unwrap_or(keys::DATA_MAX_KEY);
        let key_builder = |key: &[u8]| -> Result<Option<KeyBuilder>> {
            if !keys::validate_data_key(key) && key != keys::DATA_MAX_KEY {
                return Err(box_err!("non-mvcc area {}", log_wrappers::Value::key(key)));
            }
            Ok(Some(KeyBuilder::from_vec(key.to_vec(), 0, 0)))
        };

        let iter_opts = IterOptions::new(key_builder(from)?, key_builder(to)?, false);
        let gen_iter = |cf: &str| -> Result<Iter> {
            let mut iter = f(cf, iter_opts.clone())?;
            box_try!(iter.seek(SeekKey::Key(from)));
            Ok(iter)
        };

        Ok(MvccInfoScanner {
            lock_iter: gen_iter(CF_LOCK)?,
            default_iter: gen_iter(CF_DEFAULT)?,
            write_iter: gen_iter(CF_WRITE)?,
            observer: ob,
        })
    }

    fn next_item(&mut self) -> Result<Option<Ob::Target>> {
        let mut lock_ok = box_try!(self.lock_iter.valid());
        let mut writes_ok = box_try!(self.write_iter.valid());

        let prefix = match (lock_ok, writes_ok) {
            (false, false) => return Ok(None),
            (true, false) => self.lock_iter.key(),
            (false, true) => box_try!(Key::truncate_ts_for(self.write_iter.key())),
            (true, true) => {
                let prefix1 = self.lock_iter.key();
                let prefix2 = box_try!(Key::truncate_ts_for(self.write_iter.key()));
                match prefix1.cmp(prefix2) {
                    Ordering::Less => {
                        writes_ok = false;
                        prefix1
                    }
                    Ordering::Greater => {
                        lock_ok = false;
                        prefix2
                    }
                    Ordering::Equal => prefix1,
                }
            }
        };
        self.observer.on_new_item(prefix);

        while writes_ok {
            let (key, value) = (self.write_iter.key(), self.write_iter.value());
            writes_ok = self.observer.on_write(key, value)? && box_try!(self.write_iter.next());
        }
        while lock_ok {
            let (key, value) = (self.lock_iter.key(), self.lock_iter.value());
            lock_ok = self.observer.on_lock(key, value)? && box_try!(self.lock_iter.next());
        }

        let mut ok = box_try!(self.default_iter.valid());
        while ok {
            let (key, value) = (self.default_iter.key(), self.default_iter.value());
            ok = self.observer.on_default(key, value)? && box_try!(self.default_iter.next());
        }

        Ok(Some(self.observer.emit()))
    }
}

#[derive(Clone, Default)]
struct MvccInfoCollector {
    current_item: Vec<u8>,
    mvcc_info: MvccInfo,
}

impl MvccInfoObserver for MvccInfoCollector {
    type Target = (Vec<u8>, MvccInfo);

    fn on_new_item(&mut self, key: &[u8]) {
        self.current_item = key.to_vec();
    }

    fn emit(&mut self) -> Self::Target {
        let item = std::mem::take(&mut self.current_item);
        let info = std::mem::take(&mut self.mvcc_info);
        (item, info)
    }

    fn on_write(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let (prefix, commit_ts) = box_try!(Key::split_on_ts_for(key));
        if prefix != AsRef::<[u8]>::as_ref(&self.current_item) {
            return Ok(false);
        }

        let write = box_try!(WriteRef::parse(&value));
        let mut write_info = MvccWrite::default();
        match write.write_type {
            WriteType::Put => write_info.set_type(Op::Put),
            WriteType::Delete => write_info.set_type(Op::Del),
            WriteType::Lock => write_info.set_type(Op::Lock),
            WriteType::Rollback => write_info.set_type(Op::Rollback),
        }
        write_info.set_start_ts(write.start_ts.into_inner());
        write_info.set_commit_ts(commit_ts.into_inner());
        if let Some(ref value) = write.short_value {
            write_info.set_short_value(value.to_vec());
        }

        self.mvcc_info.mut_writes().push(write_info);
        Ok(true)
    }

    fn on_lock(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        if key != AsRef::<[u8]>::as_ref(&self.current_item) {
            return Ok(false);
        }

        let lock = box_try!(Lock::parse(value));
        let mut lock_info = MvccLock::default();
        match lock.lock_type {
            LockType::Put => lock_info.set_type(Op::Put),
            LockType::Delete => lock_info.set_type(Op::Del),
            LockType::Lock => lock_info.set_type(Op::Lock),
            LockType::Pessimistic => lock_info.set_type(Op::PessimisticLock),
        }
        lock_info.set_start_ts(lock.ts.into_inner());
        lock_info.set_primary(lock.primary);
        lock_info.set_short_value(lock.short_value.unwrap_or_default());

        self.mvcc_info.set_lock(lock_info);
        Ok(true)
    }

    fn on_default(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let (prefix, start_ts) = box_try!(Key::split_on_ts_for(key));
        if prefix != AsRef::<[u8]>::as_ref(&self.current_item) {
            return Ok(false);
        }

        let mut value_info = MvccValue::default();
        value_info.set_start_ts(start_ts.into_inner());
        value_info.set_value(value.to_vec());

        self.mvcc_info.mut_values().push(value_info);
        Ok(true)
    }
}

pub struct MvccInfoIterator<Iter: EngineIterator> {
    scanner: MvccInfoScanner<Iter, MvccInfoCollector>,
    limit: usize,
    count: usize,
}

impl<Iter: EngineIterator> MvccInfoIterator<Iter> {
    pub fn new<F>(f: F, from: Option<&[u8]>, to: Option<&[u8]>, limit: usize) -> Result<Self>
    where
        F: Fn(&str, IterOptions) -> Result<Iter>,
    {
        let scanner = MvccInfoScanner::new(f, from, to, MvccInfoCollector::default())?;
        Ok(Self {
            scanner,
            limit,
            count: 0,
        })
    }
}

impl<Iter: EngineIterator> Iterator for MvccInfoIterator<Iter> {
    type Item = Result<(Vec<u8>, MvccInfo)>;

    fn next(&mut self) -> Option<Result<(Vec<u8>, MvccInfo)>> {
        if self.limit != 0 && self.count >= self.limit {
            return None;
        }

        match self.scanner.next_item() {
            Ok(Some(item)) => {
                self.count += 1;
                Some(Ok(item))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

struct MvccChecksum {
    safe_point: u64,
    digest: crc32fast::Hasher,
    current_item: Vec<u8>,
    committed_txns: Vec<u64>,
    committed_txns_sorted: bool,
}

impl MvccChecksum {
    fn new(safe_point: u64) -> Self {
        Self {
            safe_point,
            digest: crc32fast::Hasher::new(),
            current_item: vec![],
            committed_txns: vec![],
            committed_txns_sorted: false,
        }
    }
}

impl MvccInfoObserver for MvccChecksum {
    type Target = ();

    fn on_new_item(&mut self, key: &[u8]) {
        self.current_item = key.to_vec();
    }

    fn emit(&mut self) -> Self::Target {
        self.current_item.clear();
        self.committed_txns.clear();
    }

    fn on_write(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let (prefix, commit_ts) = box_try!(Key::split_on_ts_for(key));
        if prefix != AsRef::<[u8]>::as_ref(&self.current_item) {
            return Ok(false);
        }

        let commit_ts = commit_ts.into_inner();
        if commit_ts <= self.safe_point {
            // Skip stale records.
            return Ok(true);
        }

        let write = box_try!(WriteRef::parse(&value));
        let start_ts = write.start_ts.into_inner();

        self.digest.update(key);
        self.digest.update(value);
        self.committed_txns.push(start_ts);
        Ok(true)
    }

    fn on_lock(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let lock = box_try!(Lock::parse(value));
        if lock.ts.into_inner() <= self.safe_point {
            // Skip stale records.
            return Ok(true);
        }

        self.digest.update(key);
        self.digest.update(value);
        Ok(true)
    }

    fn on_default(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let (prefix, start_ts) = box_try!(Key::split_on_ts_for(key));
        if prefix != AsRef::<[u8]>::as_ref(&self.current_item) {
            return Ok(false);
        }

        if !self.committed_txns_sorted {
            self.committed_txns.sort_unstable();
            self.committed_txns_sorted = true;
        }

        let start_ts = start_ts.into_inner();
        if start_ts > self.safe_point && self.committed_txns.binary_search(&start_ts).is_ok() {
            self.digest.update(key);
            self.digest.update(value);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::TestEngineBuilder;
    use crate::storage::txn::tests::must_rollback;
    use crate::storage::txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put};
    use engine_test::kv::KvTestEngine;

    #[test]
    fn test_update_context() {
        let safe_point = Arc::new(AtomicU64::new((123 << PHYSICAL_SHIFT_BITS) * 1000));
        let observer = Mvcc::<KvTestEngine>::new(safe_point);

        let mut context = Vec::new();
        assert!(observer.update_context(&mut context));
        assert_eq!(context.len(), 9);
        assert_eq!(context[0], ConsistencyCheckMethod::Mvcc as u8);
        let safe_point = u64::from_le_bytes(context[1..9].try_into().unwrap());
        assert_eq!(safe_point, (243 << PHYSICAL_SHIFT_BITS) * 1000);
    }

    #[test]
    fn test_mvcc_checksum() {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"zAAAAA", b"value", b"PRIMARY", 100);
        must_commit(&engine, b"zAAAAA", 100, 101);
        must_prewrite_put(&engine, b"zCCCCC", b"value", b"PRIMARY", 110);
        must_commit(&engine, b"zCCCCC", 110, 111);

        must_prewrite_put(&engine, b"zBBBBB", b"value", b"PRIMARY", 200);
        must_commit(&engine, b"zBBBBB", 200, 201);
        must_prewrite_put(&engine, b"zDDDDD", b"value", b"PRIMARY", 200);
        must_rollback(&engine, b"zDDDDD", 200, false);
        must_prewrite_put(&engine, b"zFFFFF", b"value", b"PRIMARY", 200);
        must_prewrite_delete(&engine, b"zGGGGG", b"PRIMARY", 200);

        let mut checksums = Vec::with_capacity(3);
        for &safe_point in &[150, 160, 100] {
            let raw = engine.get_rocksdb();
            let mut scanner = MvccInfoScanner::new(
                |cf, opts| raw.iterator_cf_opt(cf, opts).map_err(|e| box_err!(e)),
                Some(&keys::data_key(b"")),
                Some(&keys::data_end_key(b"")),
                MvccChecksum::new(safe_point),
            )
            .unwrap();
            while scanner.next_item().unwrap().is_some() {}
            let digest = scanner.observer.digest;
            checksums.push(digest.finalize());
        }
        assert_eq!(checksums[0], checksums[1]);
        assert_ne!(checksums[0], checksums[2]);
    }

    #[test]
    fn test_mvcc_info_collector() {
        use crate::storage::mvcc::Write;
        use engine_test::ctor::{CFOptions, ColumnFamilyOptions, DBOptions};
        use engine_traits::SyncMutable;
        use txn_types::TimeStamp;

        let tmp = tempfile::Builder::new()
            .prefix("test_debug")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();
        let engine = engine_test::kv::new_engine_opt(
            path,
            DBOptions::new(),
            vec![
                CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
                CFOptions::new(CF_WRITE, ColumnFamilyOptions::new()),
                CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
                CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
            ],
        )
        .unwrap();

        let cf_default_data = vec![
            (b"k1", b"v", 5.into()),
            (b"k2", b"x", 10.into()),
            (b"k3", b"y", 15.into()),
        ];
        for &(prefix, value, ts) in &cf_default_data {
            let encoded_key = Key::from_raw(prefix).append_ts(ts);
            let key = keys::data_key(encoded_key.as_encoded().as_slice());
            engine.put(key.as_slice(), value).unwrap();
        }

        let cf_lock_data = vec![
            (b"k1", LockType::Put, b"v", 5.into()),
            (b"k4", LockType::Lock, b"x", 10.into()),
            (b"k5", LockType::Delete, b"y", 15.into()),
        ];
        for &(prefix, tp, value, version) in &cf_lock_data {
            let encoded_key = Key::from_raw(prefix);
            let key = keys::data_key(encoded_key.as_encoded().as_slice());
            let lock = Lock::new(
                tp,
                value.to_vec(),
                version,
                0,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            );
            let value = lock.to_bytes();
            engine
                .put_cf(CF_LOCK, key.as_slice(), value.as_slice())
                .unwrap();
        }

        let cf_write_data = vec![
            (b"k2", WriteType::Put, 5.into(), 10.into()),
            (b"k3", WriteType::Put, 15.into(), 20.into()),
            (b"k6", WriteType::Lock, 25.into(), 30.into()),
            (b"k7", WriteType::Rollback, 35.into(), 40.into()),
        ];
        for &(prefix, tp, start_ts, commit_ts) in &cf_write_data {
            let encoded_key = Key::from_raw(prefix).append_ts(commit_ts);
            let key = keys::data_key(encoded_key.as_encoded().as_slice());
            let write = Write::new(tp, start_ts, None);
            let value = write.as_ref().to_bytes();
            engine
                .put_cf(CF_WRITE, key.as_slice(), value.as_slice())
                .unwrap();
        }

        let scan_mvcc = |start: &[u8], end: &[u8], limit: u64| {
            MvccInfoIterator::new(
                |cf, opts| engine.iterator_cf_opt(cf, opts).map_err(|e| box_err!(e)),
                if start.is_empty() { None } else { Some(start) },
                if end.is_empty() { None } else { Some(end) },
                limit as usize,
            )
            .unwrap()
        };

        let mut count = 0;
        for key_and_mvcc in scan_mvcc(b"z", &[], 30) {
            assert!(key_and_mvcc.is_ok());
            count += 1;
        }
        assert_eq!(count, 7);
    }
}
