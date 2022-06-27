// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, marker::PhantomData};

use bytes::{Buf, Bytes};
use kvengine::Item;
use kvproto::kvrpcpb::IsolationLevel;
use rfstore::UserMeta;
use tikv_kv::{Snapshot, Statistics};
use txn_types::{is_short_value, Key, Lock, OldValue, TimeStamp, TsSet, Value, Write, WriteType};

use crate::storage::{
    mvcc,
    mvcc::NewerTsCheckState,
    txn::{Error, ErrorInner, Result, TxnEntry, TxnEntryStore},
    REQUEST_EXCEED_BOUND,
};

pub struct CloudStore<S: Snapshot> {
    marker: PhantomData<S>,
    snapshot: kvengine::SnapAccess,
    start_ts: u64,
    bypass_locks: TsSet,
    fill_cache: bool,
    stats: Statistics,
}

const WRITE_CF: usize = 0;
const LOCK_CF: usize = 1;

impl<S: Snapshot> super::Store for CloudStore<S> {
    type Scanner = CloudStoreScanner;

    fn get(&self, user_key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let item = Self::get_inner(
            user_key,
            &self.snapshot,
            self.start_ts,
            &self.bypass_locks,
            statistics,
        )?;
        if item.value_len() > 0 {
            Ok(Some(item.get_value().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn incremental_get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        let stat = &mut self.stats;
        let item = Self::get_inner(
            user_key,
            &self.snapshot,
            self.start_ts,
            &self.bypass_locks,
            stat,
        )?;
        if item.value_len() > 0 {
            Ok(Some(item.get_value().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn incremental_get_take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }

    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Vec<Statistics>,
    ) -> Result<Vec<Result<Option<Value>>>> {
        let mut res_vec = Vec::with_capacity(keys.len());
        for key in keys {
            let mut stats = Statistics::default();
            let res = self.get(key, &mut stats);
            res_vec.push(res);
            statistics.push(stats);
        }
        Ok(res_vec)
    }

    fn scanner(
        &self,
        desc: bool,
        _key_only: bool,
        _check_has_newer_ts_data: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner> {
        self.scanner_inner(desc, lower_bound, upper_bound, false)
    }
}

impl<S: Snapshot> CloudStore<S> {
    pub fn new(snapshot: S, start_ts: u64, bypass_locks: TsSet, fill_cache: bool) -> Self {
        Self {
            marker: PhantomData::default(),
            snapshot: snapshot.get_kvengine_snap().unwrap().clone(),
            start_ts,
            bypass_locks,
            fill_cache,
            stats: Statistics::default(),
        }
    }

    fn get_inner<'a>(
        user_key: &Key,
        snap: &'a kvengine::SnapAccess,
        start_ts: u64,
        bypass_locks: &TsSet,
        statistics: &mut Statistics,
    ) -> mvcc::Result<Item<'a>> {
        let raw_key = user_key.to_raw()?;
        let item = snap.get(LOCK_CF, &raw_key, 0);
        statistics.lock.get += 1;
        statistics.lock.flow_stats.read_keys += 1;
        statistics.lock.flow_stats.read_bytes += raw_key.len() + item.value_len();
        statistics.lock.processed_keys += 1;
        if item.value_len() > 0 {
            let lock = Lock::parse(item.get_value()).unwrap();
            Lock::check_ts_conflict(
                Cow::Borrowed(&lock),
                user_key,
                TimeStamp::new(start_ts),
                bypass_locks,
                IsolationLevel::Si,
            )?;
        }
        if snap.get_start_key() > raw_key.as_slice() || snap.get_end_key() <= raw_key.as_slice() {
            panic!(
                "get key {:?} out of snap range {:?}, {:?}, {}:{}",
                raw_key.as_slice(),
                snap.get_start_key(),
                snap.get_end_key(),
                snap.get_id(),
                snap.get_version()
            );
        }
        let item = snap.get(WRITE_CF, &raw_key, start_ts);
        statistics.write.get += 1;
        statistics.write.flow_stats.read_keys += 1;
        statistics.write.flow_stats.read_bytes += user_key.len() + item.value_len();
        statistics.write.processed_keys += 1;
        statistics.processed_size += user_key.len() + item.value_len();
        Ok(item)
    }

    fn check_locks(
        &self,
        mut lock_iter: kvengine::read::Iterator,
        lower_bound: Option<Bytes>,
        upper_bound: Option<Bytes>,
        stats: &mut Statistics,
    ) -> mvcc::Result<()> {
        if lock_iter.is_reverse() {
            if let Some(lower) = lower_bound {
                lock_iter.set_bound(lower, false);
            }
            if let Some(upper) = upper_bound {
                lock_iter.seek(upper.chunk());
            }
        } else {
            if let Some(upper) = upper_bound {
                lock_iter.set_bound(upper, false);
            }
            if let Some(lower) = lower_bound {
                lock_iter.seek(lower.chunk());
            }
        }
        stats.lock.seek += 1;
        while lock_iter.valid() {
            let key = Key::from_raw(lock_iter.key());
            let item = lock_iter.item();
            stats.lock.next += 1;
            stats.lock.flow_stats.read_keys += 1;
            stats.lock.flow_stats.read_bytes += lock_iter.key().len() + item.value_len();
            stats.lock.processed_keys += 1;
            let lock = Lock::parse(item.get_value())?;
            Lock::check_ts_conflict(
                Cow::Borrowed(&lock),
                &key,
                self.start_ts.into(),
                &self.bypass_locks,
                IsolationLevel::Si,
            )?;
            lock_iter.next();
        }
        Ok(())
    }

    fn scanner_inner(
        &self,
        desc: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        output_delete: bool,
    ) -> Result<CloudStoreScanner> {
        let lower_bound = lower_bound.map(|k| Bytes::from(k.to_raw().unwrap()));
        let upper_bound = upper_bound.map(|k| Bytes::from(k.to_raw().unwrap()));
        self.verify_range(&lower_bound, &upper_bound)?;
        let mut stats = Statistics::default();
        let lock_iter = self
            .snapshot
            .new_iterator(LOCK_CF, desc, false, None, self.fill_cache);
        self.check_locks(
            lock_iter,
            lower_bound.clone(),
            upper_bound.clone(),
            &mut stats,
        )?;
        let iter =
            self.snapshot
                .new_iterator(WRITE_CF, desc, false, Some(self.start_ts), self.fill_cache);
        Ok(CloudStoreScanner {
            iter,
            desc,
            stats,
            is_started: false,
            lower_bound,
            upper_bound,
            output_delete,
        })
    }

    fn verify_range(&self, lower_bound: &Option<Bytes>, upper_bound: &Option<Bytes>) -> Result<()> {
        if let Some(ref l) = lower_bound {
            if l.chunk() < self.snapshot.get_start_key() {
                return self.range_error(lower_bound, upper_bound);
            }
        }
        if let Some(ref u) = upper_bound {
            if u.chunk() > self.snapshot.get_end_key() {
                return self.range_error(lower_bound, upper_bound);
            }
        }
        Ok(())
    }

    fn range_error(&self, lower_bound: &Option<Bytes>, upper_bound: &Option<Bytes>) -> Result<()> {
        REQUEST_EXCEED_BOUND.inc();
        let start = lower_bound
            .as_ref()
            .map(|k| Key::from_raw(k.chunk()).into_encoded());
        let end = upper_bound
            .as_ref()
            .map(|k| Key::from_raw(k.chunk()).into_encoded());
        let snap_start = Key::from_raw(self.snapshot.get_start_key());
        let snap_end = Key::from_raw(self.snapshot.get_end_key());
        Err(Error::from(ErrorInner::InvalidReqRange {
            start,
            end,
            lower_bound: Some(snap_start.into_encoded()),
            upper_bound: Some(snap_end.into_encoded()),
        }))
    }
}

pub struct CloudStoreScanner {
    iter: kvengine::read::Iterator,
    stats: Statistics,
    is_started: bool,
    lower_bound: Option<Bytes>,
    upper_bound: Option<Bytes>,
    desc: bool,
    output_delete: bool,
}

impl CloudStoreScanner {
    fn init(&mut self) {
        self.stats.write.seek += 1;
        if self.desc {
            if let Some(lower) = &self.lower_bound {
                self.iter.set_bound(lower.clone(), false);
            }
            if let Some(upper) = &self.upper_bound {
                self.iter.seek(upper.chunk());
            } else {
                self.iter.rewind();
            }
        } else {
            if let Some(upper) = &self.upper_bound {
                self.iter.set_bound(upper.clone(), false);
            }
            if let Some(lower) = &self.lower_bound {
                self.iter.seek(lower.chunk());
            } else {
                self.iter.rewind();
            }
        }
    }

    fn next_inner(&mut self) -> Result<Option<(Key, UserMeta, Value)>> {
        if self.is_started {
            self.iter.next();
        } else {
            self.init();
            self.is_started = true;
        }
        loop {
            if !self.iter.valid() {
                return Ok(None);
            }
            let iter_key = self.iter.key();
            let item = self.iter.item();
            self.stats.write.next += 1;
            self.stats.write.flow_stats.read_keys += 1;
            self.stats.write.flow_stats.read_bytes += iter_key.len() + item.value_len();
            self.stats.write.processed_keys += 1;
            self.stats.processed_size += iter_key.len() + item.value_len();
            let val = item.get_value();
            if !val.is_empty() || self.output_delete {
                let user_meta = UserMeta::from_slice(item.user_meta());
                let key = Key::from_raw(iter_key);
                return Ok(Some((key, user_meta, val.to_vec())));
            }
            self.stats.write.next_tombstone += 1;
            // Skip delete record.
            self.iter.next();
            continue;
        }
    }
}

impl super::Scanner for CloudStoreScanner {
    fn next(&mut self) -> Result<Option<(Key, Value)>> {
        Ok(self.next_inner()?.map(|(key, _user_meta, val)| (key, val)))
    }

    fn met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }
}

impl super::TxnEntryScanner for CloudStoreScanner {
    fn next_entry(&mut self) -> Result<Option<TxnEntry>> {
        Ok(self.next_inner()?.map(|(key, user_meta, val)| {
            let write_key = key.append_ts(TimeStamp::new(user_meta.commit_ts));
            let write_type = if val.is_empty() {
                WriteType::Delete
            } else {
                WriteType::Put
            };
            let (write, default_key, default_val) = if is_short_value(&val) {
                (
                    Write::new(write_type, user_meta.start_ts.into(), Some(val)),
                    vec![],
                    vec![],
                )
            } else {
                let default_key = write_key
                    .clone()
                    .truncate_ts()
                    .unwrap()
                    .append_ts(user_meta.start_ts.into());
                (
                    Write::new(write_type, user_meta.start_ts.into(), None),
                    default_key.into_encoded(),
                    val,
                )
            };
            TxnEntry::Commit {
                default: (default_key, default_val),
                write: (write_key.into_encoded(), write.as_ref().to_bytes()),
                old_value: OldValue::None,
            }
        }))
    }

    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }
}

impl<S: Snapshot> TxnEntryStore for CloudStore<S> {
    type Scanner = CloudStoreScanner;
    fn entry_scanner(
        &self,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        _after_ts: TimeStamp,
        output_delete: bool,
    ) -> Result<Self::Scanner> {
        self.scanner_inner(false, lower_bound, upper_bound, output_delete)
    }
}
