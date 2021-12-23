use std::borrow::Cow;
use std::io::Seek;
use std::marker::PhantomData;
use std::sync::Arc;
use rfstore::store::RegionSnapshot;
use tikv_kv::{Snapshot, Statistics};
use txn_types::{Key, Lock, LockType, TimeStamp, TsSet, Value};
use crate::storage::lock_manager::WaitTimeout::Default;
use crate::storage::mvcc;
use crate::storage::mvcc::NewerTsCheckState;
use crate::storage::txn::Result;

pub struct CloudStore<S: Snapshot> {
    marker: PhantomData<S>,
    snapshot: Arc<kvengine::SnapAccess>,
    start_ts: u64,
    bypass_locks: TsSet,
    stats: Statistics,
}

const WRITE_CF: usize = 0;
const LOCK_CF: usize = 1;
const EXTRA_CF: usize = 2;

impl<S: Snapshot> super::Store for CloudStore<S> {
    type Scanner = CloudStoreScanner;

    fn get(&self, user_key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let val = Self::get_inner(user_key, &self.snapshot, self.start_ts, &self.bypass_locks, statistics)?;
        Ok(val)
        //
        // let raw_key = user_key.to_raw()?;
        // statistics.lock.get += 1;
        // let val = self.snapshot.get(LOCK_CF, &raw_key, 0);
        // if let Some(ref lock_item) = val {
        //     let lock = Lock::parse(lock_item.get_value()).unwrap();
        //     let res = Lock::check_ts_conflict(Cow::Borrowed(&lock), user_key, TimeStamp::new(self.start_ts), &self.bypass_locks);
        //     res.map_err(| e | mvcc::Error::from(e))?
        // }
        // statistics.write.processed_keys += 1;
        // let val = self.snapshot.get(WRITE_CF, &raw_key, self.start_ts);
        // statistics.processed_size += user_key.len();
        // if let Some(item) = val {
        //     statistics.processed_size += item.get_value().len();
        //     return Ok(Some(item.get_value().to_vec()))
        // }
        // Ok(None)
    }



    fn incremental_get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        let mut stat = &mut self.stats;
        let val = Self::get_inner(user_key, &self.snapshot, self.start_ts, &self.bypass_locks, stat)?;
        Ok(val)
    }

    fn incremental_get_take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }

    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    fn batch_get(&self, keys: &[Key], statistics: &mut Statistics) -> Result<Vec<Result<Option<Value>>>> {
        let mut res_vec = Vec::with_capacity(keys.len());
        for key in keys {
            let res = self.get(key, statistics);
            res_vec.push(res);
        }
        Ok(res_vec)
    }

    fn scanner(&self, desc: bool, key_only: bool, check_has_newer_ts_data: bool, lower_bound: Option<Key>, upper_bound: Option<Key>) -> Result<Self::Scanner> {
        let lock_iter = self.snapshot.new_iterator(LOCK_CF, false, false);
        let mut stats = Statistics::default();
        let lower_bound = lower_bound.map(|k| k.to_raw().unwrap());
        let upper_bound = upper_bound.map(|k|k.to_raw().unwrap());
        self.check_locks(lock_iter, &lower_bound, &upper_bound, &mut stats)?;
        let iter = self.snapshot.new_data_iterator(desc, self.start_ts, false);
        Ok(CloudStoreScanner {
            iter,
            desc,
            stats,
            is_started: false,
            lower_bound,
            upper_bound,
            stopped: false,
        })
    }
}

impl<S: Snapshot> CloudStore<S> {
    pub fn new(snapshot: S, start_ts: u64, bypass_locks: TsSet) -> Self {
        Self {
            marker: PhantomData::default(),
            snapshot: snapshot.get_kvengine_snap().unwrap().clone(),
            start_ts,
            bypass_locks,
            stats: Statistics::default(),
        }
    }

    fn get_inner(user_key: &Key, snap: &kvengine::SnapAccess, start_ts: u64, bypass_locks: &TsSet, statistics: &mut Statistics) -> mvcc::Result<Option<Value>> {
        let raw_key = user_key.to_raw()?;
        statistics.lock.get += 1;
        let val = snap.get(LOCK_CF, &raw_key, 0);
        if let Some(ref lock_item) = val {
            let lock = Lock::parse(lock_item.get_value()).unwrap();
            Lock::check_ts_conflict(Cow::Borrowed(&lock), user_key, TimeStamp::new(start_ts), bypass_locks)?;
        }
        statistics.write.processed_keys += 1;
        let val = snap.get(WRITE_CF, &raw_key, start_ts);
        statistics.processed_size += user_key.len();
        if let Some(item) = val {
            statistics.processed_size += item.get_value().len();
            return Ok(Some(item.get_value().to_vec()))
        }
        Ok(None)
    }

    fn check_locks(
        &self, mut lock_iter: kvengine::read::Iterator,
        lower_bound: &Option<Vec<u8>>,
        upper_bound: &Option<Vec<u8>>,
        stats: &mut Statistics,
    ) -> mvcc::Result<()> {
        if let Some(seek_key) = lower_bound {
            lock_iter.seek(seek_key);
        } else {
            lock_iter.rewind();
        }
        stats.lock.seek += 1;
        while lock_iter.valid() {
            let key = Key::from_raw(lock_iter.key());
            if let Some(upper_bound_key) = &upper_bound {
                if lock_iter.key() >= upper_bound_key.as_slice() {
                    break;
                }
            }
            let lock = Lock::parse(lock_iter.item().get_value())?;
            Lock::check_ts_conflict(Cow::Borrowed(&lock), &key, self.start_ts.into(), &self.bypass_locks)?;
            lock_iter.next();
        }
        return Ok(());
    }
}

pub struct CloudStoreScanner {
    iter: kvengine::read::Iterator,
    stats: Statistics,
    is_started: bool,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    stopped: bool,
    desc: bool,
}

impl CloudStoreScanner {
    fn init(&mut self) {
        if self.desc {
            if let Some(seek_key) = &self.upper_bound {
                self.iter.seek(seek_key);
            } else {
                self.iter.rewind();
            }
        } else {
            if let Some(seek_key) = &self.lower_bound {
                self.iter.seek(seek_key);
            } else {
                self.iter.rewind();
            }
        }
    }
}

impl super::Scanner for CloudStoreScanner {
    fn next(&mut self) -> Result<Option<(Key, Value)>> {
        if self.stopped {
            return Ok(None)
        }
        if self.is_started {
            self.iter.next();
        } else {
            self.init();
        }
        loop {
            if !self.iter.valid() {
                return Ok(None)
            }
            let iter_key = self.iter.key();
            if self.desc {
                if let Some(bound_key) = &self.lower_bound {
                    if iter_key < bound_key.as_slice() {
                        self.stopped = true;
                        return Ok(None)
                    }
                }
            } else {
                if let Some(bound_key) = &self.upper_bound {
                    if iter_key >= bound_key.as_slice() {
                        self.stopped = true;
                        return Ok(None)
                    }
                }
            };
            let item = self.iter.item();
            let val = item.get_value();
            if val.len() > 0 {
                let key = Key::from_raw(iter_key);
                return Ok(Some((key, val.to_vec())))
            }
            // Skip delete record.
            self.iter.next();
            continue;
        }
    }

    fn met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.stats)
    }
}