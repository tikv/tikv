use std::sync::Arc;
use engine_traits::CF_LOCK;
use rfstore::{EXTRA_CF, LOCK_CF, UserMeta, WRITE_CF};
use tikv_kv::Snapshot;
use tikv_util::codec::number::NumberEncoder;
use txn_types::{Key, Lock, OldValue, TimeStamp, Value, Write, WriteRef, WriteType};
use crate::storage::mvcc::{TxnCommitRecord, Result};

pub struct CloudReader {
    snapshot: Arc<kvengine::SnapAccess>,
    pub statistics: tikv_kv::Statistics,
}

impl CloudReader {
    pub fn new(snapshot: Arc<kvengine::SnapAccess>) -> Self {
        Self {
            snapshot,
            statistics: tikv_kv::Statistics::default(),
        }
    }

    fn get_commit_by_item(item: &kvengine::Item, start_ts: TimeStamp) -> Option<TxnCommitRecord> {
        let user_meta = UserMeta::from_slice(item.user_meta());
        if user_meta.start_ts == start_ts.into_inner() {
            let write_ref = WriteRef::parse(item.get_value()).unwrap();
            let write = Write::new(write_ref.write_type, write_ref.start_ts, None);
            return Some(TxnCommitRecord::SingleRecord {commit_ts: TimeStamp::new(user_meta.commit_ts), write})
        }
        None
    }

    pub fn get_txn_commit_record(&mut self, key: &Key, start_ts: TimeStamp) -> Result<TxnCommitRecord> {
        let mut raw_key = key.to_raw()?;
        let item = self.snapshot.get(WRITE_CF, &raw_key, 0);
        if item.is_none() {
            return Ok(TxnCommitRecord::None { overlapped_write: None})
        }
        if let Some(record) = Self::get_commit_by_item(&item.unwrap(), start_ts) {
            return Ok(record)
        }
        let mut data_iter = self.snapshot.new_iterator(WRITE_CF, false, true);
        data_iter.seek(&raw_key);
        while data_iter.valid() {
            let key = data_iter.key();
            if key != raw_key {
                break;
            }
            if let Some(record) = Self::get_commit_by_item(&data_iter.item(), start_ts) {
                return Ok(record)
            }
        }
        raw_key.encode_u64_desc(start_ts.into_inner())?;
        let item = self.snapshot.get(EXTRA_CF, &raw_key, 0);
        if item.is_none() {
            return Ok(TxnCommitRecord::None { overlapped_write: None})
        }
        let item = item.unwrap();
        let user_meta = UserMeta::from_slice(item.user_meta());
        let write: Write;
        if user_meta.commit_ts == 0 {
            write = Write::new(WriteType::Rollback, start_ts, None);
        } else {
            write = Write::new(WriteType::Lock, start_ts, None);
        }
        Ok(TxnCommitRecord::SingleRecord {commit_ts: TimeStamp::new(user_meta.commit_ts), write})
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        let raw_key = key.to_raw().unwrap();
        let item = self.snapshot.get(LOCK_CF, &raw_key, 0);
        if item.is_none() {
            return Ok(None);
        }
        let item = item.unwrap();
        let lock = Lock::parse(item.get_value())?;
        return Ok(Some(lock))
    }

    pub fn get(
        &mut self,
        key: &Key,
        mut ts: TimeStamp,
        _gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Value>> {
        let raw_key = key.to_raw()?;
        if let Some(item) = self.snapshot.get(WRITE_CF, &raw_key, ts.into_inner()) {
            let val = item.get_value();
            if val.len() == 0 {
                return Ok(None)
            }
            return Ok(Some(val.to_vec()))
        }
        return Ok(None);
    }

    pub fn get_write(
        &mut self,
        key: &Key,
        mut ts: TimeStamp,
        _gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Write>> {
        self.seek_write(key, ts).map(|opt| {
            opt.map(|(ts, write) | write)
        })
    }

    pub fn seek_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<(TimeStamp, Write)>> {
        let raw_key = key.to_raw()?;
        if let Some(item) = self.snapshot.get(WRITE_CF, &raw_key, ts.into_inner()) {
            let user_meta = UserMeta::from_slice(item.user_meta());
            let write_type: WriteType;
            let short_value: Option<Value>;
            if item.get_value().len() == 0 {
                write_type = WriteType::Delete;
                short_value = None;
            } else {
                write_type = WriteType::Put;
                short_value = Some(item.get_value().to_vec())
            }
            let write = Write::new(write_type, TimeStamp::new(user_meta.start_ts), short_value);
            return Ok(Some((TimeStamp::new(user_meta.commit_ts), write.to_owned())))
        }
        return Ok(None)
    }

    #[inline(always)]
    pub fn get_old_value(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        prev_write_loaded: bool,
        prev_write: Option<Write>,
    ) -> Result<OldValue> {
        if let Some(write) = prev_write {
            if write.write_type == WriteType::Delete {
                return Ok(OldValue::None)
            }
            // Locks and Rolbacks are stored in extra CF, will not be seeked by seek_write.
            assert_eq!(write.write_type, WriteType::Put);
            return Ok(OldValue::value(write.short_value.unwrap()))
        }
        return Ok(OldValue::None)
    }
}
