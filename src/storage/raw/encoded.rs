// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::kv::{Iterator, Result, Snapshot, TTL_TOMBSTONE};
use crate::storage::Statistics;

use engine_traits::raw_value::{ttl_current_ts, RawValue};
use engine_traits::CfName;
use engine_traits::{IterOptions, ReadOptions};
use kvproto::kvrpcpb::ApiVersion;
use txn_types::{Key, Value};

// TODO: remove this
#[derive(Clone)]
pub struct RawEncodeSnapshot<S: Snapshot> {
    s: S,
    current_ts: u64,
    api_version: ApiVersion,
}

impl<S: Snapshot> RawEncodeSnapshot<S> {
    pub fn from_snapshot(s: S, api_version: ApiVersion) -> Self {
        RawEncodeSnapshot {
            s,
            current_ts: ttl_current_ts(),
            api_version,
        }
    }

    fn map_value(&self, value: Result<Option<Value>>) -> Result<Option<Value>> {
        match value? {
            Some(v) => {
                let raw_value = RawValue::from_owned_bytes(v, self.api_version)?;
                if raw_value
                    .expire_ts
                    .map(|expire_ts| expire_ts <= self.current_ts)
                    .unwrap_or(false)
                {
                    return Ok(None);
                }
                Ok(Some(raw_value.user_value))
            }
            None => Ok(None),
        }
    }

    pub fn get_key_ttl_cf(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<u64>> {
        stats.data.flow_stats.read_keys = 1;
        stats.data.flow_stats.read_bytes = key.as_encoded().len();
        if let Some(v) = self.s.get_cf(cf, key)? {
            stats.data.flow_stats.read_bytes += v.len();
            let raw_value = RawValue::from_bytes(&v, self.api_version)?;
            return match raw_value.expire_ts {
                Some(expire_ts) if expire_ts <= self.current_ts => Ok(None),
                Some(expire_ts) => Ok(Some(expire_ts - self.current_ts)),
                None => Ok(Some(0)),
            };
        }
        Ok(None)
    }
}

impl<S: Snapshot> Snapshot for RawEncodeSnapshot<S> {
    type Iter = RawEncodeIterator<S::Iter>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get(key))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get_cf(cf, key))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        self.map_value(self.s.get_cf_opt(opts, cf, key))
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawEncodeIterator::new(
            self.s.iter(iter_opt)?,
            self.current_ts,
            self.api_version,
        ))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        Ok(RawEncodeIterator::new(
            self.s.iter_cf(cf, iter_opt)?,
            self.current_ts,
            self.api_version,
        ))
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        self.s.lower_bound()
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        self.s.upper_bound()
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.s.get_data_version()
    }

    fn is_max_ts_synced(&self) -> bool {
        self.s.is_max_ts_synced()
    }
}

pub struct RawEncodeIterator<I: Iterator> {
    i: I,
    current_ts: u64,
    skip_ttl: usize,
    api_version: ApiVersion,
}

impl<I: Iterator> RawEncodeIterator<I> {
    fn new(i: I, current_ts: u64, api_version: ApiVersion) -> Self {
        RawEncodeIterator {
            i,
            current_ts,
            skip_ttl: 0,
            api_version,
        }
    }

    fn find_valid_value(&mut self, mut res: Result<bool>, forward: bool) -> Result<bool> {
        loop {
            if res.is_err() {
                break;
            }

            if *res.as_ref().unwrap() {
                println!("{:?}, {:?}", self.i.key(), self.i.value());
                let raw_value = RawValue::from_bytes(self.i.value(), self.api_version)?;
                if raw_value
                    .expire_ts
                    .map(|expire_ts| expire_ts <= self.current_ts)
                    .unwrap_or(false)
                {
                    self.skip_ttl += 1;
                    res = if forward {
                        self.i.next()
                    } else {
                        self.i.prev()
                    };
                    continue;
                }
            }
            break;
        }
        res
    }
}

impl<I: Iterator> Drop for RawEncodeIterator<I> {
    fn drop(&mut self) {
        TTL_TOMBSTONE.with(|m| {
            *m.borrow_mut() += self.skip_ttl;
        });
    }
}

impl<I: Iterator> Iterator for RawEncodeIterator<I> {
    fn next(&mut self) -> Result<bool> {
        let res = self.i.next();
        self.find_valid_value(res, true)
    }

    fn prev(&mut self) -> Result<bool> {
        let res = self.i.prev();
        self.find_valid_value(res, false)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        let res = self.i.seek(key);
        self.find_valid_value(res, true)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        let res = self.i.seek_for_prev(key);
        self.find_valid_value(res, false)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        let res = self.i.seek_to_first();
        self.find_valid_value(res, true)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let res = self.i.seek_to_last();
        self.find_valid_value(res, false)
    }

    fn valid(&self) -> Result<bool> {
        self.i.valid()
    }

    fn validate_key(&self, key: &Key) -> Result<()> {
        self.i.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        self.i.key()
    }

    fn value(&self) -> &[u8] {
        RawValue::from_bytes(self.i.value(), self.api_version)
            .unwrap()
            .user_value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::Engine;
    use crate::storage::{SnapContext, TestEngineBuilder};

    use engine_traits::{SyncMutable, CF_DEFAULT};

    #[test]
    fn test_ttl_snapshot() {
        fn inner(api_version: ApiVersion) {
            let dir = tempfile::TempDir::new().unwrap();
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .api_version(api_version)
                .build()
                .unwrap();
            let kvdb = engine.get_rocksdb();

            let key1 = b"key1";
            let value1 = RawValue {
                user_value: b"value1".to_vec(),
                expire_ts: Some(90),
            };
            kvdb.put_cf(CF_DEFAULT, key1, &value1.to_bytes(api_version))
                .unwrap();
            let value10 = RawValue {
                user_value: b"value1".to_vec(),
                expire_ts: Some(110),
            };
            kvdb.put_cf(CF_DEFAULT, key1, &value10.to_bytes(api_version))
                .unwrap();

            let key2 = b"key2";
            let value2 = RawValue {
                user_value: b"value2".to_vec(),
                expire_ts: Some(90),
            };
            kvdb.put_cf(CF_DEFAULT, key2, &value2.to_bytes(api_version))
                .unwrap();
            let value20 = RawValue {
                user_value: b"value2".to_vec(),
                expire_ts: Some(90),
            };
            kvdb.put_cf(CF_DEFAULT, key2, &value20.to_bytes(api_version))
                .unwrap();

            let key3 = b"key3";
            let value3 = RawValue {
                user_value: b"value3".to_vec(),
                expire_ts: None,
            };
            kvdb.put_cf(CF_DEFAULT, key3, &value3.to_bytes(api_version))
                .unwrap();

            let snapshot = engine.snapshot(SnapContext::default()).unwrap();
            let ttl_snapshot = RawEncodeSnapshot::from_snapshot(snapshot, api_version);
            assert_eq!(
                ttl_snapshot.get(&Key::from_encoded_slice(b"key1")).unwrap(),
                Some(b"value1".to_vec())
            );
            assert_eq!(
                ttl_snapshot.get(&Key::from_encoded_slice(b"key2")).unwrap(),
                None
            );
            assert_eq!(
                ttl_snapshot.get(&Key::from_encoded_slice(b"key3")).unwrap(),
                Some(b"value3".to_vec())
            );
            let mut stats = Statistics::default();
            assert_eq!(
                ttl_snapshot
                    .get_key_ttl_cf(CF_DEFAULT, &Key::from_encoded_slice(b"key1"), &mut stats)
                    .unwrap(),
                Some(10)
            );
            assert_eq!(
                ttl_snapshot
                    .get_key_ttl_cf(CF_DEFAULT, &Key::from_encoded_slice(b"key2"), &mut stats)
                    .unwrap(),
                None
            );
            assert_eq!(
                ttl_snapshot
                    .get_key_ttl_cf(CF_DEFAULT, &Key::from_encoded_slice(b"key3"), &mut stats)
                    .unwrap(),
                Some(0)
            );
        }
        inner(ApiVersion::V1ttl);
        inner(ApiVersion::V2);
    }

    #[test]
    fn test_ttl_iterator() {
        fn inner(api_version: ApiVersion) {
            let dir = tempfile::TempDir::new().unwrap();
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .api_version(api_version)
                .build()
                .unwrap();
            let kvdb = engine.get_rocksdb();

            let key1 = b"key1";
            let value1 = RawValue {
                user_value: b"value1".to_vec(),
                expire_ts: Some(90),
            };
            kvdb.put_cf(CF_DEFAULT, key1, &value1.to_bytes(api_version))
                .unwrap();
            let value10 = RawValue {
                user_value: b"value1".to_vec(),
                expire_ts: Some(110),
            };
            kvdb.put_cf(CF_DEFAULT, key1, &value10.to_bytes(api_version))
                .unwrap();

            let key2 = b"key2";
            let value2 = RawValue {
                user_value: b"value2".to_vec(),
                expire_ts: Some(110),
            };
            kvdb.put_cf(CF_DEFAULT, key2, &value2.to_bytes(api_version))
                .unwrap();
            let value20 = RawValue {
                user_value: b"value2".to_vec(),
                expire_ts: Some(90),
            };
            kvdb.put_cf(CF_DEFAULT, key2, &value20.to_bytes(api_version))
                .unwrap();

            let key3 = b"key3";
            let value3 = RawValue {
                user_value: b"value3".to_vec(),
                expire_ts: None,
            };
            kvdb.put_cf(CF_DEFAULT, key3, &value3.to_bytes(api_version))
                .unwrap();

            let key4 = b"key4";
            let value4 = RawValue {
                user_value: b"value4".to_vec(),
                expire_ts: Some(10),
            };
            kvdb.put_cf(CF_DEFAULT, key4, &value4.to_bytes(api_version))
                .unwrap();

            let key5 = b"key5";
            let value5 = RawValue {
                user_value: b"value5".to_vec(),
                expire_ts: None,
            };
            kvdb.put_cf(CF_DEFAULT, key5, &value5.to_bytes(api_version))
                .unwrap();
            let value50 = RawValue {
                user_value: b"value5".to_vec(),
                expire_ts: Some(90),
            };
            kvdb.put_cf(CF_DEFAULT, key5, &value50.to_bytes(api_version))
                .unwrap();

            let snapshot = engine.snapshot(SnapContext::default()).unwrap();
            let ttl_snapshot = RawEncodeSnapshot::from_snapshot(snapshot, api_version);
            let mut iter = ttl_snapshot
                .iter(IterOptions::new(None, None, false))
                .unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.key(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert_eq!(iter.next().unwrap(), true);
            assert_eq!(iter.key(), b"key3");
            assert_eq!(iter.value(), b"value3");
            assert_eq!(iter.next().unwrap(), false);

            iter.seek_to_last().unwrap();
            assert_eq!(iter.key(), b"key3");
            assert_eq!(iter.value(), b"value3");
            assert_eq!(iter.prev().unwrap(), true);
            assert_eq!(iter.key(), b"key1");
            assert_eq!(iter.value(), b"value1");
            assert_eq!(iter.prev().unwrap(), false);

            iter.seek(&Key::from_encoded_slice(b"key2")).unwrap();
            assert_eq!(iter.valid().unwrap(), true);
            assert_eq!(iter.key(), b"key3");
            assert_eq!(iter.value(), b"value3");
            iter.seek(&Key::from_encoded_slice(b"key4")).unwrap();
            assert_eq!(iter.valid().unwrap(), false);

            iter.seek_for_prev(&Key::from_encoded_slice(b"key2"))
                .unwrap();
            assert_eq!(iter.valid().unwrap(), true);
            assert_eq!(iter.key(), b"key1");
            assert_eq!(iter.value(), b"value1");
            iter.seek_for_prev(&Key::from_encoded_slice(b"key1"))
                .unwrap();
            assert_eq!(iter.valid().unwrap(), true);
            assert_eq!(iter.key(), b"key1");
            assert_eq!(iter.value(), b"value1");
        }
        inner(ApiVersion::V1ttl);
        inner(ApiVersion::V2);
    }
}
