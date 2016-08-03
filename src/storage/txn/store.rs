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

use storage::{Key, Value, KvPair};
use storage::{Snapshot, Cursor};
use storage::mvcc::{MvccSnapshot, Error as MvccError, MvccCursor};
use super::{Error, Result};

pub struct SnapshotStore<'a> {
    snapshot: &'a Snapshot,
    start_ts: u64,
}

impl<'a> SnapshotStore<'a> {
    pub fn new(snapshot: &'a Snapshot, start_ts: u64) -> SnapshotStore {
        SnapshotStore {
            snapshot: snapshot,
            start_ts: start_ts,
        }
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        let txn = MvccSnapshot::new(self.snapshot, self.start_ts);
        Ok(try!(txn.get(key)))
    }

    pub fn batch_get(&self, keys: &[Key]) -> Result<Vec<Result<Option<Value>>>> {
        let txn = MvccSnapshot::new(self.snapshot, self.start_ts);
        let mut results = Vec::with_capacity(keys.len());
        for k in keys {
            results.push(txn.get(k).map_err(Error::from));
        }
        Ok(results)
    }

    pub fn scanner(&self) -> Result<StoreScanner> {
        let cursor = try!(self.snapshot.iter());
        Ok(StoreScanner {
            cursor: cursor,
            snapshot: MvccSnapshot::new(self.snapshot, self.start_ts),
            start_ts: self.start_ts,
        })
    }
}

pub struct StoreScanner<'a> {
    cursor: Box<Cursor + 'a>,
    snapshot: MvccSnapshot<'a>,
    start_ts: u64,
}

impl<'a> StoreScanner<'a> {
    pub fn seek(&mut self, mut key: Key) -> Result<Option<(Key, Value)>> {
        loop {
            if !try!(self.cursor.seek(&key)) {
                return Ok(None);
            }
            key = try!(Key::from_encoded(self.cursor.key().to_vec()).truncate_ts());
            let cursor = self.cursor.as_mut();
            let mut txn = MvccCursor::new(cursor, &self.snapshot, self.start_ts);
            if let Some(v) = try!(txn.get(&key)) {
                // TODO: find a way to avoid copy.
                return Ok(Some((key, v.to_vec())));
            }
            // None means value is deleted, so just continue.
            key = key.append_ts(u64::max_value());
        }
    }

    pub fn reverse_seek(&mut self, mut key: Key) -> Result<Option<(Key, Value)>> {
        loop {
            if !try!(self.cursor.reverse_seek(&key)) {
                return Ok(None);
            }
            key = try!(Key::from_encoded(self.cursor.key().to_vec()).truncate_ts());
            let cursor = self.cursor.as_mut();
            let mut txn = MvccCursor::new(cursor, &self.snapshot, self.start_ts);
            if let Some(v) = try!(txn.get(&key)) {
                return Ok(Some((key, v.to_vec())));
            }
        }
    }

    #[inline]
    fn handle_mvcc_err(e: MvccError, result: &mut Vec<Result<KvPair>>) -> Result<Key> {
        let key = if let MvccError::KeyIsLocked { key: ref k, .. } = e {
            Some(Key::from_raw(k))
        } else {
            None
        };
        match key {
            Some(k) => {
                result.push(Err(e.into()));
                Ok(k)
            }
            None => Err(e.into()),
        }
    }

    pub fn scan(&mut self, mut key: Key, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        while results.len() < limit {
            match self.seek(key) {
                Ok(Some((k, v))) => {
                    results.push(Ok((try!(k.raw()), v)));
                    key = k;
                }
                Ok(None) => break,
                Err(Error::Mvcc(e)) => key = try!(StoreScanner::handle_mvcc_err(e, &mut results)),
                Err(e) => return Err(e),
            }
            key = key.append_ts(u64::max_value());
        }
        Ok(results)
    }

    pub fn reverse_scan(&mut self, mut key: Key, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        while results.len() < limit {
            match self.reverse_seek(key) {
                Ok(Some((k, v))) => {
                    results.push(Ok((try!(k.raw()), v)));
                    key = k;
                }
                Ok(None) => break,
                Err(Error::Mvcc(e)) => key = try!(StoreScanner::handle_mvcc_err(e, &mut results)),
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    pub fn get(&mut self, key: &Key, ts: u64) -> Result<Option<&[u8]>> {
        self.cursor.get(&key.append_ts(ts)).map_err(From::from)
    }
}
