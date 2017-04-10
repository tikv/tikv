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

use storage::{Key, Value, KvPair, Snapshot, ScanMode, Statistics};
use storage::mvcc::{MvccReader, Error as MvccError};
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

    pub fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let mut reader = MvccReader::new(self.snapshot, statistics, None, true, None);
        let v = try!(reader.get(key, self.start_ts));
        Ok(v)
    }

    pub fn batch_get(&self,
                     keys: &[Key],
                     statistics: &mut Statistics)
                     -> Result<Vec<Result<Option<Value>>>> {
        // TODO: sort the keys and use ScanMode::Forward
        let mut reader = MvccReader::new(self.snapshot, statistics, None, true, None);
        let mut results = Vec::with_capacity(keys.len());
        for k in keys {
            results.push(reader.get(k, self.start_ts).map_err(Error::from));
        }
        Ok(results)
    }

    /// Create a scanner.
    /// when key_only is true, all the returned value will be empty.
    pub fn scanner(&self,
                   mode: ScanMode,
                   key_only: bool,
                   upper_bound: Option<Vec<u8>>,
                   statistics: &'a mut Statistics)
                   -> Result<StoreScanner> {
        let mut reader = MvccReader::new(self.snapshot, statistics, Some(mode), true, upper_bound);
        reader.set_key_only(key_only);
        Ok(StoreScanner {
               reader: reader,
               start_ts: self.start_ts,
           })
    }
}

pub struct StoreScanner<'a> {
    reader: MvccReader<'a>,
    start_ts: u64,
}

impl<'a> StoreScanner<'a> {
    pub fn seek(&mut self, key: Key) -> Result<Option<(Key, Value)>> {
        Ok(try!(self.reader.seek(key, self.start_ts)))
    }

    pub fn reverse_seek(&mut self, key: Key) -> Result<Option<(Key, Value)>> {
        Ok(try!(self.reader.reverse_seek(key, self.start_ts)))
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
            key = key.append_ts(0);
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
}
