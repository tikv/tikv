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

use storage::engine::{Snapshot, Cursor, ScanMode};
use storage::{Key, Value, CF_LOCK, CF_WRITE};
use super::{Error, Result};
use super::lock::Lock;
use super::write::{Write, WriteType};
use raftstore::store::engine::{IterOption, SeekMode};
use std::u64;

#[derive(Clone, Default, Debug)]
pub struct ScanMetrics {
    pub scanned_keys: u64,
    pub skipped_keys: u64,
}

impl ScanMetrics {
    pub fn efficiency(&self) -> f64 {
        if self.scanned_keys == 0 {
            0.0
        } else {
            1.0 - self.skipped_keys as f64 / self.scanned_keys as f64
        }
    }
}

pub struct MvccReader<'a> {
    snapshot: &'a Snapshot,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<'a>>,
    lock_cursor: Option<Cursor<'a>>,
    write_cursor: Option<Cursor<'a>>,

    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    upper_bound: Option<Vec<u8>>,
}

impl<'a> MvccReader<'a> {
    pub fn new(snapshot: &Snapshot,
               scan_mode: Option<ScanMode>,
               fill_cache: bool,
               upper_bound: Option<Vec<u8>>)
               -> MvccReader {
        MvccReader {
            snapshot: snapshot,
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode: scan_mode,
            key_only: false,
            fill_cache: fill_cache,
            upper_bound: upper_bound,
        }
    }

    pub fn set_key_only(&mut self, key_only: bool) {
        self.key_only = key_only;
    }

    pub fn load_data(&mut self, key: &Key, ts: u64) -> Result<Value> {
        if self.key_only {
            return Ok(vec![]);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            self.data_cursor = Some(try!(self.snapshot
                .iter(IterOption::new(None, self.fill_cache, SeekMode::TotalOrderSeek),
                      self.get_scan_mode(true))));
        }

        let k = key.append_ts(ts);
        if let Some(ref mut cursor) = self.data_cursor {
            match try!(cursor.get(&k)) {
                None => panic!("key {} not found, ts {}", key, ts),
                Some(v) => Ok(v.to_vec()),
            }
        } else {
            match try!(self.snapshot.get(&k)) {
                None => panic!("key {} not found, ts: {}", key, ts),
                Some(v) => Ok(v),
            }
        }
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        if self.scan_mode.is_some() && self.lock_cursor.is_none() {
            self.lock_cursor = Some(try!(self.snapshot
                .iter_cf(CF_LOCK,
                         IterOption::new(None, true, SeekMode::TotalOrderSeek),
                         self.get_scan_mode(true))));
        }

        if let Some(ref mut cursor) = self.lock_cursor {
            match try!(cursor.get(&key)) {
                Some(v) => Ok(Some(try!(Lock::parse(v)))),
                None => Ok(None),
            }
        } else {
            match try!(self.snapshot.get_cf(CF_LOCK, &key)) {
                Some(v) => Ok(Some(try!(Lock::parse(&v)))),
                None => Ok(None),
            }
        }
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    pub fn seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, Write)>> {
        self.seek_write_impl(key, ts, false)
    }

    pub fn reverse_seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, Write)>> {
        self.seek_write_impl(key, ts, true)
    }

    fn seek_write_impl(&mut self,
                       key: &Key,
                       ts: u64,
                       reverse: bool)
                       -> Result<Option<(u64, Write)>> {
        if self.scan_mode.is_some() {
            if self.write_cursor.is_none() {
                self.write_cursor = Some(try!(self.snapshot
                    .iter_cf(CF_WRITE,
                             IterOption::new(None, self.fill_cache, SeekMode::TotalOrderSeek),
                             self.get_scan_mode(false))));
            }
        } else {
            let upper_bound_key = key.append_ts(0u64);
            let upper_bound = upper_bound_key.encoded().clone();
            // use prefix bloom filter
            self.write_cursor = Some(try!(self.snapshot
                .iter_cf(CF_WRITE,
                         IterOption::new(Some(upper_bound), true, SeekMode::PrefixSeek),
                         ScanMode::Mixed)));
        }

        let mut cursor = self.write_cursor.as_mut().unwrap();
        let ok = if reverse {
            try!(cursor.near_seek_for_prev(&key.append_ts(ts)))
        } else {
            try!(cursor.near_seek(&key.append_ts(ts)))
        };
        if !ok {
            return Ok(None);
        }
        let write_key = Key::from_encoded(cursor.key().to_vec());
        let commit_ts = try!(write_key.decode_ts());
        let k = try!(write_key.truncate_ts());
        if &k != key {
            return Ok(None);
        }
        let write = try!(Write::parse(cursor.value()));
        Ok(Some((commit_ts, write)))
    }

    pub fn get(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        // Check for locks that signal concurrent writes.
        if let Some(lock) = try!(self.load_lock(key)) {
            if lock.ts <= ts {
                if ts == u64::MAX && try!(key.raw()) == lock.primary {
                    // when ts==u64::MAX(which means to get latest committed version for
                    // primary key),and current key is the primary key, returns the latest
                    // commit version's value
                    ts = lock.ts - 1;
                } else {
                    // There is a pending lock. Client should wait or clean it.
                    return Err(Error::KeyIsLocked {
                        key: try!(key.raw()),
                        primary: lock.primary,
                        ts: lock.ts,
                        ttl: lock.ttl,
                    });
                }
            }
        }
        loop {
            match try!(self.seek_write(key, ts)) {
                Some((commit_ts, mut write)) => {
                    match write.write_type {
                        WriteType::Put => {
                            if write.short_value.is_some() {
                                if self.key_only {
                                    return Ok(Some(vec![]));
                                }
                                return Ok(write.short_value.take());
                            }
                            return self.load_data(key, write.start_ts).map(Some);
                        }
                        WriteType::Delete => return Ok(None),
                        WriteType::Lock | WriteType::Rollback => ts = commit_ts - 1,
                    }
                }
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_ts(&mut self, key: &Key, start_ts: u64) -> Result<Option<u64>> {
        if let Some((commit_ts, write)) = try!(self.reverse_seek_write(key, start_ts)) {
            if write.start_ts == start_ts {
                match write.write_type {
                    WriteType::Put | WriteType::Delete | WriteType::Lock => {
                        return Ok(Some(commit_ts))
                    }
                    WriteType::Rollback => {}
                }
            }
        }
        Ok(None)
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            self.write_cursor = Some(try!(self.snapshot
                .iter_cf(CF_WRITE,
                         IterOption::new(self.upper_bound.as_ref().cloned(),
                                         self.fill_cache,
                                         SeekMode::TotalOrderSeek),
                         self.get_scan_mode(false))));
        }
        Ok(())
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            self.lock_cursor = Some(try!(self.snapshot
                .iter_cf(CF_LOCK,
                         IterOption::new(self.upper_bound.as_ref().cloned(),
                                         true,
                                         SeekMode::TotalOrderSeek),
                         self.get_scan_mode(true))));
        }
        Ok(())
    }

    pub fn seek(&mut self,
                mut key: Key,
                ts: u64,
                metrics: &mut ScanMetrics)
                -> Result<Option<(Key, Value)>> {
        assert!(self.scan_mode.is_some());
        try!(self.create_write_cursor());
        try!(self.create_lock_cursor());

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let mut w_cur = self.write_cursor.as_mut().unwrap();
                let mut l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if try!(w_cur.near_seek(&key)) {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if try!(l_cur.near_seek(&key)) {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => try!(Key::from_encoded(k.to_vec()).truncate_ts()),
                    (Some(wk), Some(lk)) => {
                        if wk < lk {
                            try!(Key::from_encoded(wk.to_vec()).truncate_ts())
                        } else {
                            Key::from_encoded(lk.to_vec())
                        }
                    }
                }
            };
            metrics.scanned_keys += 1;
            if let Some(v) = try!(self.get(&key, ts)) {
                return Ok(Some((key, v)));
            }
            metrics.skipped_keys += 1;
            key = key.append_ts(0);
        }
    }

    pub fn reverse_seek(&mut self,
                        mut key: Key,
                        ts: u64,
                        metrics: &mut ScanMetrics)
                        -> Result<Option<(Key, Value)>> {
        assert!(self.scan_mode.is_some());
        try!(self.create_write_cursor());
        try!(self.create_lock_cursor());

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let mut w_cur = self.write_cursor.as_mut().unwrap();
                let mut l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if try!(w_cur.near_reverse_seek(&key)) {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if try!(l_cur.near_reverse_seek(&key)) {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => try!(Key::from_encoded(k.to_vec()).truncate_ts()),
                    (Some(wk), Some(lk)) => {
                        if wk < lk {
                            Key::from_encoded(lk.to_vec())
                        } else {
                            try!(Key::from_encoded(wk.to_vec()).truncate_ts())
                        }
                    }
                }
            };
            metrics.scanned_keys += 1;
            if let Some(v) = try!(self.get(&key, ts)) {
                return Ok(Some((key, v)));
            }
            metrics.skipped_keys += 1;
        }
    }

    #[allow(type_complexity)]
    pub fn scan_lock<F>(&mut self,
                        start: Option<Key>,
                        filter: F,
                        limit: Option<usize>)
                        -> Result<(Vec<(Key, Lock)>, Option<Key>)>
        where F: Fn(&Lock) -> bool
    {
        try!(self.create_lock_cursor());
        let mut cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match start {
            Some(ref x) => try!(cursor.seek(x)),
            None => cursor.seek_to_first(),
        };
        if !ok {
            return Ok((vec![], None));
        }
        let mut locks = vec![];
        while cursor.valid() {
            let key = Key::from_encoded(cursor.key().to_vec());
            let lock = try!(Lock::parse(cursor.value()));
            if filter(&lock) {
                locks.push((key.clone(), lock));
                if let Some(limit) = limit {
                    if locks.len() >= limit {
                        return Ok((locks, Some(key)));
                    }
                }
            }
            cursor.next();
        }
        Ok((locks, None))
    }

    pub fn scan_keys(&mut self,
                     mut start: Option<Key>,
                     limit: usize)
                     -> Result<(Vec<Key>, Option<Key>)> {
        let mut cursor = try!(self.snapshot
            .iter_cf(CF_WRITE,
                     IterOption::new(None, self.fill_cache, SeekMode::TotalOrderSeek),
                     self.get_scan_mode(false)));
        let mut keys = vec![];
        loop {
            let ok = match start {
                Some(ref x) => try!(cursor.near_seek(x)),
                None => cursor.seek_to_first(),
            };
            if !ok {
                return Ok((keys, None));
            }
            if keys.len() >= limit {
                return Ok((keys, start));
            }
            let key = try!(Key::from_encoded(cursor.key().to_vec()).truncate_ts());
            start = Some(key.append_ts(0));
            keys.push(key);
        }
    }
}
