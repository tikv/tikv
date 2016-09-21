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

use storage::engine::{Snapshot, Cursor};
use storage::{Key, Value, CF_LOCK, CF_WRITE};
use super::{Error, Result};
use super::lock::Lock;
use super::write::{Write, WriteType};

pub struct MvccReader<'a> {
    snapshot: &'a Snapshot,
    // cursors are used for speeding up scans.
    data_cursor: Option<Box<Cursor + 'a>>,
    lock_cursor: Option<Box<Cursor + 'a>>,
    write_cursor: Option<Box<Cursor + 'a>>,

    // true: the reader mainly used for scanning,
    // false: the reader mainly used for point get.
    used_for_scan: bool,
}

impl<'a> MvccReader<'a> {
    pub fn new(snapshot: &Snapshot, used_for_scan: bool) -> MvccReader {
        MvccReader {
            snapshot: snapshot,
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            used_for_scan: used_for_scan,
        }
    }

    pub fn load_data(&mut self, key: &Key, ts: u64) -> Result<Option<Value>> {
        let k = key.append_ts(ts);
        if let Some(ref mut cursor) = self.data_cursor {
            cursor.get(&k).map(|x| x.map(|x| x.to_vec())).map_err(Error::from)
        } else {
            self.snapshot.get(&k).map_err(Error::from)
        }
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
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
        if self.used_for_scan {
            if self.write_cursor.is_none() {
                self.write_cursor = Some(try!(self.snapshot.iter_cf(CF_WRITE, None)));
            }
        } else {
            let upper_bound_key = key.append_ts(0u64);
            let upper_bound = upper_bound_key.encoded().as_slice();
            self.write_cursor = Some(try!(self.snapshot.iter_cf(CF_WRITE, Some(upper_bound))));
        }

        let mut cursor = self.write_cursor.as_mut().unwrap();
        let ok = if reverse {
            try!(cursor.near_reverse_seek(&key.append_ts(ts)))
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
                // There is a pending lock. Client should wait or clean it.
                return Err(Error::KeyIsLocked {
                    key: try!(key.raw()),
                    primary: lock.primary,
                    ts: lock.ts,
                });
            }
        }
        loop {
            match try!(self.seek_write(key, ts)) {
                Some((commit_ts, write)) => {
                    match write.write_type {
                        WriteType::Put => return self.load_data(key, write.start_ts),
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

    fn create_data_cursor(&mut self) -> Result<()> {
        if self.data_cursor.is_none() {
            self.data_cursor = Some(try!(self.snapshot.iter(None)));
        }
        Ok(())
    }

    pub fn seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        try!(self.create_data_cursor());

        loop {
            key = {
                let mut cursor = self.data_cursor.as_mut().unwrap();
                if !try!(cursor.near_seek(&key)) {
                    return Ok(None);
                }
                try!(Key::from_encoded(cursor.key().to_vec()).truncate_ts())
            };
            if let Some(v) = try!(self.get(&key, ts)) {
                return Ok(Some((key, v)));
            }
            key = key.append_ts(0);
        }
    }

    pub fn reverse_seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        try!(self.create_data_cursor());

        loop {
            key = {
                let mut cursor = self.data_cursor.as_mut().unwrap();
                if !try!(cursor.near_reverse_seek(&key)) {
                    return Ok(None);
                }
                try!(Key::from_encoded(cursor.key().to_vec()).truncate_ts())
            };
            if let Some(v) = try!(self.get(&key, ts)) {
                return Ok(Some((key, v)));
            }
        }
    }

    pub fn scan_lock<F>(&mut self, filter: F) -> Result<Vec<(Key, Lock)>>
        where F: Fn(&Lock) -> bool
    {
        if self.lock_cursor.is_none() {
            self.lock_cursor = Some(try!(self.snapshot.iter_cf(CF_LOCK, None)));
        }
        let mut cursor = self.lock_cursor.as_mut().unwrap();
        cursor.seek_to_first();
        let mut locks = vec![];
        while cursor.valid() {
            let key = Key::from_encoded(cursor.key().to_vec());
            let lock = try!(Lock::parse(cursor.value()));
            if filter(&lock) {
                locks.push((key, lock));
            }
            cursor.next();
        }
        Ok(locks)
    }

    pub fn scan_keys(&mut self,
                     mut start: Option<Key>,
                     limit: usize)
                     -> Result<(Vec<Key>, Option<Key>)> {
        let mut cursor = try!(self.snapshot.iter_cf(CF_WRITE, None));
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
