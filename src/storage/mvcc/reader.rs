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
use storage::{Key, Value, CF_WRITE};
use util::codec::number::NumberDecoder;
use super::{Error, Result};
use super::lock::Lock;

pub struct MvccReader<'a> {
    snapshot: &'a Snapshot,
    // cursors are used for speeding up scans.
    data_cursor: Option<Box<Cursor + 'a>>,
    lock_cursor: Option<Box<Cursor + 'a>>,
    write_cursor: Option<Box<Cursor + 'a>>,
}

impl<'a> MvccReader<'a> {
    pub fn new(snapshot: &Snapshot) -> MvccReader {
        MvccReader {
            snapshot: snapshot,
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
        }
    }

    fn load_data(&mut self, key: &Key, ts: u64) -> Result<Option<Value>> {
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
            match try!(self.snapshot.get_cf("lock", &key)) {
                Some(v) => Ok(Some(try!(Lock::parse(&v)))),
                None => Ok(None),
            }
        }
    }

    pub fn seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, u64)>> {
        self.seek_write_impl(key, ts, false)
    }

    pub fn reverse_seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, u64)>> {
        self.seek_write_impl(key, ts, true)
    }

    fn seek_write_impl(&mut self, key: &Key, ts: u64, reverse: bool) -> Result<Option<(u64, u64)>> {
        let (k, v) = {
            if self.write_cursor.is_none() {
                self.write_cursor = Some(try!(self.snapshot.iter_cf(CF_WRITE)));
            }
            let mut cursor = self.write_cursor.as_mut().unwrap();
            let ok = if reverse {
                try!(cursor.reverse_seek(&key.append_ts(ts)))
            } else {
                try!(cursor.seek(&key.append_ts(ts)))
            };
            if !ok {
                return Ok(None);
            }
            (cursor.key().to_vec(), cursor.value().to_vec())
        };
        let (k, s, c) = try!(self.decode_write(k, v));
        if &k == key {
            Ok(Some((s, c)))
        } else {
            Ok(None)
        }
    }

    fn decode_write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(Key, u64, u64)> {
        let k = Key::from_encoded(key);
        let commit_ts = try!(k.decode_ts());
        let key = try!(k.truncate_ts());
        let start_ts = try!(value.as_slice().decode_var_u64());
        Ok((key, start_ts, commit_ts))
    }

    pub fn get(&mut self, key: &Key, ts: u64) -> Result<Option<Value>> {
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
        match try!(self.seek_write(key, ts)) {
            Some((start_ts, _)) => self.load_data(key, start_ts),
            None => Ok(None),
        }
    }

    pub fn get_txn_commit_ts(&mut self, key: &Key, start_ts: u64) -> Result<Option<u64>> {
        match try!(self.reverse_seek_write(key, start_ts)) {
            Some((s, commit_ts)) if s == start_ts => Ok(Some(commit_ts)),
            _ => Ok(None),
        }
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        if self.data_cursor.is_none() {
            self.data_cursor = Some(try!(self.snapshot.iter()));
        }
        Ok(())
    }

    pub fn seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        try!(self.create_data_cursor());

        loop {
            key = {
                let mut cursor = self.data_cursor.as_mut().unwrap();
                if !try!(cursor.seek(&key)) {
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
                if !try!(cursor.reverse_seek(&key)) {
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
            self.lock_cursor = Some(try!(self.snapshot.iter_cf("lock")));
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
}
