// Copyright 2017 PingCAP, Inc.
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

use server::coprocessor::Result;
use kvproto::coprocessor::KeyRange;
use storage::{Key, Value, SnapshotStore, Statistics, ScanMode};
use util::escape;

// Scanner for TableScan && IndexScan
pub struct BaseScanner<'a> {
    store: SnapshotStore<'a>,
    seek_key: Option<Vec<u8>>,
    scan_mode: ScanMode,
    upper_bound: Option<Vec<u8>>,
    region_start: Vec<u8>,
    region_end: Vec<u8>,
    statistics: &'a mut Statistics,
    desc: bool,
    key_only: bool,
}


impl<'a> BaseScanner<'a> {
    pub fn new(desc: bool,
               key_only: bool,
               store: SnapshotStore<'a>,
               region_start: Vec<u8>,
               region_end: Vec<u8>,
               statistics: &'a mut Statistics)
               -> BaseScanner<'a> {

        let scan_mode = if desc {
            ScanMode::Backward
        } else {
            ScanMode::Forward
        };
        BaseScanner {
            store: store,
            seek_key: None,
            scan_mode: scan_mode,
            upper_bound: None,
            region_start: region_start,
            region_end: region_end,
            statistics: statistics,
            desc: desc,
            key_only: key_only,
        }
    }

    pub fn get_row_from_range(&mut self, range: &KeyRange) -> (Result<Option<(Vec<u8>, Value)>>) {
        let seek_key = self.prepare_and_get_seek_key(range);
        if range.get_start() > range.get_end() {
            return Ok(None);
        }
        let mut scanner = box_try!(self.store.scanner(self.scan_mode,
                                                      self.key_only,
                                                      self.upper_bound.clone(),
                                                      self.statistics));
        let kv = if self.desc {
            box_try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
        } else {
            box_try!(scanner.seek(Key::from_raw(&seek_key)))
        };

        let (key, value) = match kv {
            Some((key, value)) => (box_try!(key.raw()), value),
            None => return Ok(None),
        };

        if range.get_start() > key.as_slice() || range.get_end() <= key.as_slice() {
            debug!("key: {} out of range [{}, {})",
                   escape(&key),
                   escape(range.get_start()),
                   escape(range.get_end()));
            return Ok(None);
        }
        Ok(Some((key, value)))
    }

    pub fn get_row_from_point(&mut self, key: &[u8]) -> Result<Option<Value>> {
        let data = box_try!(self.store
            .get(&Key::from_raw(key), &mut self.statistics));
        Ok(data)
    }

    #[inline]
    pub fn set_seek_key(&mut self, seek_key: Option<Vec<u8>>) {
        self.seek_key = seek_key;
    }

    fn prepare_and_get_seek_key(&mut self, range: &KeyRange) -> (Vec<u8>) {
        if self.seek_key.is_some() {
            let seek_key = self.seek_key.take().unwrap();
            self.seek_key = None;
            return seek_key;
        }
        self.upper_bound = None;
        if self.desc {
            let range_end = range.get_end().to_vec();
            let seek_key = if self.region_end.is_empty() || range_end < self.region_end {
                self.region_end.clone()
            } else {
                range_end
            };
            return seek_key;
        }

        if range.has_end() {
            self.upper_bound = Some(Key::from_raw(range.get_end()).encoded().clone());
        }

        let range_start = range.get_start().to_vec();
        if range_start > self.region_start {
            range_start
        } else {
            self.region_start.clone()
        }
    }
}
