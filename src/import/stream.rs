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

use std::fmt;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use crc::crc32::{self, Hasher32};
use tempdir::TempDir;
use uuid::Uuid;

use raftstore::store::keys;

use rocksdb::{DBIterator, ExternalSstFileInfo, SeekKey, SstFileWriter, DB};
use kvproto::importpb::*;

use super::{Client, Config, Engine, Result};
use super::region::*;

pub struct SSTInfo {
    path: PathBuf,
    range: Range,
    next_start: Vec<u8>,
}

impl SSTInfo {
    fn new(info: ExternalSstFileInfo, next_start: Vec<u8>) -> SSTInfo {
        SSTInfo {
            path: info.file_path(),
            range: new_range(info.smallest_key(), info.largest_key()),
            next_start: next_start,
        }
    }
}

pub struct SSTFile {
    pub meta: SSTMeta,
    pub data: Vec<u8>,
    next_start: Vec<u8>,
}

impl SSTFile {
    pub fn new(info: SSTInfo, engine: Arc<Engine>, cf_name: String) -> Result<SSTFile> {
        let mut f = engine.new_sst_reader(info.path)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;

        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&data);

        // This range doesn't contain the data prefix, like region range.
        let mut range = Range::new();
        range.set_start(keys::origin_key(info.range.get_start()).to_owned());
        range.set_end(keys::origin_key(info.range.get_end()).to_owned());

        let mut meta = SSTMeta::new();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        meta.set_range(range);
        meta.set_crc32(digest.sum32());
        meta.set_length(data.len() as u64);
        meta.set_cf_name(cf_name);

        Ok(SSTFile {
            meta: meta,
            data: data,
            next_start: info.next_start,
        })
    }

    pub fn extended_range(&self) -> Range {
        new_range(self.meta.get_range().get_start(), &self.next_start)
    }
}

impl fmt::Display for SSTFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SSTFile {{uuid: {}, length: {}, cf_name: {}, region_id: {}, region_epoch: {:?}}}",
            Uuid::from_bytes(self.meta.get_uuid()).unwrap(),
            self.meta.get_length(),
            self.meta.get_cf_name(),
            self.meta.get_region_id(),
            self.meta.get_region_epoch(),
        )
    }
}

pub struct SSTFileStream {
    dir: TempDir,
    fileno: u64,
    engine: Arc<Engine>,
    cf_name: String,
    range_iter: RangeIterator,
    region_ctx: RegionContext,
}

impl SSTFileStream {
    pub fn new(
        cfg: Config,
        dir: TempDir,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: String,
        skip_ranges: Vec<Range>,
    ) -> SSTFileStream {
        let iter = engine.new_iter(&cf_name, true);
        let range_iter = RangeIterator::new(iter, skip_ranges);
        let region_ctx = RegionContext::new(client, cfg.region_split_size);

        SSTFileStream {
            dir: dir,
            fileno: 0,
            engine: engine,
            cf_name: cf_name,
            range_iter: range_iter,
            region_ctx: region_ctx,
        }
    }

    pub fn next(&mut self) -> Result<Option<SSTInfo>> {
        if !self.range_iter.valid() {
            return Ok(None);
        }

        let mut writer = self.new_sst_writer()?;
        self.region_ctx.reset(self.range_iter.key());

        loop {
            writer.put(self.range_iter.key(), self.range_iter.value())?;
            if !self.range_iter.next() ||
                self.region_ctx.should_stop_before(self.range_iter.key())
            {
                break;
            }
        }

        let next = if self.range_iter.valid() {
            self.range_iter.key().to_owned()
        } else {
            RANGE_MAX.to_owned()
        };

        let info = writer.finish()?;
        Ok(Some(SSTInfo::new(info, next)))
    }

    fn new_sst_writer(&mut self) -> Result<SstFileWriter> {
        self.fileno += 1;
        let name = format!("{}.sst", self.fileno);
        let path = self.dir.path().join(name);
        assert!(!path.exists());
        self.engine.new_sst_writer(&self.cf_name, path)
    }
}

pub struct RangeIterator {
    iter: DBIterator<Arc<DB>>,
    ranges: Vec<Range>,
    ranges_index: usize,
}

impl RangeIterator {
    pub fn new(iter: DBIterator<Arc<DB>>, mut skip_ranges: Vec<Range>) -> RangeIterator {
        // Ranges are guaranteed to be not overlapped with each other,
        // so we just need to sort them by the start.
        skip_ranges.sort_by(|a, b| a.get_start().cmp(b.get_start()));

        // Collect unskipped ranges.
        let mut ranges = Vec::new();
        let mut start = RANGE_MIN;
        for range in &skip_ranges {
            assert!(start <= range.get_start());
            if start != range.get_start() {
                ranges.push(new_range(start, range.get_start()));
            }
            start = range.get_end();
        }
        if start != RANGE_MAX || skip_ranges.is_empty() {
            ranges.push(new_range(start, RANGE_MAX));
        }

        let mut res = RangeIterator {
            iter: iter,
            ranges: ranges,
            ranges_index: 0,
        };

        res.seek_next();
        res
    }

    pub fn next(&mut self) -> bool {
        if !self.valid() || !self.iter.next() {
            return false;
        }
        {
            let range = &self.ranges[self.ranges_index];
            if RangeEnd(self.iter.key()) < RangeEnd(range.get_end()) {
                return true;
            }
            self.ranges_index += 1;
        }
        self.seek_next()
    }

    fn seek_next(&mut self) -> bool {
        while let Some(range) = self.ranges.get(self.ranges_index) {
            // Write CF use a slice transform with fixed size, so we can not
            // just seek to RANGE_MIN.
            let seek_key = if range.get_start() == RANGE_MIN {
                SeekKey::Start
            } else {
                SeekKey::Key(range.get_start())
            };
            if !self.iter.seek(seek_key) {
                break;
            }
            assert!(self.iter.key() >= range.get_start());
            if RangeEnd(self.iter.key()) < RangeEnd(range.get_end()) {
                break;
            }
            self.ranges_index += 1;
        }
        self.valid()
    }

    pub fn key(&self) -> &[u8] {
        self.iter.key()
    }

    pub fn value(&self) -> &[u8] {
        self.iter.value()
    }

    pub fn valid(&self) -> bool {
        self.iter.valid() && self.ranges_index < self.ranges.len()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use rocksdb::{DBIterator, DBOptions, ReadOptions, Writable, DB};

    use super::*;

    fn open_db<P: AsRef<Path>>(path: P) -> Arc<DB> {
        let path = path.as_ref().to_str().unwrap();
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        let db = DB::open(opts, path).unwrap();
        Arc::new(db)
    }

    fn new_int_range(start: Option<i32>, end: Option<i32>) -> Range {
        let mut range = Range::new();
        if let Some(start) = start {
            let k = format!("k-{:04}", start);
            range.set_start(k.as_bytes().to_owned());
        }
        if let Some(end) = end {
            let k = format!("k-{:04}", end);
            range.set_end(k.as_bytes().to_owned());
        }
        range
    }

    fn new_range_iter(db: Arc<DB>, skip_ranges: Vec<Range>) -> RangeIterator {
        let ropts = ReadOptions::new();
        let iter = DBIterator::new(db.clone(), ropts);
        RangeIterator::new(iter, skip_ranges)
    }

    fn check_range_iter(iter: &mut RangeIterator, start: i32, end: i32) {
        for i in start..end {
            let k = format!("k-{:04}", i);
            let v = format!("v-{:04}", i);
            assert!(iter.valid());
            assert_eq!(iter.key(), k.as_bytes());
            assert_eq!(iter.value(), v.as_bytes());
            iter.next();
        }
    }

    #[test]
    fn test_range_iterator() {
        let dir = TempDir::new("_tikv_test_tmp_db").unwrap();
        let db = open_db(dir.path());

        let num_keys = 100;
        for i in 0..num_keys {
            let k = format!("k-{:04}", i);
            let v = format!("v-{:04}", i);
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }

        // Skip none.
        {
            let skip_ranges = Vec::new();
            let mut iter = new_range_iter(db.clone(), skip_ranges);
            check_range_iter(&mut iter, 0, num_keys);
            assert!(!iter.valid());
        }

        // Skip all.
        {
            let mut skip_ranges = Vec::new();
            skip_ranges.push(new_int_range(Some(0), Some(100)));
            let mut iter = new_range_iter(db.clone(), skip_ranges);
            assert!(!iter.next());
            assert!(!iter.valid());
        }

        // Skip all segment ranges.
        {
            let mut skip_ranges = Vec::new();
            skip_ranges.push(new_int_range(Some(0), Some(10)));
            skip_ranges.push(new_int_range(Some(30), Some(60)));
            skip_ranges.push(new_int_range(Some(10), Some(30)));
            skip_ranges.push(new_int_range(Some(60), Some(100)));
            skip_ranges.push(new_int_range(Some(100), Some(1000)));
            let mut iter = new_range_iter(db.clone(), skip_ranges);
            assert!(!iter.next());
            assert!(!iter.valid());
        }

        // Skip head and tail.
        {
            let mut skip_ranges = Vec::new();
            skip_ranges.push(new_int_range(Some(90), None));
            skip_ranges.push(new_int_range(None, Some(10)));
            let mut iter = new_range_iter(db.clone(), skip_ranges);
            check_range_iter(&mut iter, 10, 90);
            assert!(!iter.next());
            assert!(!iter.valid());
        }

        // Skip head and middle.
        {
            let mut skip_ranges = Vec::new();
            skip_ranges.push(new_int_range(None, Some(10)));
            skip_ranges.push(new_int_range(Some(60), Some(80)));
            skip_ranges.push(new_int_range(Some(20), Some(30)));
            skip_ranges.push(new_int_range(Some(30), Some(40)));
            let mut iter = new_range_iter(db.clone(), skip_ranges);
            check_range_iter(&mut iter, 10, 20);
            check_range_iter(&mut iter, 40, 60);
            check_range_iter(&mut iter, 80, 100);
            assert!(!iter.next());
            assert!(!iter.valid());
        }

        // Skip middle and tail.
        {
            let mut skip_ranges = Vec::new();
            skip_ranges.push(new_int_range(Some(90), None));
            skip_ranges.push(new_int_range(Some(60), Some(70)));
            skip_ranges.push(new_int_range(Some(30), Some(40)));
            skip_ranges.push(new_int_range(Some(40), Some(50)));
            skip_ranges.push(new_int_range(Some(70), Some(80)));
            skip_ranges.push(new_int_range(Some(10), Some(30)));
            let mut iter = new_range_iter(db.clone(), skip_ranges);
            check_range_iter(&mut iter, 0, 10);
            check_range_iter(&mut iter, 50, 60);
            check_range_iter(&mut iter, 80, 90);
            assert!(!iter.next());
            assert!(!iter.valid());
        }
    }
}
