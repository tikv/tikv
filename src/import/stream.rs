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
use kvproto::metapb::*;
use kvproto::importpb::*;

use super::{Client, Config, Engine, Result};
use super::region::*;

pub struct SSTInfo {
    path: PathBuf,
    range: Range,
    next: Vec<u8>,
}

impl SSTInfo {
    pub fn gen_sst(&self, engine: &Engine, cf_name: &str) -> Result<SSTFile> {
        let mut data = Vec::new();
        let mut f = engine.new_sst_reader(&self.path)?;
        f.read_to_end(&mut data)?;
        let _ = engine.delete_sst_file(&self.path);

        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&data);

        // This range doesn't contain the data prefix, like region range.
        let mut range = Range::new();
        range.set_start(keys::origin_key(self.range.get_start()).to_owned());
        range.set_end(keys::origin_key(self.range.get_end()).to_owned());

        let mut meta = SSTMeta::new();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        meta.set_range(range);
        meta.set_crc32(digest.sum32());
        meta.set_length(data.len() as u64);
        meta.set_cf_name(cf_name.to_owned());

        Ok(SSTFile {
            meta: meta,
            data: data,
            next: self.next.clone(),
        })
    }
}

pub struct SSTFile {
    pub meta: SSTMeta,
    pub data: Vec<u8>,
    next: Vec<u8>,
}

impl SSTFile {
    pub fn is_inside(&self, region: &Region) -> bool {
        let range = self.meta.get_range();
        range.get_start() >= region.get_start_key() &&
            belongs_in_end(range.get_end(), region.get_end_key())
    }

    pub fn extended_range(&self) -> Range {
        new_range(self.meta.get_range().get_start(), &self.next)
    }
}

impl fmt::Display for SSTFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SSTFile {{uuid: {}, range: {{{:?}}}, length: {}, cf_name: {}}}",
            Uuid::from_bytes(self.meta.get_uuid()).unwrap(),
            self.meta.get_range(),
            self.meta.get_length(),
            self.meta.get_cf_name(),
        )
    }
}

pub struct SSTFileStream {
    dir: TempDir,
    fileno: u64,
    engine: Arc<Engine>,
    cf_name: String,
    range_iter: RangeIterator,
    region_ctx: RegionContext<Client>,
}

impl SSTFileStream {
    pub fn new(
        cfg: Config,
        dir: TempDir,
        client: Arc<Client>,
        engine: Arc<Engine>,
        cf_name: String,
        sst_range: Range,
        finished_ranges: Vec<Range>,
    ) -> SSTFileStream {
        let iter = engine.new_iter(&cf_name, true);
        let range_iter = RangeIterator::new(iter, sst_range, finished_ranges);
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
            let data_key = keys::data_key(self.range_iter.key());
            writer.put(&data_key, self.range_iter.value())?;
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
        Ok(Some(self.new_sst_info(info, next)))
    }

    fn new_sst_writer(&mut self) -> Result<SstFileWriter> {
        self.fileno += 1;
        let name = format!("{}.sst", self.fileno);
        let path = self.dir.path().join(name);
        assert!(!path.exists());
        self.engine.new_sst_writer(&self.cf_name, path)
    }

    fn new_sst_info(&self, info: ExternalSstFileInfo, next: Vec<u8>) -> SSTInfo {
        SSTInfo {
            path: info.file_path(),
            range: new_range(info.smallest_key(), info.largest_key()),
            next: next,
        }
    }
}

pub struct RangeIterator {
    iter: DBIterator<Arc<DB>>,
    ranges: Vec<Range>,
    ranges_index: usize,
}

impl RangeIterator {
    pub fn new(
        iter: DBIterator<Arc<DB>>,
        range: Range,
        mut finished_ranges: Vec<Range>,
    ) -> RangeIterator {
        finished_ranges.sort_by(|a, b| a.get_start().cmp(b.get_start()));

        // Collect unfinished ranges.
        let mut ranges = Vec::new();
        let mut last_end = range.get_start();
        for range in &finished_ranges {
            if last_end < range.get_start() {
                ranges.push(new_range(last_end, range.get_start()));
            }
            if belongs_in_end(last_end, range.get_end()) {
                last_end = range.get_end();
            }
            if last_end == RANGE_MAX {
                break;
            }
        }
        // Handle the last unfinished range.
        if finished_ranges.is_empty() || is_before_end(last_end, range.get_end()) {
            ranges.push(new_range(last_end, range.get_end()));
        }

        let mut res = RangeIterator {
            iter: iter,
            ranges: ranges,
            ranges_index: 0,
        };

        // Seek to the first valid range.
        res.seek_next();
        res
    }

    pub fn next(&mut self) -> bool {
        if !self.valid() || !self.iter.next() {
            return false;
        }
        {
            let range = &self.ranges[self.ranges_index];
            if belongs_in_end(self.iter.key(), range.get_end()) {
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
            if belongs_in_end(self.iter.key(), range.get_end()) {
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

    fn new_range_iter(db: Arc<DB>, range: Range, skip_ranges: Vec<Range>) -> RangeIterator {
        let ropts = ReadOptions::new();
        let iter = DBIterator::new(db.clone(), ropts);
        RangeIterator::new(iter, range, skip_ranges)
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

    fn test_range_iterator_with(
        db: Arc<DB>,
        range_opt: (Option<i32>, Option<i32>),
        finished_ranges_opt: &[(Option<i32>, Option<i32>)],
        unfinished_ranges: &[(i32, i32)],
    ) {
        let range = new_int_range(range_opt.0, range_opt.1);
        let mut finished_ranges = Vec::new();
        for &(start, end) in finished_ranges_opt {
            finished_ranges.push(new_int_range(start, end));
        }
        let mut iter = new_range_iter(db, range, finished_ranges);
        for &(start, end) in unfinished_ranges {
            check_range_iter(&mut iter, start, end);
        }
        assert!(!iter.next());
        assert!(!iter.valid());
    }

    #[test]
    fn test_range_iterator() {
        let dir = TempDir::new("_tikv_test_tmp_db").unwrap();
        let db = open_db(dir.path());

        for i in 0..100 {
            let k = format!("k-{:04}", i);
            let v = format!("v-{:04}", i);
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }

        // No finished ranges.
        test_range_iterator_with(db.clone(), (None, None), &[], &[(0, 100)]);
        test_range_iterator_with(db.clone(), (None, Some(25)), &[], &[(0, 25)]);
        test_range_iterator_with(db.clone(), (Some(0), Some(25)), &[], &[(0, 25)]);
        test_range_iterator_with(db.clone(), (Some(25), Some(75)), &[], &[(25, 75)]);
        test_range_iterator_with(db.clone(), (Some(75), Some(100)), &[], &[(75, 100)]);
        test_range_iterator_with(db.clone(), (Some(75), None), &[], &[(75, 100)]);

        // Range [None, None) with some finished ranges.
        test_range_iterator_with(db.clone(), (None, None), &[(None, None)], &[]);
        test_range_iterator_with(
            db.clone(),
            (None, None),
            &[(None, Some(25)), (Some(50), Some(75))],
            &[(25, 50), (75, 100)],
        );
        test_range_iterator_with(
            db.clone(),
            (None, None),
            &[(Some(25), Some(50)), (Some(75), None)],
            &[(0, 25), (50, 75)],
        );
        test_range_iterator_with(
            db.clone(),
            (None, None),
            &[
                (Some(0), Some(25)),
                (Some(50), Some(60)),
                (Some(60), Some(70)),
            ],
            &[(25, 50), (70, 100)],
        );
        test_range_iterator_with(
            db.clone(),
            (None, None),
            &[
                (Some(10), Some(30)),
                (Some(50), Some(70)),
                (Some(80), Some(90)),
                (Some(20), Some(40)),
                (Some(60), Some(80)),
                (Some(70), Some(100)),
            ],
            &[(0, 10), (40, 50)],
        );

        // Range [25, 75) with some finished ranges.
        test_range_iterator_with(db.clone(), (Some(25), Some(75)), &[(None, None)], &[]);
        test_range_iterator_with(
            db.clone(),
            (Some(25), Some(75)),
            &[(None, Some(30)), (Some(50), Some(75))],
            &[(30, 50)],
        );
        test_range_iterator_with(
            db.clone(),
            (Some(25), Some(75)),
            &[(Some(30), Some(50)), (Some(60), None)],
            &[(25, 30), (50, 60)],
        );
        test_range_iterator_with(
            db.clone(),
            (Some(25), Some(75)),
            &[
                (Some(25), Some(30)),
                (Some(50), Some(60)),
                (Some(60), Some(70)),
            ],
            &[(30, 50), (70, 75)],
        );
        test_range_iterator_with(
            db.clone(),
            (Some(25), Some(75)),
            &[
                (Some(35), Some(45)),
                (Some(55), Some(60)),
                (Some(70), Some(75)),
                (Some(30), Some(40)),
                (Some(50), Some(65)),
                (Some(60), Some(75)),
            ],
            &[(25, 30), (45, 50)],
        );
    }
}
