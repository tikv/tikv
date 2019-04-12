// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

use engine::rocks::{DBIterator, SeekKey, DB};
use kvproto::import_sstpb::*;
use kvproto::metapb::*;

use super::client::*;
use super::common::*;
use super::engine::*;
use super::{Config, Result};

pub struct SSTFile {
    pub meta: SSTMeta,
    pub(crate) info: LazySSTInfo,
}

impl SSTFile {
    pub fn inside_region(&self, region: &Region) -> bool {
        let range = self.meta.get_range();
        assert!(range.get_start() <= range.get_end());
        inside_region(range.get_start(), region) && inside_region(range.get_end(), region)
    }
}

impl fmt::Debug for SSTFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uuid = Uuid::from_bytes(self.meta.get_uuid()).unwrap();
        f.debug_struct("SSTFile")
            .field("uuid", &uuid)
            .field("range", self.meta.get_range())
            .field("length", &self.meta.get_length())
            .field("cf_name", &self.meta.get_cf_name().to_owned())
            .finish()
    }
}

pub type LazySSTRange = (Range, Vec<LazySSTInfo>);

pub struct SSTFileStream<Client> {
    ctx: RangeContext<Client>,
    iter: RangeIterator,
    engine: Arc<Engine>,
    stream_range: Range,
}

impl<Client: ImportClient> SSTFileStream<Client> {
    pub fn new(
        cfg: Config,
        client: Arc<Client>,
        engine: Arc<Engine>,
        stream_range: Range,
        finished_ranges: Vec<Range>,
    ) -> SSTFileStream<Client> {
        let ctx = RangeContext::new(client, cfg.region_split_size.0 as usize);
        let engine_iter = engine.new_iter(true);
        let iter = RangeIterator::new(engine_iter, stream_range.clone(), finished_ranges);

        SSTFileStream {
            ctx,
            iter,
            engine,
            stream_range,
        }
    }

    pub fn next(&mut self) -> Result<Option<LazySSTRange>> {
        if !self.iter.valid() {
            return Ok(None);
        }

        let mut w = self.engine.new_sst_writer()?;
        let start = self.iter.key().to_owned();
        self.ctx.reset(&start);

        loop {
            {
                let k = self.iter.key();
                let v = self.iter.value();
                w.put(k, v)?;
                self.ctx.add(k.len() + v.len());
            }
            if !self.iter.next() || self.ctx.should_stop_before(self.iter.key()) {
                break;
            }
        }

        let end = if self.iter.valid() {
            self.iter.key()
        } else {
            self.stream_range.get_end()
        };
        let range = new_range(&start, end);

        let infos = w.finish()?;
        Ok(Some((range, infos)))
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
        let mut reach_end = false;
        for range in &finished_ranges {
            if last_end < range.get_start() {
                ranges.push(new_range(last_end, range.get_start()));
            }
            if before_end(last_end, range.get_end()) {
                last_end = range.get_end();
            }
            if last_end == RANGE_MAX {
                reach_end = true;
                break;
            }
        }
        // Handle the last unfinished range.
        if !reach_end && before_end(last_end, range.get_end()) {
            ranges.push(new_range(last_end, range.get_end()));
        }

        let mut res = RangeIterator {
            iter,
            ranges,
            ranges_index: 0,
        };

        // Seek to the first valid range.
        res.seek_next();
        res
    }

    pub fn next(&mut self) -> bool {
        if !self.iter.next() {
            return false;
        }
        {
            let range = &self.ranges[self.ranges_index];
            if before_end(self.iter.key(), range.get_end()) {
                return true;
            }
            self.ranges_index += 1;
        }
        self.seek_next()
    }

    fn seek_next(&mut self) -> bool {
        while let Some(range) = self.ranges.get(self.ranges_index) {
            if !self.iter.seek(SeekKey::Key(range.get_start())) {
                break;
            }
            assert!(self.iter.key() >= range.get_start());
            if before_end(self.iter.key(), range.get_end()) {
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
    use super::*;
    use crate::import::test_helpers::*;

    use std::path::Path;
    use std::sync::Arc;

    use engine::rocks::{DBIterator, DBOptions, ReadOptions, Writable, DB};
    use tempdir::TempDir;

    use crate::config::DbConfig;
    use crate::storage::types::Key;
    use tikv_util::security::SecurityConfig;

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
        let iter = DBIterator::new(Arc::clone(&db), ropts);
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
        test_range_iterator_with(Arc::clone(&db), (None, None), &[], &[(0, 100)]);
        test_range_iterator_with(Arc::clone(&db), (None, Some(25)), &[], &[(0, 25)]);
        test_range_iterator_with(Arc::clone(&db), (Some(0), Some(25)), &[], &[(0, 25)]);
        test_range_iterator_with(Arc::clone(&db), (Some(25), Some(75)), &[], &[(25, 75)]);
        test_range_iterator_with(Arc::clone(&db), (Some(75), Some(100)), &[], &[(75, 100)]);
        test_range_iterator_with(Arc::clone(&db), (Some(75), None), &[], &[(75, 100)]);

        // Range [None, None) with some finished ranges.
        test_range_iterator_with(Arc::clone(&db), (None, None), &[(None, None)], &[]);
        test_range_iterator_with(
            Arc::clone(&db),
            (None, None),
            &[(None, Some(25)), (Some(50), Some(75))],
            &[(25, 50), (75, 100)],
        );
        test_range_iterator_with(
            Arc::clone(&db),
            (None, None),
            &[(Some(25), Some(50)), (Some(75), None)],
            &[(0, 25), (50, 75)],
        );
        test_range_iterator_with(
            Arc::clone(&db),
            (None, None),
            &[
                (Some(0), Some(25)),
                (Some(50), Some(60)),
                (Some(60), Some(70)),
            ],
            &[(25, 50), (70, 100)],
        );
        test_range_iterator_with(
            Arc::clone(&db),
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
        test_range_iterator_with(Arc::clone(&db), (Some(25), Some(75)), &[(None, None)], &[]);
        test_range_iterator_with(
            Arc::clone(&db),
            (Some(25), Some(75)),
            &[(None, Some(30)), (Some(50), Some(75))],
            &[(30, 50)],
        );
        test_range_iterator_with(
            Arc::clone(&db),
            (Some(25), Some(75)),
            &[(Some(30), Some(50)), (Some(60), None)],
            &[(25, 30), (50, 60)],
        );
        test_range_iterator_with(
            Arc::clone(&db),
            (Some(25), Some(75)),
            &[
                (Some(25), Some(30)),
                (Some(50), Some(60)),
                (Some(60), Some(70)),
            ],
            &[(30, 50), (70, 75)],
        );
        test_range_iterator_with(
            Arc::clone(&db),
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

    fn new_encoded_range(start: u8, end: u8) -> Range {
        let k1 = Key::from_raw(&[start]).append_ts(0);
        let k2 = Key::from_raw(&[end]).append_ts(0);
        new_range(k1.as_encoded(), k2.as_encoded())
    }

    #[test]
    fn test_sst_file_stream() {
        let dir = TempDir::new("test_import_sst_file_stream").unwrap();
        let uuid = Uuid::new_v4();
        let db_cfg = DbConfig::default();
        let security_cfg = SecurityConfig::default();
        let engine = Arc::new(Engine::new(dir.path(), uuid, db_cfg, security_cfg).unwrap());

        for i in 0..16 {
            let k = Key::from_raw(&[i]).append_ts(0);
            assert_eq!(k.as_encoded().len(), 17);
            engine.put(k.as_encoded(), k.as_encoded()).unwrap();
        }

        let mut cfg = Config::default();
        cfg.region_split_size.0 = 128; // An SST contains at most 4 entries.

        let mut client = MockClient::new();
        let keys = vec![
            // [0, 3], [4, 6]
            7, // [7, 9]
            10,
            // [10, 13], [14, 15]
        ];
        let mut last = vec![];
        for i in keys {
            let k = Key::from_raw(&[i]).append_ts(0);
            client.add_region_range(&last, k.as_encoded());
            last = k.into_encoded();
        }
        // Add an unrelated range.
        client.add_region_range(&last, b"abc");
        client.add_region_range(b"abc", b"");

        let client = Arc::new(client);

        // Test all ranges.
        {
            let sst_range = new_range(RANGE_MIN, RANGE_MAX);
            let finished_ranges = Vec::new();
            let expected_ranges = vec![
                (0, 3, Some(4)),
                (4, 6, Some(7)),
                (7, 9, Some(10)),
                (10, 13, Some(14)),
                (14, 15, None),
            ];
            run_and_check_stream(
                cfg.clone(),
                Arc::clone(&client),
                Arc::clone(&engine),
                sst_range,
                finished_ranges,
                expected_ranges,
            );
        }

        // Test sst range [1, 15) with finished ranges [3, 5), [7, 10).
        {
            let sst_range = new_encoded_range(1, 15);
            let mut finished_ranges = Vec::new();
            finished_ranges.push(new_encoded_range(3, 5));
            finished_ranges.push(new_encoded_range(7, 11));
            let expected_ranges = vec![(1, 6, Some(11)), (11, 14, Some(15))];
            run_and_check_stream(
                cfg.clone(),
                Arc::clone(&client),
                Arc::clone(&engine),
                sst_range,
                finished_ranges,
                expected_ranges,
            );
        }
    }

    fn run_and_check_stream(
        cfg: Config,
        client: Arc<MockClient>,
        engine: Arc<Engine>,
        sst_range: Range,
        finished_ranges: Vec<Range>,
        expected_ranges: Vec<(u8, u8, Option<u8>)>,
    ) {
        let mut stream = SSTFileStream::new(cfg, client, engine, sst_range, finished_ranges);
        for (start, end, range_end) in expected_ranges {
            let (range, ssts) = stream.next().unwrap().unwrap();
            let start = Key::from_raw(&[start]).append_ts(0).into_encoded();
            let end = Key::from_raw(&[end]).append_ts(0).into_encoded();
            let range_end = match range_end {
                Some(v) => Key::from_raw(&[v]).append_ts(0).into_encoded(),
                None => RANGE_MAX.to_owned(),
            };
            assert_eq!(range.get_start(), start.as_slice());
            assert_eq!(range.get_end(), range_end.as_slice());
            for sst in ssts {
                assert_eq!(sst.range.get_start(), start.as_slice());
                assert_eq!(sst.range.get_end(), end.as_slice());
            }
        }
        assert!(stream.next().unwrap().is_none());
    }
}
