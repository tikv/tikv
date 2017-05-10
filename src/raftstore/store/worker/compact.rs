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

use util::worker::Runnable;
use util::rocksdb;
use util::escape;

use rocksdb::{DB, CompactOptions};
use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::error;
use super::metrics::COMPACT_RANGE_CF;

pub struct Task {
    pub cf_name: String,
    pub start_key: Option<Vec<u8>>, // None means smallest key
    pub end_key: Option<Vec<u8>>, // None means largest key
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "Compact CF[{}], range[{:?}, {:?}]",
               self.cf_name,
               self.start_key.as_ref().map(|k| escape(&k)),
               self.end_key.as_ref().map(|k| escape(&k)))
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("compact failed {:?}", err)
        }
    }
}

pub struct Runner {
    engine: Arc<DB>,
}

impl Runner {
    pub fn new(engine: Arc<DB>) -> Runner {
        Runner { engine: engine }
    }

    fn compact_range_cf(&mut self,
                        cf_name: String,
                        start_key: Option<Vec<u8>>,
                        end_key: Option<Vec<u8>>)
                        -> Result<(), Error> {
        let cf_handle = box_try!(rocksdb::get_cf_handle(&self.engine, &cf_name));
        let compact_range_timer = COMPACT_RANGE_CF.with_label_values(&[&cf_name])
            .start_timer();
        let mut compact_opts = CompactOptions::new();
        // manual compaction can concurrently run with background compaction threads.
        compact_opts.set_exclusive_manual_compaction(false);
        self.engine.compact_range_cf_opt(cf_handle,
                                         &compact_opts,
                                         start_key.as_ref().map(Vec::as_slice),
                                         end_key.as_ref().map(Vec::as_slice));

        compact_range_timer.observe_duration();
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let cf = task.cf_name.clone();
        if let Err(e) = self.compact_range_cf(task.cf_name, task.start_key, task.end_key) {
            error!("execute compact range for cf {} failed, err {}", &cf, e);
        } else {
            info!("compact range for cf {} finished", &cf);
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use std::thread::sleep;
    use util::rocksdb::new_engine;
    use tempdir::TempDir;
    use storage::CF_WRITE;
    use rocksdb::{WriteBatch, Writable};
    use super::*;

    const ROCKSDB_TOTAL_SST_FILES_SIZE: &'static str = "rocksdb.total-sst-files-size";

    #[test]
    fn test_compact_range() {
        let path = TempDir::new("compact-range-test").unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_WRITE]).unwrap();
        let db = Arc::new(db);

        let mut runner = Runner::new(db.clone());

        let handle = rocksdb::get_cf_handle(&db, CF_WRITE).unwrap();

        // generate first sst file.
        let wb = WriteBatch::new();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(handle, k.as_bytes(), b"whatever content").unwrap();
        }
        db.write(wb).unwrap();
        db.flush_cf(handle, true).unwrap();

        // generate another sst file has the same content with first sst file.
        let wb = WriteBatch::new();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(handle, k.as_bytes(), b"whatever content").unwrap();
        }
        db.write(wb).unwrap();
        db.flush_cf(handle, true).unwrap();

        // get total sst files size.
        let old_sst_files_size = db.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .unwrap();

        // schedule compact range task
        runner.run(Task {
            cf_name: String::from(CF_WRITE),
            start_key: None,
            end_key: None,
        });
        sleep(Duration::from_secs(5));

        // get total sst files size after compact range.
        let new_sst_files_size = db.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .unwrap();
        assert!(old_sst_files_size > new_sst_files_size);
    }
}
