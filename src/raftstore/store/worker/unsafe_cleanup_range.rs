// Copyright 2018 PingCAP, Inc.
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

use std::sync::Arc;
use std::time::{Duration, Instant};

use rocksdb::{ReadOptions, SeekKey, Writable, DB};

use util::escape;
use util::timer::Timer;
use util::worker::{Runnable, RunnableWithTimer};
use raftstore::store::{keys, util};
use storage::{decode_cf, encode_cf};
use util::rocksdb::get_cf_handle;

pub const UNSAFE_CLEANUP_INTERVAL: u64 = 5_000; // milliseconds

#[derive(Debug)]
pub struct TaskQueue {
    db: Arc<DB>, // use db to store tasks.
}

impl Clone for TaskQueue {
    fn clone(&self) -> TaskQueue {
        TaskQueue {
            db: Arc::clone(&self.db),
        }
    }
}

pub struct Task {
    cf: String,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

impl Task {
    fn new(cf: String, start_key: Vec<u8>, end_key: Vec<u8>) -> Task {
        Task {
            cf: cf,
            start_key: start_key,
            end_key: end_key,
        }
    }
}

fn encode_task_key(cf: &str, start_key: &[u8]) -> Vec<u8> {
    let mut vec = keys::encode_unsafe_cleanup_range_key(start_key);
    vec.push(encode_cf(cf));
    vec
}

fn decode_task_key(key: &[u8]) -> (&'static str, Vec<u8>) {
    let len = key.len();
    assert!(len >= keys::UNSAFE_CLEANUP_RANGE_MIN_KEY.len() + 1);
    let cf = decode_cf(key[len - 1]);
    let start_key = keys::decode_unsafe_cleanup_range_key(&key[..len - 1])
        .unwrap_or_else(|e| panic!("decode key failed: {:?}", e));
    (cf, start_key)
}

impl TaskQueue {
    pub fn new(db: Arc<DB>) -> TaskQueue {
        TaskQueue { db: db }
    }

    pub fn add_task(&self, cf: &str, start_key: &[u8], end_key: &[u8]) -> Result<(), String> {
        let task_key = encode_task_key(cf, start_key);
        self.db.put(&task_key, end_key)?;
        Ok(())
    }

    pub fn pick_task(&self) -> Option<Task> {
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(false);
        read_options.set_iterate_lower_bound(keys::UNSAFE_CLEANUP_RANGE_MIN_KEY);
        read_options.set_iterate_upper_bound(keys::UNSAFE_CLEANUP_RANGE_MAX_KEY);
        let mut iter = self.db.iter_opt(read_options);
        iter.seek(SeekKey::Start);
        if iter.valid() {
            let (cf, start_key) = decode_task_key(iter.key());
            Some(Task::new(
                String::from(cf),
                start_key,
                iter.value().to_vec(),
            ))
        } else {
            None
        }
    }

    pub fn delete_task(&self, task: Task) -> Result<(), String> {
        let task_key = encode_task_key(&task.cf, &task.start_key);
        self.db.delete(&task_key)?;
        Ok(())
    }
}

pub struct Runner {
    engine: Arc<DB>,
    task_queue: Arc<TaskQueue>,
    use_delete_range: bool,
}

impl Runner {
    pub fn new(engine: Arc<DB>, task_queue: Arc<TaskQueue>, use_delete_range: bool) -> Runner {
        Runner {
            engine: engine,
            task_queue: task_queue,
            use_delete_range: use_delete_range,
        }
    }

    fn unsafe_cleanup_range(&self, cf: &str, start_key: &[u8], end_key: &[u8]) {
        let handle = get_cf_handle(&self.engine, cf).unwrap();

        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        self.engine
            .delete_files_in_range_cf(handle, start_key, end_key, /* include_end */ false)
            .unwrap_or_else(|e| {
                panic!(
                    "failed to delete files in range [{}, {}): {:?}",
                    escape(start_key),
                    escape(end_key),
                    e
                )
            });

        // Delete all remaining keys.
        util::delete_all_in_range_cf(&self.engine, cf, start_key, end_key, self.use_delete_range)
            .unwrap_or_else(|e| {
                panic!(
                    "failed to delete all in range [{}, {}), cf: {}, err: {:?}",
                    escape(start_key),
                    escape(end_key),
                    cf,
                    e
                );
            });
    }

    fn unsafe_cleanup_ranges(&self) {
        let t = Instant::now();
        while let Some(task) = self.task_queue.pick_task() {
            // Cleanup the range.
            self.unsafe_cleanup_range(&task.cf, &task.start_key, &task.end_key);

            // Delete task after finished.
            self.task_queue
                .delete_task(task)
                .unwrap_or_else(|e| panic!("delete unsafe cleanup task failed, error {:?}", e));

            // If there are too many ranges need to cleanup, it will takes a long long
            // while to finish. In that situation if we don't limit the total time of
            // this function, shutdown will wait a long long time.
            if t.elapsed() > Duration::from_millis(UNSAFE_CLEANUP_INTERVAL) {
                break;
            }
        }
    }
}

impl Runnable<i32> for Runner {
    fn run(&mut self, _: i32) {}
}

impl RunnableWithTimer<i32, ()> for Runner {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        self.unsafe_cleanup_ranges();

        timer.add_task(Duration::from_millis(UNSAFE_CLEANUP_INTERVAL), ());
    }
}
