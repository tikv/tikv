// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::mpsc;
use std::time::Duration;
use std::thread::{self, JoinHandle};

use raftstore::coprocessor::{RegionInfoProvider};
use engine_rocks::TTLProperties;
use engine_traits::{Range, KvEngine, TableProperties, TablePropertiesCollection, CF_DEFAULT};
use tikv_util::time::{Instant, UnixSecs};


pub struct TTLChecker<E: KvEngine , R: RegionInfoProvider> {
    engine: E,
    region_info_provider: R,
    poll_interval: Duration,
}

impl<E: KvEngine , R: RegionInfoProvider> TTLChecker<E, R> {
    pub fn new(engine: E, region_info_provider: R, poll_interval: Duration) -> Self {
        TTLChecker::<E, R> {
            engine,
            region_info_provider,
            poll_interval,
        }
    }

    pub fn start(self) -> JoinHandle<()> {
        thread::Builder::new()
            .name(thd_name!("ttl-checker"))
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                self.run();
                tikv_alloc::remove_thread_memory_accessor();
            }).unwrap()
    }

    pub fn run(&self) {
        thread::sleep(self.poll_interval);

        let mut round_start_time = Instant::now();
        let mut key = vec![];
        loop {
            let (tx, rx) = mpsc::channel();
            let res = self.region_info_provider.seek_region(
                &key,
                Box::new(move |iter| {
                    for info in iter {
                        let _ = tx.send(Some(info.region.clone()));
                        return;
                    }
                    let _ = tx.send(None);
                }),
            ).unwrap();

            match rx.recv() {
                Ok(None) => {
                    // checks a round
                    let round_time = Instant::now() - round_start_time;
                    if self.poll_interval > round_time {
                        let wait = self.poll_interval - round_time;
                        thread::sleep(wait);
                    }
                    round_start_time = Instant::now();
                },
                Ok(Some(mut region)) => {
                    self.check_ttl_for_region(region.get_start_key(), region.get_end_key());
                    key = region.take_end_key();
                },
                Err(_) => {}
            }
        }
    }

    pub fn check_ttl_for_region(&self, start_key: &[u8], end_key: &[u8]) {
        let current_ts = UnixSecs::now().into_inner();
        let range = Range::new(start_key, end_key);
        let collection = match self
            .engine
            .get_properties_of_tables_in_range(CF_DEFAULT, &[range])
        {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "execute ttl compact files failed";
                    "range_start" => log_wrappers::Value::key(&start_key),
                    "range_end" => log_wrappers::Value::key(&end_key),
                    "err" => %e,
                );
                return;
            }
        };

        if collection.is_empty() {
            return;
        }

        let mut files = Vec::new();
        for (file_name, v) in collection.iter() {
            let prop = match TTLProperties::decode(&v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if prop.max_expire_ts <= current_ts {
                files.push(file_name.to_string());
            }
        }

        let timer = Instant::now();
        // let compact_range_timer = COMPACT_RANGE_CF
        //     .with_label_values(&[cf])
        //     .start_coarse_timer();
        if let Err(e) = self.engine.compact_files_cf(CF_DEFAULT, &files, None) {
            error!(
                "execute ttl compact files failed";
                "range_start" => log_wrappers::Value::key(&start_key),
                "range_end" => log_wrappers::Value::key(&end_key),
                "files" => ?files,
                "err" => %e,
            );
        } 
        // compact_range_timer.observe_duration();
        info!(
            "compact files finished";
            "files" => ?files,
            "time_takes" => ?timer.elapsed(),
        );

        // wait a while
        thread::sleep(Duration::from_secs(1));
    }
}
