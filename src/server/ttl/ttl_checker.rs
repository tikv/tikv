// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use engine_traits::{
    KvEngine, Range, TTLProperties, TableProperties, TablePropertiesCollection, CF_DEFAULT,
};
use raftstore::coprocessor::RegionInfoProvider;
use tikv_util::time::{Instant, UnixSecs};

pub struct TTLChecker<E: KvEngine, R: RegionInfoProvider> {
    runner: Option<Runner<E, R>>,

    sender: Option<Sender<bool>>,
    handle: Option<JoinHandle<()>>,
}

impl<E: KvEngine, R: RegionInfoProvider> TTLChecker<E, R> {
    pub fn new(engine: E, region_info_provider: R, poll_interval: Duration) -> Self {
        TTLChecker::<E, R> {
            runner: Some(Runner {
                engine,
                region_info_provider,
                poll_interval,
            }),
            sender: None,
            handle: None,
        }
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        let (tx, rx) = mpsc::channel();
        self.sender = Some(tx);
        let runner = self.runner.take().unwrap();
        let h = thread::Builder::new()
            .name("ttl-checker".to_owned())
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let mut interval = runner.poll_interval;
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    interval = runner.run();
                }
                tikv_alloc::remove_thread_memory_accessor();
            })?;
        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        drop(self.sender.take().unwrap());
        if let Err(e) = h.unwrap().join() {
            error!("join ttl checker failed"; "err" => ?e);
            return;
        }
    }
}

struct Runner<E: KvEngine, R: RegionInfoProvider> {
    engine: E,
    region_info_provider: R,
    poll_interval: Duration,
}

impl<E: KvEngine, R: RegionInfoProvider> Runner<E, R> {
    fn run(&self) -> Duration {
        let round_start_time = Instant::now();
        let mut key = vec![];
        loop {
            let (tx, rx) = mpsc::channel();
            if let Err(e) = self.region_info_provider.seek_region(
                &key,
                Box::new(move |iter| {
                    for info in iter {
                        let _ = tx.send(Some(info.region.clone()));
                        return;
                    }
                    let _ = tx.send(None);
                }),
            ) {
                error!(?e; "ttl checker: failed to get next region information");
            }

            match rx.recv() {
                Ok(None) => {
                    // checks a round
                    let round_time = Instant::now() - round_start_time;
                    if self.poll_interval > round_time {
                        return self.poll_interval - round_time;
                    }
                    return Duration::new(0, 0);
                }
                Ok(Some(mut region)) => {
                    self.check_ttl_for_range(region.get_start_key(), region.get_end_key());
                    key = region.take_end_key();
                }
                Err(e) => {
                    error!(?e; "ttl checker: failed to get next region information");
                }
            }
        }
    }

    pub fn check_ttl_for_range(&self, start_key: &[u8], end_key: &[u8]) {
        let current_ts = UnixSecs::now().into_inner();

        let mut files = Vec::new();
        let res = match self
            .engine
            .get_range_ttl_properties_cf(CF_DEFAULT, start_key, end_key)
        {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "execute ttl compact files failed";
                    "range_start" => log_wrappers::Value::key(&start_key),
                    "range_end" => log_wrappers::Value::key(&end_key),
                    "files" => ?files,
                    "err" => %e,
                );
                return;
            }
        };
        for (file_name, prop) in res {
            if prop.max_expire_ts <= current_ts {
                files.push(file_name);
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
