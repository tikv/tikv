// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::server::metrics::*;
use engine_traits::{KvEngine, CF_DEFAULT};
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
                    TTL_CHECKER_PROCESSED_REGIONS_GAUGE.set(0);
                    interval = runner.run();
                    info!(
                        "ttl checker finishes a round, wait {:?} to start next round",
                        interval
                    );
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
                    let mut scanned_regions = 0;
                    let mut start_key = None;
                    let mut end_key = None;
                    for info in iter {
                        if start_key.is_none() {
                            start_key = Some(info.region.get_start_key().to_owned());
                        }
                        TTL_CHECKER_PROCESSED_REGIONS_GAUGE.inc();
                        scanned_regions += 1;
                        end_key = Some(info.region.get_end_key().to_vec());
                        if scanned_regions == 10 {
                            break;
                        }
                    }
                    if scanned_regions != 0 {
                        let _ = tx.send(Some((start_key.unwrap(), end_key.unwrap())));
                    } else {
                        let _ = tx.send(None);
                    }
                }),
            ) {
                error!(?e; "ttl checker: failed to get next region information");
                TTL_CHECKER_ACTIONS_COUNTER_VEC
                    .with_label_values(&["error"])
                    .inc();
                continue;
            }

            match rx.recv() {
                Ok(None) => {}
                Ok(Some((start_key, end_key))) => {
                    Self::check_ttl_for_range(&self.engine, &start_key, &end_key);
                    if !end_key.is_empty() {
                        key = end_key;
                        continue;
                    }
                }
                Err(e) => {
                    error!(?e; "ttl checker: failed to get next region information");
                    TTL_CHECKER_ACTIONS_COUNTER_VEC
                        .with_label_values(&["error"])
                        .inc();
                    continue;
                }
            }

            // checks a round
            let round_time = Instant::now() - round_start_time;
            if self.poll_interval > round_time {
                return self.poll_interval - round_time;
            }
            return Duration::new(0, 0);
        }
    }

    pub fn check_ttl_for_range(engine: &E, start_key: &[u8], end_key: &[u8]) {
        let current_ts = UnixSecs::now().into_inner();

        let mut files = Vec::new();
        let res = match engine.get_range_ttl_properties_cf(CF_DEFAULT, start_key, end_key) {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "execute ttl compact files failed";
                    "range_start" => log_wrappers::Value::key(&start_key),
                    "range_end" => log_wrappers::Value::key(&end_key),
                    "files" => ?files,
                    "err" => %e,
                );
                TTL_CHECKER_ACTIONS_COUNTER_VEC
                    .with_label_values(&["error"])
                    .inc();
                return;
            }
        };
        for (file_name, prop) in res {
            if prop.max_expire_ts <= current_ts {
                files.push(file_name);
            }
        }
        if files.is_empty() {
            TTL_CHECKER_ACTIONS_COUNTER_VEC
                .with_label_values(&["skip"])
                .inc();
            return;
        }

        let timer = Instant::now();
        let compact_range_timer = TTL_CHECKER_COMPACT_DURATION_HISTOGRAM
            .start_coarse_timer();
        if let Err(e) = engine.compact_files_cf(CF_DEFAULT, &files, None) {
            error!(
                "execute ttl compact files failed";
                "range_start" => log_wrappers::Value::key(&start_key),
                "range_end" => log_wrappers::Value::key(&end_key),
                "files" => ?files,
                "err" => %e,
            );
            TTL_CHECKER_ACTIONS_COUNTER_VEC
                .with_label_values(&["error"])
                .inc();
            return;
        }
        compact_range_timer.observe_duration();
        info!(
            "compact files finished";
            "files" => ?files,
            "time_takes" => ?timer.elapsed(),
        );
        TTL_CHECKER_ACTIONS_COUNTER_VEC
            .with_label_values(&["compact"])
            .inc();

        // wait a while
        thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::super::ttl_compaction_filter::TEST_CURRENT_TS;
    use super::*;

    use crate::config::DbConfig;
    use crate::storage::kv::TestEngineBuilder;
    use engine_traits::util::append_expire_ts;
    use engine_traits::{MiscExt, Peekable, SyncMutable, CF_DEFAULT};
    use raftstore::RegionInfoAccessor;

    #[test]
    fn test_ttl_checker() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path()).ttl(true);
        let engine = builder.build_with_cfg(&cfg).unwrap();

        let kvdb = engine.get_rocksdb();
        let key1 = b"key1";
        let mut value1 = vec![0; 10];
        append_expire_ts(&mut value1, 10);
        kvdb.put_cf(CF_DEFAULT, key1, &value1).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();
        let key2 = b"key2";
        let mut value2 = vec![0; 10];
        append_expire_ts(&mut value2, TEST_CURRENT_TS + 20);
        kvdb.put_cf(CF_DEFAULT, key2, &value2).unwrap();
        let key3 = b"key3";
        let mut value3 = vec![0; 10];
        append_expire_ts(&mut value3, 20);
        kvdb.put_cf(CF_DEFAULT, key3, &value3).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();
        let key4 = b"key4";
        let mut value4 = vec![0; 10];
        append_expire_ts(&mut value4, 0);
        kvdb.put_cf(CF_DEFAULT, key4, &value4).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();
        let key5 = b"key5";
        let mut value5 = vec![0; 10];
        append_expire_ts(&mut value5, 10);
        kvdb.put_cf(CF_DEFAULT, key5, &value5).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

        let _ = Runner::<_, RegionInfoAccessor>::check_ttl_for_range(&kvdb, b"key1", b"key25");
        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

        let _ = Runner::<_, RegionInfoAccessor>::check_ttl_for_range(&kvdb, b"key2", b"key6");
        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_none());
    }
}
