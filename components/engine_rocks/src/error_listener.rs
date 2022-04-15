// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use regex::Regex;
use rocksdb::{DBBackgroundErrorReason, MutableStatus};
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::set_panic_mark;
use tikv_util::worker::Scheduler;
use tikv_util::{error, warn};

// Message for RocksDB status subcode kNoSpace.
const NO_SPACE_ERROR: &str = "IO error: No space left on device";

// `ErrorListener` is responsible for implementing the function of `on_background_error`.
// In order to avoid redundant panic, all rocksdb error processing should be implemented here.
pub struct ErrorListener {
    db_name: String,
    sst_recovery_tx: Option<Scheduler<String>>,
}

impl ErrorListener {
    pub fn new(db_name: &str, sst_recovery_tx: Option<Scheduler<String>>) -> ErrorListener {
        ErrorListener {
            db_name: db_name.to_owned(),
            sst_recovery_tx,
        }
    }
}

impl rocksdb::EventListener for ErrorListener {
    fn on_background_error(&self, reason: DBBackgroundErrorReason, status: MutableStatus) {
        let result = status.result();
        assert!(result.is_err());
        if let Err(err) = result {
            if matches!(
                reason,
                DBBackgroundErrorReason::Flush | DBBackgroundErrorReason::Compaction
            ) && err.starts_with(NO_SPACE_ERROR)
            {
                // Ignore NoSpace error and let RocksDB automatically recover.
                return;
            }

            let r = match reason {
                DBBackgroundErrorReason::Flush => "flush",
                DBBackgroundErrorReason::Compaction => "compaction",
                DBBackgroundErrorReason::WriteCallback => "write_callback",
                DBBackgroundErrorReason::MemTable => "memtable",
            };

            if err.starts_with("Corruption") || err.starts_with("IO error") {
                if let Some(scheduler) = self.sst_recovery_tx.as_ref() {
                    if let Some(path) = resolve_sst_filename_from_err(&err) {
                        warn!(
                            "detected rocksdb background error";
                            "sst" => &path,
                            "err" => &err
                        );
                        match scheduler.schedule(path) {
                            Ok(()) => {
                                status.reset();
                                CRITICAL_ERROR.with_label_values(&["sst corruption"]).inc();
                                return;
                            }
                            Err(e) => {
                                error!("rocksdb sst recovery job schedule failed, error: {:?}", e);
                            }
                        }
                    }
                }
                // Avoid tikv from restarting if rocksdb get corruption.
                set_panic_mark();
            }

            panic!(
                "rocksdb background error. db: {}, reason: {}, error: {}",
                self.db_name, r, &err
            );
        }
    }
}

// Here are some expected error examples:
// 1. Corruption: Sst file size mismatch: /qps/data/tikv-10014/db/000398.sst. Size recorded in manifest 6975, actual size 6959
// 2. Corruption: Bad table magic number: expected 9863518390377041911, found 759105309091689679 in /qps/data/tikv-10014/db/000021.sst
//
// We assume that only the corruption sst file path is printed inside error.
fn resolve_sst_filename_from_err(err: &str) -> Option<String> {
    let r = Regex::new(r"/\w*\.sst").unwrap();
    let matches = match r.captures(err) {
        None => return None,
        Some(v) => v,
    };
    let filename = matches.get(0).unwrap().as_str().to_owned();
    Some(filename)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_resolve_sst() {
        let err = "Corruption: Sst file size mismatch: /qps/data/tikv-10014/db/000398.sst. Size recorded in manifest 6975, actual size 6959";
        let filename = resolve_sst_filename_from_err(err).unwrap();
        assert_eq!(filename, "");
    }
}
