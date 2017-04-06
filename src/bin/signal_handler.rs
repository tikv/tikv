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


#[cfg(unix)]
mod imp {
    use std::thread;
    use std::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use rocksdb::DB;
    use jemallocator;
    use libc::c_char;

    use tikv::server::Msg;
    use tikv::util::transport::SendCh;
    use prometheus::{self, Encoder, TextEncoder};

    const ROCKSDB_DB_STATS_KEY: &'static str = "rocksdb.dbstats";
    const ROCKSDB_CF_STATS_KEY: &'static str = "rocksdb.cfstats";
    // null pointer, using the default file name.
    const DUMP_FILE_NAME: *const c_char = 0 as *const c_char;
    const PROFILE_SLEEP_SEC: u64 = 30;
    // c string should end with a '\0'.
    const PROFILE_ACTIVE: &'static [u8] = b"prof.active\0";
    const PROFILE_DUMP: &'static [u8] = b"prof.dump\0";

    pub fn handle_signal(ch: SendCh<Msg>, engine: Arc<DB>, _: &str) {
        use signal::trap::Trap;
        use nix::sys::signal::{SIGTERM, SIGINT, SIGUSR1, SIGUSR2};
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGUSR1, SIGUSR2]);
        let profiling_memory = Arc::new(AtomicBool::new(false));
        for sig in trap {
            match sig {
                SIGTERM | SIGINT => {
                    info!("receive signal {}, stopping server...", sig);
                    ch.send(Msg::Quit).unwrap();
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    let mut buffer = vec![];
                    let metric_familys = prometheus::gather();
                    let encoder = TextEncoder::new();
                    encoder.encode(&metric_familys, &mut buffer).unwrap();
                    info!("{}", String::from_utf8(buffer).unwrap());

                    // Log common rocksdb stats.
                    for name in engine.cf_names() {
                        let handler = engine.cf_handle(name).unwrap();
                        if let Some(v) =
                               engine.get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY) {
                            info!("{}", v)
                        }
                    }

                    if let Some(v) = engine.get_property_value(ROCKSDB_DB_STATS_KEY) {
                        info!("{}", v)
                    }

                    // Log more stats if enable_statistics is true.
                    if let Some(v) = engine.get_statistics() {
                        info!("{}", v)
                    }
                }
                SIGUSR2 => profile_memory(&profiling_memory),
                // TODO: handle more signal
                _ => unreachable!(),
            }
        }
    }

    fn profile_memory(flag: &Arc<AtomicBool>) {
        if flag.load(Ordering::SeqCst) {
            warn!("last memory profiling has not finished yet.");
            return;
        }
        unsafe {
            if let Err(e) = jemallocator::mallctl_set(PROFILE_DUMP, DUMP_FILE_NAME) {
                error!("failed to dump the first profile: {}", e);
                return;
            }
            if let Err(e) = jemallocator::mallctl_set(PROFILE_ACTIVE, true) {
                error!("failed to activate profiling: {}", e);
                return;
            }
        }
        flag.store(true, Ordering::SeqCst);
        let flag2 = flag.clone();
        thread::spawn(move || {
            // sleep some time to get the diff between two dumping.
            thread::sleep(Duration::from_secs(PROFILE_SLEEP_SEC));
            unsafe {
                if let Err(e) = jemallocator::mallctl_set(PROFILE_ACTIVE, false) {
                    panic!("failed to deactivate profiling: {}", e);
                }
            }
            thread::sleep(Duration::from_secs(PROFILE_SLEEP_SEC));
            unsafe {
                if let Err(e) = jemallocator::mallctl_set(PROFILE_DUMP, DUMP_FILE_NAME) {
                    error!("failed to dump the second profile: {}", e);
                    flag2.store(false, Ordering::SeqCst);
                    return;
                }
            }
            flag2.store(false, Ordering::SeqCst);
        });
    }
}

#[cfg(not(unix))]
mod imp {
    use std::sync::Arc;

    use rocksdb::DB;

    use tikv::server::Msg;
    use tikv::util::transport::SendCh;

    pub fn handle_signal(_: SendCh<Msg>, _: Arc<DB>, _: &str) {}
}

pub use self::imp::handle_signal;
