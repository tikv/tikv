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
    use std::{ptr, slice};
    use std::sync::Arc;

    use libc::{self, c_void, c_char};
    use rocksdb::DB;

    use tikv::server::Msg;
    use tikv::util::transport::SendCh;
    use prometheus::{self, Encoder, TextEncoder};

    use profiling;

    const ROCKSDB_DB_STATS_KEY: &'static str = "rocksdb.dbstats";
    const ROCKSDB_CF_STATS_KEY: &'static str = "rocksdb.cfstats";

    extern "C" {
        #[cfg_attr(target_os = "macos", link_name = "je_malloc_stats_print")]
        fn malloc_stats_print(write_cb: extern "C" fn(*mut c_void, *const c_char), cbopaque: *mut c_void, opts: *const c_char);
    }

    extern "C" fn write_cb(printer: *mut c_void, msg: *const c_char) {
        unsafe {
            let buf = &mut *{printer as *mut Vec<u8>};
            let len = libc::strlen(msg);
            let bytes = slice::from_raw_parts(msg as *const u8, len);
            buf.extend_from_slice(bytes);
        }
    }

    fn print_malloc_stats() {
        let mut buf = Vec::new();
        unsafe { malloc_stats_print(write_cb, &mut buf as *mut Vec<u8> as *mut c_void, ptr::null()) }
        info!("{}", String::from_utf8_lossy(&buf));
    }

    // TODO: remove backup_path from configuration
    pub fn handle_signal(ch: SendCh<Msg>, engine: Arc<DB>, _: &str) {
        use signal::trap::Trap;
        use nix::sys::signal::{SIGTERM, SIGINT, SIGUSR1, SIGUSR2};
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGUSR1, SIGUSR2]);
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
                    print_malloc_stats();
                }
                SIGUSR2 => profiling::dump_prof(None),
                // TODO: handle more signal
                _ => unreachable!(),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_stats_print() {
            // just print the data, ensure it doesn't core.
            super::print_malloc_stats()
        }
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
