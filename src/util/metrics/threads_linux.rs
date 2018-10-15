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

use std::fs;
use std::io::{Error, ErrorKind, Read, Result};
use std::sync::Mutex;

use libc::{self, pid_t};
use prometheus::core::{Collector, Desc};
use prometheus::{self, proto, CounterVec, IntGaugeVec, Opts};

/// monitor current process's threads.
pub fn monitor_threads<S: Into<String>>(namespace: S) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let tc = ThreadsCollector::new(pid, namespace);
    prometheus::register(Box::new(tc)).map_err(|e| to_io_err(format!("{:?}", e)))
}

struct Metrics {
    cpu_totals: CounterVec,
    io_totals: CounterVec,
    threads_state: IntGaugeVec,
}

/// A collector to collect threads metrics, including CPU usage
/// and threads state.
struct ThreadsCollector {
    pid: pid_t,
    descs: Vec<Desc>,
    metrics: Mutex<Metrics>,
}

impl ThreadsCollector {
    fn new<S: Into<String>>(pid: pid_t, namespace: S) -> ThreadsCollector {
        let mut descs: Vec<Desc> = vec![];
        let ns = namespace.into();
        let cpu_totals = CounterVec::new(
            Opts::new(
                "thread_cpu_seconds_total",
                "Total user and system CPU time spent in \
                 seconds by threads.",
            ).namespace(ns.clone()),
            &["name", "tid"],
        ).unwrap();
        descs.extend(cpu_totals.desc().into_iter().cloned());
        let threads_state = IntGaugeVec::new(
            Opts::new("threads_state", "Number of threads in each state.").namespace(ns.clone()),
            &["state"],
        ).unwrap();
        descs.extend(threads_state.desc().into_iter().cloned());
        let io_totals = CounterVec::new(
            Opts::new(
                "threads_io_bytes_total",
                "Total number of bytes which threads cause to be fetched from or sent to the storage layer.",
            ).namespace(ns),
            &["name", "tid", "io"],
        ).unwrap();
        descs.extend(io_totals.desc().into_iter().cloned());

        ThreadsCollector {
            pid,
            descs,
            metrics: Mutex::new(Metrics {
                cpu_totals,
                io_totals,
                threads_state,
            }),
        }
    }
}

impl Collector for ThreadsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // Synchronous collecting metrics.
        let metrics = self.metrics.lock().unwrap();
        // Clean previous threads state.
        metrics.threads_state.reset();

        let tids = get_thread_ids(self.pid).unwrap();
        for tid in tids {
            if let Ok(Stat {
                name,
                state,
                utime,
                stime,
            }) = Stat::collect(self.pid, tid)
            {
                // sanitize thread name before push metrics.
                let name = sanitize_thread_name(tid, &name);

                // Threads CPU time.
                let total = (utime + stime) / *CLK_TCK;
                let cpu_total = metrics
                    .cpu_totals
                    .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                    .unwrap();
                let past = cpu_total.get();
                let delta = total - past;
                if delta > 0.0 {
                    cpu_total.inc_by(delta);
                }

                // Threads states.
                let state = metrics
                    .threads_state
                    .get_metric_with_label_values(&[&state])
                    .unwrap();
                state.inc();

                if let Ok(Io {
                    read_bytes,
                    write_bytes,
                }) = Io::collect(self.pid, tid)
                {
                    // Threads IO.
                    let read_total = metrics
                        .io_totals
                        .get_metric_with_label_values(&[&name, &format!("{}", tid), "read"])
                        .unwrap();
                    let read_past = read_total.get();
                    let read_delta = read_bytes as f64 - read_past;
                    if read_delta > 0.0 {
                        read_total.inc_by(read_delta);
                    }

                    let write_total = metrics
                        .io_totals
                        .get_metric_with_label_values(&[&name, &format!("{}", tid), "write"])
                        .unwrap();
                    let write_past = write_total.get();
                    let write_delta = write_bytes as f64 - write_past;
                    if write_delta > 0.0 {
                        write_total.inc_by(write_delta);
                    }
                }
            }
        }
        let mut mfs = metrics.cpu_totals.collect();
        mfs.extend(metrics.threads_state.collect());
        mfs.extend(metrics.io_totals.collect());
        mfs
    }
}

pub fn get_thread_ids(pid: pid_t) -> Result<Vec<pid_t>> {
    let mut tids = Vec::new();
    let dirs = fs::read_dir(format!("/proc/{}/task", pid))?;
    for task in dirs {
        let file_name = match task {
            Ok(t) => t.file_name(),
            Err(e) => {
                error!("fail to read task of {}, error {:?}", pid, e);
                continue;
            }
        };

        let tid = match file_name.to_str() {
            Some(tid) => match tid.parse() {
                Ok(tid) => tid,
                Err(e) => {
                    error!("fail to read task of {}, error {:?}", pid, e);
                    continue;
                }
            },
            None => {
                error!("fail to read task of {}", pid);
                continue;
            }
        };
        tids.push(tid);
    }

    Ok(tids)
}

fn get_thread_name(stat: &str) -> Result<(&str, usize)> {
    let start = stat.find('(');
    let end = stat.rfind(')');
    if let (Some(start), Some(end)) = (start, end) {
        return Ok((&stat[start + 1..end], end));
    }

    Err(to_io_err(format!(
        "can not find thread name, stat: {}",
        stat
    )))
}

// get thread name and the index of the last character(including ')').
fn sanitize_thread_name(tid: pid_t, raw: &str) -> String {
    let mut name = String::with_capacity(raw.len());
    // sanitize thread name.
    for c in raw.chars() {
        match c {
            // Prometheus label characters `[a-zA-Z0-9_:]`
            'a'...'z' | 'A'...'Z' | '0'...'9' | '_' | ':' => {
                name.push(c);
            }
            '-' | ' ' => {
                name.push('_');
            }
            _ => (),
        }
    }
    if name.is_empty() {
        name = format!("{}", tid)
    }
    name
}

fn to_io_err(s: String) -> Error {
    Error::new(ErrorKind::Other, s)
}

pub struct Stat {
    name: String,
    state: String,
    utime: f64,
    stime: f64,
}

impl Stat {
    /// See more man proc.
    /// Index of utime and stime.
    const CPU_INDEX: [usize; 2] = [14 - 1, 15 - 1];
    /// Index of state.
    const PROCESS_STATE_INDEX: usize = 3 - 1;

    pub fn collect(pid: pid_t, tid: pid_t) -> Result<Stat> {
        let mut stat = String::new();
        fs::File::open(format!("/proc/{}/task/{}/stat", pid, tid))
            .and_then(|mut f| f.read_to_string(&mut stat))?;
        get_thread_stat_internal(&stat)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn cpu_total(&self) -> f64 {
        (self.utime + self.stime) / *CLK_TCK
    }
}

// Extracted from `Stat::collect`, for test purpose.
fn get_thread_stat_internal(stat: &str) -> Result<Stat> {
    let (name, end) = get_thread_name(stat)?;

    let stats: Vec<_> = (&stat[end + 2..]).split_whitespace().collect(); // excluding ") ".
    let utime = stats
        .get(Stat::CPU_INDEX[0] - 2) // -2 because pid and comm is truncated.
        .unwrap_or(&"0")
        .parse()
        .map_err(|e| to_io_err(format!("{:?}: {}", e, stat)))?;
    let stime = stats
        .get(Stat::CPU_INDEX[1] - 2)
        .unwrap_or(&"0")
        .parse()
        .map_err(|e| to_io_err(format!("{:?}: {}", e, stat)))?;
    let state = stats
        .get(Stat::PROCESS_STATE_INDEX - 2)
        .unwrap_or(&"unknown")
        .to_string();
    Ok(Stat {
        name: name.to_owned(),
        state,
        utime,
        stime,
    })
}

// I/O statistics for threads.
struct Io {
    // Attempt to count the number of bytes which this process really did cause
    // to be fetched from the storage layer.  This is accurate for block-backed
    // filesystems.
    read_bytes: u64,
    // Attempt to count the number of bytes which this process caused to be
    // sent to the storage layer.
    write_bytes: u64,
}

impl Io {
    // # cat /proc/3828/io
    // rchar: 323934931
    // wchar: 323929600
    // syscr: 632687
    // syscw: 632675
    // read_bytes: 0
    // write_bytes: 323932160
    // cancelled_write_bytes: 0
    const READ_BYTES_INDEX: usize = 4;
    const WRITE_BYTES_INDEX: usize = 5;

    fn collect(pid: pid_t, tid: pid_t) -> Result<Io> {
        let mut io = String::new();
        fs::File::open(format!("/proc/{}/task/{}/io", pid, tid))
            .and_then(|mut f| f.read_to_string(&mut io))?;
        get_thread_io_internal(&io)
    }
}

// Extracted from `Io::collect`, for test purpose.
fn get_thread_io_internal(io: &str) -> Result<Io> {
    let read_bytes = io
        .lines()
        .nth(Io::READ_BYTES_INDEX)
        .map_or_else(|| Err(to_io_err(io.to_owned())), Ok)?
        .split(':')
        .nth(1)
        .map_or_else(|| Err(to_io_err(io.to_owned())), Ok)?
        .trim()
        .parse()
        .map_err(|e| to_io_err(format!("{:?}: {}", e, io)))?;

    let write_bytes = io
        .lines()
        .nth(Io::WRITE_BYTES_INDEX)
        .map_or_else(|| Err(to_io_err(io.to_owned())), Ok)?
        .split(':')
        .nth(1)
        .map_or_else(|| Err(to_io_err(io.to_owned())), Ok)?
        .trim()
        .parse()
        .map_err(|e| to_io_err(format!("{:?}: {}", e, io)))?;

    Ok(Io {
        read_bytes,
        write_bytes,
    })
}

lazy_static! {
    // getconf CLK_TCK
    static ref CLK_TCK: f64 = {
        unsafe {
            libc::sysconf(libc::_SC_CLK_TCK) as f64
        }
    };
}

#[cfg(test)]
mod tests {
    use std::sync;
    use std::thread;

    use libc;

    use super::*;

    #[test]
    fn test_thread_stat() {
        let name = "theadnametest66";
        let (tx, rx) = sync::mpsc::channel();
        let (tx1, rx1) = sync::mpsc::channel();
        let h = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                tx1.send(()).unwrap();
                rx.recv().unwrap();
            })
            .unwrap();
        rx1.recv().unwrap();

        let pid = unsafe { libc::getpid() };
        let tids = get_thread_ids(pid).unwrap();
        assert!(tids.len() >= 2);

        tids.iter()
            .find(|t| {
                Stat::collect(pid, **t)
                    .map(|stat| stat.name == name)
                    .unwrap_or(false)
            })
            .unwrap();

        tx.send(()).unwrap();
        h.join().unwrap();
    }

    #[test]
    fn test_sanitize_thread_name() {
        let cases = [
            ("(ok123)", "ok123", 6),
            ("(a-b)", "a_b", 4),
            ("(Az_1:)", "Az_1:", 6),
            ("(@123)", "123", 5),
            ("1 (ab) 1", "ab", 5),
            ("1 (a b) 1", "a_b", 6),
            ("1 ((a b )) 1", "a_b_", 9),
        ];
        for &(i, o, idx) in &cases {
            let (raw_name, end) = get_thread_name(i).unwrap();
            let name = sanitize_thread_name(1, raw_name);
            assert_eq!(name, o);
            assert_eq!(end, idx);
        }

        let (raw_name, _) = get_thread_name("(@#)").unwrap();
        assert_eq!(sanitize_thread_name(1, raw_name), "1");
        assert!(get_thread_name("invalid_stat").is_err());
    }

    #[test]
    fn test_get_thread_stat() {
        let sample = "2810 (test thd) S 2550 2621 2621 0 -1 4210688 2632 0 52 0 839 138 0 \
                      0 20 0 4 0 13862 709652480 3647 18446744073709551615 4194304 4319028 \
                      140732554845776 140732554845392 140439688777693 0 0 4096 16384 0 0 0 17 3 \
                      0 0 245 0 0 6417696 6421000 8478720 140732554851684 140732554851747 \
                      140732554851747 140732554854339 0";

        let Stat {
            name,
            state,
            utime,
            stime,
        } = get_thread_stat_internal(sample).unwrap();
        assert_eq!(name, "test thd");
        assert_eq!(state, "S");
        assert_eq!(utime as i64, 839);
        assert_eq!(stime as i64, 138);
    }

    #[test]
    fn test_get_thread_io() {
        let sample = "rchar: 323934931
wchar: 323929600
syscr: 632687
syscw: 632675
read_bytes: 7878789
write_bytes: 323932170
cancelled_write_bytes: 0";

        let Io {
            read_bytes,
            write_bytes,
        } = get_thread_io_internal(sample).unwrap();
        assert_eq!(read_bytes as i64, 7878789);
        assert_eq!(write_bytes as i64, 323932170);
    }

    #[test]
    fn test_smoke() {
        let pid = unsafe { libc::getpid() };
        let tc = ThreadsCollector::new(pid, "smoke");
        tc.collect();
        tc.desc();
        monitor_threads("smoke").unwrap();
    }
}
