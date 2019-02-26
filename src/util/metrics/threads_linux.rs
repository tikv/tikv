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
use std::io::{Error, ErrorKind, Result};
use std::sync::Mutex;

use libc::{self, pid_t};
use prometheus::core::{Collector, Desc};
use prometheus::{self, proto, CounterVec, IntGaugeVec, Opts};

use procinfo::pid;

/// Monitors threads of the current process.
pub fn monitor_threads<S: Into<String>>(namespace: S) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let tc = ThreadsCollector::new(pid, namespace);
    prometheus::register(Box::new(tc)).map_err(|e| to_io_err(format!("{:?}", e)))
}

struct Metrics {
    cpu_totals: CounterVec,
    io_totals: CounterVec,
    threads_state: IntGaugeVec,
    voluntary_ctxt_switches: CounterVec,
    nonvoluntary_ctxt_switches: CounterVec,
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
            )
            .namespace(ns.clone()),
            &["name", "tid"],
        )
        .unwrap();
        descs.extend(cpu_totals.desc().into_iter().cloned());
        let threads_state = IntGaugeVec::new(
            Opts::new("threads_state", "Number of threads in each state.").namespace(ns.clone()),
            &["state"],
        )
        .unwrap();
        descs.extend(threads_state.desc().into_iter().cloned());
        let io_totals = CounterVec::new(
            Opts::new(
                "threads_io_bytes_total",
                "Total number of bytes which threads cause to be fetched from or sent to the storage layer.",
            ).namespace(ns.clone()),
            &["name", "tid", "io"],
        )
        .unwrap();
        descs.extend(io_totals.desc().into_iter().cloned());
        let voluntary_ctxt_switches = CounterVec::new(
            Opts::new(
                "thread_voluntary_context_switches",
                "Number of thread voluntary context switches.",
            )
            .namespace(ns.clone()),
            &["name", "tid"],
        )
        .unwrap();
        let nonvoluntary_ctxt_switches = CounterVec::new(
            Opts::new(
                "thread_nonvoluntary_context_switches",
                "Number of thread nonvoluntary context switches.",
            )
            .namespace(ns),
            &["name", "tid"],
        )
        .unwrap();

        ThreadsCollector {
            pid,
            descs,
            metrics: Mutex::new(Metrics {
                cpu_totals,
                io_totals,
                threads_state,
                voluntary_ctxt_switches,
                nonvoluntary_ctxt_switches,
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
            if let Ok(stat) = pid::stat_task(self.pid, tid) {
                // Threads CPU time.
                let total = cpu_total(&stat);
                // sanitize thread name before push metrics.
                let name = sanitize_thread_name(tid, &stat.command);
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
                    .get_metric_with_label_values(&[state_to_str(&stat.state)])
                    .unwrap();
                state.inc();

                if let Ok(io) = pid::io_task(self.pid, tid) {
                    let read_bytes = io.read_bytes;
                    let write_bytes = io.write_bytes;
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

                if let Ok(status) = pid::status_task(self.pid, tid) {
                    let voluntary_total = status.voluntary_ctxt_switches;
                    let voluntary_ctxt_swtiches = metrics
                        .voluntary_ctxt_switches
                        .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                        .unwrap();
                    let voluntary_past = voluntary_ctxt_swtiches.get();
                    let voluntary_delta = voluntary_total as f64 - voluntary_past;
                    if voluntary_delta > 0.0 {
                        voluntary_ctxt_swtiches.inc_by(voluntary_delta);
                    }

                    let nonvoluntary_total = status.nonvoluntary_ctxt_switches;
                    let nonvoluntary_ctxt_swtiches = metrics
                        .nonvoluntary_ctxt_switches
                        .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                        .unwrap();
                    let nonvoluntary_past = nonvoluntary_ctxt_swtiches.get();
                    let nonvoluntary_delta = nonvoluntary_total as f64 - nonvoluntary_past;
                    if nonvoluntary_delta > 0.0 {
                        nonvoluntary_ctxt_swtiches.inc_by(nonvoluntary_delta);
                    }
                }
            }
        }
        let mut mfs = metrics.cpu_totals.collect();
        mfs.extend(metrics.threads_state.collect());
        mfs.extend(metrics.io_totals.collect());
        mfs.extend(metrics.voluntary_ctxt_switches.collect());
        mfs.extend(metrics.nonvoluntary_ctxt_switches.collect());
        mfs
    }
}

/// Gets thread ids of the given process id.
pub fn get_thread_ids(pid: pid_t) -> Result<Vec<pid_t>> {
    Ok(fs::read_dir(format!("/proc/{}/task", pid))?
        .filter_map(|task| {
            let file_name = match task {
                Ok(t) => t.file_name(),
                Err(e) => {
                    error!("read task failed"; "pid" => pid, "err" => ?e);
                    return None;
                }
            };

            match file_name.to_str() {
                Some(tid) => match tid.parse() {
                    Ok(tid) => Some(tid),
                    Err(e) => {
                        error!("read task failed"; "pid" => pid, "err" => ?e);
                        None
                    }
                },
                None => {
                    error!("read task failed"; "pid" => pid);
                    None
                }
            }
        })
        .collect())
}

/// Sanitizes the thread name. Keeps `a-zA-Z0-9_:`, replaces `-` and ` ` with `_`, and drops the others.
///
/// Examples:
///
/// ```ignore
/// assert_eq!(sanitize_thread_name(0, "ok123"), "ok123");
/// assert_eq!(sanitize_thread_name(0, "Az_1"), "Az_1");
/// assert_eq!(sanitize_thread_name(0, "a-b"), "a_b");
/// assert_eq!(sanitize_thread_name(0, "a b"), "a_b");
/// assert_eq!(sanitize_thread_name(1, "@123"), "123");
/// assert_eq!(sanitize_thread_name(1, "@@@@"), "1");
/// ```
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

fn state_to_str(state: &pid::State) -> &str {
    match state {
        pid::State::Running => "R",
        pid::State::Sleeping => "S",
        pid::State::Waiting => "D",
        pid::State::Zombie => "Z",
        pid::State::Stopped => "T",
        pid::State::TraceStopped => "t",
        pid::State::Paging => "W",
        pid::State::Dead => "X",
        pid::State::Wakekill => "K",
        pid::State::Waking => "W",
        pid::State::Parked => "P",
    }
}

pub fn cpu_total(state: &pid::Stat) -> f64 {
    (state.utime + state.stime) as f64 / *CLK_TCK
}

fn to_io_err(s: String) -> Error {
    Error::new(ErrorKind::Other, s)
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
    use std::env::temp_dir;
    use std::io::Write;
    use std::{fs, sync, thread};

    use libc;

    use super::*;

    #[test]
    fn test_thread_stat_io() {
        let name = "theadnametest66";
        let (tx, rx) = sync::mpsc::channel();
        let (tx1, rx1) = sync::mpsc::channel();
        let h = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                // Make `io::write_bytes` > 0
                let mut tmp = temp_dir();
                tmp.push(name);
                tmp.set_extension("txt");
                let mut f = fs::File::create(tmp.as_path()).unwrap();
                f.write_all(name.as_bytes()).unwrap();
                f.sync_all().unwrap();
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
                pid::stat_task(pid, **t)
                    .map(|stat| stat.command == name)
                    .unwrap_or(false)
            })
            .unwrap();

        tids.iter()
            .find(|t| {
                pid::io_task(pid, **t)
                    .map(|io| io.wchar == name.len())
                    .unwrap_or(false)
            })
            .unwrap();

        tx.send(()).unwrap();
        h.join().unwrap();
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
    fn test_smoke() {
        let pid = unsafe { libc::getpid() };
        let tc = ThreadsCollector::new(pid, "smoke");
        tc.collect();
        tc.desc();
        monitor_threads("smoke").unwrap();
    }
}
