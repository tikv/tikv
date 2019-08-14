// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::io::{Error, ErrorKind, Result};
use std::sync::Mutex;
use std::time::Instant;

use crate::collections::HashMap;
use libc::{self, pid_t};
use prometheus::core::{Collector, Desc};
use prometheus::{self, proto, CounterVec, IntCounterVec, IntGaugeVec, Opts};

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
    voluntary_ctxt_switches: IntCounterVec,
    nonvoluntary_ctxt_switches: IntCounterVec,
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
        let voluntary_ctxt_switches = IntCounterVec::new(
            Opts::new(
                "thread_voluntary_context_switches",
                "Number of thread voluntary context switches.",
            )
            .namespace(ns.clone()),
            &["name", "tid"],
        )
        .unwrap();
        let nonvoluntary_ctxt_switches = IntCounterVec::new(
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
                    // Thread voluntary context switches.
                    let voluntary_ctxt_switches = status.voluntary_ctxt_switches;
                    let voluntary_total = metrics
                        .voluntary_ctxt_switches
                        .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                        .unwrap();
                    let voluntary_past = voluntary_total.get();
                    let voluntary_delta = voluntary_ctxt_switches as i64 - voluntary_past;
                    if voluntary_delta > 0 {
                        voluntary_total.inc_by(voluntary_delta);
                    }

                    // Thread nonvoluntary context switches.
                    let nonvoluntary_ctxt_switches = status.nonvoluntary_ctxt_switches;
                    let nonvoluntary_total = metrics
                        .nonvoluntary_ctxt_switches
                        .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                        .unwrap();
                    let nonvoluntary_past = nonvoluntary_total.get();
                    let nonvoluntary_delta = nonvoluntary_ctxt_switches as i64 - nonvoluntary_past;
                    if nonvoluntary_delta > 0 {
                        nonvoluntary_total.inc_by(nonvoluntary_delta);
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
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | ':' => {
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

#[inline]
fn get_name(command: &str) -> String {
    if command != "" {
        return command.to_owned();
    }
    String::from("anony")
}

fn collect_metrics_by_name(
    names: &HashMap<i32, String>,
    values: &HashMap<i32, f64>,
) -> HashMap<String, u64> {
    let mut new_map: HashMap<String, u64> = HashMap::default();
    for (tid, name) in names {
        let new_value = new_map.entry(name.to_string()).or_insert(0);
        if let Some(value) = values.get(&tid) {
            *new_value += *value as u64;
        }
    }
    new_map
}

#[inline]
fn update_metric(
    metrics: &mut HashMap<i32, f64>,
    rates: &mut HashMap<i32, f64>,
    tid: i32,
    metric_new: f64,
    time_delta: f64,
) {
    let metric_old = metrics.entry(tid).or_insert(0.0);
    let rate = rates.entry(tid).or_insert(0.0);

    let metric_delta = metric_new - *metric_old;
    if metric_delta > 0.0 && time_delta > 0.0 {
        *metric_old = metric_new;
        *rate = metric_delta / time_delta;
    }
}

#[derive(Default)]
struct ThreadMetrics {
    cpu_times: HashMap<i32, f64>,
    read_ios: HashMap<i32, f64>,
    write_ios: HashMap<i32, f64>,
}

impl ThreadMetrics {
    fn clear(&mut self) {
        self.cpu_times.clear();
        self.read_ios.clear();
        self.write_ios.clear();
    }
}

/// Use to collect cpu usages and disk I/O rates
pub struct ThreadInfoStatistics {
    pid: pid_t,
    last_instant: Instant,
    tid_names: HashMap<i32, String>,
    metrics_rate: ThreadMetrics,
    metrics_total: ThreadMetrics,
}

impl ThreadInfoStatistics {
    pub fn new() -> Self {
        let pid = unsafe { libc::getpid() };

        let mut thread_stats = ThreadInfoStatistics {
            pid,
            last_instant: Instant::now(),
            tid_names: HashMap::default(),
            metrics_rate: ThreadMetrics::default(),
            metrics_total: ThreadMetrics::default(),
        };
        thread_stats.record();
        thread_stats
    }

    pub fn record(&mut self) {
        let current_instant = Instant::now();
        let time_delta = (current_instant - self.last_instant).as_millis() as f64 / 1000.0;
        self.last_instant = current_instant;

        self.metrics_rate.clear();

        let tids = get_thread_ids(self.pid).unwrap();
        for tid in tids {
            if let Ok(stat) = pid::stat_task(self.pid, tid) {
                let name = get_name(&stat.command);
                self.tid_names.entry(tid).or_insert(name);

                let cpu_time = cpu_total(&stat) * 100.0;
                update_metric(
                    &mut self.metrics_total.cpu_times,
                    &mut self.metrics_rate.cpu_times,
                    tid,
                    cpu_time,
                    time_delta,
                );

                if let Ok(io) = pid::io_task(self.pid, tid) {
                    // Threads IO.
                    let read_bytes = io.read_bytes;
                    let write_bytes = io.write_bytes;

                    update_metric(
                        &mut self.metrics_total.read_ios,
                        &mut self.metrics_rate.read_ios,
                        tid,
                        read_bytes as f64,
                        time_delta,
                    );

                    update_metric(
                        &mut self.metrics_total.write_ios,
                        &mut self.metrics_rate.write_ios,
                        tid,
                        write_bytes as f64,
                        time_delta,
                    );
                }
            }
        }
    }

    pub fn get_cpu_usages(&self) -> HashMap<String, u64> {
        collect_metrics_by_name(&self.tid_names, &self.metrics_rate.cpu_times)
    }

    pub fn get_read_io_rates(&self) -> HashMap<String, u64> {
        collect_metrics_by_name(&self.tid_names, &self.metrics_rate.read_ios)
    }

    pub fn get_write_io_rates(&self) -> HashMap<String, u64> {
        collect_metrics_by_name(&self.tid_names, &self.metrics_rate.write_ios)
    }
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
                fs::remove_file(tmp).unwrap();
            })
            .unwrap();
        rx1.recv().unwrap();

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize };
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
                    .map(|io| io.write_bytes == page_size)
                    .unwrap_or(false)
            })
            .unwrap();

        tx.send(()).unwrap();
        h.join().unwrap();
    }

    fn write_two_string(
        str1: String,
        str2: String,
    ) -> (sync::mpsc::Sender<()>, sync::mpsc::Receiver<()>) {
        let (tx, rx) = sync::mpsc::channel();
        let (tx1, rx1) = sync::mpsc::channel();
        thread::Builder::new()
            .name(str1.to_owned())
            .spawn(move || {
                // Make `io::write_bytes` > 0
                let mut tmp = temp_dir();
                tmp.push(str1.clone());
                tmp.set_extension("txt");
                let mut f = fs::File::create(tmp.as_path()).unwrap();
                f.write_all(str1.as_bytes()).unwrap();
                f.sync_all().unwrap();
                tx1.send(()).unwrap();
                rx.recv().unwrap();

                f.write_all(str2.as_bytes()).unwrap();
                f.sync_all().unwrap();
                tx1.send(()).unwrap();
                rx.recv().unwrap();

                fs::remove_file(tmp).unwrap();
            })
            .unwrap();
        rx1.recv().unwrap();

        (tx, rx1)
    }

    #[test]
    fn test_thread_io_statistics() {
        let mut thread_info = ThreadInfoStatistics::new();

        let s1 = "testio123";
        let s2 = "test45678";

        let (tx, rx1) = write_two_string(s1.to_owned(), s2.to_owned());
        thread_info.record();

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as u64 };
        let pid = unsafe { libc::getpid() };
        let tids = get_thread_ids(pid).unwrap();
        for tid in tids {
            if let Ok(stat) = pid::stat_task(pid, tid) {
                if stat.command.starts_with(s1) {
                    {
                        let write_io = thread_info
                            .metrics_total
                            .write_ios
                            .entry(tid)
                            .or_insert(0.0);
                        assert!(*write_io as u64 == page_size);
                    }

                    tx.send(()).unwrap();
                    rx1.recv().unwrap();
                    thread_info.record();

                    {
                        let write_io = thread_info
                            .metrics_total
                            .write_ios
                            .entry(tid)
                            .or_insert(0.0);
                        assert!(*write_io as u64 == page_size * 2);
                    }

                    tx.send(()).unwrap();
                    return;
                }
            }
        }
        panic!();
    }

    fn high_cpu_thread(
        name: String,
        duration_ms: u32,
    ) -> (sync::mpsc::Sender<()>, sync::mpsc::Receiver<()>) {
        let (tx, rx) = sync::mpsc::channel();
        let (tx1, rx1) = sync::mpsc::channel();
        thread::Builder::new()
            .name(name)
            .spawn(move || {
                let start = Instant::now();
                loop {
                    if (Instant::now() - start).as_millis() > duration_ms.into() {
                        break;
                    }
                }

                tx1.send(()).unwrap();
                rx.recv().unwrap();
            })
            .unwrap();

        (tx, rx1)
    }

    #[test]
    fn test_thread_cpu_statistics() {
        let tn = "testcpu123";
        let mut thread_info = ThreadInfoStatistics::new();
        let (tx, rx) = high_cpu_thread(tn.to_owned(), 200);

        let pid = unsafe { libc::getpid() };
        let tids = get_thread_ids(pid).unwrap();
        for tid in tids {
            if let Ok(stat) = pid::stat_task(pid, tid) {
                if stat.command.starts_with(tn) {
                    rx.recv().unwrap();
                    thread_info.record();

                    let mut cpu_usages = thread_info.get_cpu_usages();
                    let cpu_usage = cpu_usages.entry(stat.command).or_insert(0);
                    assert!(*cpu_usage < 110); // Consider the error of statistics
                    if *cpu_usage < 80 {
                        panic!("the load must be heavy than 0.8, but got {}", *cpu_usage);
                    }

                    tx.send(()).unwrap();
                    return;
                }
            }
        }
        panic!();
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
