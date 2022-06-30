// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Error, ErrorKind, Result},
    sync::Mutex,
    time::Duration,
};

use collections::HashMap;
use procinfo::pid;
use prometheus::{
    self,
    core::{Collector, Desc},
    proto, GaugeVec, IntGaugeVec, Opts,
};

use crate::{
    sys::thread::{self, Pid, THREAD_NAME_HASHMAP},
    time::Instant,
};

/// Monitors threads of the current process.
pub fn monitor_threads<S: Into<String>>(namespace: S) -> Result<()> {
    let pid = thread::process_id();
    let tc = ThreadsCollector::new(pid, namespace);
    prometheus::register(Box::new(tc)).map_err(|e| to_io_err(format!("{:?}", e)))
}

struct Metrics {
    cpu_totals: GaugeVec,
    io_totals: GaugeVec,
    threads_state: IntGaugeVec,
    voluntary_ctxt_switches: IntGaugeVec,
    nonvoluntary_ctxt_switches: IntGaugeVec,
}

impl Metrics {
    fn new<S: Into<String>>(namespace: S) -> Metrics {
        let ns = namespace.into();
        let cpu_totals = GaugeVec::new(
            Opts::new(
                "thread_cpu_seconds_total",
                "Total user and system CPU time spent in \
                 seconds by threads.",
            )
            .namespace(ns.clone()),
            &["name", "tid"],
        )
        .unwrap();
        let threads_state = IntGaugeVec::new(
            Opts::new("threads_state", "Number of threads in each state.").namespace(ns.clone()),
            &["state"],
        )
        .unwrap();
        let io_totals = GaugeVec::new(
            Opts::new(
                "threads_io_bytes_total",
                "Total number of bytes which threads cause to be fetched from or sent to the storage layer.",
            ).namespace(ns.clone()),
            &["name", "tid", "io"],
        )
        .unwrap();
        let voluntary_ctxt_switches = IntGaugeVec::new(
            Opts::new(
                "thread_voluntary_context_switches",
                "Number of thread voluntary context switches.",
            )
            .namespace(ns.clone()),
            &["name", "tid"],
        )
        .unwrap();
        let nonvoluntary_ctxt_switches = IntGaugeVec::new(
            Opts::new(
                "thread_nonvoluntary_context_switches",
                "Number of thread nonvoluntary context switches.",
            )
            .namespace(ns),
            &["name", "tid"],
        )
        .unwrap();

        Metrics {
            cpu_totals,
            io_totals,
            threads_state,
            voluntary_ctxt_switches,
            nonvoluntary_ctxt_switches,
        }
    }

    fn descs(&self) -> Vec<Desc> {
        let mut descs: Vec<Desc> = vec![];
        descs.extend(self.cpu_totals.desc().into_iter().cloned());
        descs.extend(self.threads_state.desc().into_iter().cloned());
        descs.extend(self.io_totals.desc().into_iter().cloned());
        descs.extend(self.voluntary_ctxt_switches.desc().into_iter().cloned());
        descs.extend(self.nonvoluntary_ctxt_switches.desc().into_iter().cloned());
        descs
    }

    fn reset(&mut self) {
        self.cpu_totals.reset();
        self.threads_state.reset();
        self.io_totals.reset();
        self.voluntary_ctxt_switches.reset();
        self.nonvoluntary_ctxt_switches.reset();
    }
}

/// A collector to collect threads metrics, including CPU usage
/// and threads state.
struct ThreadsCollector {
    pid: Pid,
    descs: Vec<Desc>,
    metrics: Mutex<Metrics>,
    tid_retriever: Mutex<TidRetriever>,
}

impl ThreadsCollector {
    fn new<S: Into<String>>(pid: Pid, namespace: S) -> ThreadsCollector {
        let metrics = Metrics::new(namespace);
        ThreadsCollector {
            pid,
            descs: metrics.descs(),
            metrics: Mutex::new(metrics),
            tid_retriever: Mutex::new(TidRetriever::new(pid)),
        }
    }
}

impl Collector for ThreadsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        // Synchronous collecting metrics.
        let mut metrics = self.metrics.lock().unwrap();
        // Clean previous threads state.
        metrics.threads_state.reset();

        let mut tid_retriever = self.tid_retriever.lock().unwrap();
        let (tids, updated) = tid_retriever.get_tids();
        if updated {
            metrics.reset();
        }
        for tid in tids {
            let tid = *tid;
            if let Ok(stat) = thread::full_thread_stat(self.pid, tid) {
                // Threads CPU time.
                let total = thread::linux::cpu_total(&stat);
                // sanitize thread name before push metrics.
                let name = if let Some(thread_name) = THREAD_NAME_HASHMAP.lock().unwrap().get(&tid)
                {
                    sanitize_thread_name(tid, thread_name)
                } else {
                    sanitize_thread_name(tid, &stat.command)
                };
                let cpu_total = metrics
                    .cpu_totals
                    .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                    .unwrap();
                cpu_total.set(total);

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
                    read_total.set(read_bytes as f64);

                    let write_total = metrics
                        .io_totals
                        .get_metric_with_label_values(&[&name, &format!("{}", tid), "write"])
                        .unwrap();
                    write_total.set(write_bytes as f64);
                }

                if let Ok(status) = pid::status_task(self.pid, tid) {
                    // Thread voluntary context switches.
                    let voluntary_ctxt_switches = status.voluntary_ctxt_switches;
                    let voluntary_total = metrics
                        .voluntary_ctxt_switches
                        .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                        .unwrap();
                    voluntary_total.set(voluntary_ctxt_switches as i64);

                    // Thread nonvoluntary context switches.
                    let nonvoluntary_ctxt_switches = status.nonvoluntary_ctxt_switches;
                    let nonvoluntary_total = metrics
                        .nonvoluntary_ctxt_switches
                        .get_metric_with_label_values(&[&name, &format!("{}", tid)])
                        .unwrap();
                    nonvoluntary_total.set(nonvoluntary_ctxt_switches as i64);
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
fn sanitize_thread_name(tid: Pid, raw: &str) -> String {
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

fn to_io_err(s: String) -> Error {
    Error::new(ErrorKind::Other, s)
}

#[inline]
fn get_name(command: &str) -> String {
    if !command.is_empty() {
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
        if let Some(value) = values.get(tid) {
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
    pid: Pid,
    last_instant: Instant,
    tid_names: HashMap<i32, String>,
    tid_retriever: TidRetriever,
    metrics_rate: ThreadMetrics,
    metrics_total: ThreadMetrics,
}

impl ThreadInfoStatistics {
    pub fn new() -> Self {
        let pid = thread::process_id();

        let mut thread_stats = Self {
            pid,
            last_instant: Instant::now(),
            tid_names: HashMap::default(),
            tid_retriever: TidRetriever::new(pid),
            metrics_rate: ThreadMetrics::default(),
            metrics_total: ThreadMetrics::default(),
        };

        thread_stats.record();
        thread_stats
    }

    pub fn record(&mut self) {
        let current_instant = Instant::now();
        let time_delta = current_instant
            .saturating_duration_since(self.last_instant)
            .as_millis() as f64
            / 1000.0;
        self.last_instant = current_instant;
        self.metrics_rate.clear();

        let (tids, _) = self.tid_retriever.get_tids();

        for tid in tids {
            let tid = *tid;

            if let Ok(stat) = thread::full_thread_stat(self.pid, tid) {
                let name = get_name(&stat.command);
                self.tid_names.entry(tid).or_insert(name);

                // To get a percentage result,
                // we pre-multiply `cpu_time` by 100 here rather than inside the `update_metric`.
                let cpu_time = thread::linux::cpu_total(&stat) * 100.0;
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

        self.tid_names.retain(|&k, _| tids.contains(&k));
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

impl Default for ThreadInfoStatistics {
    fn default() -> Self {
        Self::new()
    }
}

const TID_MIN_UPDATE_INTERVAL: Duration = Duration::from_secs(15);
const TID_MAX_UPDATE_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// A helper that buffers the thread id list internally.
struct TidRetriever {
    pid: Pid,
    tid_buffer: Vec<i32>,
    tid_buffer_last_update: Instant,
    tid_buffer_update_interval: Duration,
}

impl TidRetriever {
    pub fn new(pid: Pid) -> Self {
        let mut tid_buffer: Vec<_> = thread::thread_ids(pid).unwrap();
        tid_buffer.sort_unstable();
        Self {
            pid,
            tid_buffer,
            tid_buffer_last_update: Instant::now(),
            tid_buffer_update_interval: TID_MIN_UPDATE_INTERVAL,
        }
    }

    pub fn get_tids(&mut self) -> (&[Pid], bool) {
        // Update the tid list according to tid_buffer_update_interval.
        // If tid is not changed, update the tid list less frequently.
        let mut updated = false;
        if self.tid_buffer_last_update.saturating_elapsed() >= self.tid_buffer_update_interval {
            let mut new_tid_buffer: Vec<_> = thread::thread_ids(self.pid).unwrap();
            new_tid_buffer.sort_unstable();
            if new_tid_buffer == self.tid_buffer {
                self.tid_buffer_update_interval *= 2;
                if self.tid_buffer_update_interval > TID_MAX_UPDATE_INTERVAL {
                    self.tid_buffer_update_interval = TID_MAX_UPDATE_INTERVAL;
                }
            } else {
                self.tid_buffer = new_tid_buffer;
                self.tid_buffer_update_interval = TID_MIN_UPDATE_INTERVAL;
                updated = true;
            }
            self.tid_buffer_last_update = Instant::now();
        }

        (&self.tid_buffer, updated)
    }
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, fs, io::Write, sync, time::Duration};

    use super::*;
    use crate::sys::thread::StdThreadBuildWrapper;

    #[test]
    fn test_thread_stat_io() {
        let name = "testthreadio";
        let (tx, rx) = sync::mpsc::channel();
        let (tx1, rx1) = sync::mpsc::channel();
        let h = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn_wrapper(move || {
                // Make `io::write_bytes` > 0
                let mut tmp = temp_dir();
                tmp.push(name);
                tmp.set_extension("txt");
                let mut f = fs::File::create(tmp.as_path()).unwrap();
                f.write_all(name.as_bytes()).unwrap();
                f.sync_all().unwrap();
                std::thread::sleep(Duration::from_secs(1));
                tx1.send(()).unwrap();
                rx.recv().unwrap();
                fs::remove_file(tmp).unwrap();
            })
            .unwrap();
        rx1.recv().unwrap();

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize };
        let pid = thread::process_id();
        let tids: Vec<_> = thread::thread_ids(pid).unwrap();
        assert!(tids.len() >= 2);

        tids.iter()
            .find(|t| {
                thread::full_thread_stat(pid, **t)
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
        std::thread::Builder::new()
            .name(str1.to_owned())
            .spawn_wrapper(move || {
                tx1.send(()).unwrap();

                // Make `io::write_bytes` > 0
                let mut tmp = temp_dir();
                tmp.push(str1.clone());
                tmp.set_extension("txt");
                let mut f = fs::File::create(tmp.as_path()).unwrap();
                f.write_all(str1.as_bytes()).unwrap();
                f.sync_all().unwrap();
                std::thread::sleep(Duration::from_secs(1));
                tx1.send(()).unwrap();
                rx.recv().unwrap();

                f.write_all(str2.as_bytes()).unwrap();
                f.sync_all().unwrap();
                std::thread::sleep(Duration::from_secs(1));
                tx1.send(()).unwrap();
                rx.recv().unwrap();

                fs::remove_file(tmp).unwrap();
                tx1.send(()).unwrap();
            })
            .unwrap();

        (tx, rx1)
    }

    #[test]
    fn test_thread_io_statistics() {
        let s1 = "testio123";
        let s2 = "test45678";

        let (tx, rx1) = write_two_string(s1.to_owned(), s2.to_owned());
        // Wait for thread creation
        rx1.recv().unwrap();

        let mut thread_info = ThreadInfoStatistics::new();

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as u64 };
        let pid = thread::process_id();
        let tids: Vec<_> = thread::thread_ids(pid).unwrap();
        for tid in tids {
            if let Ok(stat) = thread::full_thread_stat(pid, tid) {
                if stat.command.starts_with(s1) {
                    rx1.recv().unwrap();
                    thread_info.record();
                    {
                        let write_io = thread_info
                            .metrics_total
                            .write_ios
                            .entry(tid)
                            .or_insert(0.0);
                        assert_eq!(*write_io as u64, page_size);
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
                        assert_eq!(*write_io as u64, page_size * 2);
                    }

                    tx.send(()).unwrap();
                    rx1.recv().unwrap();
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
        std::thread::Builder::new()
            .name(name)
            .spawn_wrapper(move || {
                tx1.send(()).unwrap();

                let start = Instant::now();
                loop {
                    if start.saturating_elapsed().as_millis() > duration_ms.into() {
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

        let (tx, rx) = high_cpu_thread(tn.to_owned(), 200);
        // Wait for thread creation
        rx.recv().unwrap();

        let mut thread_info = ThreadInfoStatistics::new();

        let pid = thread::process_id();
        let tids: Vec<_> = thread::thread_ids(pid).unwrap();
        for tid in tids {
            if let Ok(stat) = thread::full_thread_stat(pid, tid) {
                if stat.command.starts_with(tn) {
                    rx.recv().unwrap();
                    thread_info.record();

                    let mut cpu_usages = thread_info.get_cpu_usages();
                    let cpu_usage = cpu_usages.entry(stat.command).or_insert(0);
                    assert!(*cpu_usage < 110); // Consider the error of statistics
                    if *cpu_usage < 50 {
                        panic!("the load must be heavy than 0.5, but got {}", *cpu_usage);
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
        let pid = thread::process_id();
        let tc = ThreadsCollector::new(pid, "smoke");
        tc.collect();
        tc.desc();
        monitor_threads("smoke").unwrap();
    }
}
