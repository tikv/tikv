// Copyright 2016 PingCAP, Inc.
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
use std::io::{Result, Error, ErrorKind, Read};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

use prometheus::{self, Opts, CounterVec, Desc, proto, Collector};

use libc::{self, pid_t};

/// monitor current process's threads.
pub fn monitor_threads<S: Into<String>>(namespace: S) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let tc = ThreadsColletcor::new(pid, namespace);
    try!(prometheus::register(Box::new(tc)).map_err(|e| to_err(format!("{:?}", e))));

    Ok(())
}

struct ThreadsColletcor {
    pid: pid_t,
    descs: Vec<Desc>,
    cpu_totals: Mutex<CounterVec>,
}

impl ThreadsColletcor {
    fn new<S: Into<String>>(pid: pid_t, namespace: S) -> ThreadsColletcor {
        let cpu_totals = CounterVec::new(Opts::new("thread_cpu_seconds_total",
                                                   "Total user and system CPU time spent in \
                                                    seconds by threads.")
                                             .namespace(namespace),
                                         &["name"])
            .unwrap();
        let descs = cpu_totals.desc().into_iter().cloned().collect();

        ThreadsColletcor {
            pid: pid,
            descs: descs,
            cpu_totals: Mutex::new(cpu_totals),
        }
    }
}

impl Collector for ThreadsColletcor {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let cpu_totals = self.cpu_totals.lock().unwrap();
        let pairs = get_threads(self.pid).unwrap();
        for (tid, tname) in pairs {
            if let Ok((utime, stime)) = find_thread_cpu_time(self.pid, tid) {
                let total = (utime + stime) / *CLK_TCK;
                let cpu_total = cpu_totals.get_metric_with_label_values(&[&tname]).unwrap();
                let past = cpu_total.get();
                let delta = total - past;
                if delta > 0.0 {
                    cpu_total.inc_by(delta).unwrap();
                }
            }
        }

        cpu_totals.collect()
    }
}

fn get_threads(pid: pid_t) -> Result<Vec<(pid_t, String)>> {
    let mut pairs = Vec::new();

    let dirs = try!(fs::read_dir(format!("/proc/{}/task", pid)));
    for task in dirs {
        let tid = match try!(task).file_name().to_str() {
            Some(tid) => try!(tid.parse().map_err(|e| to_err(format!("{}", e)))),
            None => return Err(to_err(format!("fail to read {} task", pid))),
        };

        let mut tname = String::new();
        try!(fs::File::open(format!("/proc/{}/task/{}/comm", pid, tid))
            .and_then(|mut f| f.read_to_string(&mut tname)));

        pairs.push((tid, sanitize_thread_name(tname)));
    }

    Ok(pairs)
}

static TH_CNT: AtomicUsize = ATOMIC_USIZE_INIT;

fn sanitize_thread_name(name: String) -> String {
    let mut san = String::with_capacity(name.len());
    for c in name.chars() {
        match c {
            // Prometheus label characters `[a-zA-Z0-9_:]`
            'a'...'z' | 'A'...'Z' | '0'...'9' | '_' | ':' => {
                san.push(c);
            }
            '-' => san.push('_'),
            _ => (),
        }
    }

    if san.is_empty() {
        format!("unkonw_{}", TH_CNT.fetch_add(1, Ordering::Relaxed))
    } else {
        san
    }
}

fn to_err(s: String) -> Error {
    Error::new(ErrorKind::Other, s)
}

// See more man proc.
const CPU_INDEX: [usize; 2] = [14 - 1, 15 - 1];

fn find_thread_cpu_time(pid: pid_t, tid: pid_t) -> Result<(f64, f64)> {
    let mut buffer = String::new();
    try!(fs::File::open(format!("/proc/{}/task/{}/stat", pid, tid))
        .and_then(|mut f| f.read_to_string(&mut buffer)));

    let stats: Vec<_> = buffer.split_whitespace().collect();
    let utime = stats[CPU_INDEX[0]];
    let stime = stats[CPU_INDEX[1]];
    Ok((utime.parse().unwrap(), stime.parse().unwrap()))
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
    use std::thread;
    use std::sync;

    use libc;

    #[test]
    fn test_get_thread_name() {
        // Thread name's length is restricted to 16 characters,
        // including the terminating null byte ('\0').
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
        let pairs = super::get_threads(pid).unwrap();

        assert!(pairs.len() >= 1);
        pairs.iter()
            .find(|p| name == p.1)
            .unwrap();

        tx.send(()).unwrap();
        h.join().unwrap();
    }

    #[test]
    fn test_sanitize_thread_name() {
        let case = [("ok123", "ok123"),
                    ("a-b", "a_b"),
                    ("Az_1:", "Az_1:"),
                    ("@123", "123"),
                    ("t1\n", "t1"),
                    ("@", "unkonw_0")];
        for &(i, o) in &case {
            assert_eq!(super::sanitize_thread_name(i.to_owned()), o);
        }
    }
}
