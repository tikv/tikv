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
use std::io::{Result, Error, ErrorKind, Read};
use std::sync::Mutex;

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
                                         &["name", "tid"])
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
        let tids = get_thread_ids(self.pid).unwrap();
        for tid in tids {
            if let Ok((tname, utime, stime)) = get_thread_stat(self.pid, tid) {
                let total = (utime + stime) / *CLK_TCK;
                let cpu_total = cpu_totals
                    .get_metric_with_label_values(&[&tname, &format!("{}", tid)])
                    .unwrap();
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

fn get_thread_ids(pid: pid_t) -> Result<Vec<pid_t>> {
    let mut tids = Vec::new();
    let dirs = try!(fs::read_dir(format!("/proc/{}/task", pid)));
    for task in dirs {
        let file_name = match task {
            Ok(t) => t.file_name(),
            Err(e) => {
                error!("fail to read task of {}, error {:?}", pid, e);
                continue;
            }
        };

        let tid = match file_name.to_str() {
            Some(tid) => {
                match tid.parse() {
                    Ok(tid) => tid,
                    Err(e) => {
                        error!("fail to read task of {}, error {:?}", pid, e);
                        continue;
                    }
                }
            }
            None => {
                error!("fail to read task of {}", pid);
                continue;
            }
        };
        tids.push(tid);
    }

    Ok(tids)
}

// get thread name and the index of the last character(including ')').
fn get_thread_name(tid: pid_t, stat: &str) -> Result<(String, usize)> {
    let start = stat.find('(');
    let end = stat.rfind(')');

    if let (Some(start), Some(end)) = (start, end) {
        let mut name = String::with_capacity(THD_NAME_LEN);
        let raw = &stat[start + 1..end];

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

        return Ok((name, end));
    }

    Err(to_err(format!("can not find thread name, stat: {}", stat)))
}

fn to_err(s: String) -> Error {
    Error::new(ErrorKind::Other, s)
}

// Thread name's length is restricted to 16 characters,
// including the terminating null byte ('\0').
const THD_NAME_LEN: usize = 16;

// See more man proc.
// Index of utime and stime.
const CPU_INDEX: [usize; 2] = [14 - 1, 15 - 1];

fn get_thread_stat(pid: pid_t, tid: pid_t) -> Result<(String, f64, f64)> {
    let mut stat = String::new();
    try!(fs::File::open(format!("/proc/{}/task/{}/stat", pid, tid))
        .and_then(|mut f| f.read_to_string(&mut stat)));
    get_thread_stat_internal(tid, &stat)
}

// Extracted from `get_thread_stat`, for test purpose.
fn get_thread_stat_internal(tid: pid_t, stat: &str) -> Result<(String, f64, f64)> {
    let (name, end) = try!(get_thread_name(tid, stat));
    let stats: Vec<_> = (&stat[end + 2..]).split_whitespace().collect(); // excluding ") ".
    let utime = stats[CPU_INDEX[0] - 2]; // pid and comm is truncated.
    let stime = stats[CPU_INDEX[1] - 2];
    Ok((name, utime.parse().unwrap(), stime.parse().unwrap()))
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
        let tids = super::get_thread_ids(pid).unwrap();
        assert!(tids.len() >= 2);

        tids.iter()
            .find(|t| super::get_thread_stat(pid, **t).map(|stat| stat.0 == name).unwrap_or(false))
            .unwrap();

        tx.send(()).unwrap();
        h.join().unwrap();
    }

    #[test]
    fn test_get_thread_name() {
        let cases = [("(ok123)", "ok123", 6),
                     ("(a-b)", "a_b", 4),
                     ("(Az_1:)", "Az_1:", 6),
                     ("(@123)", "123", 5),
                     ("1 (ab) 1", "ab", 5),
                     ("1 (a b) 1", "a_b", 6),
                     ("1 ((a b )) 1", "a_b_", 9)];
        for &(i, o, idx) in &cases {
            let (name, end) = super::get_thread_name(1, i).unwrap();
            assert_eq!(name, o);
            assert_eq!(end, idx);
        }

        assert_eq!(super::get_thread_name(1, "(@#)").unwrap().0, "1");
        assert!(super::get_thread_name(1, "invalid_stat").is_err());
    }

    #[test]
    fn test_get_thread_stat() {
        let sample = "2810 (test thd) S 2550 2621 2621 0 -1 4210688 2632 0 52 0 839 138 0 \
                      0 20 0 4 0 13862 709652480 3647 18446744073709551615 4194304 4319028 \
                      140732554845776 140732554845392 140439688777693 0 0 4096 16384 0 0 0 17 3 \
                      0 0 245 0 0 6417696 6421000 8478720 140732554851684 140732554851747 \
                      140732554851747 140732554854339 0";

        let (name, utime, stime) = super::get_thread_stat_internal(2810, sample).unwrap();
        assert_eq!(name, "test_thd");
        assert_eq!(utime as i64, 839);
        assert_eq!(stime as i64, 138);
    }
}
