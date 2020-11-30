// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bcc::perf_event::PerfMapBuilder;
use bcc::{Kprobe, BPF};

use core::sync::atomic::{AtomicBool, Ordering};
use std::ptr;
use std::sync::{Arc, Mutex};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use tikv_util::collections::HashMap;

lazy_static! {
    pub static ref IO_READ_BYTES_PER_PID: Mutex<HashMap<u32, u64>> = Mutex::new(HashMap::default());
}

pub struct IOContext {
    tid: u32,
    read_bytes: u64,
}

impl IOContext {
    pub fn new() -> Self {
        let tid = nix::unistd::gettid().as_raw() as u32;
        IOContext {
            tid,
            read_bytes: *IO_READ_BYTES_PER_PID
                .lock()
                .unwrap()
                .entry(tid)
                .or_default(),
        }
    }

    fn delta(&self) -> u64 {
        *IO_READ_BYTES_PER_PID
            .lock()
            .unwrap()
            .entry(self.tid)
            .or_default()
            - self.read_bytes
    }
}

#[repr(C)]
struct data_t {
    pid: u32,
    rwflag: u64,
    len: u64,
}

pub struct IOSnooper {
    handle: Option<JoinHandle<()>>,
    runnable: Arc<AtomicBool>,
}

impl IOSnooper {
    pub fn new() -> Self {
        IOSnooper {
            handle: None,
            runnable: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn start(&mut self) -> Result<(), String> {
        let code = include_str!("biosnoop.c");
        info!("My pid is {}", nix::unistd::getpid());
        let code = code.replace("##TGID##", &nix::unistd::getpid().to_string());
        // compile the above BPF code!
        let mut bpf = BPF::new(&code).map_err(|e| e.to_string())?;
        // attach kprobes
        Kprobe::new()
            .handler("trace_pid_start")
            .function("blk_account_io_start")
            .attach(&mut bpf)
            .map_err(|e| e.to_string())?;
        Kprobe::new()
            .handler("trace_req_completion")
            .function("blk_account_io_done")
            .attach(&mut bpf)
            .map_err(|e| e.to_string())?;
        // the "events" table is where the "open file" events get sent
        let table = bpf.table("events").map_err(|e| e.to_string())?;
        let mut perf_map = PerfMapBuilder::new(table, perf_data_callback)
            .build()
            .map_err(|e| e.to_string())?;
        let runnable = self.runnable.clone();
        self.handle = Some(
            ThreadBuilder::new()
                .name("io-snoop".to_owned())
                .spawn(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    while runnable.load(Ordering::SeqCst) {
                        // this `.poll()` loop is what makes our callback get called
                        perf_map.poll(200);
                    }
                    tikv_alloc::remove_thread_memory_accessor();
                })
                .unwrap(),
        );
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        self.runnable.store(false, Ordering::SeqCst);
        if let Err(e) = h.unwrap().join() {
            error!("join io snooper failed"; "err" => ?e);
            return;
        }
    }
}

fn perf_data_callback() -> Box<dyn FnMut(&[u8]) + Send> {
    Box::new(|x| {
        let data = parse_struct(x);
        if data.rwflag != 1 {
            // Read
            info!(
                "get event from thread {}, bytes: {}",
                data.pid, data.len
            );
            IO_READ_BYTES_PER_PID
                .lock()
                .unwrap()
                .entry(data.pid)
                .and_modify(|e| *e += data.len)
                .or_insert(data.len);
        }
    })
}

fn parse_struct(x: &[u8]) -> data_t {
    unsafe { ptr::read(x.as_ptr() as *const data_t) }
}

#[cfg(test)]
mod tests {
    use crate::{IOContext, IOSnooper};
    use std::process::Command;
    use std::time::Duration;
    use std::{
        fs, fs::File, fs::OpenOptions, io::Read, io::Write, os::unix::fs::OpenOptionsExt,
        path::Path,
    };
    use tempfile::TempDir;

    use libc::O_DIRECT;
    use maligned::A512;
    use maligned::{AsBytes, AsBytesMut};

    #[test]
    fn test_io_context() {
        let mut snooper = IOSnooper::new();
        snooper.start().unwrap();

        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("test_io_context");
        // write something with direct io to not fill cache
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = vec![A512::default(); 2];
        w.as_bytes_mut()[512] = 42;

        f.write(w.as_bytes()).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let mut f = File::open(&file_path).unwrap();
        let mut buffer = String::new();
        let ctx = IOContext::new();
        f.read_to_string(&mut buffer).unwrap();
        std::thread::sleep(Duration::from_secs(10));
        assert_ne!(ctx.delta(), 0);
        drop(f);

        snooper.stop();
    }
}
