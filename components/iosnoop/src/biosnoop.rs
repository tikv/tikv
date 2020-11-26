// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bcc::perf_event::PerfMapBuilder;
use bcc::BccError;
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
        IOContext{
            tid,
            read_bytes: *IO_READ_BYTES_PER_PID.lock().unwrap().entry(tid).or_default(),
        }
    }

    fn delta(&self) -> u64 {
        *IO_READ_BYTES_PER_PID.lock().unwrap().entry(self.tid).or_default() - self.read_bytes
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
        code.replace("##TGID##", &nix::unistd::getpid().to_string());
        // compile the above BPF code!
        let mut bpf = BPF::new(&code).map_err(|e| e.to_string())?;
        // attach kprobes
        Kprobe::new()
            .handler("trace_req_completion")
            .function("blk_account_io_completion")
            .attach(&mut bpf).map_err(|e| e.to_string())?;
        // the "events" table is where the "open file" events get sent
        let table = bpf.table("events").map_err(|e| e.to_string())?;
        let mut perf_map = PerfMapBuilder::new(table, perf_data_callback).build().map_err(|e| e.to_string())?;
        let runnable = self.runnable.clone();
        self.handle = Some(ThreadBuilder::new()
            .name("io-snoop".to_owned())
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                while runnable.load(Ordering::SeqCst) {
                    // this `.poll()` loop is what makes our callback get called
                    perf_map.poll(200);
                }
                tikv_alloc::remove_thread_memory_accessor();
            }).unwrap());
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        self.runnable.store(false, Ordering::SeqCst);
        if let Err(e) = h.unwrap().join()  {
            error!("join io snooper failed"; "err" => ?e);
            return;
        }
    }
}

fn perf_data_callback() -> Box<dyn FnMut(&[u8]) + Send> {
    Box::new(|x| {
        let data = parse_struct(x);
        if data.rwflag != 1 { // Read
            info!("get event from thread {}, bytes: {}", data.pid, data.len);
            IO_READ_BYTES_PER_PID.lock().unwrap().entry(data.pid)
                .and_modify(|e| { *e += data.len })
                .or_insert(data.len);
        }
    })
}

fn parse_struct(x: &[u8]) -> data_t {
    unsafe { ptr::read(x.as_ptr() as *const data_t) }
}