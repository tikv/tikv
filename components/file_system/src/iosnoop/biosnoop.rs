// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::metrics::*;
use super::{IOStats, IOType};

use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bcc::{table::Table, Kprobe, BPF};
use crossbeam_utils::CachePadded;

static mut BPF_TABLE: Option<(BPF, Table, Table)> = None;
static IDX_COUNTER: AtomicUsize = AtomicUsize::new(0);
// For simplicity, just open large enough array. TODO: make it to be Vec
static mut IO_TYPE_ARRAY: [CachePadded<IOType>; 100] = [CachePadded::new(IOType::Other); 100];

thread_local! {
    static IDX: usize = unsafe {
        let idx = IDX_COUNTER.fetch_add(1, Ordering::SeqCst);
        if idx == 100 {
            panic!("exceed maximum thread count");
        }
        if let Some((_, _, t)) = BPF_TABLE.as_mut() {
            let tid = nix::unistd::gettid().as_raw() as u32;
            let ptr : *const *const _ = &IO_TYPE_ARRAY.as_ptr().add(idx);
            t.set(&mut tid.to_ne_bytes(), std::slice::from_raw_parts_mut(ptr as *mut u8, std::mem::size_of::<*const IOType>())).unwrap();
        }
        idx
    }
}

pub fn set_io_type(new_io_type: IOType) {
    unsafe {
        IDX.with(|idx| {
            *IO_TYPE_ARRAY[*idx] = new_io_type;
        })
    };
}

pub fn get_io_type() -> IOType {
    unsafe { *IDX.with(|idx| IO_TYPE_ARRAY[*idx]) }
}

unsafe fn get_io_stats() -> Option<HashMap<IOType, IOStats>> {
    if let Some((_, t, _)) = BPF_TABLE.as_mut() {
        let mut map = HashMap::new();
        for e in t.iter() {
            let typ = ptr::read(e.key.as_ptr() as *const IOType);
            let stats = ptr::read(e.value.as_ptr() as *const IOStats);
            map.insert(typ, stats);
        }
        Some(map)
    } else {
        None
    }
}

pub struct IOContext {
    io_stats_map: Option<HashMap<IOType, IOStats>>,
}

impl IOContext {
    pub fn new() -> Self {
        IOContext {
            io_stats_map: unsafe { get_io_stats() },
        }
    }

    #[allow(dead_code)]
    pub fn delta(self) -> HashMap<IOType, IOStats> {
        if let Some(prev_map) = self.io_stats_map {
            if let Some(mut now_map) = unsafe { get_io_stats() } {
                for (typ, stats) in prev_map {
                    now_map.entry(typ).and_modify(|e| {
                        e.read -= stats.read;
                        e.write -= stats.write;
                    });
                }
                return now_map;
            }
        }
        HashMap::default()
    }

    #[allow(dead_code)]
    pub fn delta_and_refresh(&mut self) -> HashMap<IOType, IOStats> {
        if self.io_stats_map.is_some() {
            if let Some(map) = unsafe { get_io_stats() } {
                for (typ, stats) in &map {
                    self.io_stats_map
                        .as_mut()
                        .unwrap()
                        .entry(*typ)
                        .and_modify(|e| {
                            e.read = stats.read - e.read;
                            e.write = stats.write - e.write;
                        })
                        .or_insert(stats.clone());
                }

                return self.io_stats_map.replace(map).unwrap();
            }
        }
        HashMap::default()
    }
}

pub fn init_io_snooper() -> Result<(), String> {
    let code = include_str!("biosnoop.c");
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
        .function("blk_account_io_completion")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    // the "events" table is where the "open file" events get sent
    let stats_table = bpf.table("stats_by_type").map_err(|e| e.to_string())?;
    let type_table = bpf.table("type_by_pid").map_err(|e| e.to_string())?;
    unsafe {
        BPF_TABLE = Some((bpf, stats_table, type_table));
    }
    Ok(())
}

lazy_static! {
    static ref IO_CONTEXT: Mutex<IOContext> = Mutex::new(IOContext::new());
}

macro_rules! flush_io_latency {
    ($bpf:ident, $delta:ident, $metrics:ident) => {
        let mut t = $bpf.table(concat!(stringify!($metrics), "_latency")).unwrap();
        for e in t.iter() {
            let bucket = unsafe { ptr::read(e.key.as_ptr() as *const u64) };
            let count = unsafe { ptr::read(e.value.as_ptr() as *const u64) };

            for _ in 0..count {
                IO_LATENCY_MICROS.$metrics.read.observe(bucket as f64);
            }
        }
        t.delete_all().unwrap();

        // IO_BYTES.$metrics.read.inc_by($delta.get(IOType::pascal!($metrics)).unwrap_or_default().read);
        // IO_BYTES.$metrics.write.inc_by($delta.get(IOType::pascal!($metrics)).unwrap_or_default().write);
    };
}

pub unsafe fn flush_io_metrics() {
    if let Some((bpf, _, _)) = BPF_TABLE.as_mut() {
        let delta = IO_CONTEXT.lock().unwrap().delta_and_refresh();
        flush_io_latency!(bpf, delta, other);
        flush_io_latency!(bpf, delta, read);
        flush_io_latency!(bpf, delta, write);
        flush_io_latency!(bpf, delta, coprocessor);
        flush_io_latency!(bpf, delta, flush);
        flush_io_latency!(bpf, delta, compaction);
        flush_io_latency!(bpf, delta, replication);
        flush_io_latency!(bpf, delta, loadbalance);
        flush_io_latency!(bpf, delta, import);
        flush_io_latency!(bpf, delta, export);
    }
}

#[cfg(test)]
mod tests {
    use crate::{get_io_type, init_io_snooper, set_io_type, IOContext, IOType};
    use std::{fs::OpenOptions, io::Read, io::Write, os::unix::fs::OpenOptionsExt};
    use tempfile::TempDir;

    use libc::O_DIRECT;
    use maligned::A512;
    use maligned::{AsBytes, AsBytesMut};

    #[test]
    fn test_io_context() {
        init_io_snooper().unwrap();
        set_io_type(IOType::Compaction);
        assert_eq!(get_io_type(), IOType::Compaction);
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("test_io_context");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut w = vec![A512::default(); 2];
        w.as_bytes_mut()[512] = 42;
        let mut ctx = IOContext::new();
        f.write(w.as_bytes()).unwrap();
        f.sync_all().unwrap();
        let delta = ctx.delta_and_refresh();
        assert_ne!(delta.get(&IOType::Compaction).unwrap().write, 0);
        assert_eq!(delta.get(&IOType::Compaction).unwrap().read, 0);
        drop(f);

        std::thread::spawn(move || {
            set_io_type(IOType::Other);
            let mut f = OpenOptions::new()
                .read(true)
                .custom_flags(O_DIRECT)
                .open(&file_path)
                .unwrap();
            let mut r = vec![A512::default(); 2];
            f.read(&mut r.as_bytes_mut()).unwrap();
            drop(f);
        })
        .join()
        .unwrap();

        let delta = ctx.delta();
        assert_eq!(delta.get(&IOType::Compaction).unwrap().write, 0);
        assert_eq!(delta.get(&IOType::Compaction).unwrap().read, 0);
        assert_eq!(delta.get(&IOType::Other).unwrap().write, 0);
        assert_ne!(delta.get(&IOType::Other).unwrap().read, 0);
    }
}
