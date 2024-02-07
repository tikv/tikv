// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    fs::File,
    io::{BufRead, BufReader, Seek},
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use strum::EnumCount;
use thread_local::ThreadLocal;
use tikv_util::sys::thread::{self, Pid};

use crate::{IoBytes, IoType};

lazy_static! {
    /// Total I/O bytes read/written by each I/O type.
    static ref GLOBAL_IO_STATS: [AtomicIoBytes; IoType::COUNT] = Default::default();
    /// Incremental I/O bytes read/written by the thread's own I/O type.
    static ref LOCAL_IO_STATS: ThreadLocal<CachePadded<Mutex<LocalIoStats>>> = ThreadLocal::new();
}

thread_local! {
    /// A private copy of I/O type. Optimized for local access.
    static IO_TYPE: Cell<IoType> = const { Cell::new(IoType::Other) };
}

#[derive(Debug)]
struct ThreadId {
    pid: Pid,
    tid: Pid,
    proc_reader: Option<BufReader<File>>,
}

impl ThreadId {
    fn current() -> ThreadId {
        let pid = thread::process_id();
        let tid = thread::thread_id();
        ThreadId {
            pid,
            tid,
            proc_reader: None,
        }
    }

    fn fetch_io_bytes(&mut self) -> Result<IoBytes, String> {
        if self.proc_reader.is_none() {
            let path = PathBuf::from("/proc")
                .join(format!("{}", self.pid))
                .join("task")
                .join(format!("{}", self.tid))
                .join("io");
            self.proc_reader = Some(BufReader::new(
                File::open(path).map_err(|e| format!("open: {}", e))?,
            ));
        }
        let reader = self.proc_reader.as_mut().unwrap();
        reader
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| format!("seek: {}", e))?;
        let mut io_bytes = IoBytes::default();
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    if line.len() > 11 {
                        let mut s = line.split_whitespace();
                        if let (Some(field), Some(value)) = (s.next(), s.next()) {
                            if field.starts_with("read_bytes") {
                                io_bytes.read = u64::from_str(value)
                                    .map_err(|e| format!("parse read_bytes: {}", e))?;
                            } else if field.starts_with("write_bytes") {
                                io_bytes.write = u64::from_str(value)
                                    .map_err(|e| format!("parse write_bytes: {}", e))?;
                            }
                        }
                    }
                }
                // ESRCH 3 No such process
                Err(e) if e.raw_os_error() == Some(3) => break,
                Err(e) => return Err(format!("read: {}", e)),
            }
        }
        Ok(io_bytes)
    }
}

struct LocalIoStats {
    id: ThreadId,
    io_type: IoType,
    last_flushed: IoBytes,
}

impl LocalIoStats {
    fn current() -> Self {
        LocalIoStats {
            id: ThreadId::current(),
            io_type: IoType::Other,
            last_flushed: IoBytes::default(),
        }
    }
}

#[derive(Default)]
struct AtomicIoBytes {
    read: AtomicU64,
    write: AtomicU64,
}

impl AtomicIoBytes {
    fn load(&self, order: Ordering) -> IoBytes {
        IoBytes {
            read: self.read.load(order),
            write: self.write.load(order),
        }
    }

    fn fetch_add(&self, other: IoBytes, order: Ordering) {
        self.read.fetch_add(other.read, order);
        self.write.fetch_add(other.write, order);
    }
}

/// Flushes the local I/O stats to global I/O stats.
#[inline]
fn flush_thread_io(sentinel: &mut LocalIoStats) {
    if let Ok(io_bytes) = sentinel.id.fetch_io_bytes() {
        GLOBAL_IO_STATS[sentinel.io_type as usize]
            .fetch_add(io_bytes - sentinel.last_flushed, Ordering::Relaxed);
        sentinel.last_flushed = io_bytes;
    }
}

pub fn init() -> Result<(), String> {
    ThreadId::current()
        .fetch_io_bytes()
        .map_err(|e| format!("failed to fetch I/O bytes from proc: {}", e))?;
    // Manually initialize the sentinel so that `fetch_io_bytes` doesn't miss any
    // thread.
    LOCAL_IO_STATS.get_or(|| CachePadded::new(Mutex::new(LocalIoStats::current())));
    tikv_util::sys::thread::hook_thread_start(Box::new(|| {
        LOCAL_IO_STATS.get_or(|| CachePadded::new(Mutex::new(LocalIoStats::current())));
    }));
    Ok(())
}

/// Bind I/O type for the current thread.
/// Following calls to the [`file_system`](crate) APIs would be throttled and
/// recorded via this information.
/// Generally, when you are creating new threads playing with the local disks,
/// you should call this before doing so.
pub fn set_io_type(new_io_type: IoType) {
    IO_TYPE.with(|io_type| {
        if io_type.get() != new_io_type {
            let mut sentinel = LOCAL_IO_STATS
                .get_or(|| CachePadded::new(Mutex::new(LocalIoStats::current())))
                .lock();
            flush_thread_io(&mut sentinel);
            sentinel.io_type = new_io_type;
            io_type.set(new_io_type);
        }
    });
}

pub fn get_io_type() -> IoType {
    IO_TYPE.with(|io_type| io_type.get())
}

pub fn fetch_io_bytes() -> [IoBytes; IoType::COUNT] {
    let mut bytes: [IoBytes; IoType::COUNT] = Default::default();
    LOCAL_IO_STATS.iter().for_each(|sentinel| {
        flush_thread_io(&mut sentinel.lock());
    });
    for i in 0..IoType::COUNT {
        bytes[i] = GLOBAL_IO_STATS[i].load(Ordering::Relaxed);
    }
    bytes
}

pub fn get_thread_io_bytes_total() -> Result<IoBytes, String> {
    match LOCAL_IO_STATS.get() {
        Some(s) => s.lock().id.fetch_io_bytes(),
        None => Err("thread local io stats is None".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        os::unix::fs::OpenOptionsExt,
        sync::mpsc,
    };

    use libc::O_DIRECT;
    use tempfile::{tempdir, tempdir_in};
    use tikv_util::sys::thread::StdThreadBuildWrapper;

    use super::*;
    use crate::{io_stats::A512, OpenOptions, WithIoType};

    #[test]
    fn test_read_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_read_bytes.txt");
        let mut id = ThreadId::current();
        let _type = WithIoType::new(IoType::Compaction);
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .custom_flags(O_DIRECT)
                .open(&file_path)
                .unwrap();
            let w = Box::new(A512([0u8; 512 * 10]));
            f.write_all(&w.0).unwrap();
            f.sync_all().unwrap();
        }
        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut w = A512([0u8; 512]);
        let base_local_bytes = id.fetch_io_bytes().unwrap();
        for i in 1..=10 {
            f.read_exact(&mut w.0).unwrap();

            let local_bytes = id.fetch_io_bytes().unwrap();
            assert_eq!(i * 512 + base_local_bytes.read, local_bytes.read);
        }
    }

    #[test]
    fn test_write_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_write_bytes.txt");
        let mut id = ThreadId::current();
        let _type = WithIoType::new(IoType::Compaction);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(file_path)
            .unwrap();
        let w = Box::new(A512([0u8; 512 * 8]));
        let base_local_bytes = id.fetch_io_bytes().unwrap();
        for i in 1..=10 {
            f.write_all(&w.0).unwrap();
            f.sync_all().unwrap();

            let local_bytes = id.fetch_io_bytes().unwrap();
            assert_eq!(i * 4096 + base_local_bytes.write, local_bytes.write);
        }
    }

    #[test]
    fn test_fetch_all_io_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());

        init().unwrap();

        let file_path = tmp.path().join("test_fetch_all_io_bytes_1.txt");
        let (tx1, rx1) = mpsc::sync_channel(0);
        let t1 = std::thread::Builder::new()
            .spawn_wrapper(move || {
                set_io_type(IoType::ForegroundWrite);
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .custom_flags(O_DIRECT)
                    .open(file_path)
                    .unwrap();
                let w = Box::new(A512([0u8; 512 * 8]));
                f.write_all(&w.0).unwrap();
                f.sync_all().unwrap();
                tx1.send(()).unwrap();
                tx1.send(()).unwrap();
            })
            .unwrap();

        let file_path = tmp.path().join("test_fetch_all_io_bytes_2.txt");
        let (tx2, rx2) = mpsc::sync_channel(0);
        let t2 = std::thread::Builder::new()
            .spawn_wrapper(move || {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .custom_flags(O_DIRECT)
                    .open(file_path)
                    .unwrap();
                let w = Box::new(A512([0u8; 512 * 8]));
                f.write_all(&w.0).unwrap();
                f.sync_all().unwrap();
                tx2.send(()).unwrap();
                tx2.send(()).unwrap();
            })
            .unwrap();

        rx1.recv().unwrap();
        rx2.recv().unwrap();
        let bytes = fetch_io_bytes();
        assert_eq!(bytes[IoType::ForegroundWrite as usize].write, 4096);
        assert_eq!(bytes[IoType::Other as usize].write, 4096);

        rx1.recv().unwrap();
        rx2.recv().unwrap();
        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[bench]
    fn bench_fetch_thread_io_bytes(b: &mut test::Bencher) {
        let mut id = ThreadId::current();
        b.iter(|| id.fetch_io_bytes().unwrap());
    }
}
