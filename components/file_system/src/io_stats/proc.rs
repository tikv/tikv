// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    fs::File,
    io::{BufRead, BufReader, Seek},
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use strum::EnumCount;
use thread_local::ThreadLocal;
use tikv_util::{
    sys::thread::{self, Pid},
    warn,
};

use crate::{IoBytes, IoContext, IoType};

lazy_static! {
    /// Total I/O bytes read/written by each I/O type.
    static ref GLOBAL_IO_STATS: [AtomicIoBytes; IoType::COUNT] = Default::default();
    /// Incremental I/O bytes read/written by the thread's own I/O type. This
    /// counter is updated and synchronized to the global counter in
    /// [`flush_thread_io`]. It is called by user or when the thread-local I/O
    /// type changes.
    static ref LOCAL_IO_STATS: ThreadLocal<CachePadded<Mutex<LocalIoStats>>> = ThreadLocal::new();
}

thread_local! {
    /// A private copy of I/O type. Optimized for local access.
    static IO_CTX: Cell<IoContext> = Cell::new(init_io_context());
}

/// IO context will always be accessed by IO rate limiter regardless of whether
/// the IO type is set correctly. We do some thread initialization work here.
fn init_io_context() -> IoContext {
    // Initialize thread local context.
    LOCAL_IO_STATS.get_or(|| CachePadded::new(Mutex::new(LocalIoStats::current())));
    IoContext::new(IoType::Other)
}

#[derive(Debug)]
struct ThreadID {
    pid: Pid,
    tid: Pid,
    proc_reader: Option<BufReader<File>>,
}

impl ThreadID {
    fn current() -> ThreadID {
        let pid = thread::process_id();
        let tid = thread::thread_id();
        ThreadID {
            pid,
            tid,
            proc_reader: None,
        }
    }

    /// Gets the accumulated disk IO bytes of current thread from procfs.
    // proc file example:
    // test:/tmp # cat /proc/3828/io
    // rchar: 323934931
    // wchar: 323929600
    // syscr: 632687
    // syscw: 632675
    // read_bytes: 0
    // write_bytes: 323932160
    // cancelled_write_bytes: 0
    fn fetch_io_bytes(&mut self) -> Option<IoBytes> {
        if self.proc_reader.is_none() {
            let path = PathBuf::from("/proc")
                .join(self.pid.to_string())
                .join("task")
                .join(self.tid.to_string())
                .join("io");
            match File::open(path) {
                Ok(file) => {
                    self.proc_reader = Some(BufReader::new(file));
                }
                Err(e) => {
                    warn!("failed to open proc file: {}", e);
                }
            }
        }
        if let Some(ref mut reader) = self.proc_reader {
            reader
                .seek(std::io::SeekFrom::Start(0))
                .map_err(|e| {
                    warn!("failed to seek proc file: {}", e);
                })
                .ok()?;
            let mut io_bytes = IoBytes::default();
            for line in reader.lines() {
                let line = line
                    .map_err(|e| {
                        // ESRCH 3 No such process
                        if e.raw_os_error() != Some(3) {
                            warn!("failed to read proc file: {}", e);
                        }
                    })
                    .ok()?;
                if line.len() > 11 {
                    let mut s = line.split_whitespace();
                    if let (Some(field), Some(value)) = (s.next(), s.next()) {
                        if field.starts_with("read_bytes") {
                            io_bytes.read = usize::from_str(value).ok()?;
                        } else if field.starts_with("write_bytes") {
                            io_bytes.write = usize::from_str(value).ok()?;
                        }
                    }
                }
            }
            Some(io_bytes)
        } else {
            None
        }
    }
}

struct LocalIoStats {
    id: ThreadID,
    io_type: IoType,
    last_flushed: IoBytes,
}

impl LocalIoStats {
    fn current() -> Self {
        LocalIoStats {
            id: ThreadID::current(),
            io_type: IoType::Other,
            last_flushed: IoBytes::default(),
        }
    }
}

#[derive(Default)]
struct AtomicIoBytes {
    read: AtomicUsize,
    write: AtomicUsize,
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

/// Fetches latest thread I/O stats from procfs, then flushes it to global I/O
/// stats. Returns the renewed total I/O bytes of current thread.
#[inline]
fn flush_thread_io(sentinel: &mut LocalIoStats) -> IoBytes {
    if let Some(io_bytes) = sentinel.id.fetch_io_bytes() {
        let delta = io_bytes - sentinel.last_flushed;
        GLOBAL_IO_STATS[sentinel.io_type as usize].fetch_add(delta, Ordering::Relaxed);
        sentinel.last_flushed = io_bytes;
        io_bytes
    } else {
        IoBytes::default()
    }
}

pub fn init() -> Result<(), String> {
    Ok(())
}

pub(crate) fn get_io_context() -> IoContext {
    IO_CTX.with(|ctx| ctx.get())
}

pub(crate) fn set_io_context(new_ctx: IoContext) {
    IO_CTX.with(|ctx| {
        let old_ctx = ctx.get();
        debug_assert!(new_ctx.total_read_bytes >= old_ctx.total_read_bytes);
        if new_ctx.io_type != old_ctx.io_type {
            let mut sentinel = LOCAL_IO_STATS
                .get_or(|| CachePadded::new(Mutex::new(LocalIoStats::current())))
                .lock();
            flush_thread_io(&mut sentinel);
            // The `outstanding_read_bytes` of `old_ctx` is inherited by the
            // `new_ctx` even though they are of different I/O types.
            sentinel.io_type = new_ctx.io_type;
        }
        ctx.set(new_ctx);
    });
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

pub fn fetch_thread_io_bytes() -> IoBytes {
    let mut sentinel = LOCAL_IO_STATS
        .get_or(|| CachePadded::new(Mutex::new(LocalIoStats::current())))
        .lock();
    flush_thread_io(&mut sentinel)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        os::unix::fs::OpenOptionsExt,
    };

    use libc::O_DIRECT;
    use maligned::{AsBytes, AsBytesMut, A512};
    use tempfile::{tempdir, tempdir_in};

    use super::*;
    use crate::{OpenOptions, WithIoType};

    #[test]
    fn test_read_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_read_bytes.txt");
        let mut id = ThreadID::current();
        let _type = WithIoType::new(IoType::Compaction);
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .custom_flags(O_DIRECT)
                .open(&file_path)
                .unwrap();
            let w = vec![A512::default(); 10];
            f.write_all(w.as_bytes()).unwrap();
            f.sync_all().unwrap();
        }
        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut w = vec![A512::default(); 1];
        let base_local_bytes = id.fetch_io_bytes().unwrap();
        for i in 1..=10 {
            f.read_exact(w.as_bytes_mut()).unwrap();

            let local_bytes = id.fetch_io_bytes().unwrap();
            assert_eq!(i * 512 + base_local_bytes.read, local_bytes.read);
        }
    }

    #[test]
    fn test_write_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_write_bytes.txt");
        let mut id = ThreadID::current();
        let _type = WithIoType::new(IoType::Compaction);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let w = vec![A512::default(); 8];
        let base_local_bytes = id.fetch_io_bytes().unwrap();
        for i in 1..=10 {
            f.write_all(w.as_bytes()).unwrap();
            f.sync_all().unwrap();

            let local_bytes = id.fetch_io_bytes().unwrap();
            assert_eq!(i * 4096 + base_local_bytes.write, local_bytes.write);
        }
    }

    #[bench]
    fn bench_fetch_thread_io_bytes(b: &mut test::Bencher) {
        let mut id = ThreadID::current();
        b.iter(|| id.fetch_io_bytes().unwrap());
    }
}
