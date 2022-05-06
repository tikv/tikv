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
use tikv_util::{
    sys::thread::{self, Pid},
    warn,
};

use crate::{IOBytes, IOType};

lazy_static! {
    /// Total I/O bytes read/written by each I/O type.
    static ref GLOBAL_IO_STATS: [AtomicIOBytes; IOType::COUNT] = Default::default();
    /// Incremental I/O bytes read/written by the thread's own I/O type.
    static ref LOCAL_IO_STATS: ThreadLocal<CachePadded<Mutex<LocalIOStats>>> = ThreadLocal::new();
}

thread_local! {
    /// A private copy of I/O type. Optimized for local access.
    static IO_TYPE: Cell<IOType> = Cell::new(IOType::Other);
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

    fn fetch_io_bytes(&mut self) -> Option<IOBytes> {
        if self.proc_reader.is_none() {
            let path = PathBuf::from("/proc")
                .join(format!("{}", self.pid))
                .join("task")
                .join(format!("{}", self.tid))
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
            let mut io_bytes = IOBytes::default();
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
                            io_bytes.read = u64::from_str(value).ok()?;
                        } else if field.starts_with("write_bytes") {
                            io_bytes.write = u64::from_str(value).ok()?;
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

struct LocalIOStats {
    id: ThreadID,
    io_type: IOType,
    last_flushed: IOBytes,
}

impl LocalIOStats {
    fn current() -> Self {
        LocalIOStats {
            id: ThreadID::current(),
            io_type: IOType::Other,
            last_flushed: IOBytes::default(),
        }
    }
}

#[derive(Default)]
struct AtomicIOBytes {
    read: AtomicU64,
    write: AtomicU64,
}

impl AtomicIOBytes {
    fn load(&self, order: Ordering) -> IOBytes {
        IOBytes {
            read: self.read.load(order),
            write: self.write.load(order),
        }
    }

    fn fetch_add(&self, other: IOBytes, order: Ordering) {
        self.read.fetch_add(other.read, order);
        self.write.fetch_add(other.write, order);
    }
}

/// Flushes the local I/O stats to global I/O stats.
#[inline]
fn flush_thread_io(sentinel: &mut LocalIOStats) {
    if let Some(io_bytes) = sentinel.id.fetch_io_bytes() {
        GLOBAL_IO_STATS[sentinel.io_type as usize]
            .fetch_add(io_bytes - sentinel.last_flushed, Ordering::Relaxed);
        sentinel.last_flushed = io_bytes;
    }
}

pub fn init() -> Result<(), String> {
    Ok(())
}

pub fn set_io_type(new_io_type: IOType) {
    IO_TYPE.with(|io_type| {
        if io_type.get() != new_io_type {
            let mut sentinel = LOCAL_IO_STATS
                .get_or(|| CachePadded::new(Mutex::new(LocalIOStats::current())))
                .lock();
            flush_thread_io(&mut sentinel);
            sentinel.io_type = new_io_type;
            io_type.set(new_io_type);
        }
    });
}

pub fn get_io_type() -> IOType {
    IO_TYPE.with(|io_type| io_type.get())
}

pub fn fetch_io_bytes() -> [IOBytes; IOType::COUNT] {
    let mut bytes: [IOBytes; IOType::COUNT] = Default::default();
    LOCAL_IO_STATS.iter().for_each(|sentinel| {
        flush_thread_io(&mut sentinel.lock());
    });
    for i in 0..IOType::COUNT {
        bytes[i] = GLOBAL_IO_STATS[i].load(Ordering::Relaxed);
    }
    bytes
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
    use crate::{OpenOptions, WithIOType};

    #[test]
    fn test_read_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_read_bytes.txt");
        let mut id = ThreadID::current();
        let _type = WithIOType::new(IOType::Compaction);
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
        let _type = WithIOType::new(IOType::Compaction);
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
