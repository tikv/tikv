// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nix::unistd::getpid;
use nix::unistd::gettid;
use nix::unistd::Pid;
use strum::EnumCount;
use thread_local::ThreadLocal;

use crate::IOBytes;
use crate::IOType;

lazy_static! {
    static ref THREAD_IO_SENTINEL_VEC: ThreadLocal<Arc<ThreadIOSentinel>> = ThreadLocal::new();
}

thread_local! {
    static THREAD_IO_SENTINEL: Arc<ThreadIOSentinel> = THREAD_IO_SENTINEL_VEC.get_or(||
        Arc::new(ThreadIOSentinel::current_default())
    ).clone();
}

struct ThreadIOSentinel {
    id: ThreadID,
    bytes: BytesBuffer,
}

struct ThreadID {
    pid: Pid,
    tid: Pid,
}

struct BytesBuffer {
    io_type: Mutex<IOType>,
    total_bytes: [AtomicIOBytes; IOType::COUNT],
    last_bytes: AtomicIOBytes,
}

struct AtomicIOBytes {
    read: AtomicU64,
    write: AtomicU64,
}

pub fn fetch_all_thread_io_bytes() {
    THREAD_IO_SENTINEL_VEC.iter().for_each(|sentinel| {
        fetch_sentinel(sentinel, false, None);
    });
}

fn fetch_sentinel(sentinel: &ThreadIOSentinel, enforce: bool, new_io_type: Option<IOType>) {
    let io_bytes = fetch_exact_thread_io_bytes(&sentinel.id);

    let mut io_type = {
        if enforce {
            sentinel.bytes.io_type.lock().unwrap()
        } else {
            match sentinel.bytes.io_type.try_lock() {
                Ok(io_type) => io_type,
                Err(_) => return,
            }
        }
    };
    let last_io_bytes = sentinel.bytes.last_bytes.load(Ordering::Relaxed);

    sentinel.bytes.total_bytes[*io_type as usize]
        .fetch_add(io_bytes - last_io_bytes, Ordering::Relaxed);
    sentinel.bytes.last_bytes.store(io_bytes, Ordering::Relaxed);
    if let Some(new_io_type) = new_io_type {
        *io_type = new_io_type;
    }
}

pub fn set_io_type(new_io_type: IOType) {
    THREAD_IO_SENTINEL.with(|sentinel| {
        fetch_sentinel(sentinel, true, Some(new_io_type));
    })
}

pub fn get_io_type() -> IOType {
    THREAD_IO_SENTINEL.with(|sentinel| *sentinel.bytes.io_type.lock().unwrap())
}

pub(crate) fn fetch_buffered_thread_io_bytes(io_type: IOType) -> IOBytes {
    THREAD_IO_SENTINEL
        .with(|sentinel| sentinel.bytes.total_bytes[io_type as usize].load(Ordering::Relaxed))
}

pub(crate) fn fetch_thread_io_bytes(_io_type: IOType) -> IOBytes {
    fetch_exact_thread_io_bytes(&ThreadID::current())
}

fn fetch_exact_thread_io_bytes(id: &ThreadID) -> IOBytes {
    let io_file_path = PathBuf::from("/proc")
        .join(format!("{}", id.pid))
        .join("task")
        .join(format!("{}", id.tid))
        .join("io");

    if let Ok(io_file) = File::open(io_file_path) {
        return IOBytes::from_io_file(io_file);
    }

    IOBytes::default()
}

impl ThreadIOSentinel {
    fn current_default() -> ThreadIOSentinel {
        ThreadIOSentinel {
            id: ThreadID::current(),
            bytes: BytesBuffer::default(),
        }
    }
}

impl ThreadID {
    fn current() -> ThreadID {
        ThreadID {
            pid: getpid(),
            tid: gettid(),
        }
    }
}

impl Default for BytesBuffer {
    fn default() -> BytesBuffer {
        BytesBuffer {
            io_type: Mutex::new(IOType::Other),
            total_bytes: Default::default(),
            last_bytes: AtomicIOBytes::default(),
        }
    }
}

impl IOBytes {
    fn from_io_file<R: std::io::Read>(r: R) -> IOBytes {
        let reader = BufReader::new(r);
        let mut io_bytes = IOBytes::default();

        for line in reader.lines().flatten() {
            if line.is_empty() || !line.contains(' ') {
                continue;
            }
            let mut s = line.split_whitespace();

            if let (Some(field), Some(value)) = (s.next(), s.next()) {
                if let Ok(value) = u64::from_str(value) {
                    match &field[..field.len() - 1] {
                        "read_bytes" => io_bytes.read = value,
                        "write_bytes" => io_bytes.write = value,
                        _ => continue,
                    }
                }
            }
        }

        io_bytes
    }
}

impl Default for AtomicIOBytes {
    fn default() -> Self {
        let bytes = IOBytes::default();

        AtomicIOBytes {
            read: AtomicU64::new(bytes.read),
            write: AtomicU64::new(bytes.write),
        }
    }
}

impl AtomicIOBytes {
    fn load(&self, order: Ordering) -> IOBytes {
        IOBytes {
            read: self.read.load(order),
            write: self.write.load(order),
        }
    }
    fn store(&self, val: IOBytes, order: Ordering) {
        self.read.store(val.read, order);
        self.write.store(val.write, order);
    }
    fn fetch_add(&self, other: IOBytes, order: Ordering) {
        self.read.fetch_add(other.read, order);
        self.write.fetch_add(other.write, order);
    }
}

#[cfg(test)]
mod tests {
    use libc::O_DIRECT;
    use maligned::{AsBytes, AsBytesMut, A512};
    use std::{
        fs::OpenOptions,
        io::{Read, Write},
        os::unix::prelude::OpenOptionsExt,
    };
    use tempfile::{tempdir, tempdir_in};

    use super::*;

    #[test]
    fn test_read_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_read_bytes.txt");
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
        let origin_io_bytes = fetch_thread_io_bytes(IOType::Other);

        for i in 1..=10 {
            f.read_exact(w.as_bytes_mut()).unwrap();

            let io_bytes = fetch_thread_io_bytes(IOType::Other);

            assert_eq!(i * 512 + origin_io_bytes.read, io_bytes.read);
        }
    }

    #[test]
    fn test_write_bytes() {
        let tmp = tempdir_in("/var/tmp").unwrap_or_else(|_| tempdir().unwrap());
        let file_path = tmp.path().join("test_write_bytes.txt");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let w = vec![A512::default(); 1];

        let origin_io_bytes = fetch_thread_io_bytes(IOType::Other);
        for i in 1..=10 {
            f.write_all(w.as_bytes()).unwrap();
            f.sync_all().unwrap();

            let io_bytes = fetch_thread_io_bytes(IOType::Other);

            assert_eq!(i * 512 + origin_io_bytes.write, io_bytes.write);
        }
    }

    #[bench]
    fn bench_fetch_thread_io_bytes(b: &mut test::Bencher) {
        b.iter(|| fetch_thread_io_bytes(IOType::Other));
    }
}
