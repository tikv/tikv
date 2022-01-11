// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        borrow::BorrowMut,
        cell::Cell,
        fs::File,
        io::{BufRead, BufReader},
        path::PathBuf,
        str::FromStr,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard,
        },
        time::Duration,
    };

    use strum::EnumCount;
    use thread_local::ThreadLocal;
    use tikv_util::sys::thread::{self, Pid};

    use crate::{IOBytes, IOType};

    lazy_static! {
        static ref THREAD_IO_SENTINEL_VEC: ThreadLocal<Arc<ThreadIOSentinel>> = ThreadLocal::new();
        static ref THREAD_IO_TOTAL: [AtomicIOBytes; IOType::COUNT] = Default::default();
    }

    thread_local! {
        static THREAD_IO_SENTINEL: Arc<ThreadIOSentinel> = THREAD_IO_SENTINEL_VEC.get_or(||
            Arc::new(ThreadIOSentinel::current_default())
        ).clone();

        static THREAD_IO_PRIVATE: Cell<ThreadIOPrivate> = Cell::new(ThreadIOPrivate::default())
    }

    pub fn fetch_all_thread_io_bytes(io_type: IOType) -> IOBytes {
        THREAD_IO_SENTINEL_VEC.iter().for_each(|sentinel| {
            flush_sentinel(sentinel, None);
        });
        THREAD_IO_TOTAL[io_type as usize].load(Ordering::Relaxed)
    }

    pub fn set_io_type(new_io_type: IOType) {
        THREAD_IO_SENTINEL.with(|sentinel| {
            flush_sentinel(sentinel, Some(new_io_type));
        })
    }

    pub fn get_io_type() -> IOType {
        THREAD_IO_PRIVATE.with(|private| private.get().io_type)
    }

    struct ThreadIOSentinel {
        id: ThreadID,
        bytes: [AtomicIOBytes; IOType::COUNT],
    }

    impl ThreadIOSentinel {
        fn current_default() -> ThreadIOSentinel {
            ThreadIOSentinel {
                id: ThreadID::current(),
                bytes: Default::default(),
            }
        }
    }

    struct ThreadID {
        pid: Pid,
        tid: Pid,
    }

    impl ThreadID {
        fn current() -> ThreadID {
            ThreadID {
                pid: thread::process_id(),
                tid: thread::thread_id(),
            }
        }
    }

    #[derive(Clone, Copy)]
    struct ThreadIOPrivate {
        io_type: IOType,
        last_bytes: IOBytes,
    }

    struct AtomicIOBytes {
        read: AtomicU64,
        write: AtomicU64,
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

    fn flush_sentinel(sentinel: &ThreadIOSentinel, new_io_type: Option<IOType>) {
        let io_bytes = fetch_exact_thread_io_bytes(&sentinel.id);

        THREAD_IO_PRIVATE.with(|private_cell| {
            let mut private = private_cell.get();
            let io_type = private.io_type;
            let last_io_bytes = private.last_bytes;

            sentinel.bytes[io_type as usize].fetch_add(io_bytes - last_io_bytes, Ordering::Relaxed);
            THREAD_IO_TOTAL[io_type as usize]
                .fetch_add(io_bytes - last_io_bytes, Ordering::Relaxed);
            private.last_bytes = io_bytes;
            if let Some(new_io_type) = new_io_type {
                private.io_type = new_io_type;
            }
            private_cell.set(private)
        })
    }

    pub fn fetch_thread_io_bytes(_io_type: IOType) -> IOBytes {
        fetch_exact_thread_io_bytes(&ThreadID::current())
    }

    fn fetch_exact_thread_io_bytes(id: &ThreadID) -> IOBytes {
        let io_file_path = PathBuf::from("/proc")
            .join(format!("{}", id.pid))
            .join("task")
            .join(format!("{}", id.tid))
            .join("io");

        if let Ok(io_file) = File::open(io_file_path) {
            let reader = BufReader::new(io_file);
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

            return io_bytes;
        }

        IOBytes::default()
    }

    impl Default for ThreadIOPrivate {
        fn default() -> ThreadIOPrivate {
            ThreadIOPrivate {
                io_type: IOType::Other,
                last_bytes: IOBytes::default(),
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub use self::linux::{fetch_all_thread_io_bytes, get_io_type, set_io_type};

#[cfg(target_os = "linux")]
use self::linux::fetch_thread_io_bytes;

#[cfg(not(target_os = "linux"))]
mod non_linux {
    use crate::{IOBytes, IOType};

    pub fn fetch_all_thread_io_bytes(io_type: IOType) -> IOBytes {
        IOBytes::default()
    }

    pub fn set_io_type(new_io_type: IOType) {}

    pub fn get_io_type() -> IOType {
        IOType::Other
    }
}

#[cfg(not(target_os = "linux"))]
pub use self::non_linux::{fetch_all_thread_io_bytes, get_io_type, set_io_type};

#[cfg(target_os = "linux")]
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

    use crate::IOType;

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

        let w = vec![A512::default(); 8];

        let origin_io_bytes = fetch_thread_io_bytes(IOType::Other);
        for i in 1..=10 {
            f.write_all(w.as_bytes()).unwrap();
            f.sync_all().unwrap();

            let io_bytes = fetch_thread_io_bytes(IOType::Other);

            assert_eq!(i * 4096 + origin_io_bytes.write, io_bytes.write);
        }
    }

    #[bench]
    fn bench_fetch_thread_io_bytes(b: &mut test::Bencher) {
        b.iter(|| fetch_thread_io_bytes(IOType::Other));
    }
}
