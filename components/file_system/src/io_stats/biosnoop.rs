// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use bcc::{table::Table, Kprobe, BPF};
use crossbeam_utils::CachePadded;
use strum::{EnumCount, IntoEnumIterator};
use tikv_util::sys::thread;

use crate::{metrics::*, IoBytes, IoType};

/// Biosnoop leverages BCC to make use of eBPF to get disk IO of TiKV requests.
/// The BCC code is in `biosnoop.c` which is compiled and attached kernel on
/// TiKV bootstrap. The code hooks on the start and completion of blk_account_io
/// in kernel, so it's easily to get the latency and bytes of IO requests issued
/// by current PID.
///
/// The main usage of iosnoop is to get accurate disk IO of different tasks
/// separately, like compaction, coprocessor and raftstore, instead of a global
/// disk throughput. So IO-types should be tagged for different threads by
/// `set_io_type()`. And BCC code is available to get the IO-type for one thread
/// by address, then all the IO requests for that thread will be recorded in
/// corresponding type's map in BCC.
///
/// With that information, every time calling `IoContext` it get the stored
/// stats from corresponding type's map in BCC. Thus it enables TiKV to get the
/// latency and bytes of read/write request per IO-type.

const MAX_THREAD_IDX: usize = 192;

// Hold the BPF to keep it not dropped.
// The two tables are `stats_by_type` and `type_by_pid` respectively.
static mut BPF_CONTEXT: Option<BpfContext> = None;

struct BpfContext {
    bpf: BPF,
    stats_table: Table,
    type_table: Table,
}

// This array records the IO-type for every thread. The address of this array
// will be passed into eBPF, so eBPF code can get IO-type for specific thread
// without an extra syscall.
// It should be a thread local variable, but the address of thread local is not
// reliable. So define a global array and let each thread writes on a specific
// element. And the IO-type is read when blk_account_io_start is called which is
// fired in the process context. That is to say, it's called in kernel space of
// user thread rather than kernel thread. So there is no contention between user
// and kernel. Thus no need to make the elements atomic. Also use padding to
// avoid false sharing.
// Leave the last element as reserved, when there is no available index, all
// other threads will be allocated to that index with IoType::Other always.
static mut IO_TYPE_ARRAY: [CachePadded<IoType>; MAX_THREAD_IDX + 1] =
    [CachePadded::new(IoType::Other); MAX_THREAD_IDX + 1];

// The index of the element of IO_TYPE_ARRAY for this thread to access.
thread_local! {
    static IDX: IdxWrapper = unsafe {
        let idx = IDX_ALLOCATOR.allocate();
        if let Some(ctx) = BPF_CONTEXT.as_mut() {
            let tid = thread::thread_id();
            let ptr : *const *const _ = &IO_TYPE_ARRAY.as_ptr().add(idx.0);
            ctx.type_table.set(
                &mut tid.to_ne_bytes(),
                std::slice::from_raw_parts_mut(
                    ptr as *mut u8,
                    std::mem::size_of::<*const IoType>(),
                ),
            ).unwrap();
        }
        idx
    }
}

struct IdxWrapper(usize);

impl Drop for IdxWrapper {
    fn drop(&mut self) {
        unsafe { *IO_TYPE_ARRAY[self.0] = IoType::Other };
        IDX_ALLOCATOR.free(self.0);

        // drop() of static variables won't be called when program exits.
        // We need to call drop() of BPF to detach kprobe.
        if IDX_ALLOCATOR.is_all_free() {
            unsafe { BPF_CONTEXT.take() };
        }
    }
}

lazy_static! {
    static ref IDX_ALLOCATOR: IdxAllocator = IdxAllocator::new();
}

struct IdxAllocator {
    free_list: Mutex<VecDeque<usize>>,
    count: AtomicUsize,
}

impl IdxAllocator {
    fn new() -> Self {
        IdxAllocator {
            free_list: Mutex::new((0..MAX_THREAD_IDX).into_iter().collect()),
            count: AtomicUsize::new(0),
        }
    }

    fn allocate(&self) -> IdxWrapper {
        self.count.fetch_add(1, Ordering::SeqCst);
        IdxWrapper(
            if let Some(idx) = self.free_list.lock().unwrap().pop_front() {
                idx
            } else {
                MAX_THREAD_IDX
            },
        )
    }

    fn free(&self, idx: usize) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        if idx != MAX_THREAD_IDX {
            self.free_list.lock().unwrap().push_back(idx);
        }
    }

    fn is_all_free(&self) -> bool {
        self.count.load(Ordering::SeqCst) == 0
    }
}

pub fn set_io_type(new_io_type: IoType) {
    unsafe {
        IDX.with(|idx| {
            // if MAX_THREAD_IDX, keep IoType::Other always
            if idx.0 != MAX_THREAD_IDX {
                *IO_TYPE_ARRAY[idx.0] = new_io_type;
            }
        })
    };
}

pub fn get_io_type() -> IoType {
    unsafe { *IDX.with(|idx| IO_TYPE_ARRAY[idx.0]) }
}

pub fn fetch_io_bytes() -> [IoBytes; IoType::COUNT] {
    let mut bytes: [IoBytes; IoType::COUNT] = Default::default();
    unsafe {
        if let Some(ctx) = BPF_CONTEXT.as_mut() {
            for io_type in IoType::iter() {
                let mut io_type = io_type;
                let io_type_buf_ptr = &mut io_type as *mut IoType as *mut u8;
                let mut io_type_buf =
                    std::slice::from_raw_parts_mut(io_type_buf_ptr, std::mem::size_of::<IoType>());
                if let Ok(e) = ctx.stats_table.get(&mut io_type_buf) {
                    assert!(e.len() == std::mem::size_of::<IoBytes>());
                    bytes[io_type as usize] =
                        std::ptr::read_unaligned(e.as_ptr() as *const IoBytes);
                }
            }
        }
    }
    bytes
}

pub fn init() -> Result<(), String> {
    unsafe {
        if BPF_CONTEXT.is_some() {
            return Ok(());
        }
    }

    let code = include_str!("biosnoop.c").replace("##TGID##", &thread::process_id().to_string());

    // TODO: When using bpf_get_ns_current_pid_tgid of newer kernel, need
    // to get the device id and inode number.
    //
    // let stat = unsafe {
    //     let mut stat: libc::stat = std::mem::zeroed();
    //     if libc::stat(
    //         CString::new("/proc/self/ns/pid").unwrap().as_ptr(),
    //         &mut stat,
    //     ) != 0
    //     {
    //         return Err(String::from("Can't get namespace stats"));
    //     }
    //     stat
    // };
    // let code = code.replace("##DEV##", &stat.st_dev.to_string())
    //   .replace("##INO##", &stat.st_ino.to_string());

    // compile the above BPF code!
    let mut bpf = BPF::new(&code).map_err(|e| e.to_string())?;
    // attach kprobes
    Kprobe::new()
        .handler("trace_req_start")
        .function("blk_account_io_start")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    Kprobe::new()
        .handler("trace_req_completion")
        .function("blk_account_io_completion")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    let stats_table = bpf.table("stats_by_type").map_err(|e| e.to_string())?;
    let type_table = bpf.table("type_by_pid").map_err(|e| e.to_string())?;
    unsafe {
        BPF_CONTEXT = Some(BpfContext {
            bpf,
            stats_table,
            type_table,
        });
    }
    Ok(())
}

macro_rules! flush_io_latency {
    ($bpf:expr, $metrics:ident) => {
        let mut t = $bpf
            .table(concat!(stringify!($metrics), "_read_latency"))
            .unwrap();
        for mut e in t.iter() {
            let bucket = 2_u64.pow(ptr::read(e.key.as_ptr() as *const libc::c_int) as u32);
            let count = ptr::read(e.value.as_ptr() as *const u64);

            for _ in 0..count {
                IO_LATENCY_MICROS_VEC.$metrics.read.observe(bucket as f64);
            }
            let zero: u64 = 0;
            t.set(&mut e.key, &mut zero.to_ne_bytes()).unwrap();
        }

        let mut t = $bpf
            .table(concat!(stringify!($metrics), "_write_latency"))
            .unwrap();
        for mut e in t.iter() {
            let bucket = 2_u64.pow(ptr::read(e.key.as_ptr() as *const libc::c_int) as u32);
            let count = ptr::read(e.value.as_ptr() as *const u64);

            for _ in 0..count {
                IO_LATENCY_MICROS_VEC.$metrics.write.observe(bucket as f64);
            }
            let zero: u64 = 0;
            t.set(&mut e.key, &mut zero.to_ne_bytes()).unwrap();
        }
    };
}

#[allow(dead_code)]
pub fn flush_io_latency_metrics() {
    unsafe {
        if let Some(ctx) = BPF_CONTEXT.as_mut() {
            flush_io_latency!(ctx.bpf, other);
            flush_io_latency!(ctx.bpf, foreground_read);
            flush_io_latency!(ctx.bpf, foreground_write);
            flush_io_latency!(ctx.bpf, flush);
            flush_io_latency!(ctx.bpf, compaction);
            flush_io_latency!(ctx.bpf, replication);
            flush_io_latency!(ctx.bpf, load_balance);
            flush_io_latency!(ctx.bpf, gc);
            flush_io_latency!(ctx.bpf, import);
            flush_io_latency!(ctx.bpf, export);
        }
    }
}

pub fn get_thread_io_bytes_total() -> Result<IoBytes, String> {
    Err("unimplemented".into())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Seek, SeekFrom, Write},
        os::unix::fs::OpenOptionsExt,
        sync::{Arc, Condvar, Mutex},
    };

    use libc::O_DIRECT;
    use rand::Rng;
    use tempfile::TempDir;
    use test::Bencher;

    use super::{
        fetch_io_bytes, flush_io_latency_metrics, get_io_type, init, set_io_type, BPF_CONTEXT,
        MAX_THREAD_IDX,
    };
    use crate::{io_stats::A512, metrics::*, IoType, OpenOptions};

    #[test]
    fn test_biosnoop() {
        init().unwrap();
        // Test cases are running in parallel, while they depend on the same global
        // variables. To make them not affect each other, run them in sequence.
        test_thread_idx_allocation();
        test_io_context();
        unsafe {
            BPF_CONTEXT.take();
        }
    }

    fn test_io_context() {
        set_io_type(IoType::Compaction);
        assert_eq!(get_io_type(), IoType::Compaction);
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("test_io_context");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut w = Box::new(A512([0u8; 512 * 2]));
        w.0[512] = 42;
        let mut compaction_bytes_before = fetch_io_bytes()[IoType::Compaction as usize];
        f.write(&w.0).unwrap();
        f.sync_all().unwrap();
        let compaction_bytes = fetch_io_bytes()[IoType::Compaction as usize];
        assert_ne!((compaction_bytes - compaction_bytes_before).write, 0);
        assert_eq!((compaction_bytes - compaction_bytes_before).read, 0);
        compaction_bytes_before = compaction_bytes;
        drop(f);

        let other_bytes_before = fetch_io_bytes()[IoType::Other as usize];
        std::thread::spawn(move || {
            set_io_type(IoType::Other);
            let mut f = OpenOptions::new()
                .read(true)
                .custom_flags(O_DIRECT)
                .open(&file_path)
                .unwrap();
            let mut r = Box::new(A512([0u8; 512 * 2]));
            assert_ne!(f.read(&mut r.0).unwrap(), 0);
            drop(f);
        })
        .join()
        .unwrap();

        let compaction_bytes = fetch_io_bytes()[IoType::Compaction as usize];
        let other_bytes = fetch_io_bytes()[IoType::Other as usize];
        assert_eq!((compaction_bytes - compaction_bytes_before).write, 0);
        assert_eq!((compaction_bytes - compaction_bytes_before).read, 0);
        assert_eq!((other_bytes - other_bytes_before).write, 0);
        assert_ne!((other_bytes - other_bytes_before).read, 0);

        flush_io_latency_metrics();
        assert_ne!(IO_LATENCY_MICROS_VEC.compaction.write.get_sample_count(), 0);
        assert_ne!(IO_LATENCY_MICROS_VEC.other.read.get_sample_count(), 0);
    }

    fn test_thread_idx_allocation() {
        // the thread indexes should be recycled.
        for _ in 1..=MAX_THREAD_IDX * 2 {
            std::thread::spawn(|| {
                set_io_type(IoType::Other);
            })
            .join()
            .unwrap();
        }

        // use up all available thread index.
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let mut handles = Vec::new();
        for _ in 1..=MAX_THREAD_IDX {
            let pair1 = pair.clone();
            let h = std::thread::spawn(move || {
                set_io_type(IoType::Compaction);
                let (lock, cvar) = &*pair1;
                let mut stop = lock.lock().unwrap();
                while !*stop {
                    stop = cvar.wait(stop).unwrap();
                }
            });
            handles.push(h);
        }

        // the reserved index is used, io type should be IoType::Other
        for _ in 1..=MAX_THREAD_IDX {
            std::thread::spawn(|| {
                set_io_type(IoType::Compaction);
                assert_eq!(get_io_type(), IoType::Other);
            })
            .join()
            .unwrap();
        }

        {
            let (lock, cvar) = &*pair;
            let mut stop = lock.lock().unwrap();
            *stop = true;
            cvar.notify_all();
        }

        for h in handles {
            h.join().unwrap();
        }

        // the thread indexes should be available again.
        for _ in 1..=MAX_THREAD_IDX {
            std::thread::spawn(|| {
                set_io_type(IoType::Compaction);
                assert_eq!(get_io_type(), IoType::Compaction);
            })
            .join()
            .unwrap();
        }
    }

    #[bench]
    #[ignore]
    fn bench_write_enable_io_snoop(b: &mut Bencher) {
        init().unwrap();
        bench_write(b);
    }

    #[bench]
    #[ignore]
    fn bench_write_disable_io_snoop(b: &mut Bencher) {
        unsafe { BPF_CONTEXT = None };
        bench_write(b);
    }

    #[bench]
    #[ignore]
    fn bench_read_enable_io_snoop(b: &mut Bencher) {
        init().unwrap();
        bench_read(b);
    }

    #[bench]
    #[ignore]
    fn bench_read_disable_io_snoop(b: &mut Bencher) {
        unsafe { BPF_CONTEXT = None };
        bench_read(b);
    }

    #[bench]
    #[ignore]
    fn bench_flush_io_latency_metrics(b: &mut Bencher) {
        init().unwrap();
        set_io_type(IoType::ForegroundWrite);

        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("bench_flush_io_latency_metrics");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = Box::new(A512([0u8; 512 * 1]));
        w.0[64] = 42;
        for _ in 1..=100 {
            f.write(&w.0).unwrap();
        }
        f.sync_all().unwrap();

        b.iter(|| {
            flush_io_latency_metrics();
        });
    }

    fn bench_write(b: &mut Bencher) {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("bench_write_io_snoop");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = Box::new(A512([0u8; 512 * 1]));
        w.0[64] = 42;

        b.iter(|| {
            set_io_type(IoType::ForegroundWrite);
            f.write(&w.0).unwrap();
            f.sync_all().unwrap();
        });
    }

    fn bench_read(b: &mut Bencher) {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("bench_read_io_snoop");
        let mut f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = Box::new(A512([0u8; 512 * 2]));
        w.0[64] = 42;
        for _ in 0..100 {
            f.write(&w.0).unwrap();
        }
        f.sync_all().unwrap();
        drop(f);

        let mut rng = rand::thread_rng();
        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut r = Box::new(A512([0u8; 512 * 2]));
        b.iter(|| {
            set_io_type(IoType::ForegroundRead);
            f.seek(SeekFrom::Start(rng.gen_range(0..100) * 512))
                .unwrap();
            assert_ne!(f.read(&mut r.0).unwrap(), 0);
        });
    }
}
