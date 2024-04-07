// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// The implementation of this crate when jemalloc is turned on

use std::{
    collections::HashMap,
    ptr::{self, NonNull},
    slice,
    sync::Mutex,
    thread,
};

use libc::{self, c_char, c_void};
use tikv_jemalloc_ctl::{epoch, stats, Error};
use tikv_jemalloc_sys::malloc_stats_print;

use super::error::{ProfError, ProfResult};
use crate::AllocStats;

pub type Allocator = tikv_jemallocator::Jemalloc;
pub const fn allocator() -> Allocator {
    tikv_jemallocator::Jemalloc
}

lazy_static! {
    static ref THREAD_MEMORY_MAP: Mutex<HashMap<ThreadId, MemoryStatsAccessor>> =
        Mutex::new(HashMap::new());
    // thread id -> (thread name, arena index)
    static ref THREAD_ARENA_MAP: Mutex<HashMap<ThreadId, (String, usize)>> = Mutex::new(HashMap::new());
}

/// The struct for tracing the statistic of another thread.
/// The target pointer should be bound to some TLS of another thread, this
/// structure is just "peeking" it -- with out modifying.
// It should be covariant so we wrap it with `NonNull`.
#[repr(transparent)]
struct PeekableRemoteStat<T>(Option<NonNull<T>>);

// SAFETY: all constructors of `PeekableRemoteStat` returns pointer points to a
// thread local variable. Once this be sent, a reasonable life time of this
// variable should be as long as the thread holding the underlying thread local
// variable. But it is impossible to express such lifetime in current Rust.
// Then it is the user's responsibility to trace that lifetime.
unsafe impl<T: Send> Send for PeekableRemoteStat<T> {}

impl<T: Copy> PeekableRemoteStat<T> {
    /// Try access the underlying data. When the pointer is `nullptr`, returns
    /// `None`.
    ///
    /// # Safety
    ///
    /// The pointer should not be dangling. (i.e. the thread to be traced should
    /// be accessible.)
    unsafe fn peek(&self) -> Option<T> {
        self.0
            .map(|nlp| unsafe { core::intrinsics::atomic_load_seqcst(nlp.as_ptr()) })
    }

    fn from_raw(ptr: *mut T) -> Self {
        Self(NonNull::new(ptr))
    }
}

impl PeekableRemoteStat<u64> {
    fn allocated() -> Self {
        // SAFETY: it is transparent.
        // NOTE: perhaps we'd better add something like `as_raw()` for `ThreadLocal`...
        Self::from_raw(
            tikv_jemalloc_ctl::thread::allocatedp::read()
                .map(|x| unsafe { std::mem::transmute(x) })
                .unwrap_or(std::ptr::null_mut()),
        )
    }

    fn deallocated() -> Self {
        // SAFETY: it is transparent.
        Self::from_raw(
            tikv_jemalloc_ctl::thread::deallocatedp::read()
                .map(|x| unsafe { std::mem::transmute(x) })
                .unwrap_or(std::ptr::null_mut()),
        )
    }
}

struct MemoryStatsAccessor {
    allocated: PeekableRemoteStat<u64>,
    deallocated: PeekableRemoteStat<u64>,
    thread_name: String,
}

impl MemoryStatsAccessor {
    fn get_allocated(&self) -> u64 {
        // SAFETY: `add_thread_memory_accessor` is unsafe, and that is the only way for
        // outer crates to create this.
        unsafe { self.allocated.peek().unwrap_or_default() }
    }

    fn get_deallocated(&self) -> u64 {
        // SAFETY: `add_thread_memory_accessor` is unsafe, and that is the only way for
        // outer crates to create this.
        unsafe { self.deallocated.peek().unwrap_or_default() }
    }
}

/// Register the current thread to the collector that collects the jemalloc
/// allocation / deallocation info.
///
/// Generally you should call this via `spawn_wrapper`s instead of invoke this
/// directly. The former is a safe function.
///
/// # Safety
///
/// Make sure the `remove_thread_memory_accessor` is called before the thread
/// exits.
pub unsafe fn add_thread_memory_accessor() {
    let mut thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    thread_memory_map
        .entry(thread::current().id())
        .or_insert_with(|| {
            let allocated = PeekableRemoteStat::allocated();
            let deallocated = PeekableRemoteStat::deallocated();

            MemoryStatsAccessor {
                thread_name: thread::current().name().unwrap_or("<unknown>").to_string(),
                allocated,
                deallocated,
            }
        });
}

pub fn remove_thread_memory_accessor() {
    let mut thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    thread_memory_map.remove(&thread::current().id());
}

use std::thread::ThreadId;

pub use self::profiling::*;

pub fn dump_stats() -> String {
    let mut buf = Vec::with_capacity(1024);

    unsafe {
        malloc_stats_print(
            Some(write_cb),
            &mut buf as *mut Vec<u8> as *mut c_void,
            ptr::null(),
        );
    }
    let mut memory_stats = format!(
        "Memory stats summary: {}\n",
        String::from_utf8_lossy(&buf).into_owned()
    );
    memory_stats.push_str("Memory stats by thread:\n");

    let thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    for (_, accessor) in thread_memory_map.iter() {
        let alloc = accessor.get_allocated();
        let dealloc = accessor.get_deallocated();
        memory_stats.push_str(
            format!(
                "Thread [{}]: alloc_bytes={alloc},dealloc_bytes={dealloc}\n",
                accessor.thread_name
            )
            .as_str(),
        );
    }
    memory_stats
}

pub fn fetch_stats() -> Result<Option<AllocStats>, Error> {
    // Stats are cached. Need to advance epoch to refresh.
    epoch::advance()?;

    Ok(Some(vec![
        ("allocated", stats::allocated::read()?),
        ("active", stats::active::read()?),
        ("metadata", stats::metadata::read()?),
        ("resident", stats::resident::read()?),
        ("mapped", stats::mapped::read()?),
        ("retained", stats::retained::read()?),
        (
            "dirty",
            stats::resident::read()? - stats::active::read()? - stats::metadata::read()?,
        ),
        (
            "fragmentation",
            stats::active::read()? - stats::allocated::read()?,
        ),
    ]))
}

/// remove the postfix of threads generated by the YATP (-*).
/// YATP will append the id of the threads in a thread pool, which will bring
/// too many labels to the metric (and usually the memory usage should be evenly
/// distributed among these threads).
/// Fine-grained memory statistic is still available in the interface provided
/// for `tikv-ctl`.
fn trim_yatp_suffix(s: &str) -> &str {
    s.trim_end_matches(|c: char| c.is_ascii_digit() || c == '-')
}

/// Iterate over the allocation stat.
/// Format of the callback: `(name, allocated, deallocated)`.
pub fn iterate_thread_allocation_stats(mut f: impl FnMut(&str, u64, u64)) {
    // Given we have called `epoch::advance()` in `fetch_stats`, we (magically!)
    // skip advancing the epoch here.
    let thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    let mut collected = HashMap::<&str, (u64, u64)>::with_capacity(thread_memory_map.len());
    for (_, accessor) in thread_memory_map.iter() {
        let ent = collected
            .entry(trim_yatp_suffix(&accessor.thread_name))
            .or_default();
        ent.0 += accessor.get_allocated();
        ent.1 += accessor.get_deallocated();
    }
    for (name, val) in collected {
        f(name, val.0, val.1)
    }
}

/// Iterate over the allocation stat.
/// Format of the callback: `(name, allocated, deallocated)`.
pub fn iterate_arena_allocation_stats(mut f: impl FnMut(&str, u64, u64, u64)) {
    // Given we have called `epoch::advance()` in `fetch_stats`, we (magically!)
    // skip advancing the epoch here.
    let thread_arena_map = THREAD_ARENA_MAP.lock().unwrap();
    let mut collected = HashMap::<&str, (u64, u64, u64)>::with_capacity(thread_arena_map.len());
    for (_, (name, index)) in thread_arena_map.iter() {
        let stats = fetch_arena_stats(*index);
        let ent = collected.entry(trim_yatp_suffix(name)).or_default();
        ent.0 += stats.0;
        ent.1 += stats.1;
        ent.2 += stats.2;
    }
    for (name, val) in collected {
        f(name, val.0, val.1, val.2)
    }
}

#[allow(clippy::cast_ptr_alignment)]
extern "C" fn write_cb(printer: *mut c_void, msg: *const c_char) {
    unsafe {
        // This cast from *c_void to *Vec<u8> looks like a bad
        // cast to clippy due to pointer alignment, but we know
        // what type the pointer is.
        let buf = &mut *(printer as *mut Vec<u8>);
        let len = libc::strlen(msg);
        let bytes = slice::from_raw_parts(msg as *const u8, len);
        buf.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        add_thread_memory_accessor, imp::THREAD_MEMORY_MAP, remove_thread_memory_accessor,
    };

    fn assert_delta(name: impl std::fmt::Display, delta: f64, a: u64, b: u64) {
        let (base, diff) = if a > b { (a, a - b) } else { (b, b - a) };
        let error = diff as f64 / base as f64;
        assert!(
            error < delta,
            "{name}: the error is too huge: a={a}, b={b}, base={base}, diff={diff}, error={error}"
        );
    }
    #[test]
    fn dump_stats() {
        assert_ne!(super::dump_stats().len(), 0);
    }

    #[test]
    fn test_allocation_stat() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut threads = vec![];
        for i in 1..6 {
            let tx = tx.clone();
            // It is in test... let skip calling hooks.
            #[allow(clippy::disallowed_methods)]
            let hnd = std::thread::Builder::new()
                .name(format!("test_allocation_stat_{i}"))
                .spawn(move || {
                    if i == 5 {
                        return;
                    }
                    // SAFETY: we call `remove_thread_memory_accessor` below.
                    unsafe {
                        add_thread_memory_accessor();
                    }
                    let (tx2, rx2) = std::sync::mpsc::channel::<()>();
                    let v = vec![42u8; 1024 * 1024 * i];
                    drop(v);
                    let _v2 = vec![42u8; 512 * 1024 * i];
                    tx.send((i, std::thread::current().id(), tx2)).unwrap();
                    drop(tx);
                    rx2.recv().unwrap();
                    remove_thread_memory_accessor();
                })
                .unwrap();
            threads.push(hnd);
        }
        drop(tx);

        let chs = rx.into_iter().collect::<Vec<_>>();
        let l = THREAD_MEMORY_MAP.lock().unwrap();
        for (i, tid, tx) in chs {
            let a = l.get(&tid).unwrap();
            unsafe {
                let alloc = a.allocated.peek().unwrap();
                let dealloc = a.deallocated.peek().unwrap();
                assert_delta(i, 0.05, alloc, (1024 + 512) * 1024 * i as u64);
                assert_delta(i, 0.05, dealloc, (1024) * 1024 * i as u64);
            }
            tx.send(()).unwrap();
        }
        drop(l);
        for th in threads.into_iter() {
            th.join().unwrap();
        }
    }
}

#[cfg(feature = "mem-profiling")]
mod profiling {
    use std::ffi::CString;

    use libc::c_char;

    use super::{ProfError, ProfResult};

    // C string should end with a '\0'.
    const PROF_ACTIVE: &[u8] = b"prof.active\0";
    const PROF_DUMP: &[u8] = b"prof.dump\0";
    const PROF_RESET: &[u8] = b"prof.reset\0";
    const PROF_SAMPLE: &[u8] = b"prof.lg_sample\0";
    const OPT_PROF: &[u8] = b"opt.prof\0";
    const ARENAS_CREATE: &[u8] = b"arenas.create\0";
    const THREAD_ARENA: &[u8] = b"thread.arena\0";

    // Set exclusive arena for the current thread to avoid contention.
    pub fn thread_allocate_exclusive_arena() -> ProfResult<()> {
        unsafe {
            let mut index: u32 = tikv_jemalloc_ctl::raw::read(THREAD_ARENA).map_err(|e| {
                ProfError::JemallocError(format!("failed to get thread's arena: {}", e))
            })?;
            let count: usize = tikv_jemalloc_ctl::raw::read(
                format!("stats.arenas.{}.nthreads\0", index).as_bytes(),
            )
            .unwrap_or(0);
            // If the arena has already been bind to the other thread, create a new arena.
            if count >= 1 {
                index = tikv_jemalloc_ctl::raw::read(ARENAS_CREATE).map_err(|e| {
                    ProfError::JemallocError(format!("failed to create arena: {}", e))
                })?;
                if let Err(e) = tikv_jemalloc_ctl::raw::write(THREAD_ARENA, index) {
                    return Err(ProfError::JemallocError(format!(
                        "failed to set thread's arena: {}",
                        e
                    )));
                }
            }
            super::THREAD_ARENA_MAP.lock().unwrap().insert(
                std::thread::current().id(),
                (
                    std::thread::current()
                        .name()
                        .unwrap_or("unknown")
                        .to_string(),
                    index as usize,
                ),
            );
        }
        Ok(())
    }

    pub fn fetch_arena_stats(index: usize) -> (u64, u64, u64) {
        let resident = unsafe {
            tikv_jemalloc_ctl::raw::read(format!("stats.arenas.{}.resident\0", index).as_bytes())
                .unwrap_or(0)
        };
        let mapped = unsafe {
            tikv_jemalloc_ctl::raw::read(format!("stats.arenas.{}.mapped\0", index).as_bytes())
                .unwrap_or(0)
        };
        let retained = unsafe {
            tikv_jemalloc_ctl::raw::read(format!("stats.arenas.{}.retained\0", index).as_bytes())
                .unwrap_or(0)
        };
        (resident, mapped, retained)
    }

    pub fn set_prof_sample(rate: u64) -> ProfResult<()> {
        let rate = (rate as f64).log2().ceil() as u64;
        unsafe {
            if let Err(e) = tikv_jemalloc_ctl::raw::write(PROF_RESET, rate) {
                return Err(ProfError::JemallocError(format!(
                    "failed to set prof sample: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    pub fn get_prof_sample() -> u64 {
        let rate: u64 = match unsafe { tikv_jemalloc_ctl::raw::read(PROF_SAMPLE) } {
            Err(e) => {
                panic!("get_prof_sample: {:?}", e);
            }
            Ok(prof) => prof,
        };
        2_u64.pow(rate as u32)
    }

    pub fn activate_prof() -> ProfResult<()> {
        unsafe {
            if let Err(e) = tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, true) {
                return Err(ProfError::JemallocError(format!(
                    "failed to activate profiling: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    pub fn deactivate_prof() -> ProfResult<()> {
        unsafe {
            if let Err(e) = tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, false) {
                return Err(ProfError::JemallocError(format!(
                    "failed to deactivate profiling: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    /// Dump the profile to the `path`.
    pub fn dump_prof(path: &str) -> ProfResult<()> {
        let mut bytes = CString::new(path)?.into_bytes_with_nul();
        let ptr = bytes.as_mut_ptr() as *mut c_char;
        unsafe {
            if let Err(e) = tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr) {
                return Err(ProfError::JemallocError(format!(
                    "failed to dump the profile to {:?}: {}",
                    path, e
                )));
            }
        }
        Ok(())
    }

    pub fn is_profiling_active() -> bool {
        match unsafe { tikv_jemalloc_ctl::raw::read(PROF_ACTIVE) } {
            Err(e) => {
                panic!("is_profiling_active: {:?}", e);
            }
            Ok(prof) => prof,
        }
    }

    pub fn is_profiling_enabled() -> bool {
        match unsafe { tikv_jemalloc_ctl::raw::read(OPT_PROF) } {
            Err(e) => {
                // Shouldn't be possible since mem-profiling is set
                panic!("is_profiling_enabled: {:?}", e);
            }
            Ok(prof) => prof,
        }
    }

    #[cfg(test)]
    mod tests {
        use std::fs;

        use tempfile::Builder;

        use super::*;

        #[test]
        #[ignore = "#ifdef MALLOC_CONF"]
        fn test_profiling_active() {
            // Make sure somebody has turned on profiling
            assert!(is_profiling_enabled(), "set MALLOC_CONF=prof:true");
            activate_prof().unwrap();
            assert!(is_profiling_active());
            deactivate_prof().unwrap();
            assert!(!is_profiling_active());

            super::set_prof_sample(256 * 1024).unwrap();
            assert_eq!(256 * 1024, super::get_prof_sample());
        }

        // Only trigger this test with jemallocs `opt.prof` set to
        // true ala `MALLOC_CONF="prof:true"`. It can be run by
        // passing `-- --ignored` to `cargo test -p tikv_alloc`.
        //
        // TODO: could probably unignore this by running a second
        // copy of the executable with MALLOC_CONF set.
        //
        // TODO: need a test for the dump_prof(None) case, but
        // the cleanup afterward is not simple.
        #[test]
        #[ignore = "#ifdef MALLOC_CONF"]
        fn test_profiling_memory_ifdef_malloc_conf() {
            // Make sure somebody has turned on profiling
            assert!(is_profiling_enabled(), "set MALLOC_CONF=prof:true");

            let dir = Builder::new()
                .prefix("test_profiling_memory")
                .tempdir()
                .unwrap();

            let os_path = dir.path().to_path_buf().join("test1.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            dump_prof(&path).unwrap();

            let os_path = dir.path().to_path_buf().join("test2.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            dump_prof(&path).unwrap();

            let files = fs::read_dir(dir.path()).unwrap().count();
            assert_eq!(files, 2);

            // Find the created files and check properties that
            // indicate they contain something interesting
            let mut prof_count = 0;
            for file_entry in fs::read_dir(dir.path()).unwrap() {
                let file_entry = file_entry.unwrap();
                let path = file_entry.path().to_str().unwrap().to_owned();
                if path.contains("test1.dump") || path.contains("test2.dump") {
                    let metadata = file_entry.metadata().unwrap();
                    let file_len = metadata.len();
                    assert!(file_len > 10); // arbitrary number
                    prof_count += 1
                }
            }
            assert_eq!(prof_count, 2);
        }
    }
}

#[cfg(not(feature = "mem-profiling"))]
mod profiling {
    use super::{ProfError, ProfResult};

    pub fn dump_prof(_path: &str) -> ProfResult<()> {
        Err(ProfError::MemProfilingNotEnabled)
    }
    pub fn activate_prof() -> ProfResult<()> {
        Err(ProfError::MemProfilingNotEnabled)
    }
    pub fn deactivate_prof() -> ProfResult<()> {
        Err(ProfError::MemProfilingNotEnabled)
    }
    pub fn set_prof_sample(_rate: u64) -> ProfResult<()> {
        Err(ProfError::MemProfilingNotEnabled)
    }
    pub fn is_profiling_active() -> bool {
        false
    }
    pub fn thread_allocate_exclusive_arena() -> ProfResult<()> {
        Ok(())
    }
    pub fn fetch_arena_stats(_index: usize) -> (u64, u64, u64) {
        (0, 0, 0)
    }
}
