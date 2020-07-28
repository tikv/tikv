// The implementation of this crate when jemalloc is turned on

use crate::AllocStats;
use jemalloc_ctl::{stats, Epoch as JeEpoch};
use jemallocator::ffi::malloc_stats_print;
use libc::{self, c_char, c_void};
use std::collections::HashMap;
use std::io::Write;
use std::thread::ThreadId;
use std::{io, ptr, slice, sync::Mutex, thread};

pub type Allocator = jemallocator::Jemalloc;
pub const fn allocator() -> Allocator {
    jemallocator::Jemalloc
}
lazy_static! {
    static ref THREAD_MEMORY_MAP: Mutex<HashMap<ThreadId, MemoryStatsAccessor>> =
        Mutex::new(HashMap::new());
}

struct MemoryStatsAccessor {
    // Hack: thread local is expected to be accessed in the same thread. Hence
    // should not be Send. We violate the assumption by send the pointer to
    // other thread.
    // > When the address-of operator is applied to a thread-local variable,
    // > it is evaluated at run time and returns the address of the current
    // > thread’s instance of that variable. An address so obtained may be used
    // > by any thread. When a thread terminates, any pointers to thread-local
    // > variables in that thread become invalid.
    // Referenced from https://gcc.gnu.org/onlinedocs/gcc/Thread-Local.html.
    // However it's not guaranteed to work the same way in rust or llvm backend.
    allocatedp: usize,
    deallocatedp: usize,
    arena: u32,
    thread_name: String,
}

impl MemoryStatsAccessor {
    fn allocated(&self) -> u64 {
        unsafe { *(self.allocatedp as *const u64) }
    }

    fn deallocated(&self) -> u64 {
        unsafe { *(self.deallocatedp as *const u64) }
    }
}

pub fn add_thread_memory_accessor() {
    let mut thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    let thread_name = thread::current().name().unwrap_or("unknown").to_string();
    let mut allocatedp = 0;
    let mut deallocatedp = 0;
    let mut arena = 0;
    unsafe {
        if let Err(e) = jemallocator::mallctl_fetch(b"thread.allocatedp\0", &mut allocatedp) {
            eprintln!("failed to get thread.allocatedp for {}: {}", thread_name, e);
            return;
        }
        if let Err(e) = jemallocator::mallctl_fetch(b"thread.deallocatedp\0", &mut deallocatedp) {
            eprintln!(
                "failed to get thread.deallocatedp for {}: {}",
                thread_name, e
            );
            return;
        }
        if let Err(e) = jemallocator::mallctl_fetch(b"thread.arena\0", &mut arena) {
            eprintln!("failed to get thread.arena for {}: {}", thread_name, e);
            return;
        }
    }
    thread_memory_map.insert(
        thread::current().id(),
        MemoryStatsAccessor {
            allocatedp,
            deallocatedp,
            thread_name,
            arena,
        },
    );
}

pub fn remove_thread_memory_accessor() {
    let mut thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    thread_memory_map.remove(&thread::current().id());
}

pub use self::profiling::dump_prof;

pub fn dump_stats() -> String {
    let mut buf = Vec::with_capacity(1024);
    unsafe {
        malloc_stats_print(
            write_cb,
            &mut buf as *mut Vec<u8> as *mut c_void,
            ptr::null(),
        );
    }
    writeln!(buf, "Memory stats by thread:").unwrap();
    writeln!(buf, "Allocated\tDeallocated\tArena\tThread").unwrap();
    let thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    for accessor in thread_memory_map.values() {
        let allocated = accessor.allocated();
        let deallocated = accessor.deallocated();
        writeln!(
            buf,
            "{}\t{}\t{}\t{}",
            allocated, deallocated, accessor.arena, accessor.thread_name
        )
        .unwrap();
    }
    String::from_utf8(buf).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

pub fn fetch_stats() -> io::Result<Option<AllocStats>> {
    // Stats are cached. Need to advance epoch to refresh.
    JeEpoch::new()?.advance()?;

    Ok(Some(vec![
        ("allocated", stats::allocated()?),
        ("active", stats::active()?),
        ("metadata", stats::metadata()?),
        ("resident", stats::resident()?),
        ("mapped", stats::mapped()?),
        ("retained", stats::retained()?),
    ]))
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

#[cfg(feature = "mem-profiling")]
mod profiling {
    use std::ffi::CString;
    use std::{env, ptr};

    use jemallocator;
    use libc::c_char;

    // C string should end with a '\0'.
    const PROF_ACTIVE: &'static [u8] = b"prof.active\0";
    const PROF_DUMP: &'static [u8] = b"prof.dump\0";

    struct DumpPathGuard(Option<Vec<u8>>);

    impl DumpPathGuard {
        fn from_cstring(s: Option<CString>) -> DumpPathGuard {
            DumpPathGuard(s.map(|s| s.into_bytes_with_nul()))
        }

        /// caller should ensure that the pointer should not be accessed after
        /// the guard is dropped.
        #[inline]
        unsafe fn get_mut_ptr(&mut self) -> *mut c_char {
            self.0
                .as_mut()
                .map_or(ptr::null_mut(), |v| v.as_mut_ptr() as *mut c_char)
        }
    }

    /// Dump the profile to the `path`.
    ///
    /// If `path` is `None`, will dump it in the working directory with an auto-generated name.
    pub fn dump_prof(path: Option<&str>) {
        unsafe {
            // First set the `prof.active` value in case profiling is deactivated
            // as with MALLOC_CONF="opt.prof:true,opt.prof_active:false"
            if let Err(e) = jemallocator::mallctl_set(PROF_ACTIVE, true) {
                error!("failed to activate profiling: {}", e);
                return;
            }
        }
        let mut c_path = DumpPathGuard::from_cstring(path.map(|p| CString::new(p).unwrap()));
        let res = unsafe { jemallocator::mallctl_set(PROF_DUMP, c_path.get_mut_ptr()) };
        match res {
            Err(e) => error!("failed to dump the profile to {:?}: {}", path, e),
            Ok(_) => {
                if let Some(p) = path {
                    info!("dump profile to {}", p);
                    return;
                }

                info!("dump profile to {}", env::current_dir().unwrap().display());
            }
        }
    }

    #[cfg(test)]
    mod tests {
        extern crate tempdir;

        use self::tempdir::TempDir;
        use jemallocator;
        use std::fs;

        const OPT_PROF: &'static [u8] = b"opt.prof\0";

        fn is_profiling_on() -> bool {
            let mut prof = false;
            let res = unsafe { jemallocator::mallctl_fetch(OPT_PROF, &mut prof) };
            match res {
                Err(e) => {
                    // Shouldn't be possible since mem-profiling is set
                    panic!("is_profiling_on: {:?}", e);
                }
                Ok(_) => prof,
            }
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
        #[ignore]
        fn test_profiling_memory() {
            // Make sure somebody has turned on profiling
            assert!(is_profiling_on(), r#"Set MALLOC_CONF="prof:true""#);

            let dir = TempDir::new("test_profiling_memory").unwrap();

            let os_path = dir.path().to_path_buf().join("test1.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(Some(&path));

            let os_path = dir.path().to_path_buf().join("test2.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(Some(&path));

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
            assert!(prof_count == 2);
        }
    }
}

#[cfg(not(feature = "mem-profiling"))]
mod profiling {
    pub fn dump_prof(_path: Option<&str>) {}
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn dump_stats() {
        assert_ne!(super::dump_stats().len(), 0);
    }

    #[test]
    fn test_aggr_stats() {
        let (tx1, rx1) = mpsc::channel();
        let (_tx2, rx2) = mpsc::channel::<()>();
        let j = thread::spawn(move || {
            super::add_thread_memory_accessor();
            drop(Vec::<usize>::with_capacity(10200));
            let _ = tx1.send(());
            let _ = rx2.recv();
            super::remove_thread_memory_accessor();
        });
        let id = j.thread().id();
        rx1.recv().unwrap();
        let thread_memory_map = super::THREAD_MEMORY_MAP.lock().unwrap();
        let accessor = thread_memory_map.get(&id).unwrap();
        assert!(accessor.allocated() >= 10200);
        assert!(accessor.deallocated() >= 10200);
        // Arena is about multiple times of core count.
        assert!(accessor.arena < 10200);
        assert_eq!(accessor.thread_name, "unknown");
    }
}
