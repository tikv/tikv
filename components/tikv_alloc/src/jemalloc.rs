// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// The implementation of this crate when jemalloc is turned on

use std::{collections::HashMap, ptr, slice, sync::Mutex, thread};

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
}

struct MemoryStatsAccessor {
    // TODO: trace arena, allocated, deallocated. Original implement doesn't
    // work actually.
    thread_name: String,
}

pub fn add_thread_memory_accessor() {
    let mut thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    thread_memory_map.insert(
        thread::current().id(),
        MemoryStatsAccessor {
            thread_name: thread::current().name().unwrap().to_string(),
        },
    );
}

pub fn remove_thread_memory_accessor() {
    let mut thread_memory_map = THREAD_MEMORY_MAP.lock().unwrap();
    thread_memory_map.remove(&thread::current().id());
}

use std::thread::ThreadId;

pub use self::profiling::{activate_prof, deactivate_prof, dump_prof};

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
        memory_stats.push_str(format!("Thread [{}]: \n", accessor.thread_name).as_str());
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

    #[test]
    fn dump_stats() {
        assert_ne!(super::dump_stats().len(), 0);
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

    #[cfg(test)]
    mod tests {
        use std::fs;

        use tempfile::Builder;

        const OPT_PROF: &[u8] = b"opt.prof\0";

        fn is_profiling_on() -> bool {
            match unsafe { tikv_jemalloc_ctl::raw::read(OPT_PROF) } {
                Err(e) => {
                    // Shouldn't be possible since mem-profiling is set
                    panic!("is_profiling_on: {:?}", e);
                }
                Ok(prof) => prof,
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
        #[ignore = "#ifdef MALLOC_CONF"]
        fn test_profiling_memory_ifdef_malloc_conf() {
            // Make sure somebody has turned on profiling
            assert!(is_profiling_on(), "set MALLOC_CONF=prof:true");

            let dir = Builder::new()
                .prefix("test_profiling_memory")
                .tempdir()
                .unwrap();

            let os_path = dir.path().to_path_buf().join("test1.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(&path).unwrap();

            let os_path = dir.path().to_path_buf().join("test2.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(&path).unwrap();

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
}
