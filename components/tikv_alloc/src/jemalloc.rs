// The implementation of this crate when jemalloc is turned on

use super::error::{ProfError, ProfResult};
use crate::AllocStats;
use jemalloc_ctl::{stats, Epoch as JeEpoch};
use jemallocator::ffi::malloc_stats_print;
use libc::{self, c_char, c_void};
use std::{io, ptr, slice};

pub type Allocator = jemallocator::Jemalloc;
pub const fn allocator() -> Allocator {
    jemallocator::Jemalloc
}

pub use self::profiling::{activate_prof, deactivate_prof, dump_prof};

pub fn dump_stats() -> String {
    let mut buf = Vec::with_capacity(1024);
    unsafe {
        malloc_stats_print(
            write_cb,
            &mut buf as *mut Vec<u8> as *mut c_void,
            ptr::null(),
        )
    }
    String::from_utf8_lossy(&buf).into_owned()
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
    use std::ptr;

    use jemallocator;
    use libc::c_char;

    use super::{ProfError, ProfResult};

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

    pub fn activate_prof() -> ProfResult<()> {
        info!("start profiler");
        unsafe {
            if let Err(e) = jemallocator::mallctl_set(PROF_ACTIVE, true) {
                error!("failed to activate profiling: {}", e);
                return Err(ProfError::JemallocError(e));
            }
        }
        Ok(())
    }

    pub fn deactivate_prof() -> ProfResult<()> {
        info!("stop profiler");
        unsafe {
            if let Err(e) = jemallocator::mallctl_set(PROF_ACTIVE, false) {
                error!("failed to deactivate profiling: {}", e);
                return Err(ProfError::JemallocError(e));
            }
        }
        Ok(())
    }

    /// Dump the profile to the `path`.
    ///
    /// If `path` is `None`, will dump it in the working directory with an auto-generated name.
    pub fn dump_prof(path: &str) -> ProfResult<()> {
        // TODO: return errors in this function
        let mut c_path = DumpPathGuard::from_cstring(Some(CString::new(path).unwrap()));
        let res = unsafe { jemallocator::mallctl_set(PROF_DUMP, c_path.get_mut_ptr()) };
        match res {
            Err(e) => {
                error!("failed to dump the profile to {:?}: {}", path, e);
                Err(ProfError::JemallocError(e))
            }
            Ok(_) => {
                info!("dump profile to {}", path);
                Ok(())
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use jemallocator;
        use std::fs;
        use tempfile::Builder;

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

            let dir = Builder::new()
                .prefix("test_profiling_memory")
                .tempdir()
                .unwrap();

            let os_path = dir.path().to_path_buf().join("test1.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(&path);

            let os_path = dir.path().to_path_buf().join("test2.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(&path);

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
