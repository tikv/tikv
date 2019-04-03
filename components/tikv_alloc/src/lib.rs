// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
//! This crate controls the global allocator used by TiKV.
//!
//! As of now TiKV always turns on jemalloc on Unix, though libraries
//! generally shouldn't be opinionated about their allocators like
//! this. It's easier to do this in one place than to have all our
//! bins turn it on themselves.
//!
//! Writing `extern crate tikv_alloc;` will link it to jemalloc when
//! appropriate. The TiKV library itself links to `tikv_alloc` to
//! ensure that any binary linking to it will use jemalloc.
//!
//! With few exceptions, _every binary and project in the TiKV workspace
//! should link (perhaps transitively) to tikv_alloc_. This is to ensure
//! that tests and benchmarks run with the production allocator. In other
//! words, binaries and projects that don't link to `tikv` should link
//! to `tikv_alloc`.
//!
//! At present all Unixes use jemalloc, and others don't. Whichever
//! allocator is used, this crate presents the same API, and some
//! profiling functions become no-ops. Note however that _not all
//! platforms override C malloc, including macOS_. This means on some
//! platforms RocksDB is using the system malloc. On Linux C malloc is
//! redirected to jemalloc.
//!
//! This crate accepts two cargo features:
//!
//! - mem-profiling - compiles jemalloc and this crate with profiling
//!   capability
//!
//! - no-jemalloc - compiles without jemalloc, such that TiKV
//!   will use the system allocator
//!
//! cfg `fuzzing` is defined by `run_libfuzzer` in `fuzz/cli.rs` and
//! is passed to rustc directly with `--cfg`; in other words it's not
//! controlled through a crate feature.
//!
//! Ideally there should be no jemalloc-specific code outside this
//! crate.
//!
//! # Profiling
//!
//! Profiling with jemalloc requires both build-time and run-time
//! configuration. At build time cargo needs the `--mem-profiling`
//! feature, and at run-time jemalloc needs to set the `opt.prof`
//! option to true, ala `MALLOC_CONF="opt.prof:true".
//!
//! In production you might also set `opt.prof_active` to `false` to
//! keep profiling off until there's an incident. Jemalloc has
//! a variety of run-time [profiling options].
//!
//! [profiling options]: http://jemalloc.net/jemalloc.3.html#opt.prof
//!
//! Here's an example of how you might build and run via cargo, with
//! profiling:
//!
//! ```notrust
//! export MALLOC_CONF="prof:true,prof_active:false,prof_prefix:$(pwd)/jeprof"
//! cargo test --features mem-profiling -p tikv_alloc -- --ignored
//! ```
//!
//! (In practice you might write this as a single statement, setting
//! `MALLOC_CONF` only temporarily, e.g. `MALLOC_CONF="..." cargo test
//! ...`).
//!
//! When running cargo while `prof:true`, you will see messages like
//!
//! ```notrust
//! <jemalloc>: Invalid conf pair: prof:true
//! <jemalloc>: Invalid conf pair: prof_active:false
//! ```
//!
//! This is normal - they are being emitting by the jemalloc in cargo
//! and rustc, which are both configured without profiling. TiKV's
//! jemalloc is configured for profiling if you pass
//! `--features=mem-profiling` to cargo for eather `tikv_alloc` or
//! `tikv`.

#[cfg(feature = "mem-profiling")]
#[macro_use]
extern crate log;

// The global allocator, usually.
#[cfg(all(unix, not(fuzzing), not(feature = "no-jemalloc")))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// The global allocator, without jemalloc.
// This is only necessary prior to the removal of jemalloc from
// Rust. Afterwards this might be removed.
#[cfg(not(all(unix, not(fuzzing), not(feature = "no-jemalloc"))))]
#[global_allocator]
static ALLOC: std::alloc::System = std::alloc::System;

pub use self::imp::*;

pub type AllocStats = Vec<(&'static str, usize)>;

// The implementation of this crate when jemalloc is turned on
#[cfg(all(unix, not(fuzzing), not(feature = "no-jemalloc")))]
mod imp {
    use super::AllocStats;
    use jemalloc_ctl::{stats, Epoch as JeEpoch};
    use jemallocator::ffi::malloc_stats_print;
    use libc::{self, c_char, c_void};
    use std::{io, ptr, slice};

    pub use self::profiling::dump_prof;

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
                        panic!("is_profilng_on: {:?}", e);
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
}

#[cfg(not(all(unix, not(fuzzing), not(feature = "no-jemalloc"))))]
mod imp {

    use super::AllocStats;
    use std::io;

    pub fn dump_stats() -> String {
        String::new()
    }
    pub fn dump_prof(_path: Option<&str>) {}

    pub fn fetch_stats() -> io::Result<Option<AllocStats>> {
        Ok(None)
    }
}
