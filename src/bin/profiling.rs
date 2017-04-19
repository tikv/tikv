// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "mem-profiling")]
mod imp {
    use std::ffi::CString;
    use std::{ptr, env};

    use jemallocator;
    use libc::c_char;

    // c string should end with a '\0'.
    const PROFILE_ACTIVE: &'static [u8] = b"prof.active\0";
    const PROFILE_DUMP: &'static [u8] = b"prof.dump\0";

    struct DumpPathGuard(Option<Vec<u8>>);

    impl DumpPathGuard {
        fn from_cstring(s: Option<CString>) -> DumpPathGuard {
            DumpPathGuard(s.map(|s| s.into_bytes_with_nul()))
        }

        /// caller should ensure that the pointer should not be accessed after
        /// the guard is dropped.
        #[inline]
        unsafe fn get_mut_ptr(&mut self) -> *mut c_char {
            self.0.as_mut().map_or(ptr::null_mut(), |v| v.as_mut_ptr() as *mut c_char)
        }
    }

    /// Toggle the prof.active option, return the final value.
    pub fn toggle_prof() -> Result<bool, i32> {
        let mut enabled = false;
        unsafe {
            jemallocator::mallctl_fetch(PROFILE_ACTIVE, &mut enabled).and_then(|_| {
                jemallocator::mallctl_set(PROFILE_ACTIVE, !enabled)
            })?;
        }
        if enabled {
            info!("memory profiling is disabled");
        } else {
            info!("memory profiling is enabled");
        }
        Ok(!enabled)
    }

    /// Dump the profile to the `path`.
    ///
    /// If `path` is `None`, will dump it in the working directory with a auto-generated name.
    pub fn dump_prof(path: Option<&str>) {
        let mut c_path = DumpPathGuard::from_cstring(path.map(|p| CString::new(p).unwrap()));
        let res = unsafe { jemallocator::mallctl_set(PROFILE_DUMP, c_path.get_mut_ptr()) };
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
    mod test {
        use std::fs;

        use tempdir::TempDir;

        // Only trigger this test with prof set to true.
        #[test]
        #[ignore]
        fn test_profiling_memory() {
            let dir = TempDir::new("test_profiling").unwrap();
            let os_path = dir.path().to_path_buf().join("test.dump").into_os_string();
            let path = os_path.into_string().unwrap();

            assert_eq!(super::toggle_prof(), Ok(true));
            super::dump_prof(Some(&path));
            let files = fs::read_dir(dir.path()).unwrap().count();
            assert_eq!(files, 1);
        }
    }
}

#[cfg(not(feature = "mem-profiling"))]
mod imp {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use std::thread::JoinHandle;

    pub fn toggle_prof() -> Result<bool, i32> {
        Ok(false)
    }

    pub fn dump_prof(_: Option<&str>) {}
}

pub use self::imp::*;
