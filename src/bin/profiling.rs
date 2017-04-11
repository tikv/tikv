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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::{self, Builder, JoinHandle};
    use std::time::Duration;
    use std::ffi::CString;
    use std::ptr;

    use jemallocator;
    use libc::c_char;

    // c string should end with a '\0'.
    const PROFILE_ACTIVE: &'static [u8] = b"prof.active\0";
    const PROFILE_DUMP: &'static [u8] = b"prof.dump\0";

    struct DumpFileNameGuard(Option<Vec<u8>>);

    impl DumpFileNameGuard {
        fn from_cstring(s: Option<CString>) -> DumpFileNameGuard {
            DumpFileNameGuard(s.map(|s| s.into_bytes_with_nul()))
        }

        /// caller should ensure that the pointer should not be accessed after
        /// the guard is dropped.
        #[inline]
        unsafe fn get_mut_ptr(&mut self) -> *mut c_char {
            self.0.as_mut().map_or(ptr::null_mut(), |v| v.as_mut_ptr() as *mut c_char)
        }
    }

    #[inline]
    fn get_file_name(base_name: &str, postfix: &str) -> CString {
        CString::new(format!("{}{}", base_name, postfix)).unwrap()
    }

    pub fn profile_memory(file: Option<&str>,
                          flag: &Arc<AtomicBool>,
                          sleep_interval: Duration)
                          -> Option<JoinHandle<()>> {
        if flag.load(Ordering::SeqCst) {
            warn!("last memory profiling has not finished yet.");
            return None;
        }

        unsafe {
            let mut first_file_name =
                DumpFileNameGuard::from_cstring(file.map(|f| get_file_name(f, "-fir")));
            if let Err(e) = jemallocator::mallctl_set(PROFILE_DUMP, first_file_name.get_mut_ptr()) {
                println!("failed to dump the first profile: {}", e);
                return None;
            }
            if let Err(e) = jemallocator::mallctl_set(PROFILE_ACTIVE, true) {
                println!("failed to activate profiling: {}", e);
                return None;
            }
        }
        flag.store(true, Ordering::SeqCst);
        let flag2 = flag.clone();
        let mut second_file_name =
            DumpFileNameGuard::from_cstring(file.map(|f| get_file_name(f, "-sec")));
        Some(Builder::new()
            .name(thd_name!("memory_prof"))
            .spawn(move || {
                // sleep some time to get the diff between two dumping.
                thread::sleep(sleep_interval);
                unsafe {
                    if let Err(e) = jemallocator::mallctl_set(PROFILE_ACTIVE, false) {
                        panic!("failed to deactivate profiling: {}", e);
                    }
                }
                thread::sleep(sleep_interval);
                unsafe {
                    if let Err(e) = jemallocator::mallctl_set(PROFILE_DUMP,
                                                              second_file_name.get_mut_ptr()) {
                        error!("failed to dump the second profile: {}", e);
                        flag2.store(false, Ordering::SeqCst);
                        return;
                    }
                }
                flag2.store(false, Ordering::SeqCst);
            })
            .unwrap())
    }

    #[cfg(test)]
    mod test {
        use std::fs;
        use std::time::*;
        use std::sync::*;
        use std::sync::atomic::*;

        use tempdir::TempDir;

        // Only trigger this test with prof set to true.
        #[test]
        #[ignore]
        fn test_profiling_memory() {
            let dir = TempDir::new("test_profiling").unwrap();
            let sleep_time = Duration::from_millis(1000);
            let os_path = dir.path().to_path_buf().join("test.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            let profiling = Arc::new(AtomicBool::new(true));
            let handle = super::profile_memory(Some(&path), &profiling, sleep_time);
            assert!(handle.is_none(),
                    "no profile should be run when last profile has not finished.");
            assert!(profiling.load(Ordering::SeqCst));

            profiling.store(false, Ordering::SeqCst);
            let handle = super::profile_memory(Some(&path), &profiling, sleep_time);
            assert!(profiling.load(Ordering::SeqCst));
            assert!(handle.is_some());
            handle.unwrap().join().unwrap();
            let files = fs::read_dir(dir.path()).unwrap().count();
            assert_eq!(files, 2);
        }
    }
}

#[cfg(not(feature = "mem-profiling"))]
mod imp {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use std::thread::JoinHandle;

    pub fn profile_memory(_: Option<&str>,
                          _: &Arc<AtomicBool>,
                          _: Duration)
                          -> Option<JoinHandle<()>> {
        None
    }
}

pub use self::imp::*;
