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


use std::{ptr, slice};

use libc::{self, c_void, c_char};

extern "C" {
    #[cfg_attr(target_os = "macos", link_name = "je_malloc_stats_print")]
    fn malloc_stats_print(write_cb: extern "C" fn(*mut c_void, *const c_char), cbopaque: *mut c_void, opts: *const c_char);
}

extern "C" fn write_cb(printer: *mut c_void, msg: *const c_char) {
    unsafe {
        let buf = &mut *{printer as *mut Vec<u8>};
        let len = libc::strlen(msg);
        let bytes = slice::from_raw_parts(msg as *const u8, len);
        buf.extend_from_slice(bytes);
    }
}

pub fn print_prof() {
    let mut buf = Vec::new();
    unsafe { malloc_stats_print(write_cb, &mut buf as *mut Vec<u8> as *mut c_void, ptr::null()) }
    info!("{}", String::from_utf8_lossy(&buf));
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_stats_print() {
        // just print the data, ensure it doesn't core.
        super::print_prof()
    }
}

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

    /// Dump the profile to the `path`.
    ///
    /// If `path` is `None`, will dump it in the working directory with a auto-generated name.
    pub fn dump_prof(path: Option<&str>) {
        unsafe {
            if let Err(e) = jemallocator::mallctl_set(PROFILE_ACTIVE, true) {
                error!("failed to activate profiling: {}", e);
                return;
            }
        }
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
            let os_path = dir.path().to_path_buf().join("test1.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(Some(&path));

            let os_path = dir.path().to_path_buf().join("test2.dump").into_os_string();
            let path = os_path.into_string().unwrap();
            super::dump_prof(Some(&path));

            let files = fs::read_dir(dir.path()).unwrap().count();
            assert_eq!(files, 2);
        }
    }
}

#[cfg(not(feature = "mem-profiling"))]
mod imp {
    pub fn dump_prof(_: Option<&str>) {}
}

pub use self::imp::*;
