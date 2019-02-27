// Copyright 2018 PingCAP, Inc.
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

#[cfg(unix)]
mod jemalloc {
    use libc::{self, c_char, c_void};
    use std::{ptr, slice};

    extern "C" {
        #[cfg_attr(target_os = "macos", link_name = "je_malloc_stats_print")]
        fn malloc_stats_print(
            write_cb: extern "C" fn(*mut c_void, *const c_char),
            cbopaque: *mut c_void,
            opts: *const c_char,
        );
    }

    #[cfg_attr(feature = "cargo-clippy", allow(cast_ptr_alignment))]
    extern "C" fn write_cb(printer: *mut c_void, msg: *const c_char) {
        unsafe {
            let buf = &mut *(printer as *mut Vec<u8>);
            let len = libc::strlen(msg);
            let bytes = slice::from_raw_parts(msg as *const u8, len);
            buf.extend_from_slice(bytes);
        }
    }

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

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_dump_stats() {
            // just dump the data, ensure it doesn't core.
            super::dump_stats();
        }
    }
}

#[cfg(not(unix))]
mod jemalloc {
    use tikv::raftstore::store::Engines;

    pub fn dump_stats() -> String {
        String::default()
    }
}

pub use self::jemalloc::dump_stats;
