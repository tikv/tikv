// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

extern "C" {
    // External jemalloc
    pub fn mallctl(
        name: *const ::std::os::raw::c_char,
        oldp: *mut ::std::os::raw::c_void,
        oldlenp: *mut u64,
        newp: *mut ::std::os::raw::c_void,
        newlen: u64,
    ) -> ::std::os::raw::c_int;

    // Embedded jemalloc
    pub fn _rjem_mallctl(
        name: *const ::std::os::raw::c_char,
        oldp: *mut ::std::os::raw::c_void,
        oldlenp: *mut u64,
        newp: *mut ::std::os::raw::c_void,
        newlen: u64,
    ) -> ::std::os::raw::c_int;
}

#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_unsafe)]
pub fn issue_mallctl_args(
    command: &str,
    oldptr: *mut ::std::os::raw::c_void,
    oldsize: *mut u64,
    newptr: *mut ::std::os::raw::c_void,
    newsize: u64,
) -> ::std::os::raw::c_int {
    unsafe {
        let c_str = std::ffi::CString::new(command).unwrap();
        let c_ptr: *const ::std::os::raw::c_char = c_str.as_ptr() as *const ::std::os::raw::c_char;
        // See unprefixed_malloc_on_supported_platforms in tikv-jemalloc-sys.
        #[cfg(any(test, feature = "testexport"))]
        {
            // Test part
            #[cfg(feature = "jemalloc")]
            {
                // See NO_UNPREFIXED_MALLOC
                #[cfg(any(target_os = "android", target_os = "dragonfly", target_os = "macos"))]
                return _rjem_mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
                #[cfg(not(any(
                    target_os = "android",
                    target_os = "dragonfly",
                    target_os = "macos"
                )))]
                return mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
            }
            0
        }

        #[cfg(not(any(test, feature = "testexport")))]
        {
            // No test part
            #[cfg(feature = "external-jemalloc")]
            {
                // Must linked to tiflash.
                return mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
            }
            #[cfg(not(feature = "external-jemalloc"))]
            {
                // Happens only with `raftstore-proxy-main`
                return mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
            }
        }
    }
}

#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_unsafe)]
fn issue_mallctl(command: &str) -> u64 {
    type PtrUnderlying = u64;
    let mut ptr: PtrUnderlying = 0;
    let mut size = std::mem::size_of::<PtrUnderlying>() as u64;
    issue_mallctl_args(
        command,
        &mut ptr as *mut _ as *mut ::std::os::raw::c_void,
        &mut size as *mut u64,
        std::ptr::null_mut(),
        0,
    );
    ptr
}

pub fn get_allocatep_on_thread_start() -> u64 {
    issue_mallctl("thread.allocatedp")
}

pub fn get_deallocatep_on_thread_start() -> u64 {
    issue_mallctl("thread.deallocatedp")
}

pub fn get_allocate() -> u64 {
    issue_mallctl("thread.allocated")
}

pub fn get_deallocate() -> u64 {
    issue_mallctl("thread.deallocated")
}
