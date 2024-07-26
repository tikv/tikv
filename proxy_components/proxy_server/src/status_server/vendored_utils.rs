// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use proxy_ffi::jemalloc_utils::issue_mallctl_args;
use tikv_alloc::error::ProfResult;

pub fn activate_prof() -> ProfResult<()> {
    {
        tikv_util::debug!("activate_prof");
        let mut value: bool = true;
        let len = std::mem::size_of_val(&value) as u64;
        issue_mallctl_args(
            "prof.active",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut value as *mut _ as *mut _,
            len,
        );
    }
    Ok(())
}

pub fn has_activate_prof() -> bool {
    let mut value: bool = false;
    let mut len = std::mem::size_of_val(&value) as u64;
    issue_mallctl_args(
        "prof.active",
        &mut value as *mut _ as *mut _,
        &mut len as *mut _ as *mut _,
        std::ptr::null_mut(),
        0,
    );
    value
}

pub fn deactivate_prof() -> ProfResult<()> {
    {
        tikv_util::debug!("deactivate_prof");
        let mut value: bool = false;
        let len = std::mem::size_of_val(&value) as u64;
        issue_mallctl_args(
            "prof.active",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut value as *mut _ as *mut _,
            len,
        );
    }
    Ok(())
}

extern crate libc;

fn print_error_message(s: &str, r: ::std::os::raw::c_int) {
    #[cfg(not(any(target_os = "android", target_os = "dragonfly", target_os = "macos")))]
    {
        unsafe {
            let err = *libc::__errno_location();
            let err_msg = libc::strerror(err);
            let c_str = std::ffi::CStr::from_ptr(err_msg);
            let str_slice = c_str.to_str().unwrap_or("Unknown error");
            tikv_util::warn!(
                "{} returns non-zero {} error_code: {} error_message: {}",
                s,
                r,
                err,
                str_slice
            );
        }
    }
}

pub fn dump_prof(path: &str) -> tikv_alloc::error::ProfResult<()> {
    {
        let mut bytes = std::ffi::CString::new(path)?.into_bytes_with_nul();
        let mut ptr = bytes.as_mut_ptr() as *mut ::std::os::raw::c_char;
        let len = std::mem::size_of_val(&ptr) as u64;
        let r = issue_mallctl_args(
            "prof.dump",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut ptr as *mut _ as *mut _,
            len,
        );
        if r != 0 {
            print_error_message("dump_prof", r);
        }
    }
    Ok(())
}

pub fn adhoc_dump(path: &str) -> tikv_alloc::error::ProfResult<()> {
    {
        let mut bytes = std::ffi::CString::new(path)?.into_bytes_with_nul();
        let mut ptr = bytes.as_mut_ptr() as *mut ::std::os::raw::c_char;
        let len = std::mem::size_of_val(&ptr) as u64;
        let r = issue_mallctl_args(
            "prof.dump",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut ptr as *mut _ as *mut _,
            len,
        );
        if r != 0 {
            print_error_message("adhoc_dump", r);
        }
    }
    Ok(())
}
