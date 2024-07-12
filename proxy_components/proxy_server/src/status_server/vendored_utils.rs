// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use proxy_ffi::jemalloc_utils::issue_mallctl_args;
use tikv_alloc::error::ProfResult;

pub fn activate_prof() -> ProfResult<()> {
    {
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

pub fn deactivate_prof() -> ProfResult<()> {
    {
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

pub fn dump_prof(path: &str) -> tikv_alloc::error::ProfResult<()> {
    {
        let mut bytes = std::ffi::CString::new(path)?.into_bytes_with_nul();
        let mut ptr = bytes.as_mut_ptr() as *mut ::std::os::raw::c_char;
        let len = std::mem::size_of_val(&ptr) as u64;
        issue_mallctl_args(
            "prof.dump",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut ptr as *mut _ as *mut _,
            len,
        );
    }
    Ok(())
}
