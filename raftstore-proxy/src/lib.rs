use std::os::raw::{c_char, c_int};

#[no_mangle]
pub unsafe extern "C" fn print_raftstore_proxy_version() {
    cmd::print_proxy_version();
}

#[no_mangle]
pub unsafe extern "C" fn run_raftstore_proxy_ffi(
    argc: c_int,
    argv: *const *const c_char,
    helper: *const u8,
) {
    cmd::run_proxy(argc, argv, helper);
}
