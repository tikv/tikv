use std::os::raw::{c_char, c_int};

/// # Safety
/// Print version infomatin to std output.
#[no_mangle]
pub unsafe extern "C" fn print_raftstore_proxy_version() {
    server::print_proxy_version();
}

/// # Safety
/// Please make sure such function will be run in an independent thread. Usage about interfaces can be found in `struct EngineStoreServerHelper`.
#[no_mangle]
pub unsafe extern "C" fn run_raftstore_proxy_ffi(
    argc: c_int,
    argv: *const *const c_char,
    helper: *const u8,
) {
    server::run_proxy(argc, argv, helper);
}
