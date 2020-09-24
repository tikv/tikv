use std::os::raw::{c_char, c_int};

#[no_mangle]
fn ____useless_func_to_make_compiler_happy(pp: &mut engine_rocks::raw::Env) {
    *pp = Default::default();
    engine_rocks::encryption::get_env(None, None).unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn print_tiflash_proxy_version() {
    cmd::print_proxy_version();
}

#[no_mangle]
pub unsafe extern "C" fn run_tiflash_proxy_ffi(
    argc: c_int,
    argv: *const *const c_char,
    helper: *const u8,
) {
    cmd::run_proxy(argc, argv, helper);
}
