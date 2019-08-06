// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! A simple panic hook that allows skipping printing stack trace conditionally.
//!
//! This crate is only for use during testing - in production, any panic in TiKV
//! is fatal, and it is compiled with panic=abort, so use of this crate in
//! production is an error.

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe, PanicInfo};
use std::sync::Once;

static INIT: Once = Once::new();
// store the default panic hook defined in std.
static mut DEFAULT_HOOK: Option<*mut (dyn Fn(&PanicInfo<'_>) + 'static + Sync + Send)> = None;

thread_local! {
    static MUTED: RefCell<bool> = RefCell::new(false)
}

/// Replace the default hook if we haven't.
fn initialize() {
    unsafe {
        DEFAULT_HOOK = Some(Box::into_raw(panic::take_hook()));
        panic::set_hook(Box::new(track_hook));
    }
}

/// Skip printing the stack trace if panic.
pub fn mute() {
    INIT.call_once(initialize);
    MUTED.with(|m| *m.borrow_mut() = true);
}

/// Print the stack trace if panic.
pub fn unmute() {
    MUTED.with(|m| *m.borrow_mut() = false);
}

/// Print the stacktrace according to the static MUTED.
fn track_hook(p: &PanicInfo<'_>) {
    MUTED.with(|m| {
        if *m.borrow() {
            return;
        }
        unsafe {
            if let Some(hook) = DEFAULT_HOOK {
                (*hook)(p);
            }
        }
    });
}

/// Recover from closure which may panic.
///
/// This function assumes the closure is able to be forced to implement `UnwindSafe`.
///
/// Also see [`AssertUnwindSafe`](https://doc.rust-lang.org/std/panic/struct.AssertUnwindSafe.html).
pub fn recover_safe<F, R>(f: F) -> std::thread::Result<R>
where
    F: FnOnce() -> R,
{
    mute();
    let res = panic::catch_unwind(AssertUnwindSafe(f));
    unmute();
    res
}
