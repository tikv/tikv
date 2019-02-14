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

//! A simple panic hook that allows skipping printing stack trace conditionally.

#![feature(box_syntax)]

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe, PanicInfo};
use std::sync::{Once, ONCE_INIT};

static INIT: Once = ONCE_INIT;
// store the default panic hook defined in std.
static mut DEFAULT_HOOK: Option<*mut (Fn(&PanicInfo) + 'static + Sync + Send)> = None;

thread_local! {
    static MUTED: RefCell<bool> = RefCell::new(false)
}

/// Replace the default hook if we haven't.
fn initialize() {
    unsafe {
        DEFAULT_HOOK = Some(Box::into_raw(panic::take_hook()));
        panic::set_hook(box track_hook);
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
fn track_hook(p: &PanicInfo) {
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
