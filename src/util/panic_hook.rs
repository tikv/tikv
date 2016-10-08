// Copyright 2016 PingCAP, Inc.
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


use std::panic::{self, PanicInfo};
use std::cell::RefCell;
use std::sync::{Once, ONCE_INIT};
use std::{process, thread};

use backtrace::Backtrace;
use log::LogLevel;


/// A simple panic hook that allows skiping printing stacktrace conditionaly.

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

/// Skip printing the stacktrace if panic.
pub fn mute() {
    INIT.call_once(initialize);
    MUTED.with(|m| *m.borrow_mut() = true);
}

/// Print the stacktrace if panic.
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

/// Exit the whole process when panic.
pub fn set_exit_hook() {
    let orig_hook = panic::take_hook();
    panic::set_hook(box move |info: &PanicInfo| {
        if log_enabled!(LogLevel::Error) {
            let msg = match info.payload().downcast_ref::<&'static str>() {
                Some(s) => *s,
                None => {
                    match info.payload().downcast_ref::<String>() {
                        Some(s) => &s[..],
                        None => "Box<Any>",
                    }
                }
            };
            let thread = thread::current();
            let name = thread.name().unwrap_or("<unnamed>");
            let loc = info.location().map(|l| format!("{}:{}", l.file(), l.line()));
            let bt = Backtrace::new();
            error!("thread '{}' panicked '{}' at {:?}\n{:?}",
                   name,
                   msg,
                   loc.unwrap_or_else(|| "<unknown>".to_owned()),
                   bt);
        } else {
            orig_hook(info);
        }
        process::exit(1);
    })
}
