use std::panic::{self, PanicInfo};
use std::cell::RefCell;
use std::sync::StaticRwLock;


/// A simple panic hook that allows skiping printing stacktrace conditionaly.

static HOOK_LOCK: StaticRwLock = StaticRwLock::new();
// store the default panic hook defined in std.
static mut DEFAULT_HOOK: Option<*mut (Fn(&PanicInfo) + 'static + Sync + Send)> = None;

thread_local! {
    static MUTED: RefCell<bool> = RefCell::new(false)
}

/// Replace the default hook if we haven't.
fn initialize() {
    unsafe {
        let rl = HOOK_LOCK.read().unwrap();
        if let Some(_) = DEFAULT_HOOK {
            return;
        }
        drop(rl);
        let wl = HOOK_LOCK.write().unwrap();
        // in case multiple thread is waiting for the wl.
        if let Some(_) = DEFAULT_HOOK {
            return;
        }
        DEFAULT_HOOK = Some(Box::into_raw(panic::take_hook()));
        panic::set_hook(box track_hook);
        // mute unused warning.
        drop(wl);
    }
}

/// Skip printing the stacktrace if panic.
pub fn mute() {
    initialize();
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
