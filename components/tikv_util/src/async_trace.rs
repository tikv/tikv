use std::{
    cell::{Cell, LazyCell},
    collections::HashMap,
    future::Future,
    io::Cursor,
    sync::{Arc, Mutex, Weak},
    thread::ThreadId,
};

pub use async_trace_macros::framed;
use await_tree::{Config, Registry, Span};

lazy_static::lazy_static! {
    static ref ALL_REG: Mutex<HashMap<ThreadId, (String, Weak<Mutex<await_tree::Registry<u64>>>)>> =
        Mutex::default();
}

thread_local! {
    static REG: LazyCell<Arc<Mutex<await_tree::Registry<u64>>>> = LazyCell::new(|| {
        let mut regs = ALL_REG.lock().unwrap();
        let reg = Arc::new(Mutex::new(Registry::new(Config::default())));
        let th = std::thread::current();
        let old = regs.insert(th.id(), (th.name().unwrap_or("UNKNOWN").to_owned(), Arc::downgrade(&reg)));
        debug_assert!(old.is_none(), "local registry initialized twice, tid = {:?}, thread = {:?}", th.id(), th);
        reg
    });
    static TID: Cell<u64> = Cell::new(0);
}

/// Dump the current running asynchronous tasks traced.
pub fn dump_async_tasks() -> Vec<u8> {
    use std::io::Write;
    let mut result = Cursor::new(vec![]);
    let mut regs = ALL_REG.lock().unwrap();
    let mut dead_threads = vec![];
    for (tid, (tname, reg)) in regs.iter() {
        match reg.upgrade() {
            Some(reg) => {
                writeln!(result, "<spawned at {tid:?}, {tname}> {{").expect("infallible failed");
                for (id, tree) in reg.lock().unwrap().iter() {
                    // NOTE: should we reduce some allocation here?
                    writeln!(result, "[{id}] =>\n{tree}").expect("infallible failed");
                }
                writeln!(result, "}}\n").expect("infallible failed");
            }
            None => {
                dead_threads.push(*tid);
            }
        }
    }
    for dead_thread in dead_threads {
        regs.remove(&dead_thread);
    }
    result.into_inner()
}

pub fn root<T>(fut: impl Future<Output = T>, location: impl Into<Span>) -> impl Future<Output = T> {
    let id = TID.with(|i| {
        i.set(i.get() + 1);
        i.get()
    });
    REG.with(|reg| reg.lock().unwrap().register(id, location).instrument(fut))
}

pub fn frame<T>(
    fut: impl Future<Output = T>,
    location: impl Into<Span>,
) -> impl Future<Output = T> {
    await_tree::InstrumentAwait::instrument_await(fut, location)
}

// Note: in fact them can be plain function with #[track_caller].
// But the extra flexibility still helps.

#[macro_export]
macro_rules! root {
    ($($t:tt)*) => {
        $crate::trace_fut!($($t)*; use root)
    }
}

#[macro_export]
macro_rules! frame {
    ($($t:tt)*) => {
        $crate::trace_fut!($($t)*; use frame)
    }
}

#[macro_export(crate)]
macro_rules! trace_fut {
    ($t:expr; use $trace_fn:ident) => {
        tikv_util::async_trace::$trace_fn($t, concat!(file!(), ":", line!(), ",", column!()))
    };
    ($name:literal; $t:expr; use $trace_fn:ident) => {
        tikv_util::async_trace::$trace_fn(
            $t,
            concat!($name, " at ", file!(), ":", line!(), ",", column!()),
        )
    };
    (dyn $desc:expr; $t:expr; use $trace_fn:ident) => {
        tikv_util::async_trace::$trace_fn($t, {
            let mut res = $desc;
            res.push_str(concat!(" at ", file!(), ":", line!(), ",", column!()));
            res
        })
    };
}
