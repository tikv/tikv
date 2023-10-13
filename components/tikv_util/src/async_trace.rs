use std::{
    future::Future,
    io::Cursor,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Mutex,
    },
};

pub use async_trace_macros::framed;
use await_tree::{Config, Span};

lazy_static::lazy_static! {
    static ref REG: Mutex<await_tree::Registry<u64>> = Mutex::new(await_tree::Registry::new(Config::default()));
    static ref TID: AtomicU64 = AtomicU64::new(0);
}

/// Dump the current running asynchronous tasks traced.
pub fn dump_async_tasks() -> Vec<u8> {
    use std::io::Write;
    let mut result = Cursor::new(vec![]);
    for (id, tree) in REG.lock().unwrap().iter() {
        // NOTE: should we reduce some allocation here?
        writeln!(result, "[{id}] =>\n{tree}\n").expect("infallible failed");
    }
    result.into_inner()
}

pub fn root<T>(fut: impl Future<Output = T>, location: impl Into<Span>) -> impl Future<Output = T> {
    let id = TID.fetch_add(1, SeqCst);
    REG.lock().unwrap().register(id, location).instrument(fut)
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
