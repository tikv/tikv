//! This mod provides the basic ability of tracing asynchronous tasks.
//! Generally it will collect all enabled (no need to be entered) spans.
//! When applying to the asynchronous tasks' spans, this implements something
//! like `async-backtrace`.
//!
//! # Notice
//!
//! We maintain the span tree ourselves via the enter order. (instead of the
//! spans' hierarchy -- it is hard to traverse all enabled leaf spans!)
//! So, you may need making the enter order represent the hierarchy of spans.
//!
//! That is pretty easy: because in sequential code, it is trivially kept. What
//! you need to notice is that every time you spawning a task to background,
//! prefer **using the root span (you can use the shortcut [`root!`])**.
//!
//! There is an known issue that if you spawn a task with parent of the current
//! span, the span of spawning will not be deleted before the spawned task
//! finished -- looks like you have `join`ed the task each time.
//!
//! # Macros
//!
//! **These macros relays on the `tracing` crate. You may add it to dependency
//! list before using them.**
//!
//! You may manually create the span by the [`tracing::span!`] macro family or
//! using the shortcut macros provided in this module.
//!
//! There are two macros provided: [`root!`] and [`frame!`], they have similar
//! syntax:
//!
//! *basic usage* `root!(fut)`, this will generate an span with the current
//! callsite and the expression itself will be the span name. For easier
//! reading, please don't use this over a long expression.
//!
//! *named* `root!("my_cool_span"; fut)`, this will generate a named span.
//!
//! *with fields* `root!("exploring"; fut; answer = 42)`, this will register a
//! root with fields. The syntax of fields is the same as [`tracing::span!`] as
//! they will be forwarded to there. In this case, you can use the `default`
//! placeholder for generating a name by the expression.
//!
//! You may also use the `#[instrument]` macro provided by the `tracing`
//! package.
//!
//! # Performance
//!
//! When attaching with spans, futures will enter and leave this span in each
//! `poll`. Which requires a cross-thread syncing. According to the test result
//! of `minitrace`, this may cost an extra 1~3 μs for each `poll`.
//!
//! When you are attaching fields with type `impl Debug` or `String`, it may be
//! cloned, which costs time depending on how complex the field is.
//!
//! Generally, you shouldn't attach a span to any frequent called, CPU bounded
//! functions. Prefer adding spans to where may block or run for a long time.
//!
//! # Acknowledgement
//!
//! This module is inspired by the library `await-tree` and `async-backtrace`.
//! But reused the span type and ecosystem from `tracing`.

pub use tracing::Instrument;

mod data;
pub mod layer;
pub mod tree;

#[cfg(test)]
mod test;

pub use layer::{dump_all_tree_bytes, init};

#[macro_export]
#[doc(hidden)]
macro_rules! stringify_limited {
    ($($t:tt)*) => { {
        const NAME: &str = stringify!($($t)*);
        const _: () = assert!(NAME.len() < 256,
            "the expression is too long to be a span name, try give it a name by prepending `<name>;` to the macro call?");
        NAME
    } }
}

/// Register the root of a span tree. (i.e. asynchronous execution).
///
/// You may use this to wrap a future while spawning a task if you want to trace
/// such task. Also notice that even your function is wrapped by the
/// `#[instrument]` attribute, you may still need to wrap it while spawning.
///
/// See the [module level document](doc#Macros) for more details.
#[macro_export]
macro_rules! root {
    ($t:expr) => {
        $crate::async_trace::Instrument::instrument($t, ::tracing::trace_span!(parent:None, $crate::stringify_limited!($t)))
    };
    (default; $f:expr; $($t:tt)*) => {
        $crate::async_trace::Instrument::instrument($f, ::tracing::trace_span!(parent:None, $crate::stringify_limited!($f), $($t)*))
    };
    ($name:literal; $t:expr) => {
        $crate::async_trace::Instrument::instrument($t, ::tracing::trace_span!(parent:None, $name))
    };
    ($name:literal; $f:expr; $($t:tt)*) => {
        $crate::async_trace::Instrument::instrument($f, ::tracing::trace_span!(parent:None, $name, $($t)*))
    };
}

/// Shorthand for instrumenting a future.
///
/// See the [module level document](doc#Macros) for more details.
#[macro_export]
macro_rules! frame {
    ($t:expr) => {
        $crate::async_trace::Instrument::instrument($t, ::tracing::trace_span!($crate::stringify_limited!($t)))
    };
    (default; $f:expr; $($t:tt)*) => {
        $crate::async_trace::Instrument::instrument($f, ::tracing::trace_span!($crate::stringify_limited!($f), $($t)*))
    };
    ($name:literal; $t:expr) => {
        $crate::async_trace::Instrument::instrument($t, ::tracing::trace_span!($name))
    };
    ($name:literal; $f:expr; $($t:tt)*) => {
        $crate::async_trace::Instrument::instrument($f, ::tracing::trace_span!($name, $($t)*))
    };
}
