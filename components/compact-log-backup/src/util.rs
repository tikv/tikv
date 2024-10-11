// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, future::Future, task::Poll};

use engine_traits::{CfName, SstCompressionType, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use external_storage::ExternalStorage;

/// A helper for cooperative concurrency.
///
/// When doing a tight loop (say, traversing a huge slice) that may use many
/// CPU time, you may inject [`Cooperate::step`] to each run. This will try
/// to yield the current task periodically so other coroutines can be
/// executed.
pub struct Cooperate {
    work_count: usize,
    yield_every: usize,
}

pub struct Step(bool);

impl Future for Step {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.0 {
            cx.waker().wake_by_ref();
            self.0 = false;
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl Cooperate {
    /// Create a new [`Cooperate`] that yields when a fixed number of works
    /// done.
    pub fn new(yield_every: usize) -> Self {
        Self {
            work_count: 0,
            yield_every,
        }
    }

    /// Finishing one tiny task. This will yield the current carrier thread
    /// when needed.
    pub fn step(&mut self) -> Step {
        self.work_count += 1;
        if self.work_count > self.yield_every {
            self.work_count = 0;
            Step(true)
        } else {
            Step(false)
        }
    }
}

/// Select any future completes from a vector.
/// The resolved future will be removed from the vector by `swap_remove`,
/// hence the order of execution may vary. Prefer using this for joining
/// unordered background tasks.
pub fn select_vec<'a, T, F>(v: &'a mut Vec<F>) -> impl Future<Output = T> + 'a
where
    // Note: this `Unpin` might be removed, as the returned future have
    // a mutable reference to the vector, the vector itself cannot be moved.
    F: Future<Output = T> + Unpin + 'a,
{
    use futures::FutureExt;

    futures::future::poll_fn(|cx| {
        for (idx, fut) in v.iter_mut().enumerate() {
            match fut.poll_unpin(cx) {
                std::task::Poll::Ready(item) => {
                    let _ = v.swap_remove(idx);
                    return item.into();
                }
                std::task::Poll::Pending => continue,
            }
        }
        std::task::Poll::Pending
    })
}

/// The extra config for [`execute_all_ext`].
pub struct ExecuteAllExt {
    /// The max number of concurrent tasks.
    pub max_concurrency: usize,
}

impl Default for ExecuteAllExt {
    fn default() -> Self {
        Self {
            max_concurrency: 16,
        }
    }
}

/// Execute a set of tasks concurrently.
///
/// You may restrict the max concurrency by the `ext`.
#[tracing::instrument(skip_all, fields(size = futs.len()))]
pub async fn execute_all_ext<T, F, E>(futs: Vec<F>, ext: ExecuteAllExt) -> Result<Vec<T>, E>
where
    F: Future<Output = Result<T, E>> + Unpin,
{
    let mut pending_futures = vec![];
    let mut result = Vec::with_capacity(futs.len());
    for fut in futs {
        pending_futures.push(fut);
        if pending_futures.len() >= ext.max_concurrency {
            result.push(select_vec(&mut pending_futures).await?);
        }
    }
    result.append(&mut futures::future::try_join_all(pending_futures.into_iter()).await?);
    Ok(result)
}

/// Transform a str to a [`engine_traits::CfName`]\(`&'static str`).
/// If the argument isn't one of `""`, `"DEFAULT"`, `"default"`, `"WRITE"`,
/// `"write"`, `"LOCK"`, `"lock"`... returns "ERR_CF". (Which would be
/// ignored then.)
pub fn cf_name(s: &str) -> CfName {
    match s {
        "" | "DEFAULT" | "default" => CF_DEFAULT,
        "WRITE" | "write" => CF_WRITE,
        "LOCK" | "lock" => CF_LOCK,
        "RAFT" | "raft" => CF_RAFT,
        _ => "ERR_CF",
    }
}

/// A wrapper that make a `u64` always be displayed as {:016X}.
#[derive(Debug)]
struct HexU64(u64);

impl Display for HexU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016X}", self.0)
    }
}

pub fn aligned_u64(v: u64) -> impl Display {
    HexU64(v)
}

pub fn compression_type_to_u8(c: SstCompressionType) -> u8 {
    match c {
        SstCompressionType::Lz4 => 0,
        SstCompressionType::Snappy => 1,
        SstCompressionType::Zstd => 2,
    }
}

#[derive(Eq, PartialEq)]
pub struct EndKey<'a>(pub &'a [u8]);

impl<'a> PartialOrd for EndKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering::*;
        match (self, other) {
            (EndKey(b""), EndKey(b"")) => Some(Equal),
            (EndKey(b""), _) => Some(Greater),
            (_, EndKey(b"")) => Some(Less),
            (EndKey(a), EndKey(b)) => Some(a.cmp(b)),
        }
    }
}

impl<'a> Ord for EndKey<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Get the storage URL string.
/// Once an error occurred, return the error message.
pub fn storage_url(s: &dyn ExternalStorage) -> String {
    s.url()
        .map(|v| v.to_string())
        .unwrap_or_else(|err| format!("<err: {err}>"))
}
