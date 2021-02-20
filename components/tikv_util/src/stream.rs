// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use futures::future::{self};
use futures::stream::{self, Stream};
use futures_util::io::AsyncRead;
use http::status::StatusCode;
use rand::{thread_rng, Rng};
use rusoto_core::{request::HttpDispatchError, RusotoError};
use std::{
    future::Future,
    io, iter,
    marker::Unpin,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{runtime::Builder, time::delay_for};

/// Wrapper of an `AsyncRead` instance, exposed as a `Sync` `Stream` of `Bytes`.
pub struct AsyncReadAsSyncStreamOfBytes<R> {
    // we need this Mutex to ensure the type is Sync (provided R is Send).
    // this is because rocksdb::SequentialFile is *not* Sync
    // (according to the documentation it cannot be Sync either,
    // requiring "external synchronization".)
    reader: Mutex<R>,
    // we use this member to ensure every call to `poll_next()` reuse the same
    // buffer.
    buf: Vec<u8>,
}

pub const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

impl<R> AsyncReadAsSyncStreamOfBytes<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: Mutex::new(reader),
            buf: vec![0; READ_BUF_SIZE],
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncReadAsSyncStreamOfBytes<R> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let reader = this.reader.get_mut().expect("lock was poisoned");
        let read_size = Pin::new(reader).poll_read(cx, &mut this.buf);

        match read_size {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(n)) => Poll::Ready(Some(Ok(Bytes::copy_from_slice(&this.buf[..n])))),
        }
    }
}

pub fn error_stream(e: io::Error) -> impl Stream<Item = io::Result<Bytes>> + Unpin + Send + Sync {
    stream::iter(iter::once(Err(e)))
}

/// Runs a future on the current thread involving external storage.
///
/// # Caveat
///
/// This function must never be nested. The future invoked by
/// `block_on_external_io` must never call `block_on_external_io` again itself,
/// otherwise the executor's states may be disrupted.
///
/// This means the future must only use async functions.
// FIXME: get rid of this function, so that futures_executor::block_on is sufficient.
pub fn block_on_external_io<F: Future>(f: F) -> F::Output {
    // we need a Tokio runtime, Tokio futures require Tokio executor.
    Builder::new()
        .basic_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to create Tokio runtime")
        .block_on(f)
}

/// Trait for errors which can be retried inside [`retry()`].
pub trait RetryError {
    /// Returns whether this error can be retried.
    fn is_retryable(&self) -> bool;
}

/// Retries a future execution.
///
/// This method implements truncated exponential back-off retry strategies outlined in
/// https://docs.aws.amazon.com/general/latest/gr/api-retries.html and
/// https://cloud.google.com/storage/docs/exponential-backoff
/// Since rusoto does not have transparent auto-retry (https://github.com/rusoto/rusoto/issues/234),
/// we need to implement this manually.
pub async fn retry<G, T, F, E>(mut action: G) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError,
{
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(32);
    const MAX_RETRY_TIMES: usize = 4;
    let mut retry_wait_dur = Duration::from_secs(1);

    let mut final_result = action().await;
    for _ in 1..MAX_RETRY_TIMES {
        if let Err(e) = &final_result {
            if e.is_retryable() {
                delay_for(retry_wait_dur + Duration::from_millis(thread_rng().gen_range(0, 1000)))
                    .await;
                retry_wait_dur = MAX_RETRY_DELAY.min(retry_wait_dur * 2);
                final_result = action().await;
                continue;
            }
        }
        break;
    }
    final_result
}

// Return an error if the future does not finish by the timeout
pub async fn with_timeout<T, E, Fut>(timeout_duration: Duration, fut: Fut) -> Result<T, E>
where
    Fut: Future<Output = Result<T, E>> + std::marker::Unpin,
    E: From<Box<dyn std::error::Error + Send + Sync>>,
{
    let timeout = tokio::time::delay_for(timeout_duration);
    match future::select(fut, timeout).await {
        future::Either::Left((resp, _)) => resp,
        future::Either::Right(((), _)) => Err(box_err!(
            "request timeout. duration: {:?}",
            timeout_duration
        )),
    }
}

pub fn http_retriable(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT
}

impl<E> RetryError for RusotoError<E> {
    fn is_retryable(&self) -> bool {
        match self {
            Self::HttpDispatch(e) => e.is_retryable(),
            Self::Unknown(resp) if http_retriable(resp.status) => true,
            _ => false,
        }
    }
}

impl RetryError for HttpDispatchError {
    fn is_retryable(&self) -> bool {
        true
    }
}
