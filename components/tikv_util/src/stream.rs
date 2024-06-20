// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    io, iter,
    marker::Unpin,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::stream::{self, Stream};
use futures_util::io::AsyncRead;
use http::status::StatusCode;
use rand::{thread_rng, Rng};
use rusoto_core::{request::HttpDispatchError, RusotoError};
use tokio::{runtime::Builder, time::sleep};

const MAX_RETRY_DELAY: Duration = Duration::from_secs(32);
const MAX_RETRY_TIMES: usize = 14;

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
// FIXME: get rid of this function, so that futures_executor::block_on is
// sufficient.
pub fn block_on_external_io<F: Future>(f: F) -> F::Output {
    // we need a Tokio runtime, Tokio futures require Tokio executor.
    Builder::new_current_thread()
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
/// This method implements truncated exponential back-off retry strategies
/// outlined in <https://docs.aws.amazon.com/general/latest/gr/api-retries.html> and
/// <https://cloud.google.com/storage/docs/exponential-backoff>
/// Since rusoto does not have transparent auto-retry
/// (<https://github.com/rusoto/rusoto/issues/234>), we need to implement this manually.
pub async fn retry<G, T, F, E>(action: G) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError,
{
    retry_ext(action, RetryExt::default()).await
}

/// The extra configuration for retry.
pub struct RetryExt<E> {
    pub on_failure: Option<Box<dyn FnMut(&E) + Send + Sync + 'static>>,
    pub max_retry_times: usize,
    pub max_retry_delay: Duration,
}

impl<E> RetryExt<E> {
    /// Attaches the failure hook to the ext.
    pub fn with_fail_hook<F>(mut self, f: F) -> Self
    where
        F: FnMut(&E) + Send + Sync + 'static,
    {
        self.on_failure = Some(Box::new(f));
        self
    }

    /// Attaches the maximum retry times to the ext.
    pub fn with_max_retry_times(mut self, max_retry_times: usize) -> Self {
        self.max_retry_times = max_retry_times;
        self
    }

    /// Attaches the maximum retry delay to the ext.
    pub fn with_max_retry_delay(mut self, max_retry_delay: Duration) -> Self {
        self.max_retry_delay = max_retry_delay;
        self
    }
}

// If we use the default derive macro, it would complain that `E` isn't
// `Default` :(
impl<E> Default for RetryExt<E> {
    fn default() -> Self {
        Self {
            on_failure: Default::default(),
            max_retry_times: MAX_RETRY_TIMES,
            max_retry_delay: MAX_RETRY_DELAY,
        }
    }
}

#[doc(hidden)]
pub mod __macro_helper {
    #[doc(hidden)]
    pub use rand::thread_rng as __thread_rng;
    #[doc(hidden)]
    pub use rand::Rng as __rand_Rng;
    #[doc(hidden)]
    pub use tokio::time::sleep as __tokio_sleep;
}

#[derive(Debug)]
pub struct JustRetry<E>(pub E);

impl<E> RetryError for JustRetry<E> {
    fn is_retryable(&self) -> bool {
        true
    }
}

/// retry_expr! executes retry over an expression itself. The expression will be
/// evaluated multi-times. This would be useful when your action cannot be
/// sealed into a `FnMut`, say, the action captures some local variables, rustc
/// will complain that `FnMut` cannot return reference to things it captures.
///
/// When possible, prefer using `retry_ext`, which is a normal function. Normal
/// functions are more friendly to static analysis.
#[macro_export]
macro_rules! retry_expr {
    ($action:expr) => {
        $crate::retry_expr!($action, RetryExt::default())
    };
    ($action:expr, $ext:expr) => {
        async {
            use $crate::stream::{RetryError, __macro_helper};

            let mut ext: RetryExt<_> = $ext;
            let max_retry_times = ext.max_retry_times;
            let mut retry_wait_dur = Duration::from_secs(1);
            let mut retry_time = 0;
            loop {
                match { $action }.await {
                    Ok(r) => return Ok(r),
                    Err(e) => {
                        if let Some(ref mut f) = ext.on_failure {
                            f(&e);
                        }
                        if !RetryError::is_retryable(&e) {
                            return Err(e);
                        }
                        retry_time += 1;
                        if retry_time > max_retry_times {
                            return Err(e);
                        }
                    }
                }
                use __macro_helper::__rand_Rng;
                let backoff = __macro_helper::__thread_rng().gen_range(0..1000);
                __macro_helper::__tokio_sleep(retry_wait_dur + Duration::from_millis(backoff))
                    .await;
                retry_wait_dur = ext.max_retry_delay.min(retry_wait_dur * 2);
            }
        }
    };
}

/// Retires a future execution. Comparing to `retry`, this version allows more
/// configurations.
pub async fn retry_all_ext<'a, G, T, F, E>(
    mut action: G,
    ext: RetryExt<JustRetry<E>>,
) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    use futures::TryFutureExt;
    retry_expr!(action().map_err(JustRetry), ext)
        .await
        .map_err(|err| err.0)
}

/// Retires a future execution. Comparing to `retry`, this version allows more
/// configurations.
pub async fn retry_ext<G, T, F, E>(mut action: G, mut ext: RetryExt<E>) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError,
{
    ext.max_retry_times = (|| {
        fail::fail_point!("retry_count", |t| t
            .and_then(|v| dbg!(v.parse::<usize>().ok()))
            .unwrap_or(ext.max_retry_times));
        ext.max_retry_times
    })();

    retry_expr!(action(), ext).await
}

// Return an error if the future does not finish by the timeout
pub async fn with_timeout<T, E, Fut>(timeout_duration: Duration, fut: Fut) -> Result<T, E>
where
    Fut: Future<Output = Result<T, E>> + std::marker::Unpin,
    E: From<Box<dyn std::error::Error + Send + Sync>>,
{
    match tokio::time::timeout(timeout_duration, fut).await {
        Ok(resp) => resp,
        Err(_) => Err(box_err!(
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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, pin::Pin};

    use futures::{Future, FutureExt};
    use rusoto_core::HttpDispatchError;

    use super::RetryError;
    use crate::stream::retry;

    #[derive(Debug)]
    struct TriviallyRetry;

    impl RetryError for TriviallyRetry {
        fn is_retryable(&self) -> bool {
            true
        }
    }

    fn assert_send<T: Send>(_t: T) {}

    #[test]
    fn test_retry_is_send_even_return_type_not_sync() {
        struct BangSync(Option<RefCell<()>>);
        let fut = retry(|| futures::future::ok::<_, HttpDispatchError>(BangSync(None)));
        assert_send(fut)
    }

    fn gen_action_fail_for(
        n_times: usize,
    ) -> impl FnMut() -> Pin<Box<dyn Future<Output = Result<(), TriviallyRetry>>>> {
        let mut n = 0;
        move || {
            if n < n_times {
                n += 1;
                futures::future::err(TriviallyRetry).boxed()
            } else {
                futures::future::ok(()).boxed()
            }
        }
    }

    #[tokio::test]
    async fn test_failure() {
        fail::cfg("retry_count", "return(2)").unwrap();
        let r = retry(gen_action_fail_for(3)).await;
        assert!(r.is_err(), "{:?}", r);
        let r = retry(gen_action_fail_for(1)).await;
        assert!(r.is_ok(), "{:?}", r);
    }
}
