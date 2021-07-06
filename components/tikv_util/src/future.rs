// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::callback::must_call;
use crate::Either;
use futures::executor::{self, Notify, Spawn};
use futures::{Async, Future, IntoFuture, Poll};
use futures03::future::{self as std_future, Future as StdFuture, FutureExt, TryFutureExt};
use futures03::stream::{Stream, StreamExt};
use std::sync::{Arc, Mutex};
use tokio_sync::oneshot;

/// Generates a paired future and callback so that when callback is being called, its result
/// is automatically passed as a future result.
pub fn paired_future_callback<T>() -> (Box<dyn FnOnce(T) + Send>, oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = oneshot::channel::<T>();
    let callback = Box::new(move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    });
    (callback, future)
}

pub fn paired_std_future_callback<T>() -> (
    Box<dyn FnOnce(T) + Send>,
    futures03::channel::oneshot::Receiver<T>,
)
where
    T: Send + 'static,
{
    let (tx, future) = futures03::channel::oneshot::channel::<T>();
    let callback = Box::new(move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    });
    (callback, future)
}

pub fn paired_must_called_std_future_callback<T>(
    arg_on_drop: impl FnOnce() -> T + Send + 'static,
) -> (
    Box<dyn FnOnce(T) + Send>,
    futures03::channel::oneshot::Receiver<T>,
)
where
    T: Send + 'static,
{
    let (tx, future) = futures03::channel::oneshot::channel::<T>();
    let callback = must_call(
        move |result| {
            let r = tx.send(result);
            if r.is_err() {
                warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
            }
        },
        arg_on_drop,
    );
    (callback, future)
}

/// A shortcut for `f1.and_then(|()| f2.flatten())`. Note that
/// the expression is just a simplified version as f2's Error
/// type may not be easy handled via combinators.
pub struct AndThenWith<F1, F2>
where
    F2: Future,
    F2::Item: IntoFuture,
{
    f1: Option<F1>,
    f2: Either<F2, <F2::Item as IntoFuture>::Future>,
}

impl<F1, F2> AndThenWith<F1, F2>
where
    F2: Future,
    F2::Item: IntoFuture,
{
    #[inline]
    pub fn new(f1: F1, f2: F2) -> AndThenWith<F1, F2> {
        AndThenWith {
            f1: Some(f1),
            f2: Either::Left(f2),
        }
    }
}

impl<E, F2> Future for AndThenWith<Result<(), E>, F2>
where
    F2: Future,
    F2::Item: IntoFuture<Error = E>,
{
    type Item = Result<<F2::Item as IntoFuture>::Item, E>;
    type Error = F2::Error;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.f1.is_some() {
            match self.f1.take().unwrap() {
                Ok(()) => {}
                Err(e) => return Ok(Async::Ready(Err(e))),
            }
        }

        loop {
            let res = match self.f2 {
                Either::Left(ref mut f1) => try_ready!(f1.poll()).into_future(),
                Either::Right(ref mut f2) => {
                    return match f2.poll() {
                        Ok(Async::Ready(r)) => Ok(Async::Ready(Ok(r))),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Err(e) => Ok(Async::Ready(Err(e))),
                    };
                }
            };
            self.f2 = Either::Right(res);
        }
    }
}

struct BatchCommandsNotify<F>(Arc<Mutex<Option<Spawn<F>>>>);
impl<F> Clone for BatchCommandsNotify<F> {
    fn clone(&self) -> BatchCommandsNotify<F> {
        BatchCommandsNotify(Arc::clone(&self.0))
    }
}
impl<F> Notify for BatchCommandsNotify<F>
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn notify(&self, id: usize) {
        let n = Arc::new(self.clone());
        let mut s = self.0.lock().unwrap();
        match s.as_mut().map(|spawn| spawn.poll_future_notify(&n, id)) {
            Some(Ok(Async::NotReady)) | None => {}
            _ => *s = None,
        };
    }
}

/// Polls the provided future immediately. If the future is not ready,
/// it will register `Notify`. When the event is ready, the waker will
/// be notified, then the internal future is immediately polled in the
/// thread calling `notify()`.
pub fn poll_future_notify<F: Future<Item = (), Error = ()> + Send + 'static>(f: F) {
    let spawn = Arc::new(Mutex::new(Some(executor::spawn(f))));
    let notify = BatchCommandsNotify(spawn);
    notify.notify(0);
}

/// Create a stream proxy with buffer representing the remote stream. The returned task
/// will receive messages from the remote stream as much as possible.
pub fn create_stream_with_buffer<T, S>(
    s: S,
    size: usize,
) -> (
    impl Stream<Item = T> + Send + 'static,
    impl StdFuture<Output = ()> + Send + 'static,
)
where
    S: Stream<Item = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = futures03::channel::mpsc::channel::<T>(size);
    let driver = s
        .then(std_future::ok::<T, futures03::channel::mpsc::SendError>)
        .forward(tx)
        .map_err(|e| warn!("stream with buffer send error"; "error" => %e))
        .map(|_| ());
    (rx, driver)
}
