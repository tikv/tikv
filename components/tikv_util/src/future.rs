// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::Either;
use futures::sync::oneshot;
use futures::{Async, Future, IntoFuture, Poll};

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
