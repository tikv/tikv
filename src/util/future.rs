// Copyright 2018 PingCAP, Inc.
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

use crate::util::Either;
use futures::sync::oneshot;
use futures::{Async, Future, IntoFuture, Poll};
use std::boxed;

/// Generates a paired future and callback so that when callback is being called, its result
/// is automatically passed as a future result.
pub fn paired_future_callback<T>() -> (Box<dyn boxed::FnBox(T) + Send>, oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = oneshot::channel::<T>();
    let callback = box move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    };
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
