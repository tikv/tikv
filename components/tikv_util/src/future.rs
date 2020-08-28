// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::callback::must_call;
use crate::Either;
use futures::executor::{self, Notify, Spawn};
use futures::{Async, Future, IntoFuture, Poll};
use std::sync::{Arc, Mutex};
use futures03::channel::oneshot as futures_oneshot;

/// Generates a paired future and callback so that when callback is being called, its result
/// is automatically passed as a future result.
pub fn paired_future_callback<T>() -> (Box<dyn FnOnce(T) + Send>, futures_oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = futures_oneshot::channel::<T>();
    let callback = Box::new(move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    });
    (callback, future)
}

pub fn paired_must_called_future_callback<T>(
    arg_on_drop: impl FnOnce() -> T + Send + 'static,
) -> (Box<dyn FnOnce(T) + Send>, futures_oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = futures_oneshot::channel::<T>();
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

// BatchCommandsNotify is used to make business pool notifiy completion queues directly.
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

pub fn poll_future_notify<F: Future<Item = (), Error = ()> + Send + 'static>(f: F) {
    let spawn = Arc::new(Mutex::new(Some(executor::spawn(f))));
    let notify = BatchCommandsNotify(spawn);
    notify.notify(0);
}
