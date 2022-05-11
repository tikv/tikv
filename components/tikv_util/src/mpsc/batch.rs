// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    pin::Pin,
    ptr::null_mut,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam::channel::{
    self, RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError,
};
use futures::{
    stream::Stream,
    task::{Context, Poll, Waker},
};

struct State {
    // If the receiver can't get any messages temporarily in `poll` context, it will put its
    // current task here.
    recv_task: AtomicPtr<Waker>,
    notify_size: usize,
    // How many messages are sent without notify.
    pending: AtomicUsize,
    notifier_registered: AtomicBool,
}

impl State {
    fn new(notify_size: usize) -> State {
        State {
            // Any pointer that is put into `recv_task` must be a valid and owned
            // pointer (it must not be dropped). When a pointer is retrieved from
            // `recv_task`, the user is responsible for its proper destruction.
            recv_task: AtomicPtr::new(null_mut()),
            notify_size,
            pending: AtomicUsize::new(0),
            notifier_registered: AtomicBool::new(false),
        }
    }

    #[inline]
    fn try_notify_post_send(&self) {
        let old_pending = self.pending.fetch_add(1, Ordering::AcqRel);
        if old_pending >= self.notify_size - 1 {
            self.notify();
        }
    }

    #[inline]
    fn notify(&self) {
        let t = self.recv_task.swap(null_mut(), Ordering::AcqRel);
        if !t.is_null() {
            self.pending.store(0, Ordering::Release);
            // Safety: see comment on `recv_task`.
            let t = unsafe { Box::from_raw(t) };
            t.wake();
        }
    }

    /// When the `Receiver` that holds the `State` is running on an `Executor`,
    /// the `Receiver` calls this to yield from the current `poll` context,
    /// and puts the current task handle to `recv_task`, so that the `Sender`
    /// respectively can notify it after sending some messages into the channel.
    #[inline]
    fn yield_poll(&self, waker: Waker) -> bool {
        let t = Box::into_raw(Box::new(waker));
        let origin = self.recv_task.swap(t, Ordering::AcqRel);
        if !origin.is_null() {
            // Safety: see comment on `recv_task`.
            unsafe { drop(Box::from_raw(origin)) };
            return true;
        }
        false
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let t = self.recv_task.swap(null_mut(), Ordering::AcqRel);
        if !t.is_null() {
            // Safety: see comment on `recv_task`.
            unsafe { drop(Box::from_raw(t)) };
        }
    }
}

/// `Notifier` is used to notify receiver whenever you want.
pub struct Notifier(Arc<State>);
impl Notifier {
    #[inline]
    pub fn notify(self) {
        drop(self);
    }
}

impl Drop for Notifier {
    #[inline]
    fn drop(&mut self) {
        let notifier_registered = &self.0.notifier_registered;
        if notifier_registered
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            unreachable!("notifier_registered must be true");
        }
        self.0.notify();
    }
}

pub struct Sender<T> {
    sender: Option<channel::Sender<T>>,
    state: Arc<State>,
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Sender<T> {
        Sender {
            sender: self.sender.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        drop(self.sender.take());
        self.state.notify();
    }
}

pub struct Receiver<T> {
    receiver: channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    pub fn is_empty(&self) -> bool {
        // When there is no sender references, it can't be known whether
        // it's empty or not.
        self.sender.as_ref().map_or(false, |s| s.is_empty())
    }

    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.sender.as_ref().unwrap().send(t)?;
        self.state.try_notify_post_send();
        Ok(())
    }

    #[inline]
    pub fn send_and_notify(&self, t: T) -> Result<(), SendError<T>> {
        self.sender.as_ref().unwrap().send(t)?;
        self.state.notify();
        Ok(())
    }

    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.sender.as_ref().unwrap().try_send(t)?;
        self.state.try_notify_post_send();
        Ok(())
    }

    #[inline]
    pub fn get_notifier(&self) -> Option<Notifier> {
        let notifier_registered = &self.state.notifier_registered;
        if notifier_registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Some(Notifier(Arc::clone(&self.state)));
        }
        None
    }
}

impl<T> Receiver<T> {
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }
}

/// Creates a unbounded channel with a given `notify_size`, which means if there are more pending
/// messages in the channel than `notify_size`, the `Sender` will auto notify the `Receiver`.
///
/// # Panics
/// if `notify_size` equals to 0.
#[inline]
pub fn unbounded<T>(notify_size: usize) -> (Sender<T>, Receiver<T>) {
    assert!(notify_size > 0);
    let state = Arc::new(State::new(notify_size));
    let (sender, receiver) = channel::unbounded();
    (
        Sender {
            sender: Some(sender),
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

/// Creates a bounded channel with a given `notify_size`, which means if there are more pending
/// messages in the channel than `notify_size`, the `Sender` will auto notify the `Receiver`.
///
/// # Panics
/// if `notify_size` equals to 0.
#[inline]
pub fn bounded<T>(cap: usize, notify_size: usize) -> (Sender<T>, Receiver<T>) {
    assert!(notify_size > 0);
    let state = Arc::new(State::new(notify_size));
    let (sender, receiver) = channel::bounded(cap);
    (
        Sender {
            sender: Some(sender),
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.try_recv() {
            Ok(m) => Poll::Ready(Some(m)),
            Err(TryRecvError::Empty) => {
                if self.state.yield_poll(cx.waker().clone()) {
                    Poll::Pending
                } else {
                    // For the case that all senders are dropped before the current task is saved.
                    self.poll_next(cx)
                }
            }
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
        }
    }
}

/// A Collector Used in `BatchReceiver`.
pub trait BatchCollector<Collection, Elem> {
    /// If `elem` is collected into `collection` successfully, return `None`.
    /// Otherwise return `elem` back, and `collection` should be spilled out.
    fn collect(&mut self, collection: &mut Collection, elem: Elem) -> Option<Elem>;
}

pub struct VecCollector;

impl<E> BatchCollector<Vec<E>, E> for VecCollector {
    fn collect(&mut self, v: &mut Vec<E>, e: E) -> Option<E> {
        v.push(e);
        None
    }
}

/// `BatchReceiver` is a `futures::Stream`, which returns a batched type.
pub struct BatchReceiver<T, E, I, C> {
    rx: Receiver<T>,
    max_batch_size: usize,
    elem: Option<E>,
    initializer: I,
    collector: C,
}

impl<T, E, I, C> BatchReceiver<T, E, I, C>
where
    T: Unpin,
    E: Unpin,
    I: Fn() -> E + Unpin,
    C: BatchCollector<E, T> + Unpin,
{
    /// Creates a new `BatchReceiver` with given `initializer` and `collector`. `initializer` is
    /// used to generate a initial value, and `collector` will collect every (at most
    /// `max_batch_size`) raw items into the batched value.
    pub fn new(rx: Receiver<T>, max_batch_size: usize, initializer: I, collector: C) -> Self {
        BatchReceiver {
            rx,
            max_batch_size,
            elem: None,
            initializer,
            collector,
        }
    }
}

impl<T, E, I, C> Stream for BatchReceiver<T, E, I, C>
where
    T: Unpin,
    E: Unpin,
    I: Fn() -> E + Unpin,
    C: BatchCollector<E, T> + Unpin,
{
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let ctx = self.get_mut();
        let (mut count, mut received) = (0, None);
        let finished = loop {
            match ctx.rx.try_recv() {
                Ok(m) => {
                    let collection = ctx.elem.get_or_insert_with(&ctx.initializer);
                    if let Some(m) = ctx.collector.collect(collection, m) {
                        received = Some(m);
                        break false;
                    }
                    count += 1;
                    if count >= ctx.max_batch_size {
                        break false;
                    }
                }
                Err(TryRecvError::Disconnected) => break true,
                Err(TryRecvError::Empty) => {
                    if ctx.rx.state.yield_poll(cx.waker().clone()) {
                        break false;
                    }
                }
            }
        };

        if ctx.elem.is_none() && finished {
            return Poll::Ready(None);
        } else if ctx.elem.is_none() {
            return Poll::Pending;
        }
        let elem = ctx.elem.take();
        if let Some(m) = received {
            let collection = ctx.elem.get_or_insert_with(&ctx.initializer);
            let _received = ctx.collector.collect(collection, m);
            debug_assert!(_received.is_none());
        }
        Poll::Ready(elem)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{mpsc, Mutex},
        thread, time,
    };

    use futures::{
        future::{self, BoxFuture, FutureExt},
        stream::{self, StreamExt},
        task::{self, ArcWake, Poll},
    };
    use tokio::runtime::Builder;

    use super::*;

    #[test]
    fn test_receiver() {
        let (tx, rx) = unbounded::<u64>(4);

        let msg_counter = Arc::new(AtomicUsize::new(0));
        let msg_counter1 = Arc::clone(&msg_counter);
        let pool = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let _res = pool.spawn(rx.for_each(move |_| {
            msg_counter1.fetch_add(1, Ordering::AcqRel);
            future::ready(())
        }));

        // Wait until the receiver is suspended.
        loop {
            thread::sleep(time::Duration::from_millis(10));
            if !tx.state.recv_task.load(Ordering::SeqCst).is_null() {
                break;
            }
        }

        // Send without notify, the receiver can't get batched messages.
        assert!(tx.send(0).is_ok());
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 0);

        // Send with notify.
        let notifier = tx.get_notifier().unwrap();
        assert!(tx.get_notifier().is_none());
        notifier.notify();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 1);

        // Auto notify with more sendings.
        for _ in 0..4 {
            assert!(tx.send(0).is_ok());
        }
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 5);
    }

    #[test]
    fn test_batch_receiver() {
        let (tx, rx) = unbounded::<u64>(4);

        let rx = BatchReceiver::new(rx, 8, || Vec::with_capacity(4), VecCollector);
        let msg_counter = Arc::new(AtomicUsize::new(0));
        let msg_counter_spawned = Arc::clone(&msg_counter);
        let (nty, polled) = mpsc::sync_channel(1);
        let pool = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let _res = pool.spawn(
            stream::select(
                rx,
                stream::poll_fn(move |_| -> Poll<Option<Vec<u64>>> {
                    nty.send(()).unwrap();
                    Poll::Ready(None)
                }),
            )
            .for_each(move |v| {
                let len = v.len();
                assert!(len <= 8);
                msg_counter_spawned.fetch_add(len, Ordering::AcqRel);
                future::ready(())
            }),
        );

        // Wait until the receiver has been polled in the spawned thread.
        polled.recv().unwrap();

        // Send without notify, the receiver can't get batched messages.
        assert!(tx.send(0).is_ok());
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 0);

        // Send with notify.
        let notifier = tx.get_notifier().unwrap();
        assert!(tx.get_notifier().is_none());
        notifier.notify();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 1);

        // Auto notify with more sendings.
        for _ in 0..16 {
            assert!(tx.send(0).is_ok());
        }
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 17);
    }

    #[test]
    fn test_switch_between_sender_and_receiver() {
        let (tx, mut rx) = unbounded::<i32>(4);
        let future = async move { rx.next().await };
        let task = Task {
            future: Arc::new(Mutex::new(Some(future.boxed()))),
        };
        // Receiver has not received any messages, so the future is not be finished
        // in this tick.
        task.tick();
        assert!(task.future.lock().unwrap().is_some());
        // After sender is dropped, the task will be waked and then it tick self
        // again to advance the progress.
        drop(tx);
        assert!(task.future.lock().unwrap().is_none());
    }

    #[derive(Clone)]
    struct Task {
        future: Arc<Mutex<Option<BoxFuture<'static, Option<i32>>>>>,
    }

    impl Task {
        fn tick(&self) {
            let task = Arc::new(self.clone());
            let mut future_slot = self.future.lock().unwrap();
            if let Some(mut future) = future_slot.take() {
                let waker = task::waker_ref(&task);
                let cx = &mut Context::from_waker(&*waker);
                match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        *future_slot = Some(future);
                    }
                    Poll::Ready(None) => {}
                    _ => unimplemented!(),
                }
            }
        }
    }

    impl ArcWake for Task {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.tick();
        }
    }
}
