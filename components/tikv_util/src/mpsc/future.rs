// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A module provides the implementation of receiver that supports async/await.

use std::{
    mem::ManuallyDrop,
    pin::Pin,
    ptr,
    sync::{
        atomic::{AtomicPtr, AtomicU8, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use crossbeam::channel::{self, SendError, TryRecvError, TrySendError};
use futures::{Stream, StreamExt};

pub enum WakePolicy {
    Immediately,
    // Notify by period.
    Period { step: u8, tick: AtomicU8 },
}

impl WakePolicy {
    pub fn period(step: u8) -> WakePolicy {
        WakePolicy::Period {
            step,
            tick: AtomicU8::new(0),
        }
    }
}

struct QueueState {
    waker: AtomicPtr<Waker>,
    policy: WakePolicy,
}

impl QueueState {
    #[inline]
    fn wake(&self, policy: &WakePolicy) {
        if let WakePolicy::Period { step, tick } = policy {
            let t = tick.fetch_add(1, Ordering::AcqRel);
            if t + 1 < *step {
                return;
            }
            tick.store(0, Ordering::Release);
        }
        let ptr = self.waker.swap(ptr::null_mut(), Ordering::AcqRel);
        unsafe {
            if !ptr.is_null() {
                Box::from_raw(ptr).wake();
            }
        }
    }

    // If there is already a waker, true is returned.
    fn register_waker(&self, waker: &Waker) -> bool {
        let w = Box::new(waker.clone());
        let ptr = self.waker.swap(Box::into_raw(w), Ordering::AcqRel);
        unsafe {
            if ptr.is_null() {
                false
            } else {
                drop(Box::from_raw(ptr));
                true
            }
        }
    }
}

impl Drop for QueueState {
    #[inline]
    fn drop(&mut self) {
        let ptr = self.waker.swap(ptr::null_mut(), Ordering::SeqCst);
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw(ptr));
            }
        }
    }
}

struct WakeOnDrop<T> {
    queue: Arc<QueueState>,
    sender: ManuallyDrop<channel::Sender<T>>,
}

impl<T> Drop for WakeOnDrop<T> {
    #[inline]
    fn drop(&mut self) {
        // Drop sender first, otherwise the receiver may not detect disconnection.
        unsafe { ManuallyDrop::drop(&mut self.sender) };
        self.queue.wake(&WakePolicy::Immediately);
    }
}

pub struct Sender<T> {
    sender: Arc<WakeOnDrop<T>>,
}

impl<T: Send> Sender<T> {
    /// Sends the message with predefined wake policy.
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        let policy = &self.sender.queue.policy;
        self.send_with(t, policy)
    }

    /// Sends the message with the specified wake policy.
    #[inline]
    fn send_with(&self, t: T, policy: &WakePolicy) -> Result<(), SendError<T>> {
        match self.sender.sender.try_send(t) {
            Ok(()) => (),
            Err(TrySendError::Disconnected(t)) => return Err(SendError(t)),
            // Channel is unbounded, can't be full.
            Err(TrySendError::Full(_)) => unreachable!(),
        }
        self.sender.queue.wake(policy);
        Ok(())
    }

    /// Send and notify immediately despite the configured wake policy.
    #[inline]
    pub fn send_immediately(&self, t: T) -> Result<(), SendError<T>> {
        if let WakePolicy::Period { tick, .. } = &self.sender.queue.policy {
            tick.store(0, Ordering::Release);
        }
        self.send_with(t, &WakePolicy::Immediately)
    }
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Sender<T> {
        Sender {
            sender: self.sender.clone(),
        }
    }
}

pub struct Receiver<T> {
    queue: Arc<QueueState>,
    receiver: channel::Receiver<T>,
}

impl<T: Send> Stream for Receiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.try_recv() {
            Ok(t) => Poll::Ready(Some(t)),
            Err(TryRecvError::Empty) => {
                // If there is no previous waker, we still need to poll again in case some
                // task is pushed before registering current waker.
                if self.queue.register_waker(cx.waker()) {
                    Poll::Pending
                } else {
                    // In case the message is pushed right before registering waker.
                    self.poll_next(cx)
                }
            }
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
        }
    }
}

impl<T: Send> Receiver<T> {
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }
}

pub fn unbounded<T>(policy: WakePolicy) -> (Sender<T>, Receiver<T>) {
    let queue = Arc::new(QueueState {
        waker: AtomicPtr::default(),
        policy,
    });
    let (sender, receiver) = channel::unbounded();
    (
        Sender {
            sender: Arc::new(WakeOnDrop {
                queue: queue.clone(),
                sender: ManuallyDrop::new(sender),
            }),
        },
        Receiver { queue, receiver },
    )
}

/// `BatchReceiver` is a `futures::Stream`, which returns a batched type.
pub struct BatchReceiver<T, I, C> {
    rx: Receiver<T>,
    max_batch_size: usize,
    initializer: I,
    collector: C,
}

impl<T, I, C> BatchReceiver<T, I, C> {
    /// Creates a new `BatchReceiver` with given `initializer` and `collector`.
    /// `initializer` is used to generate a initial value, and `collector`
    /// will collect every (at most `max_batch_size`) raw items into the
    /// batched value.
    pub fn new(rx: Receiver<T>, max_batch_size: usize, initializer: I, collector: C) -> Self {
        BatchReceiver {
            rx,
            max_batch_size,
            initializer,
            collector,
        }
    }
}

impl<T, E, I, C> Stream for BatchReceiver<T, I, C>
where
    T: Send + Unpin,
    E: Unpin,
    I: Fn() -> E + Unpin,
    C: FnMut(&mut E, T) + Unpin,
{
    type Item = E;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let ctx = self.get_mut();
        let mut collector = match ctx.rx.poll_next_unpin(cx) {
            Poll::Ready(Some(m)) => {
                let mut c = (ctx.initializer)();
                (ctx.collector)(&mut c, m);
                c
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        for _ in 1..ctx.max_batch_size {
            if let Poll::Ready(Some(m)) = ctx.rx.poll_next_unpin(cx) {
                (ctx.collector)(&mut collector, m);
            }
        }
        Poll::Ready(Some(collector))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize},
            mpsc, Arc, Mutex,
        },
        thread, time,
    };

    use futures::{
        future::{self, BoxFuture, FutureExt},
        stream::{self, StreamExt},
        task::{self, ArcWake, Poll},
    };
    use tokio::runtime::{Builder, Runtime};

    use super::*;

    fn spawn_and_wait<S: Stream + Send + 'static>(
        rx_builder: impl FnOnce() -> S,
    ) -> (Runtime, Arc<AtomicUsize>) {
        let msg_counter = Arc::new(AtomicUsize::new(0));
        let msg_counter1 = msg_counter.clone();
        let pool = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let (nty, polled) = mpsc::sync_channel(1);
        _ = pool.spawn(
            stream::select(
                rx_builder(),
                stream::poll_fn(move |_| -> Poll<Option<S::Item>> {
                    nty.send(()).unwrap();
                    Poll::Ready(None)
                }),
            )
            .for_each(move |_| {
                msg_counter1.fetch_add(1, Ordering::AcqRel);
                future::ready(())
            }),
        );

        // Wait until the receiver has been polled in the spawned thread.
        polled.recv().unwrap();
        (pool, msg_counter)
    }

    #[test]
    fn test_period_wake() {
        let (tx, rx) = unbounded::<u64>(WakePolicy::period(4));

        let (_pool, msg_counter) = spawn_and_wait(move || rx);

        // Receiver should not be woken up until its length reach specified value.
        for _ in 0..3 {
            tx.send(0).unwrap();
        }
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 0);

        tx.send(0).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 4);

        // Should start new batch.
        tx.send(0).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 4);

        let tx1 = tx.clone();
        drop(tx);
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 4);
        // If all senders are dropped, receiver should be woken up.
        drop(tx1);
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 5);
    }

    #[test]
    fn test_immediately_wake() {
        let (tx, rx) = unbounded::<u64>(WakePolicy::Immediately);

        let (_pool, msg_counter) = spawn_and_wait(move || rx);

        // Receiver should be woken up immediately.
        for _ in 0..3 {
            tx.send(0).unwrap();
        }
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 3);

        tx.send(0).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::Acquire), 4);
    }

    #[test]
    fn test_batch_receiver() {
        let (tx, rx) = unbounded::<u64>(WakePolicy::period(4));

        let len = Arc::new(AtomicUsize::new(0));
        let l = len.clone();
        let rx = BatchReceiver::new(rx, 8, || Vec::with_capacity(4), Vec::push);
        let (_pool, msg_counter) = spawn_and_wait(move || {
            stream::unfold((rx, l), |(mut rx, l)| async move {
                rx.next().await.map(|i| {
                    l.fetch_add(i.len(), Ordering::SeqCst);
                    (i, (rx, l))
                })
            })
        });

        tx.send(0).unwrap();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::SeqCst), 0);

        // Auto notify with more messages.
        for _ in 0..16 {
            tx.send(0).unwrap();
        }
        thread::sleep(time::Duration::from_millis(10));
        let batch_count = msg_counter.load(Ordering::SeqCst);
        assert!(batch_count < 17, "{}", batch_count);
        assert_eq!(len.load(Ordering::SeqCst), 17);
    }

    #[test]
    fn test_switch_between_sender_and_receiver() {
        let (tx, mut rx) = unbounded::<i32>(WakePolicy::period(4));
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
                let cx = &mut Context::from_waker(&waker);
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

    #[derive(Default)]
    struct SetOnDrop(Arc<AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn test_drop() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (tx, rx) = super::unbounded(WakePolicy::Immediately);
        tx.send(SetOnDrop(dropped.clone())).unwrap();
        drop(tx);
        assert!(!dropped.load(Ordering::SeqCst));

        drop(rx);
        assert!(dropped.load(Ordering::SeqCst));

        let dropped = Arc::new(AtomicBool::new(false));
        let (tx, rx) = super::unbounded(WakePolicy::Immediately);
        tx.send(SetOnDrop(dropped.clone())).unwrap();
        drop(rx);
        // Messages in queue should be dropped actively to release memory.
        assert!(dropped.load(Ordering::SeqCst));

        tx.send(SetOnDrop::default()).unwrap_err();
    }
}
