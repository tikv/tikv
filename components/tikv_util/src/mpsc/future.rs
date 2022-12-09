// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A module provides the implementation of receiver that supports async/await.

use std::{
    pin::Pin,
    sync::atomic::{self, AtomicUsize, Ordering},
    task::{Context, Poll},
};

use crossbeam::{
    channel::{SendError, TryRecvError},
    queue::{ArrayQueue, SegQueue},
};
use futures::{task::AtomicWaker, Stream, StreamExt};

enum QueueType<T> {
    Unbounded(SegQueue<T>),
    Bounded(ArrayQueue<T>),
}

impl<T> QueueType<T> {
    fn len(&self) -> usize {
        match self {
            QueueType::Unbounded(q) => q.len(),
            QueueType::Bounded(q) => q.len(),
        }
    }

    fn bounded(cap: usize) -> QueueType<T> {
        QueueType::Bounded(ArrayQueue::new(cap))
    }

    fn unbounded() -> QueueType<T> {
        QueueType::Unbounded(SegQueue::new())
    }

    fn push_back(&self, t: T) -> Result<(), SendError<T>> {
        match self {
            QueueType::Unbounded(q) => {
                q.push(t);
                Ok(())
            }
            QueueType::Bounded(q) => q.push(t).map_err(SendError),
        }
    }

    fn pop_front(&self) -> Option<T> {
        match self {
            QueueType::Unbounded(q) => q.pop(),
            QueueType::Bounded(q) => q.pop(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum WakePolicy {
    Immediately,
    TillReach(usize),
}

struct Queue<T> {
    queue: QueueType<T>,
    waker: AtomicWaker,
    liveness: AtomicUsize,
    policy: WakePolicy,
}

impl<T> Queue<T> {
    #[inline]
    fn wake(&self, policy: WakePolicy) {
        match policy {
            WakePolicy::Immediately => self.waker.wake(),
            WakePolicy::TillReach(n) => {
                if self.queue.len() < n {
                    return;
                }
                self.waker.wake();
            }
        }
    }
}

const SENDER_COUNT_BASE: usize = 1 << 1;
const RECEIVER_COUNT_BASE: usize = 1;

pub struct Sender<T> {
    queue: *mut Queue<T>,
}

impl<T: Send> Sender<T> {
    /// Sends the message with predefined wake policy.
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        let policy = unsafe { (*self.queue).policy };
        self.send_with(t, policy)
    }

    /// Sends the message with the specified wake policy.
    #[inline]
    pub fn send_with(&self, t: T, policy: WakePolicy) -> Result<(), SendError<T>> {
        let queue = unsafe { &*self.queue };
        if queue.liveness.load(Ordering::Acquire) & RECEIVER_COUNT_BASE != 0 {
            let res = queue.queue.push_back(t);
            queue.wake(policy);
            return res;
        }
        Err(SendError(t))
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let queue = unsafe { &*self.queue };
        queue
            .liveness
            .fetch_add(SENDER_COUNT_BASE, Ordering::Relaxed);
        Self { queue: self.queue }
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        let queue = unsafe { &*self.queue };
        let previous = queue
            .liveness
            .fetch_sub(SENDER_COUNT_BASE, Ordering::Release);
        if previous == SENDER_COUNT_BASE | RECEIVER_COUNT_BASE {
            // The last sender is dropped, we need to wake up the receiver.
            queue.waker.wake();
        } else if previous == SENDER_COUNT_BASE {
            atomic::fence(Ordering::Acquire);
            drop(unsafe { Box::from_raw(self.queue) });
        }
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}

pub struct Receiver<T> {
    queue: *mut Queue<T>,
}

impl<T: Send> Stream for Receiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let queue = unsafe { &*self.queue };
        if let Some(t) = queue.queue.pop_front() {
            return Poll::Ready(Some(t));
        }
        queue.waker.register(cx.waker());
        // In case the message is pushed right before registering waker.
        if let Some(t) = queue.queue.pop_front() {
            return Poll::Ready(Some(t));
        }
        if queue.liveness.load(Ordering::Acquire) & !RECEIVER_COUNT_BASE != 0 {
            return Poll::Pending;
        }
        Poll::Ready(None)
    }
}

impl<T: Send> Receiver<T> {
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let queue = unsafe { &*self.queue };
        if let Some(t) = queue.queue.pop_front() {
            return Ok(t);
        }
        if queue.liveness.load(Ordering::Acquire) & !RECEIVER_COUNT_BASE != 0 {
            return Err(TryRecvError::Empty);
        }
        Err(TryRecvError::Disconnected)
    }
}

impl<T> Drop for Receiver<T> {
    #[inline]
    fn drop(&mut self) {
        let queue = unsafe { &*self.queue };
        if RECEIVER_COUNT_BASE
            == queue
                .liveness
                .fetch_sub(RECEIVER_COUNT_BASE, Ordering::Release)
        {
            atomic::fence(Ordering::Acquire);
            drop(unsafe { Box::from_raw(self.queue) });
        }
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

#[inline]
pub fn unbounded<T>(policy: WakePolicy) -> (Sender<T>, Receiver<T>) {
    with_queue(QueueType::unbounded(), policy)
}

#[inline]
pub fn bounded<T>(cap: usize, policy: WakePolicy) -> (Sender<T>, Receiver<T>) {
    with_queue(QueueType::bounded(cap), policy)
}

fn with_queue<T>(queue: QueueType<T>, policy: WakePolicy) -> (Sender<T>, Receiver<T>) {
    let queue = Box::into_raw(Box::new(Queue {
        queue,
        waker: AtomicWaker::new(),
        liveness: AtomicUsize::new(SENDER_COUNT_BASE | RECEIVER_COUNT_BASE),
        policy,
    }));
    (Sender { queue }, Receiver { queue })
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
            } else {
                break;
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
    fn test_till_reach_wake() {
        let (tx, rx) = unbounded::<u64>(WakePolicy::TillReach(4));

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
        let (tx, rx) = unbounded::<u64>(WakePolicy::TillReach(4));

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
        let (tx, mut rx) = unbounded::<i32>(WakePolicy::TillReach(4));
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
        assert!(!dropped.load(Ordering::SeqCst));

        tx.send(SetOnDrop::default()).unwrap_err();
        let tx1 = tx.clone();
        drop(tx);
        assert!(!dropped.load(Ordering::SeqCst));

        tx1.send(SetOnDrop::default()).unwrap_err();
        drop(tx1);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_bounded() {
        let (tx, mut rx) = super::bounded(1, WakePolicy::Immediately);
        tx.send(1).unwrap();
        tx.send(2).unwrap_err();
        assert_eq!(rx.try_recv().unwrap(), 1);
        rx.try_recv().unwrap_err();
    }
}
