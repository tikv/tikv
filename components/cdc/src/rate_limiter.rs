// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use crossbeam::queue::SegQueue as Queue;
use futures::select;
use futures::task::{noop_waker_ref, AtomicWaker};
use futures::{future::Fuse, pin_mut, FutureExt, Sink, SinkExt, StreamExt};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{
    channel as async_channel, Receiver as AsyncReceiver, Sender as AsyncSender,
};
use tokio::time::interval as tokio_timer_interval;

#[derive(Debug, PartialEq, Eq)]
pub enum RateLimiterError {
    DisconnectedError,
    CongestedError(usize /* queue length when the congestion is detected */),
}

#[derive(Debug, PartialEq, Eq)]
pub enum DrainerError<RpcError> {
    RateLimitExceededError,
    RpcSinkError(RpcError),
}

pub struct RateLimiter<E> {
    sink: Sender<E>,
    close_tx: AsyncSender<()>,
    state: Arc<State>,
}

pub struct Drainer<E> {
    // the receiver of the internal queue
    receiver: Receiver<E>,
    // the receiver of the channel used for signalling exits.
    close_rx: Option<AsyncReceiver<()>>,
    // shared states with RateLimiter
    state: Arc<State>,
}

struct State {
    // used for configuration
    block_scan_threshold: usize,
    close_sink_threshold: usize,

    // used for fast-path check in senders.
    is_sink_closed: AtomicBool,
    // reference count for **RateLimiter**.
    ref_count: AtomicUsize,
    // used to store the wakers of the senders' tasks
    wait_queue: RwLock<Queue<Arc<Mutex<Option<Waker>>>>>,
    // used to store the waker of the drainer's task
    recv_task: AtomicWaker,

    #[cfg(test)]
    blocked_sender_count: AtomicUsize,
}

impl State {
    /// Should only be called from the drainer's task.
    #[inline]
    fn yield_drainer(&self, cx: &mut Context<'_>) {
        self.recv_task.register(cx.waker());
    }

    /// Should only be called from the drainer's task.
    #[inline]
    fn unyield_drainer(&self) {
        let _ = self.recv_task.take();
    }

    /// Should only be called from a sender's task.
    #[inline]
    fn wake_up_drainer(&self) {
        self.recv_task.wake();
    }

    /// Wakes up at most one blocked sender. It is no-op if none is blocked.
    /// Should only be called from the drainer's task.
    fn wake_up_one_sender(&self) {
        // Acquires the write lock on wait_queue.
        let queue = self.wait_queue.write().unwrap();
        loop {
            match queue.pop() {
                Ok(waker) => {
                    let mut waker = waker.lock().unwrap();
                    // waker may have already been taken away, because the runtime has
                    // decided to poll the sender for some reason and the sender has unblocked itself.
                    if let Some(waker) = waker.take() {
                        waker.wake();
                        return;
                    }
                }
                // Queue is empty now.
                Err(_) => break,
            }
        }
    }

    /// Wakes up all blocked senders. It is no-op if none is blocked.
    /// Should only be called from the drainer's task.
    fn wake_up_all_senders(&self) {
        // Acquires the write lock on wait_queue.
        let queue = self.wait_queue.write().unwrap();
        loop {
            match queue.pop() {
                Ok(waker) => {
                    let mut waker = waker.lock().unwrap();
                    // see more comments on implementation details in wake_up_one_sender.
                    if let Some(waker) = waker.take() {
                        waker.wake();
                    }
                }
                // Queue is empty now.
                Err(_) => break,
            }
        }
    }
}

pub fn new_pair<E>(
    block_scan_threshold: usize,
    close_sink_threshold: usize,
) -> (RateLimiter<E>, Drainer<E>) {
    let (sender, receiver) = unbounded::<E>();
    let state = Arc::new(State {
        is_sink_closed: AtomicBool::new(false),
        block_scan_threshold,
        close_sink_threshold,
        ref_count: AtomicUsize::new(0),
        wait_queue: RwLock::new(Queue::new()),
        recv_task: AtomicWaker::new(),
        #[cfg(test)]
        blocked_sender_count: AtomicUsize::new(0),
    });
    let (close_tx, close_rx) = async_channel::<()>(1);
    let rate_limiter = RateLimiter::new(sender, state.clone(), close_tx);
    let drainer = Drainer::new(receiver, state, close_rx);

    (rate_limiter, drainer)
}

impl<E> RateLimiter<E> {
    fn new(sink: Sender<E>, state: Arc<State>, close_tx: AsyncSender<()>) -> RateLimiter<E> {
        state.ref_count.fetch_add(1, Ordering::SeqCst);
        return RateLimiter {
            sink,
            close_tx,
            state,
        };
    }

    pub fn send_realtime_event(&self, event: E) -> Result<(), RateLimiterError> {
        if self.state.is_sink_closed.load(Ordering::SeqCst) {
            return Err(RateLimiterError::DisconnectedError);
        }

        let queue_size = self.sink.len();
        CDC_SINK_QUEUE_SIZE_HISTOGRAM.observe(queue_size as f64);
        if queue_size >= self.state.close_sink_threshold {
            warn!("cdc send_realtime_event queue length reached threshold"; "queue_size" => queue_size);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            let _ = self.close_tx.clone().try_send(());
            return Err(RateLimiterError::CongestedError(queue_size));
        }

        self.sink.try_send(event).map_err(|e| {
            warn!("cdc send_realtime_event error"; "err" => ?e);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            RateLimiterError::DisconnectedError
        })?;

        self.state.wake_up_drainer();

        Ok(())
    }

    pub async fn send_scan_event(&self, event: E) -> Result<(), RateLimiterError> {
        let sink_clone = self.sink.clone();
        let state_clone = self.state.clone();
        let threshold = self.state.block_scan_threshold;

        let timer = CDC_SCAN_BLOCK_DURATION_HISTOGRAM.start_coarse_timer();
        BlockSender::block_sender(self.state.as_ref(), move || {
            sink_clone.len() >= threshold && !state_clone.is_sink_closed.load(Ordering::SeqCst)
        })
        .await;

        if self.state.is_sink_closed.load(Ordering::SeqCst) {
            return Err(RateLimiterError::DisconnectedError);
        }

        timer.observe_duration();

        match self.sink.try_send(event) {
            Ok(_) => {
                self.state.wake_up_drainer();
                Ok(())
            }
            Err(_err) => {
                // We don't need to care about the value of `_err`, because since
                // we are using the unbounded queue, `_err` is bound to be `TrySendError::Disconnected`.
                return Err(RateLimiterError::DisconnectedError)
            }
        }
    }

    pub fn notify_close(self) {
        self.state.is_sink_closed.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn inject_instant_drainer_exit(&self) {
        let _ = self.close_tx.clone().try_send(());
        self.state.wake_up_all_senders();
        self.state.recv_task.wake();
    }
}

impl<E> Clone for RateLimiter<E> {
    fn clone(&self) -> Self {
        self.state.ref_count.fetch_add(1, Ordering::SeqCst);
        RateLimiter {
            sink: self.sink.clone(),
            close_tx: self.close_tx.clone(),
            state: self.state.clone(),
        }
    }
}

impl<E> Drop for RateLimiter<E> {
    fn drop(&mut self) {
        if self.state.ref_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ = self.close_tx.try_send(());
        }
    }
}

struct BlockSender<'a, Cond>
where
    Cond: Fn() -> bool + Unpin + 'a,
{
    state: &'a State,
    cond: Cond,
    waker: Option<Arc<Mutex<Option<Waker>>>>,
}

impl<'a, Cond> BlockSender<'a, Cond>
where
    Cond: Fn() -> bool + Unpin + 'a,
{
    fn block_sender(state: &'a State, cond: Cond) -> Self {
        Self {
            state,
            cond,
            waker: None,
        }
    }
}

impl<'a, Cond> Future for BlockSender<'a, Cond>
where
    Cond: Fn() -> bool + Unpin + 'a,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !(self.cond)() || self.state.is_sink_closed.load(Ordering::SeqCst) {
            let mut_self = self.get_mut();
            if let Some(waker_arc) = mut_self.waker.take() {
                waker_arc.lock().unwrap().take();
                #[cfg(test)]
                {
                    let prev_count = mut_self
                        .state
                        .blocked_sender_count
                        .fetch_sub(1, Ordering::SeqCst);
                    debug_assert!(prev_count > 0, "prev_count = {}", prev_count);
                }
            }
            Poll::Ready(())
        } else {
            let queue = self.state.wait_queue.read().unwrap();
            if !(self.cond)() || self.state.is_sink_closed.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                if self.waker.is_some() {
                    return Poll::Pending;
                }
                let waker_arc = Arc::new(Mutex::new(Some(cx.waker().clone())));
                queue.push(waker_arc.clone());
                let mut mut_self = self.get_mut();
                mut_self.waker = Some(waker_arc);

                #[cfg(test)]
                {
                    mut_self
                        .state
                        .blocked_sender_count
                        .fetch_add(1, Ordering::SeqCst);
                }

                Poll::Pending
            }
        }
    }
}

impl<E> Drainer<E> {
    fn new(receiver: Receiver<E>, state: Arc<State>, close_rx: AsyncReceiver<()>) -> Drainer<E> {
        Drainer {
            receiver,
            close_rx: Some(close_rx),
            state,
        }
    }

    pub async fn drain<F: Copy, Error, S: Sink<(E, F), Error = Error> + Unpin>(
        mut self,
        mut rpc_sink: S,
        flag: F,
    ) -> Result<(), DrainerError<Error>> {
        let mut close_rx = self.close_rx.take().unwrap().fuse();
        let mut unflushed_size: usize = 0;
        let mut interval = tokio_timer_interval(std::time::Duration::from_millis(100)).fuse();
        loop {
            let drain_one = Fuse::terminated();
            pin_mut!(drain_one);
            let sink_ready = Fuse::terminated();
            pin_mut!(sink_ready);

            // We try to poll the rpc_sink to determine if it is ready to accept new data.
            let mut noop_context = Context::from_waker(noop_waker_ref());
            if let Poll::Ready(_) = rpc_sink.poll_ready_unpin(&mut noop_context) {
                drain_one.set(DrainOne::wrap(&self.receiver, self.state.as_ref()).fuse());
            } else {
                sink_ready.set(
                    RpcSinkReady {
                        sink: &mut rpc_sink,
                        _phantom: Default::default(),
                    }
                    .fuse(),
                );
            }

            select! {
                // handles the event where the downstream sink becomes ready
                sink_ready_status = sink_ready => {
                    sink_ready_status.map_err(|err| {
                        self.state.wake_up_all_senders();
                        DrainerError::RpcSinkError(err)
                    })?;
                },

                // handles the event where one event is successfully consumed from the upstream queue
                next_event = drain_one => {
                    match next_event {
                        Some(v) => {
                            // One event has been successfully taken out of the queue,
                            // so one sender (who has been blocking) can now proceed.
                            self.state.wake_up_one_sender();
                            rpc_sink.start_send_unpin((v, flag))
                                .map_err(|err| {
                                    self.state.wake_up_all_senders();
                                    DrainerError::RpcSinkError(err)
                                })?;

                            unflushed_size += 1;
                            if unflushed_size >= 128 {
                                rpc_sink.flush().await.map_err(|err| {
                                    self.state.wake_up_all_senders();
                                    DrainerError::RpcSinkError(err)
                                })?;
                                unflushed_size = 0;
                            }
                        },
                        None => {
                            // The upstream queue has closed.
                            return Ok(())
                        },
                    }
                },

                // handles a close signal, where usually means that congestion has caused the drainer to abort.
                _ = close_rx.next() => {
                    self.state.wake_up_all_senders();
                    return Err(DrainerError::RateLimitExceededError);
                },

                // handles the timer event that flushes the sink periodically
                _ = interval.next() => {
                    rpc_sink.flush().await.map_err(|err| {
                        self.state.wake_up_all_senders();
                        DrainerError::RpcSinkError(err)
                    })?;
                }
            }
        }
    }
}

impl<E> Drop for Drainer<E> {
    fn drop(&mut self) {
        self.state.is_sink_closed.store(true, Ordering::SeqCst);
        self.state.wake_up_all_senders();
    }
}

struct DrainOne<'a, E> {
    receiver: &'a Receiver<E>,
    state: &'a State,
    terminated: AtomicBool,
}

impl<'a, E> DrainOne<'a, E> {
    fn wrap(receiver: &'a Receiver<E>, state: &'a State) -> Self {
        Self {
            receiver,
            state,
            terminated: AtomicBool::new(false),
        }
    }
}

struct RpcSinkReady<'a, I, S: Sink<I> + Unpin> {
    sink: &'a mut S,
    _phantom: PhantomData<&'a I>,
}

impl<'a, I, S> Future for RpcSinkReady<'a, I, S>
where
    S: Sink<I> + Unpin,
{
    type Output = Result<(), <S as Sink<I>>::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().sink.poll_ready_unpin(cx)
    }
}

impl<'a, E> Future for DrainOne<'a, E> {
    // TODO returns a Result to provide details on errors.
    type Output = Option<E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if self.receiver.is_empty() && self.state.ref_count.load(Ordering::SeqCst) == 0 {
                println!("wee0");
                return Poll::Ready(None);
            }
            return match self.receiver.try_recv() {
                Ok(v) => {
                    self.terminated.store(true, Ordering::SeqCst);
                    Poll::Ready(Some(v))
                }
                Err(TryRecvError::Empty) => {
                    self.state.yield_drainer(cx);
                    if !self.receiver.is_empty() || self.state.ref_count.load(Ordering::SeqCst) == 0
                    {
                        self.state.unyield_drainer();
                        continue;
                    }
                    Poll::Pending
                }
                Err(TryRecvError::Disconnected) => {
                    self.terminated.store(true, Ordering::SeqCst);
                    println!("wee1");
                    Poll::Ready(None)
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    type MockCdcEvent = u64;
    /// MockWriteFlag is used to mock grpcio::WriteFlags,
    /// which is required for writing into the grpc stream sink (grpcio::DuplexSink).
    type MockWriteFlag = ();

    #[derive(Debug, PartialEq, Eq)]
    enum MockRpcError {
        SinkClosed,
        InjectedRpcError,
    }

    /// MockRpcSink is a mock for grpcio::DuplexSink.
    /// It has an internal buffer of size 1.
    #[derive(Clone)]
    struct MockRpcSink {
        // the internal buffer. It has only one slot.
        value: Arc<Mutex<Option<MockCdcEvent>>>,
        send_waker: Arc<AtomicWaker>,
        recv_waker: Arc<AtomicWaker>,
        injected_send_error: Arc<Mutex<Option<MockRpcError>>>,
        sink_closed: Arc<AtomicBool>,
    }

    /// MockRpcSinkBlockRecv is an auxiliary data structure for implementing
    /// MockRpcSink::recv.
    struct MockRpcSinkBlockRecv<'a, Cond>
    where
        Cond: Fn() -> bool + 'a,
    {
        sink: &'a MockRpcSink,
        cond: Cond,
    }

    impl<'a, Cond> MockRpcSinkBlockRecv<'a, Cond>
    where
        Cond: Fn() -> bool + 'a,
    {
        fn new(sink: &'a MockRpcSink, cond: Cond) -> Self {
            Self { sink, cond }
        }
    }

    impl<'a, Cond> Future for MockRpcSinkBlockRecv<'a, Cond>
    where
        Cond: Fn() -> bool + 'a,
    {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !(self.cond)() {
                Poll::Ready(())
            } else {
                self.sink.recv_waker.register(cx.waker());
                if !(self.cond)() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }
    }

    impl MockRpcSink {
        fn new() -> MockRpcSink {
            MockRpcSink {
                value: Arc::new(Mutex::new(None)),
                send_waker: Arc::new(AtomicWaker::new()),
                recv_waker: Arc::new(AtomicWaker::new()),
                injected_send_error: Arc::new(Mutex::new(None)),
                sink_closed: Arc::new(AtomicBool::new(false)),
            }
        }

        async fn recv(&self) -> Option<MockCdcEvent> {
            let value_clone = self.value.clone();
            MockRpcSinkBlockRecv::new(self, move || value_clone.lock().unwrap().is_none()).await;

            let ret = self.value.lock().unwrap().take();
            self.send_waker.wake();
            ret
        }

        fn inject_send_error(&self, err: MockRpcError) {
            *self.injected_send_error.lock().unwrap() = Some(err);
        }
    }

    impl Sink<(MockCdcEvent, MockWriteFlag)> for MockRpcSink {
        type Error = MockRpcError;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.sink_closed.load(Ordering::SeqCst) {
                return Poll::Ready(Err(MockRpcError::SinkClosed));
            }
            if let Some(err) = self.injected_send_error.lock().unwrap().take() {
                return Poll::Ready(Err(err));
            }
            let value_guard = self.value.lock().unwrap();
            if value_guard.is_none() {
                Poll::Ready(Ok(()))
            } else {
                self.send_waker.register(cx.waker());
                Poll::Pending
            }
        }

        fn start_send(self: Pin<&mut Self>, item: (u64, MockWriteFlag)) -> Result<(), Self::Error> {
            if self.sink_closed.load(Ordering::SeqCst) {
                return Err(MockRpcError::SinkClosed);
            }
            if let Some(err) = self.injected_send_error.lock().unwrap().take() {
                return Err(err);
            }
            let (value, _) = item;
            *self.value.lock().unwrap() = Some(value);
            self.recv_waker.wake();
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if let Some(err) = self.injected_send_error.lock().unwrap().take() {
                return Poll::Ready(Err(err));
            }
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if let Some(err) = self.injected_send_error.lock().unwrap().take() {
                return Poll::Ready(Err(err));
            }
            self.sink_closed.store(true, Ordering::SeqCst);
            self.recv_waker.wake();
            Poll::Ready(Ok(()))
        }
    }

    /// test_basic_realtime tests the situation where a sender sends 10 real-time events consecutively,
    /// and then the receiver reads them.
    #[tokio::test]
    async fn test_basic_realtime() -> Result<(), RateLimiterError> {
        println!("started test");
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        for i in 0..10u64 {
            rate_limiter.send_realtime_event(i)?;
            tokio::task::yield_now().await;
        }

        for i in 0..10u64 {
            assert_eq!(mock_sink.recv().await.unwrap(), i);
        }

        mock_sink.close().await.unwrap();
        drain_handle.await.unwrap();
        Ok(())
    }

    /// test_basic_scan tests the situation where a sender sends 10 scan events consecutively,
    /// and the the receiver reads them. Blocking is NOT expected in the test.
    #[tokio::test]
    async fn test_basic_scan() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        // to give the drainer a chance to run,
        // so that its implementation for Future can be properly tested.
        tokio::task::yield_now().await;

        for i in 0..10u64 {
            rate_limiter.send_scan_event(i).await?;
            tokio::task::yield_now().await;
        }

        for i in 0..10u64 {
            assert_eq!(mock_sink.recv().await.unwrap(), i);
        }

        mock_sink.close().await.unwrap();
        drain_handle.await.unwrap();
        Ok(())
    }

    /// test_realtime_disconnected tests the situation where the drainer is dropped for some reason
    /// (usually due to congestion protection), and expects that we will NO LONGER be able to send
    /// real-time events.
    #[tokio::test]
    async fn test_realtime_disconnected() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        // to give the drainer a chance to run,
        // so that its implementation for Future can be properly tested.
        tokio::task::yield_now().await;

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;

        rate_limiter.inject_instant_drainer_exit();
        // wait for the drainer to drop
        drain_handle.await.unwrap();

        assert_eq!(
            rate_limiter.send_realtime_event(4),
            Err(RateLimiterError::DisconnectedError)
        );
        assert_eq!(
            rate_limiter.send_realtime_event(5),
            Err(RateLimiterError::DisconnectedError)
        );

        mock_sink.close().await.unwrap();
        Ok(())
    }

    /// test_scan_disconnected tests the situation where the drainer is dropped and expects that we
    /// will NO LONGER be able to send scan events.
    #[tokio::test]
    async fn test_scan_disconnected() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        // to give the drainer a chance to run,
        // so that its implementation for Future can be properly tested.
        tokio::task::yield_now().await;

        rate_limiter.send_scan_event(1).await?;
        rate_limiter.send_scan_event(2).await?;
        rate_limiter.send_scan_event(3).await?;

        rate_limiter.inject_instant_drainer_exit();
        // wait for the drainer to drop
        drain_handle.await.unwrap();

        assert_eq!(
            rate_limiter.send_scan_event(4).await,
            Err(RateLimiterError::DisconnectedError)
        );
        assert_eq!(
            rate_limiter.send_scan_event(5).await,
            Err(RateLimiterError::DisconnectedError)
        );

        mock_sink.close().await.unwrap();
        Ok(())
    }

    /// test_realtime_congested tests that congestions where the queue is longer than `close_sink_threshold`
    /// closes the drainer as expected.
    #[tokio::test]
    async fn test_realtime_congested() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 5);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        // to give the drainer a chance to run,
        // so that its implementation for Future can be properly tested.
        tokio::task::yield_now().await;

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;
        rate_limiter.send_realtime_event(4)?;
        rate_limiter.send_realtime_event(5)?;
        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::CongestedError(len)) => assert_eq!(len, 5),
            _ => panic!("expected CongestedError"),
        }

        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(err) => assert_eq!(err, RateLimiterError::DisconnectedError),
        }

        mock_sink.close().await.unwrap();
        drain_handle.await.unwrap();
        Ok(())
    }

    /// test_scan_block_normal tests that congestions where the queue is longer than `block_scan_threshold`
    /// but shorter than `close_sink_threshold` blocks the senders of scan events as expected, and that
    /// they get unblocked when the congested events are finally consumed.
    #[tokio::test]
    async fn test_scan_block_normal() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(5, 1024);
        let mut mock_sink = MockRpcSink::new();

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_scan_event(3).await?;
        rate_limiter.send_scan_event(4).await?;
        rate_limiter.send_scan_event(5).await?;
        assert_eq!(
            rate_limiter
                .state
                .blocked_sender_count
                .load(Ordering::SeqCst),
            0
        );

        let rate_limiter_clone = rate_limiter.clone();
        let handle = tokio::spawn(async move {
            rate_limiter_clone.send_scan_event(6).await.unwrap();
        });

        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            rate_limiter
                .state
                .blocked_sender_count
                .load(Ordering::SeqCst),
            1
        );

        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));
        assert_eq!(mock_sink.recv().await.unwrap(), 1);
        assert_eq!(mock_sink.recv().await.unwrap(), 2);
        assert_eq!(mock_sink.recv().await.unwrap(), 3);
        assert_eq!(mock_sink.recv().await.unwrap(), 4);
        assert_eq!(mock_sink.recv().await.unwrap(), 5);
        assert_eq!(mock_sink.recv().await.unwrap(), 6);

        mock_sink.close().await.unwrap();
        drain_handle.await.unwrap();
        handle.await.unwrap();
        Ok(())
    }

    /// test_scan_block_disconnected tests that senders that are blocked due to congestion gets unblocked
    /// when the drainer is dropped.
    #[tokio::test]
    async fn test_scan_block_disconnected() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(5, 1024);
        let mut mock_sink = MockRpcSink::new();

        rate_limiter.send_realtime_event(1)?;
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));
        // Waits for the drainer to drain one event (given that MockRpcSink has internal buffer size 1),
        // so that the queue length becomes predictable for this unit test.
        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;

        assert_eq!(rate_limiter.sink.len(), 0);
        rate_limiter.send_realtime_event(2)?;
        assert_eq!(rate_limiter.sink.len(), 1);
        rate_limiter.send_scan_event(3).await?;
        assert_eq!(rate_limiter.sink.len(), 2);
        rate_limiter.send_scan_event(4).await?;
        assert_eq!(rate_limiter.sink.len(), 3);
        rate_limiter.send_scan_event(5).await?;
        assert_eq!(rate_limiter.sink.len(), 4);
        rate_limiter.send_scan_event(6).await?;

        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            rate_limiter
                .state
                .blocked_sender_count
                .load(Ordering::SeqCst),
            0
        );

        let rate_limiter_clone = rate_limiter.clone();

        let handle = tokio::spawn(async move {
            assert_eq!(rate_limiter_clone.sink.len(), 5);
            // A queue length of 5 implies the blocking of the subsequent send_scan_event call.
            match rate_limiter_clone.send_scan_event(7).await {
                Ok(_) => panic!("expected error"),
                Err(err) => assert_eq!(err, RateLimiterError::DisconnectedError),
            }
        });

        // waits for the sender to be blocked.
        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        // asserts that the sender has been blocked
        assert_eq!(
            rate_limiter
                .state
                .blocked_sender_count
                .load(Ordering::SeqCst),
            1
        );
        // injects a drainer exit
        rate_limiter.inject_instant_drainer_exit();
        // wait for the drainer to drop
        drain_handle.await.unwrap();

        mock_sink.close().await.unwrap();
        handle.await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_rpc_sink_error() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        for i in 0..10u64 {
            rate_limiter.send_realtime_event(i)?;
            tokio::task::yield_now().await;
        }

        mock_sink.inject_send_error(MockRpcError::InjectedRpcError);
        let res = drain_handle.await.unwrap();
        assert_eq!(res, Err(DrainerError::RpcSinkError(MockRpcError::InjectedRpcError)));

        Ok(())
    }

    #[tokio::test]
    async fn test_single_thread_many_events() -> Result<(), RateLimiterError> {
        let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        tokio::task::yield_now().await;

        let verifier_handler = tokio::spawn(async move {
            for i in 0..10000000u64 {
                assert_eq!(mock_sink.recv().await.unwrap(), i);
            }
            let close_res = mock_sink.close().await;
            assert_eq!(close_res, Ok(()));
        });

        for i in 0..10000000u64 {
            rate_limiter.send_scan_event(i).await?;
        }

        verifier_handler.await.unwrap();
        let drain_res = drain_handle.await.unwrap();
        assert_eq!(drain_res, Err(DrainerError::RpcSinkError(MockRpcError::SinkClosed)));

        Ok(())
    }
}
