// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crossbeam::channel::TrySendError;
use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use crossbeam::queue::SegQueue as Queue;
use futures::future::FusedFuture;
use futures::select;
use futures::task::{noop_waker, noop_waker_ref, AtomicWaker};
use futures::{future::Fuse, pin_mut, FutureExt, Sink, SinkExt, StreamExt};
use std::cmp::min;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc::{
    channel as async_channel, Receiver as AsyncReceiver, Sender as AsyncSender,
};
use tokio::time::interval as tokio_timer_interval;

#[derive(Debug, PartialEq, Eq)]
pub enum RateLimiterError<E> {
    SenderError(TrySendError<E>),
    SinkClosedError(usize),
    TryAgainError,
}

#[derive(Debug)]
pub enum DrainerError {
    RateLimitExceededError,
    RpcSinkError,
}

pub struct RateLimiter<E> {
    sink: Sender<E>,
    close_tx: AsyncSender<()>,
    state: Arc<State>,
}

pub struct Drainer<E> {
    receiver: Receiver<E>,
    close_rx: Option<AsyncReceiver<()>>,
    state: Arc<State>,
}

struct State {
    is_sink_closed: AtomicBool,
    block_scan_threshold: usize,
    close_sink_threshold: usize,
    ref_count: AtomicUsize,
    wait_queue: RwLock<Queue<Arc<Mutex<Option<Waker>>>>>,
    recv_task: AtomicWaker,

    #[cfg(test)]
    blocked_sender_count: AtomicUsize,
}

impl State {
    fn yield_drain(&self, cx: &mut Context<'_>) {
        self.recv_task.register(cx.waker());
    }

    fn unyield_drain(&self) {
        let _ = self.recv_task.take();
    }

    fn wake_up_drain(&self) {
        self.recv_task.wake();
    }

    fn wake_up_one(&self) {
        println!("wake_up_one");
        let queue = self.wait_queue.write().unwrap();
        loop {
            match queue.pop() {
                Ok(waker) => {
                    let mut waker = waker.lock().unwrap();
                    if let Some(waker) = waker.take() {
                        waker.wake();
                        println!("wake_up_one done");
                        return;
                    }
                }
                Err(_) => break,
            }
        }
    }

    fn wake_up_all(&self) {
        println!("wake_up_all");
        let queue = self.wait_queue.write().unwrap();
        loop {
            match queue.pop() {
                Ok(waker) => {
                    let mut waker = waker.lock().unwrap();
                    if let Some(waker) = waker.take() {
                        waker.wake();
                    }
                }
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

    pub fn send_realtime_event(&self, event: E) -> Result<(), RateLimiterError<E>> {
        if self.state.is_sink_closed.load(Ordering::SeqCst) {
            return Err(RateLimiterError::SinkClosedError(0));
        }

        let queue_size = self.sink.len();
        CDC_SINK_QUEUE_SIZE_HISTOGRAM.observe(queue_size as f64);
        if queue_size >= self.state.close_sink_threshold {
            warn!("cdc send_realtime_event queue length reached threshold"; "queue_size" => queue_size);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            let _ = self.close_tx.clone().try_send(());
            return Err(RateLimiterError::SinkClosedError(queue_size));
        }

        self.sink.try_send(event).map_err(|e| {
            warn!("cdc send_realtime_event error"; "err" => ?e);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            RateLimiterError::SenderError(e)
        })?;

        self.state.wake_up_drain();

        Ok(())
    }

    pub async fn send_scan_event(&self, event: E) -> Result<(), RateLimiterError<E>> {
        let mut attempts: u64 = 0;
        let sink_clone = self.sink.clone();
        let state_clone = self.state.clone();
        let threshold = self.state.block_scan_threshold;

        let timer = CDC_SCAN_BLOCK_DURATION_HISTOGRAM.start_coarse_timer();
        BlockSender::block_sender(self.state.as_ref(), move || {
            sink_clone.len() >= threshold && !state_clone.is_sink_closed.load(Ordering::SeqCst)
        })
        .await;

        if self.state.is_sink_closed.load(Ordering::SeqCst) {
            return Err(RateLimiterError::SinkClosedError(0));
        }

        timer.observe_duration();

        let state_clone = self.state.clone();
        self.sink.try_send(event).map_err(|e| {
            warn!("cdc send_scan_event error"; "err" => ?e);
            match e {
                crossbeam::TrySendError::Disconnected(_) => {
                    state_clone.is_sink_closed.store(true, Ordering::SeqCst);
                    info!("cdc sink closed");
                }
                _ => {}
            }
            RateLimiterError::SenderError(e)
        })?;

        self.state.wake_up_drain();
        Ok(())
    }

    pub fn notify_close(self) {
        self.state.is_sink_closed.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn inject_instant_drainer_exit(&self) {
        let _ = self.close_tx.clone().try_send(());
        self.state.wake_up_all();
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
        println!("poll");
        if !(self.cond)() || self.state.is_sink_closed.load(Ordering::SeqCst) {
            println!("poll 1");
            let mut mut_self = self.get_mut();
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
                println!("poll 2");
                Poll::Ready(())
            } else {
                println!("poll 3");
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
    ) -> Result<(), DrainerError> {
        let mut close_rx = self.close_rx.take().unwrap().fuse();
        let mut unflushed_size: usize = 0;
        let mut interval = tokio_timer_interval(std::time::Duration::from_millis(100)).fuse();
        loop {
            let mut drain_one = Fuse::terminated();
            pin_mut!(drain_one);
            let mut sink_ready = Fuse::terminated();
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
                sink_ready_status = sink_ready => {
                    match sink_ready_status {
                        Ok(_) => continue,
                        Err(_) => {
                            // The rpc sink has returned an error when being polled for readiness.
                            println!("rpc sink error");
                            self.state.wake_up_all();
                            return Err(DrainerError::RpcSinkError);
                        }
                    }
                },
                next = drain_one => {
                    info!("drained one!");
                    match next {
                        Some(v) => {
                            // One event has been successfully taken out of the queue,
                            // so one sender (who has been blocking) can now proceed.
                            self.state.wake_up_one();
                            match rpc_sink.start_send_unpin((v, flag)) {
                                Ok(_) => {
                                    unflushed_size += 1;
                                    if unflushed_size >= 128 {
                                        match rpc_sink.flush().await {
                                            Ok(_) => {
                                                unflushed_size = 0;
                                            },
                                            Err(_) => {
                                                // The rpc sink has returned an error when being flushed.
                                                println!("rpc sink error");
                                                self.state.wake_up_all();
                                                return Err(DrainerError::RpcSinkError);
                                            }
                                        }
                                    }
                                },
                                Err(_) => {
                                        // The rpc sink has returned an error when being written to.
                                        println!("rpc sink error");
                                        self.state.wake_up_all();
                                        return Err(DrainerError::RpcSinkError);
                                },
                            }
                        },
                        None => {
                            println!("drainer received none");
                            return Ok(())
                        },
                    }
                },

                _ = close_rx.next() => {
                    println!("drainer got exit signal");
                    self.state.wake_up_all();
                    return Err(DrainerError::RateLimitExceededError);
                },

                _ = interval.next() => {
                    match rpc_sink.flush().await {
                        Ok(_) => {
                            unflushed_size = 0;
                        },
                        Err(_) => {
                            // The rpc sink has returned an error when being flushed.
                            println!("rpc sink error");
                            self.state.wake_up_all();
                            return Err(DrainerError::RpcSinkError);
                        }
                    }
                }
            }
        }
    }
}

impl<E> Drop for Drainer<E> {
    fn drop(&mut self) {
        self.state.is_sink_closed.store(true, Ordering::SeqCst);
        self.state.wake_up_all();
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
                    self.state.yield_drain(cx);
                    if !self.receiver.is_empty() || self.state.ref_count.load(Ordering::SeqCst) == 0
                    {
                        self.state.unyield_drain();
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

    #[derive(Debug)]
    enum MockRpcError {
        RpcFailure,
        SinkClosed,
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
            let mut value_guard = self.value.lock().unwrap();
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

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.sink_closed.store(true, Ordering::SeqCst);
            self.recv_waker.wake();
            Poll::Ready(Ok(()))
        }
    }

    /// test_basic_realtime tests the situation where a sender sends 10 real-time events consecutively,
    /// and then the receiver reads them.
    #[tokio::test]
    async fn test_basic_realtime() -> Result<(), RateLimiterError<MockCdcEvent>> {
        println!("started test");
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        for i in 0..10u64 {
            rate_limiter.send_realtime_event(i)?;
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
    async fn test_basic_scan() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        for i in 0..10u64 {
            rate_limiter.send_scan_event(i).await?;
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
    async fn test_realtime_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;

        rate_limiter.inject_instant_drainer_exit();
        // wait for the drainer to drop
        drain_handle.await.unwrap();

        assert_eq!(
            rate_limiter.send_realtime_event(4),
            Err(RateLimiterError::SinkClosedError(0))
        );
        assert_eq!(
            rate_limiter.send_realtime_event(5),
            Err(RateLimiterError::SinkClosedError(0))
        );

        mock_sink.close().await.unwrap();
        Ok(())
    }

    /// test_scan_disconnected tests the situation where the drainer is dropped and expects that we
    /// will NO LONGER be able to send scan events.
    #[tokio::test]
    async fn test_scan_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 1024);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        rate_limiter.send_scan_event(1).await?;
        rate_limiter.send_scan_event(2).await?;
        rate_limiter.send_scan_event(3).await?;

        rate_limiter.inject_instant_drainer_exit();
        // wait for the drainer to drop
        drain_handle.await.unwrap();

        assert_eq!(
            rate_limiter.send_scan_event(4).await,
            Err(RateLimiterError::SinkClosedError(0))
        );
        assert_eq!(
            rate_limiter.send_scan_event(5).await,
            Err(RateLimiterError::SinkClosedError(0))
        );

        mock_sink.close().await.unwrap();
        Ok(())
    }

    /// test_realtime_congested tests that congestions where the queue is longer than `close_sink_threshold`
    /// closes the drainer as expected.
    #[tokio::test]
    async fn test_realtime_congested() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(1024, 5);
        let mut mock_sink = MockRpcSink::new();
        let drain_handle = tokio::spawn(drainer.drain(mock_sink.clone(), ()));

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;
        rate_limiter.send_realtime_event(4)?;
        rate_limiter.send_realtime_event(5)?;
        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 5),
            _ => panic!("expected SinkClosedError"),
        }

        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        mock_sink.close().await.unwrap();
        drain_handle.await.unwrap();
        Ok(())
    }

    /// test_scan_block_normal tests that congestions where the queue is longer than `block_scan_threshold`
    /// but shorter than `close_sink_threshold` blocks the senders of scan events as expected, and that
    /// they get unblocked when the congested events are finally consumed.
    #[tokio::test]
    async fn test_scan_block_normal() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(5, 1024);
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
    async fn test_scan_block_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (mut rate_limiter, drainer) = new_pair::<MockCdcEvent>(5, 1024);
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
                Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
                _ => panic!("expected SinkClosedError"),
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
}
