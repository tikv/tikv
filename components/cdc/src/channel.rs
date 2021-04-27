// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::Ordering,
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::{
    channel::mpsc::{
        channel, unbounded, Receiver, SendError as FuturesSendError, Sender, TrySendError,
        UnboundedReceiver, UnboundedSender,
    },
    executor::block_on,
    stream, SinkExt, Stream, StreamExt,
};
use grpcio::{Result as GrpcResult, WriteFlags};
use kvproto::cdcpb::ChangeDataEvent;

use tikv_util::{impl_display_as_debug, slow_log, warn};

use crate::service::{CdcEvent, EventBatcher};

const CDC_MSG_MAX_BATCH_SIZE: usize = 128;
// Assume the average size of event is 1KB.
// 2 = (CDC_MSG_MAX_BATCH_SIZE * 1KB / service::CDC_MAX_RESP_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
pub const CDC_EVENT_MAX_BATCH_SIZE: usize = 2;

#[derive(Clone)]
pub struct MemoryQuota {
    max_bytes: usize,
    total_bytes: Arc<AtomicUsize>,
}

impl MemoryQuota {
    pub fn new(max_bytes: usize) -> MemoryQuota {
        MemoryQuota {
            max_bytes,
            total_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }
    pub fn cap(&self) -> usize {
        self.max_bytes
    }
    fn alloc(&self, bytes: usize) -> bool {
        let mut total_bytes = self.total_bytes.load(Ordering::Relaxed);
        loop {
            if total_bytes + bytes > self.max_bytes {
                return false;
            }
            let new_total_bytes = total_bytes + bytes;
            match self.total_bytes.compare_exchange(
                total_bytes,
                new_total_bytes,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => total_bytes = current,
            }
        }
    }
    fn free(&self, bytes: usize) {
        self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub fn canal(buffer: usize, memory_quota: MemoryQuota) -> (Sink, Drain) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = channel(buffer);
    let closed = Arc::new(AtomicBool::new(false));
    (
        Sink {
            unbounded_sender,
            bounded_sender,
            memory_quota: memory_quota.clone(),
            closed: closed.clone(),
        },
        Drain {
            unbounded_receiver,
            bounded_receiver,
            memory_quota,
            closed,
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendError {
    Full,
    Disconnected,
    Congested,
}

impl std::error::Error for SendError {}

impl_display_as_debug!(SendError);

macro_rules! impl_from_future_send_error {
    ($($f:ty,)+) => {
        $(
            impl From<$f> for SendError {
                fn from(e: $f) -> Self {
                    if e.is_disconnected() {
                        SendError::Disconnected
                    } else if e.is_full() {
                        Self::Full
                    } else {
                        unreachable!()
                    }
                }
            }
        )+
    };
}

impl_from_future_send_error! {
    FuturesSendError,
    TrySendError<(CdcEvent, usize)>,
}

#[derive(Clone)]
pub struct Sink {
    unbounded_sender: UnboundedSender<(CdcEvent, usize)>,
    bounded_sender: Sender<(CdcEvent, usize)>,
    memory_quota: MemoryQuota,
    closed: Arc<AtomicBool>,
}

impl Sink {
    pub fn unbounded_send(&self, event: CdcEvent) -> Result<(), SendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SendError::Disconnected);
        }
        let bytes = event.size() as usize;
        if !self.memory_quota.alloc(bytes) {
            return Err(SendError::Congested);
        }
        self.unbounded_sender
            .unbounded_send((event, bytes))
            .map_err(SendError::from)
    }

    pub async fn send_all(&mut self, events: Vec<CdcEvent>) -> Result<(), SendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SendError::Disconnected);
        }
        for event in events {
            let bytes = event.size() as usize;
            if !self.memory_quota.alloc(bytes) {
                return Err(SendError::Congested);
            }
            self.bounded_sender.feed((event, bytes)).await?;
        }
        self.bounded_sender.flush().await?;
        Ok(())
    }
}

pub struct Drain {
    unbounded_receiver: UnboundedReceiver<(CdcEvent, usize)>,
    bounded_receiver: Receiver<(CdcEvent, usize)>,
    memory_quota: MemoryQuota,
    closed: Arc<AtomicBool>,
}

impl Drain {
    pub fn drain(self) -> impl Stream<Item = (CdcEvent, usize)> {
        stream::select(self.bounded_receiver, self.unbounded_receiver).map(|(mut event, size)| {
            if let CdcEvent::Barrier(ref mut barrier) = event {
                if let Some(barrier) = barrier.take() {
                    // Unset barrier when it is received.
                    barrier(());
                }
            }
            (event, size)
        })
    }

    pub fn drain_grpc_message(
        self,
    ) -> impl Stream<Item = GrpcResult<(ChangeDataEvent, WriteFlags)>> {
        let memory_quota = self.memory_quota.clone();
        let closed = self.closed.clone();
        let stream = self
            .drain()
            .ready_chunks(CDC_MSG_MAX_BATCH_SIZE)
            .map(move |events| {
                let mut bytes = 0;
                let mut batcher = EventBatcher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
                events.into_iter().for_each(|(e, size)| {
                    bytes += size;
                    batcher.push(e);
                });
                let resps = batcher.build();
                let last_idx = resps.len() - 1;
                // Events are about to be sent, free pending events memory counter.
                memory_quota.free(bytes as _);
                stream::iter(resps.into_iter().enumerate().map(move |(i, e)| {
                    // Buffer messages and flush them at once.
                    let write_flags = WriteFlags::default().buffer_hint(i != last_idx);
                    GrpcResult::Ok((e, write_flags))
                }))
            })
            .flatten();
        DrainGrpcMessage { stream, closed }
    }
}

struct DrainGrpcMessage<S: Stream + Unpin> {
    stream: S,
    closed: Arc<AtomicBool>,
}

impl<S: Stream + Unpin> Stream for DrainGrpcMessage<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S: Stream + Unpin> Drop for DrainGrpcMessage<S> {
    // Must poll all pending messages to free memory quota.
    fn drop(&mut self) {
        // Receiver maybe dropped before senders, so set closed flag to prevent
        // futher messages.
        self.closed.store(true, Ordering::Release);
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
        let start = Instant::now();
        let mut drain = Box::pin(async {
            loop {
                let mut next = self.stream.next();
                let item = futures::future::poll_fn(|cx| {
                    match Pin::new(&mut next).poll(cx) {
                        // In case it is dropped before senders.
                        Poll::Pending => Poll::Ready(None),
                        t => t,
                    }
                })
                .await;
                if item.is_none() {
                    break;
                }
            }
        });
        block_on(&mut drain);
        let takes = start.elapsed();
        if takes >= Duration::from_millis(500) {
            slow_log!(takes, "drop DrainGrpcMessage too slow");
        }
    }
}

#[cfg(test)]
pub fn recv_timeout<S, I>(s: &mut S, dur: std::time::Duration) -> Result<Option<I>, ()>
where
    S: Stream<Item = I> + Unpin,
{
    poll_timeout(&mut s.next(), dur)
}

#[cfg(test)]
pub fn poll_timeout<F, I>(fut: &mut F, dur: std::time::Duration) -> Result<I, ()>
where
    F: std::future::Future<Output = I> + Unpin,
{
    use futures::FutureExt;
    let mut timeout = futures_timer::Delay::new(dur).fuse();
    let mut f = fut.fuse();
    futures::executor::block_on(async {
        futures::select! {
            () = timeout => Err(()),
            item = f => Ok(item),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::mpsc;
    use std::time::Duration;

    type Send = Box<dyn FnMut(CdcEvent) -> Result<(), SendError>>;
    fn new_test_cancal(buffer: usize, max_bytes: usize) -> (Send, Drain) {
        let memory_quota = MemoryQuota::new(max_bytes);
        let (mut tx, rx) = canal(buffer, memory_quota);
        let mut flag = true;
        let send = move |event| {
            flag = !flag;
            if flag {
                tx.unbounded_send(event)
            } else {
                block_on(tx.send_all(vec![event]))
            }
        };
        (Box::new(send), rx)
    }

    #[test]
    fn test_barrier() {
        let (mut send, rx) = new_test_cancal(10, usize::MAX);
        send(CdcEvent::Event(Default::default())).unwrap();
        let (btx1, brx1) = mpsc::channel();
        send(CdcEvent::Barrier(Some(Box::new(move |()| {
            btx1.send(()).unwrap();
        }))))
        .unwrap();
        send(CdcEvent::ResolvedTs(Default::default())).unwrap();
        let (btx2, brx2) = mpsc::channel();
        send(CdcEvent::Barrier(Some(Box::new(move |()| {
            btx2.send(()).unwrap();
        }))))
        .unwrap();

        let mut drain = rx.drain();
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::Event(_), _)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::Barrier(_), _)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::ResolvedTs(_), _)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some((CdcEvent::Barrier(_), _)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap();
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
    }

    #[test]
    fn test_nonblocking_batch() {
        let (mut send, rx) = new_test_cancal(CDC_MSG_MAX_BATCH_SIZE * 2, usize::MAX);
        let mut drain = rx.drain_grpc_message();
        for count in 1..CDC_EVENT_MAX_BATCH_SIZE + CDC_EVENT_MAX_BATCH_SIZE / 2 {
            for _ in 0..count {
                send(CdcEvent::Event(Default::default())).unwrap();
            }
            recv_timeout(&mut drain, Duration::from_millis(100)).unwrap();
        }

        if recv_timeout(&mut drain, Duration::from_millis(100)).is_ok() {
            panic!("expect to be timeout");
        }
    }

    #[test]
    fn test_congest() {
        let mut e = kvproto::cdcpb::Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert!(event.size() != 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let (mut send, _rx) = new_test_cancal(buffer as _, max_pending_bytes as _);
        for _ in 0..buffer {
            send(CdcEvent::Event(e.clone())).unwrap();
        }
        assert_matches!(send(CdcEvent::Event(e)).unwrap_err(), SendError::Congested);
    }

    #[test]
    fn test_channel_memory_leak() {
        let mut e = kvproto::cdcpb::Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert!(event.size() != 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size() + 1;
        // Make sure memory quota is freed when rx is dropped before tx.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _);
            loop {
                match send(CdcEvent::Event(e.clone())) {
                    Ok(_) => (),
                    Err(e) => {
                        assert_matches!(e, SendError::Congested);
                        break;
                    }
                }
            }
            let memory_quota = rx.memory_quota.clone();
            assert_eq!(memory_quota.alloc(event.size() as _), false,);
            drop(rx.drain_grpc_message());
            assert_eq!(memory_quota.alloc(1024), true,);
        }
        // Make sure memory quota is freed when tx is dropped before rx.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _);
            loop {
                match send(CdcEvent::Event(e.clone())) {
                    Ok(_) => (),
                    Err(e) => {
                        assert_matches!(e, SendError::Congested);
                        break;
                    }
                }
            }
            let memory_quota = rx.memory_quota.clone();
            assert_eq!(memory_quota.alloc(event.size() as _), false,);
            drop(send);
            drop(rx.drain_grpc_message());
            assert_eq!(memory_quota.alloc(1024), true,);
        }
    }
}
