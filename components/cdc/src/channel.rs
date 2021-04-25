// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};

use futures03::{
    channel::mpsc::{
        channel, unbounded, Receiver, SendError as FuturesSendError, Sender, TrySendError,
        UnboundedReceiver, UnboundedSender,
    },
    stream, SinkExt, Stream, StreamExt,
};
use grpcio::{Result as GrpcResult, WriteFlags};
use kvproto::cdcpb::ChangeDataEvent;

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
    pub fn in_use(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }
    pub fn cap(&self) -> usize {
        self.max_bytes
    }
    fn alloc(&self, bytes: usize) -> bool {
        if self.total_bytes.load(Ordering::Relaxed) <= self.max_bytes {
            self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
    fn free(&self, bytes: usize) {
        self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub fn canal(buffer: usize, memory_quota: MemoryQuota) -> (Sink, Drain) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = channel(buffer);
    (
        Sink {
            unbounded_sender,
            bounded_sender,
            memory_quota: memory_quota.clone(),
        },
        Drain {
            unbounded_receiver,
            bounded_receiver,
            memory_quota,
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

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

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
}

impl Sink {
    pub fn unbounded_send(&self, event: CdcEvent) -> Result<(), SendError> {
        let bytes = event.size() as usize;
        if !self.memory_quota.alloc(bytes) {
            return Err(SendError::Congested);
        }
        self.unbounded_sender
            .unbounded_send((event, bytes))
            .map_err(SendError::from)
    }

    pub async fn send_all(&mut self, events: Vec<CdcEvent>) -> Result<(), SendError> {
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
        self.drain()
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
            .flatten()
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
    use futures03::FutureExt;
    let mut timeout = futures_timer::Delay::new(dur).fuse();
    let mut f = fut.fuse();
    futures03::executor::block_on(async {
        futures03::select! {
            () = timeout => Err(()),
            item = f => Ok(item),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures03::executor::block_on;
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
        let (mut send, rx) = new_test_cancal(10, std::usize::MAX);
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
        let (mut send, rx) = new_test_cancal(CDC_MSG_MAX_BATCH_SIZE * 2, std::usize::MAX);
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
        let buffer = max_pending_bytes / event.size() + 1;
        let (mut send, _rx) = new_test_cancal(buffer as _, max_pending_bytes as _);
        for _ in 0..buffer {
            send(CdcEvent::Event(e.clone())).unwrap();
        }
        assert_matches!(send(CdcEvent::Event(e)).unwrap_err(), SendError::Congested);
    }
}
