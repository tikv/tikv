// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use std::time::Duration;

use tikv_util::time::Instant;

use futures::{
    channel::mpsc::{
        channel as bounded, unbounded, Receiver, SendError as FuturesSendError, Sender,
        TrySendError, UnboundedReceiver, UnboundedSender,
    },
    executor::block_on,
    stream, SinkExt, Stream, StreamExt,
};
use grpcio::WriteFlags;
use kvproto::cdcpb::ChangeDataEvent;

use tikv_util::{impl_display_as_debug, warn};

use crate::service::{CdcEvent, EventBatcher};

const CDC_MSG_MAX_BATCH_SIZE: usize = 128;
// Assume the average size of event is 1KB.
// 2 = (CDC_MSG_MAX_BATCH_SIZE * 1KB / service::CDC_MAX_RESP_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
pub const CDC_EVENT_MAX_BATCH_SIZE: usize = 2;

#[derive(Clone)]
pub struct MemoryQuota {
    capacity: usize,
    in_use: Arc<AtomicUsize>,
}

impl MemoryQuota {
    pub fn new(capacity: usize) -> MemoryQuota {
        MemoryQuota {
            capacity,
            in_use: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }
    pub fn cap(&self) -> usize {
        self.capacity
    }
    fn alloc(&self, bytes: usize) -> bool {
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            if in_use_bytes + bytes > self.capacity {
                return false;
            }
            let new_in_use_bytes = in_use_bytes + bytes;
            match self.in_use.compare_exchange(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => in_use_bytes = current,
            }
        }
    }
    fn free(&self, bytes: usize) {
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            // Saturating at the numeric bounds instead of overflowing.
            let new_in_use_bytes = in_use_bytes - std::cmp::min(bytes, in_use_bytes);
            match self.in_use.compare_exchange(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => in_use_bytes = current,
            }
        }
    }
}

pub fn channel(buffer: usize, memory_quota: MemoryQuota) -> (Sink, Drain) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = bounded(buffer);
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
}

impl Sink {
    pub fn unbounded_send(&self, event: CdcEvent, force: bool) -> Result<(), SendError> {
        // Try it's best to send error events.
        let bytes = if !force { event.size() as usize } else { 0 };
        if bytes != 0 && !self.memory_quota.alloc(bytes) {
            return Err(SendError::Congested);
        }
        match self.unbounded_sender.unbounded_send((event, bytes)) {
            Ok(_) => Ok(()),
            Err(e) => {
                // Free quota if send fails.
                self.memory_quota.free(bytes);
                Err(SendError::from(e))
            }
        }
    }

    pub async fn send_all(&mut self, events: Vec<CdcEvent>) -> Result<(), SendError> {
        // Allocate quota in advance.
        let mut total_bytes = 0;
        for event in &events {
            total_bytes += event.size();
        }
        if !self.memory_quota.alloc(total_bytes as _) {
            return Err(SendError::Congested);
        }
        for event in events {
            let bytes = event.size() as usize;
            if let Err(e) = self.bounded_sender.feed((event, bytes)).await {
                // Free quota if send fails.
                self.memory_quota.free(total_bytes as _);
                return Err(SendError::from(e));
            }
        }
        if let Err(e) = self.bounded_sender.flush().await {
            // Free quota if send fails.
            self.memory_quota.free(total_bytes as _);
            return Err(SendError::from(e));
        }
        Ok(())
    }
}

pub struct Drain {
    unbounded_receiver: UnboundedReceiver<(CdcEvent, usize)>,
    bounded_receiver: Receiver<(CdcEvent, usize)>,
    memory_quota: MemoryQuota,
}

impl<'a> Drain {
    pub fn drain(&'a mut self) -> impl Stream<Item = (CdcEvent, usize)> + 'a {
        stream::select(&mut self.bounded_receiver, &mut self.unbounded_receiver).map(
            |(mut event, size)| {
                if let CdcEvent::Barrier(ref mut barrier) = event {
                    if let Some(barrier) = barrier.take() {
                        // Unset barrier when it is received.
                        barrier(());
                    }
                }
                (event, size)
            },
        )
    }

    // Forwards contents to the sink, simulates StreamExt::forward.
    pub async fn forward<S, E>(&'a mut self, sink: &mut S) -> Result<(), E>
    where
        S: futures::Sink<(ChangeDataEvent, WriteFlags), Error = E> + Unpin,
    {
        let memory_quota = self.memory_quota.clone();
        let mut chunks = self.drain().ready_chunks(CDC_MSG_MAX_BATCH_SIZE);
        while let Some(events) = chunks.next().await {
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
            for (i, e) in resps.into_iter().enumerate() {
                // Buffer messages and flush them at once.
                let write_flags = WriteFlags::default().buffer_hint(i != last_idx);
                sink.feed((e, write_flags)).await?;
            }
            sink.flush().await?;
        }
        Ok(())
    }
}

impl Drop for Drain {
    fn drop(&mut self) {
        self.bounded_receiver.close();
        self.unbounded_receiver.close();
        let start = Instant::now();
        let mut drain = Box::pin(async {
            let memory_quota = self.memory_quota.clone();
            let mut total_bytes = 0;
            let mut drain = self.drain();
            while let Some((_, bytes)) = drain.next().await {
                total_bytes += bytes;
            }
            memory_quota.free(total_bytes);
        });
        block_on(&mut drain);
        let takes = start.saturating_elapsed();
        if takes >= Duration::from_millis(200) {
            warn!("drop Drain too slow"; "takes" => ?takes);
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
    fn new_test_cancal(buffer: usize, capacity: usize, force_send: bool) -> (Send, Drain) {
        let memory_quota = MemoryQuota::new(capacity);
        let (mut tx, rx) = channel(buffer, memory_quota);
        let mut flag = true;
        let send = move |event| {
            flag = !flag;
            if flag {
                tx.unbounded_send(event, force_send)
            } else {
                block_on(tx.send_all(vec![event]))
            }
        };
        (Box::new(send), rx)
    }

    #[test]
    fn test_barrier() {
        let force_send = false;
        let (mut send, mut rx) = new_test_cancal(10, usize::MAX, force_send);
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
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let force_send = false;
        let (mut send, mut drain) =
            new_test_cancal(CDC_MSG_MAX_BATCH_SIZE * 2, usize::MAX, force_send);
        let (mut tx, mut rx) = unbounded();
        runtime.spawn(async move {
            drain.forward(&mut tx).await.unwrap();
        });
        for count in 1..CDC_EVENT_MAX_BATCH_SIZE + CDC_EVENT_MAX_BATCH_SIZE / 2 {
            for _ in 0..count {
                send(CdcEvent::Event(Default::default())).unwrap();
            }
            recv_timeout(&mut rx, Duration::from_millis(100)).unwrap();
        }

        if recv_timeout(&mut rx, Duration::from_millis(100)).is_ok() {
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
        let force_send = false;
        let (mut send, _rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
        for _ in 0..buffer {
            send(CdcEvent::Event(e.clone())).unwrap();
        }
        assert_matches!(send(CdcEvent::Event(e)).unwrap_err(), SendError::Congested);
    }

    #[test]
    fn test_force_send() {
        let mut e = kvproto::cdcpb::Event::default();
        e.region_id = 1;
        let event = CdcEvent::Event(e.clone());
        assert!(event.size() != 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let memory_quota = MemoryQuota::new(max_pending_bytes as _);
        let (tx, _rx) = channel(buffer as _, memory_quota);
        for _ in 0..buffer {
            tx.unbounded_send(CdcEvent::Event(e.clone()), false)
                .unwrap();
        }
        assert_matches!(
            tx.unbounded_send(CdcEvent::Event(e.clone()), false)
                .unwrap_err(),
            SendError::Congested
        );
        tx.unbounded_send(CdcEvent::Event(e), true).unwrap();
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
        let force_send = false;
        // Make sure memory quota is freed when rx is dropped before tx.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
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
            drop(rx);
            assert_eq!(memory_quota.alloc(1024), true);
        }
        // Make sure memory quota is freed when tx is dropped before rx.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
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
            drop(rx);
            assert_eq!(memory_quota.alloc(1024), true);
        }
        // Make sure sending message to a closed channel does not leak memory quota.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
            let memory_quota = rx.memory_quota.clone();
            assert_eq!(memory_quota.in_use(), 0);
            drop(rx);
            for _ in 0..max_pending_bytes {
                send(CdcEvent::Event(e.clone())).unwrap_err();
            }
            assert_eq!(memory_quota.in_use(), 0);
            assert_eq!(memory_quota.alloc(1024), true);

            // Freeing bytes should not cause overflow.
            memory_quota.free(1024);
            assert_eq!(memory_quota.in_use(), 0);
            memory_quota.free(1024);
            assert_eq!(memory_quota.in_use(), 0);
        }
    }
}
