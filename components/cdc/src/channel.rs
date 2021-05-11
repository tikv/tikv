// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{
    channel::mpsc::{
        channel, unbounded, Receiver, SendError as FuturesSendError, Sender, TrySendError,
        UnboundedReceiver, UnboundedSender,
    },
    stream, SinkExt, Stream, StreamExt,
};
use grpcio::{Result as GrpcResult, WriteFlags};
use kvproto::cdcpb::ChangeDataEvent;

use tikv_util::impl_display_as_debug;

use crate::service::{CdcEvent, EventBatcher};

const CDC_MSG_MAX_BATCH_SIZE: usize = 128;
// Assume the average size of event is 1KB.
// 2 = (CDC_MSG_MAX_BATCH_SIZE * 1KB / service::CDC_MAX_RESP_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
pub const CDC_EVENT_MAX_BATCH_SIZE: usize = 2;

pub fn canal(buffer: usize) -> (Sink, Drain) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = channel(buffer);
    (
        Sink {
            unbounded_sender,
            bounded_sender,
        },
        Drain {
            unbounded_receiver,
            bounded_receiver,
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendError {
    Full,
    Disconnected,
    Congest,
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
}

impl Sink {
    pub fn unbounded_send(&self, event: CdcEvent) -> Result<(), SendError> {
        // TODO support memory quota by using event size.
        let bytes = event.size() as usize;
        self.unbounded_sender
            .unbounded_send((event, bytes))
            .map_err(SendError::from)
    }

    pub async fn send_all(&mut self, events: Vec<CdcEvent>) -> Result<(), SendError> {
        for event in events {
            let bytes = event.size() as usize;
            self.bounded_sender.feed((event, bytes)).await?;
        }
        self.bounded_sender.flush().await?;
        Ok(())
    }
}

pub struct Drain {
    unbounded_receiver: UnboundedReceiver<(CdcEvent, usize)>,
    bounded_receiver: Receiver<(CdcEvent, usize)>,
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

    use futures::executor::block_on;
    use std::sync::mpsc;
    use std::time::Duration;

    type Send = Box<dyn FnMut(CdcEvent) -> Result<(), SendError>>;
    fn new_test_cancal(buffer: usize) -> (Send, Drain) {
        let (mut tx, rx) = canal(buffer);
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
        let (mut send, rx) = new_test_cancal(10);
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
        let (mut send, rx) = new_test_cancal(CDC_MSG_MAX_BATCH_SIZE * 2);
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
}
