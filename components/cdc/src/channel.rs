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
// 2 = (CDC_MSG_MAX_BATCH_SIZE * 1KB / CDC_EVENT_MAX_BATCH_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
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
    TrySendError<CdcEvent>,
}

#[derive(Clone)]
pub struct Sink {
    unbounded_sender: UnboundedSender<CdcEvent>,
    bounded_sender: Sender<CdcEvent>,
}

impl Sink {
    pub fn unbounded_send(&self, event: CdcEvent) -> Result<(), SendError> {
        self.unbounded_sender
            .unbounded_send(event)
            .map_err(SendError::from)
    }

    pub fn bounded_sink(&self) -> impl futures::Sink<CdcEvent, Error = SendError> {
        self.bounded_sender.clone().sink_map_err(SendError::from)
    }
}

pub struct Drain {
    unbounded_receiver: UnboundedReceiver<CdcEvent>,
    bounded_receiver: Receiver<CdcEvent>,
}

impl Drain {
    pub fn drain(self) -> impl Stream<Item = CdcEvent> {
        stream::select(self.bounded_receiver, self.unbounded_receiver).map(|mut event| {
            if let CdcEvent::Barrier(ref mut barrier) = event {
                if let Some(barrier) = barrier.take() {
                    // Unset barrier when it is received.
                    barrier(());
                }
            }
            event
        })
    }

    pub fn drain_grpc_message(
        self,
    ) -> impl Stream<Item = GrpcResult<(ChangeDataEvent, WriteFlags)>> {
        self.drain()
            .ready_chunks(CDC_MSG_MAX_BATCH_SIZE)
            .map(|events| {
                let mut batcher = EventBatcher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
                events.into_iter().for_each(|e| batcher.push(e));
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
    use futures::FutureExt;
    let mut timeout = futures_timer::Delay::new(dur).fuse();
    let mut s = s.fuse();
    futures::executor::block_on(async {
        futures::select! {
            () = timeout => Err(()),
            item = s.next() => Ok(item),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::executor::block_on;
    use std::sync::mpsc;
    use std::time::Duration;

    fn new_test_cancal(
        buffer: usize,
    ) -> (Box<dyn FnMut(CdcEvent) -> Result<(), SendError>>, Drain) {
        let (tx, rx) = canal(buffer);
        let mut bounded_tx = tx.bounded_sink();
        let mut flag = true;
        let send = move |event| {
            flag = !flag;
            if flag {
                tx.unbounded_send(event)
            } else {
                block_on(bounded_tx.send(event))
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
        assert_matches!(block_on(drain.next()), Some(CdcEvent::Event(_)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some(CdcEvent::Barrier(_)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some(CdcEvent::ResolvedTs(_)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(drain.next()), Some(CdcEvent::Barrier(_)));
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

        if let Ok(_) = recv_timeout(&mut drain, Duration::from_millis(100)) {
            panic!("expect to be timeout");
        }
    }
}
