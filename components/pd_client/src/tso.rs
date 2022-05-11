// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is the low-level mechanisms for getting timestamps from a PD
//! cluster. It should be used via the `get_tso` API in `PdClient`.
//!
//! Once a `TimestampOracle` is created, there will be two futures running in a background working
//! thread created automatically. The `get_timestamp` method creates a oneshot channel whose
//! transmitter is served as a `TimestampRequest`. `TimestampRequest`s are sent to the working
//! thread through a bounded multi-producer, single-consumer channel. Every time the first future
//! is polled, it tries to exhaust the channel to get as many requests as possible and sends a
//! single `TsoRequest` to the PD server. The other future receives `TsoResponse`s from the PD
//! server and allocates timestamps for the requests.

use std::{cell::RefCell, collections::VecDeque, pin::Pin, rc::Rc, thread};

use futures::{
    executor::block_on,
    join,
    prelude::*,
    task::{AtomicWaker, Context, Poll},
};
use grpcio::{CallOption, WriteFlags};
use kvproto::pdpb::{PdClient, TsoRequest, TsoResponse};
use tikv_util::{box_err, info};
use tokio::sync::{mpsc, oneshot, watch};
use txn_types::TimeStamp;

use crate::{metrics::PD_PENDING_TSO_REQUEST_GAUGE, Error, Result};

/// It is an empirical value.
const MAX_BATCH_SIZE: usize = 64;

const MAX_PENDING_COUNT: usize = 1 << 16;

struct TimestampRequest {
    sender: oneshot::Sender<TimeStamp>,
    count: u32,
}

/// The timestamp oracle (TSO) which provides monotonically increasing timestamps.
pub struct TimestampOracle {
    /// The transmitter of a bounded channel which transports requests of getting a single
    /// timestamp to the TSO working thread. A bounded channel is used to prevent using
    /// too much memory unexpectedly.
    /// In the working thread, the `TimestampRequest`, which is actually a one channel sender,
    /// is used to send back the timestamp result.
    request_tx: mpsc::Sender<TimestampRequest>,
    close_rx: watch::Receiver<()>,
}

impl TimestampOracle {
    pub(crate) fn new(
        cluster_id: u64,
        pd_client: &PdClient,
        call_option: CallOption,
    ) -> Result<TimestampOracle> {
        let (request_tx, request_rx) = mpsc::channel(MAX_BATCH_SIZE);
        let (rpc_sender, rpc_receiver) = pd_client.tso_opt(call_option)?;
        let (close_tx, close_rx) = watch::channel(());

        // Start a background thread to handle TSO requests and responses
        thread::Builder::new()
            .name("tso-worker".into())
            .spawn(move || {
                block_on(run_tso(
                    cluster_id,
                    rpc_sender.sink_err_into(),
                    rpc_receiver.err_into(),
                    request_rx,
                    close_tx,
                ))
            })
            .expect("unable to create tso worker thread");

        Ok(TimestampOracle {
            request_tx,
            close_rx,
        })
    }

    pub(crate) fn get_timestamp(
        &self,
        count: u32,
    ) -> impl Future<Output = Result<TimeStamp>> + 'static {
        let (request, response) = oneshot::channel();
        let request_tx = self.request_tx.clone();
        async move {
            request_tx
                .send(TimestampRequest {
                    sender: request,
                    count,
                })
                .await
                .map_err(|_| -> Error { box_err!("TimestampRequest channel is closed") })?;
            response
                .await
                .map_err(|_| box_err!("Timestamp channel is dropped"))
        }
    }

    pub(crate) fn closed(&self) -> impl Future<Output = ()> {
        let mut close_rx = self.close_rx.clone();
        async move {
            let _ = close_rx.changed().await;
        }
    }
}

async fn run_tso(
    cluster_id: u64,
    mut rpc_sender: impl Sink<(TsoRequest, WriteFlags), Error = Error> + Unpin,
    mut rpc_receiver: impl Stream<Item = Result<TsoResponse>> + Unpin,
    mut request_rx: mpsc::Receiver<TimestampRequest>,
    close_tx: watch::Sender<()>,
) {
    // The `TimestampRequest`s which are waiting for the responses from the PD server
    let pending_requests = Rc::new(RefCell::new(VecDeque::with_capacity(MAX_PENDING_COUNT)));

    // When there are too many pending requests, the `send_request` future will refuse to fetch
    // more requests from the bounded channel. This waker is used to wake up the sending future
    // if the queue containing pending requests is no longer full.
    let sending_future_waker = Rc::new(AtomicWaker::new());

    let mut request_stream = TsoRequestStream {
        cluster_id,
        request_rx: &mut request_rx,
        pending_requests: pending_requests.clone(),
        self_waker: sending_future_waker.clone(),
    }
    .map(Ok);

    let send_requests = async move {
        rpc_sender.send_all(&mut request_stream).await?;
        rpc_sender.close().await?;
        Ok(())
    };

    let receive_and_handle_responses = async move {
        while let Some(Ok(resp)) = rpc_receiver.next().await {
            let mut pending_requests = pending_requests.borrow_mut();

            // Wake up the sending future blocked by too many pending requests as we are consuming
            // some of them here.
            if pending_requests.len() >= MAX_PENDING_COUNT {
                sending_future_waker.wake();
            }

            allocate_timestamps(&resp, &mut pending_requests)?;
            PD_PENDING_TSO_REQUEST_GAUGE.set(pending_requests.len() as i64);
        }
        Ok(())
    };

    let (send_res, recv_res): (Result<()>, Result<()>) =
        join!(send_requests, receive_and_handle_responses);
    let _ = close_tx.send(());
    info!("TSO worker terminated"; "sender_cause" => ?send_res.err(), "receiver_cause" => ?recv_res.err());
}

struct RequestGroup {
    tso_request: TsoRequest,
    requests: Vec<TimestampRequest>,
}

struct TsoRequestStream<'a> {
    cluster_id: u64,
    request_rx: &'a mut mpsc::Receiver<TimestampRequest>,
    pending_requests: Rc<RefCell<VecDeque<RequestGroup>>>,
    self_waker: Rc<AtomicWaker>,
}

impl<'a> Stream for TsoRequestStream<'a> {
    type Item = (TsoRequest, WriteFlags);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pending_requests = self.pending_requests.clone();
        let mut pending_requests = pending_requests.borrow_mut();
        let mut requests = Vec::new();

        while requests.len() < MAX_BATCH_SIZE && pending_requests.len() < MAX_PENDING_COUNT {
            match self.request_rx.poll_recv(cx) {
                Poll::Ready(Some(sender)) => {
                    requests.push(sender);
                }
                Poll::Ready(None) if requests.is_empty() => {
                    return Poll::Ready(None);
                }
                _ => break,
            }
        }

        if !requests.is_empty() {
            let mut req = TsoRequest::default();
            req.mut_header().cluster_id = self.cluster_id;
            req.count = requests.iter().map(|r| r.count).sum();

            let request_group = RequestGroup {
                tso_request: req.clone(),
                requests,
            };
            pending_requests.push_back(request_group);
            PD_PENDING_TSO_REQUEST_GAUGE.set(pending_requests.len() as i64);

            let write_flags = WriteFlags::default().buffer_hint(false);
            Poll::Ready(Some((req, write_flags)))
        } else {
            // Set the waker to the context, then the stream can be waked up after the pending queue
            // is no longer full.
            self.self_waker.register(cx.waker());
            Poll::Pending
        }
    }
}

fn allocate_timestamps(
    resp: &TsoResponse,
    pending_requests: &mut VecDeque<RequestGroup>,
) -> Result<()> {
    // PD returns the timestamp with the biggest logical value. We can send back timestamps
    // whose logical value is from `logical - count + 1` to `logical` using the senders
    // in `pending`.
    let tail_ts = resp
        .timestamp
        .as_ref()
        .ok_or_else(|| -> Error { box_err!("No timestamp in TsoResponse") })?;

    let mut offset = resp.count;
    if let Some(RequestGroup {
        tso_request,
        requests,
    }) = pending_requests.pop_front()
    {
        if tso_request.count != offset {
            return Err(box_err!("PD gives unexpected number of timestamps"));
        }

        for request in requests {
            offset -= request.count;
            let physical = tail_ts.physical as u64;
            let logical = tail_ts.logical as u64 - offset as u64;
            let ts = TimeStamp::compose(physical, logical);
            let _ = request.sender.send(ts);
        }
    } else {
        return Err(box_err!("PD gives more TsoResponse than expected"));
    };
    Ok(())
}
