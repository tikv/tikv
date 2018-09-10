// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::sync::{mpsc, oneshot};
use futures::{Future, Stream};
use protobuf::Message;

use kvproto::coprocessor::{Request, Response};
use tipb::schema::ColumnInfo;
use tipb::select::{SelectResponse, StreamResponse};

use tikv::coprocessor::{EndPointTask, RequestTask};
use tikv::server::OnResponse;
use tikv::storage::Engine;
use tikv::util::worker::Scheduler;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);

pub fn next_id() -> i64 {
    ID_GENERATOR.fetch_add(1, Ordering::Relaxed) as i64
}

pub fn handle_request<E>(end_point_scheduler: &Scheduler<EndPointTask<E>>, req: Request) -> Response
where
    E: Engine,
{
    let (tx, rx) = oneshot::channel();
    let on_resp = OnResponse::Unary(tx);
    let req = RequestTask::new(String::from("127.0.0.1"), req, on_resp, 100).unwrap();
    end_point_scheduler
        .schedule(EndPointTask::Request(req))
        .unwrap();
    rx.wait().unwrap()
}

pub fn handle_select<E>(
    end_point_scheduler: &Scheduler<EndPointTask<E>>,
    req: Request,
) -> SelectResponse
where
    E: Engine,
{
    let resp = handle_request(end_point_scheduler, req);
    assert!(!resp.get_data().is_empty(), "{:?}", resp);
    let mut sel_resp = SelectResponse::new();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
}

pub fn handle_streaming_select<E, F>(
    end_point_scheduler: &Scheduler<EndPointTask<E>>,
    req: Request,
    mut check_range: F,
) -> Vec<StreamResponse>
where
    E: Engine,
    F: FnMut(&Response) + Send + 'static,
{
    let (stream_tx, stream_rx) = mpsc::channel(10);
    let req = RequestTask::new(
        String::from("127.0.0.1"),
        req,
        OnResponse::Streaming(stream_tx),
        100,
    ).unwrap();
    end_point_scheduler
        .schedule(EndPointTask::Request(req))
        .unwrap();
    stream_rx
        .wait()
        .into_iter()
        .map(|resp| {
            let resp = resp.unwrap();
            check_range(&resp);
            assert!(!resp.get_data().is_empty());
            let mut stream_resp = StreamResponse::new();
            stream_resp.merge_from_bytes(resp.get_data()).unwrap();
            stream_resp
        })
        .collect()
}

pub fn offset_for_column(cols: &[ColumnInfo], col_id: i64) -> i64 {
    for (offset, column) in cols.iter().enumerate() {
        if column.get_column_id() == col_id {
            return offset as i64;
        }
    }
    0 as i64
}
