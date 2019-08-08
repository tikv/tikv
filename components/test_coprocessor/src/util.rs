// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Future, Stream};
use protobuf::Message;

use kvproto::coprocessor::{Request, Response};
use tipb::ColumnInfo;
use tipb::{SelectResponse, StreamResponse};

use tikv::coprocessor::Endpoint;
use tikv::storage::Engine;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);

pub fn next_id() -> i64 {
    ID_GENERATOR.fetch_add(1, Ordering::Relaxed) as i64
}

pub fn handle_request<E>(cop: &Endpoint<E>, req: Request) -> Response
where
    E: Engine,
{
    cop.parse_and_handle_unary_request(req, None)
        .wait()
        .unwrap()
}

pub fn handle_select<E>(cop: &Endpoint<E>, req: Request) -> SelectResponse
where
    E: Engine,
{
    let resp = handle_request(cop, req);
    assert!(!resp.get_data().is_empty(), "{:?}", resp);
    let mut sel_resp = SelectResponse::default();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
}

pub fn handle_streaming_select<E, F>(
    cop: &Endpoint<E>,
    req: Request,
    mut check_range: F,
) -> Vec<StreamResponse>
where
    E: Engine,
    F: FnMut(&Response) + Send + 'static,
{
    cop.parse_and_handle_stream_request(req, None)
        .wait()
        .map(|resp| {
            let resp = resp.unwrap();
            check_range(&resp);
            assert!(!resp.get_data().is_empty());
            let mut stream_resp = StreamResponse::default();
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
