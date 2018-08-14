// Copyright 2017 PingCAP, Inc.
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

use std::marker::PhantomData;
use std::sync::Arc;

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message as PbMsg, RepeatedField};
use tipb::select::{Chunk, DAGRequest, EncodeType, SelectResponse, StreamResponse};

use coprocessor::codec::chunk::{Chunk as ArrowChunk, ChunkEncoder};
use coprocessor::dag::executor::Row;
use coprocessor::dag::expr::{EvalConfig, EvalContext, EvalWarnings};
use coprocessor::*;
use storage::{Snapshot, SnapshotStore};

use super::executor::{build_exec, Executor, ExecutorMetrics};

pub struct DAGContext<S: Snapshot + 'static> {
    _phantom: PhantomData<S>,
    req_ctx: ReqContext,
    exec: Box<Executor + Send>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    encode_type: EncodeType,
    eval_cfg: Arc<EvalConfig>,
}

impl<S: Snapshot + 'static> DAGContext<S> {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: S,
        req_ctx: ReqContext,
        batch_row_limit: usize,
    ) -> Result<Self> {
        let mut eval_cfg = box_try!(EvalConfig::new(req.get_flags()));

        // We respect time zone name first, then offset.
        if req.has_time_zone_name() && !req.get_time_zone_name().is_empty() {
            box_try!(eval_cfg.set_time_zone_by_name(req.get_time_zone_name()));
        } else if req.has_time_zone_offset() {
            box_try!(eval_cfg.set_time_zone_by_offset(req.get_time_zone_offset()));
        } else {
            // This should not be reachable. However we will not panic here in case
            // of compatibility issues.
        }

        if req.has_max_warning_count() {
            eval_cfg.set_max_warning_cnt(req.get_max_warning_count() as usize);
        }
        if req.has_sql_mode() {
            eval_cfg.set_sql_mode(req.get_sql_mode())
        }
        if req.has_is_strict_sql_mode() {
            eval_cfg.set_strict_sql_mode(req.get_is_strict_sql_mode());
        }
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
        );

        let arc_eval_cfg = Arc::new(eval_cfg);

        let dag_executor = build_exec(
            req.take_executors().into_vec(),
            store,
            ranges,
            arc_eval_cfg.clone(),
            req.get_collect_range_counts(),
        )?;
        Ok(Self {
            _phantom: Default::default(),
            req_ctx,
            exec: dag_executor,
            output_offsets: req.take_output_offsets(),
            batch_row_limit,
            encode_type: req.get_encode_type(),
            eval_cfg: arc_eval_cfg,
        })
    }
}

enum ResChunks {
    DEFAULT(Vec<Chunk>),
    ARROW(Vec<ArrowChunk>),
}

impl ResChunks {
    fn new(encode_type: EncodeType) -> ResChunks {
        match encode_type {
            EncodeType::TypeDefault => ResChunks::DEFAULT(Vec::new()),
            EncodeType::TypeArrow => ResChunks::ARROW(Vec::new()),
        }
    }

    fn push_row(&mut self, ctx: &mut EvalContext, row: Row, output_offsets: &[u32]) -> Result<()> {
        match self {
            ResChunks::DEFAULT(ref mut chunks) => {
                let chunk = chunks.last_mut().unwrap();
                let value = row.get_binary(output_offsets)?;
                chunk.mut_rows_data().extend_from_slice(&value);
                Ok(())
            }
            ResChunks::ARROW(ref mut chunks) => {
                let chunk = chunks.last_mut().unwrap();
                let datums = row.into_datums(ctx, &output_offsets)?;
                chunk.append_row(&datums)?;
                Ok(())
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            ResChunks::DEFAULT(ref chunks) => chunks.is_empty(),
            ResChunks::ARROW(ref chunks) => chunks.is_empty(),
        }
    }
}

struct ResponseData {
    chunks: ResChunks,
    ctx: EvalContext,
    rows_in_last_chunk: usize,
    chunk_rows_limit: usize,
}

impl ResponseData {
    fn new(encode_type: EncodeType, cfg: Arc<EvalConfig>, chunk_rows_limit: usize) -> ResponseData {
        ResponseData {
            chunks: ResChunks::new(encode_type),
            ctx: EvalContext::new(cfg),
            rows_in_last_chunk: 0,
            chunk_rows_limit,
        }
    }

    fn last_chunk_is_full(&self) -> bool {
        self.rows_in_last_chunk >= self.chunk_rows_limit
    }

    fn push_row(&mut self, row: Row, output_offsets: &[u32]) -> Result<()> {
        if self.chunks.is_empty() || self.last_chunk_is_full() {
            match self.chunks {
                ResChunks::DEFAULT(ref mut chunks) => chunks.push(Chunk::new()),
                ResChunks::ARROW(ref mut chunks) => match &row {
                    Row::Origin(ref row) => chunks.push(ArrowChunk::new(
                        &row.get_field_types(output_offsets),
                        self.chunk_rows_limit,
                    )),
                    Row::Agg(ref row) => chunks.push(ArrowChunk::new(
                        row.get_field_types(),
                        self.chunk_rows_limit,
                    )),
                },
            }
            self.rows_in_last_chunk = 0;
        }

        self.chunks.push_row(&mut self.ctx, row, output_offsets)?;
        self.rows_in_last_chunk += 1;

        Ok(())
    }

    fn into_sel_response(
        mut self,
        eval_warnings: Option<EvalWarnings>,
        output_counts: Vec<i64>,
    ) -> Result<Response> {
        let mut sel_resp = SelectResponse::new();
        match self.chunks {
            ResChunks::DEFAULT(chunks) => sel_resp.set_chunks(RepeatedField::from_vec(chunks)),
            ResChunks::ARROW(chunks) => {
                for c in &chunks {
                    sel_resp.mut_row_batch_data().encode_chunk(c)?;
                }
            }
        };
        let mut eval_warnings = eval_warnings.unwrap_or_default();
        eval_warnings.merge(self.ctx.take_warnings());
        if eval_warnings.warning_cnt > 0 {
            sel_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
            sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        sel_resp.set_output_counts(output_counts);
        let data = box_try!(sel_resp.write_to_bytes());
        let mut resp = Response::new();
        resp.set_data(data);
        Ok(resp)
    }

    fn into_stream_response(
        mut self,
        eval_warnings: Option<EvalWarnings>,
        output_counts: Vec<i64>,
        range: Option<KeyRange>,
    ) -> Result<(Option<Response>, bool)> {
        let finished = !self.last_chunk_is_full();
        if self.chunks.is_empty() {
            return Ok((None, finished));
        }
        let mut sel_resp = StreamResponse::new();

        match self.chunks {
            ResChunks::DEFAULT(chunks) => sel_resp.set_data(box_try!(chunks[0].write_to_bytes())),
            ResChunks::ARROW(chunks) => sel_resp.mut_data().encode_chunk(&chunks[0])?,
        };

        let mut eval_warnings = eval_warnings.unwrap_or_default();
        eval_warnings.merge(self.ctx.take_warnings());
        if eval_warnings.warning_cnt > 0 {
            sel_resp.set_warnings(RepeatedField::from_vec(eval_warnings.warnings));
            sel_resp.set_warning_count(eval_warnings.warning_cnt as i64);
        }
        sel_resp.set_output_counts(output_counts);

        let mut resp = Response::new();
        resp.set_data(box_try!(sel_resp.write_to_bytes()));
        if let Some(range) = range {
            resp.set_range(range);
        }
        Ok((Some(resp), finished))
    }
}

impl<S: Snapshot> RequestHandler for DAGContext<S> {
    fn handle_request(&mut self) -> Result<Response> {
        let mut chunks = ResponseData::new(
            self.encode_type,
            self.eval_cfg.clone(),
            self.batch_row_limit,
        );
        loop {
            match self.exec.next() {
                Ok(Some(row)) => {
                    self.req_ctx.check_if_outdated()?;
                    chunks.push_row(row, &self.output_offsets)?;
                }
                Ok(None) => {
                    let mut output_counts = vec![];
                    self.exec.collect_output_counts(&mut output_counts);
                    return chunks.into_sel_response(self.exec.take_eval_warnings(), output_counts);
                }
                Err(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        self.exec.start_scan();
        let mut resp = ResponseData::new(
            self.encode_type,
            self.eval_cfg.clone(),
            self.batch_row_limit,
        );
        while !resp.last_chunk_is_full() {
            match self.exec.next() {
                Ok(Some(row)) => {
                    resp.push_row(row, &self.output_offsets)?;
                }
                Ok(None) => {
                    break;
                }
                Err(Error::Eval(err)) => {
                    let mut resp = Response::new();
                    let mut sel_resp = StreamResponse::new();
                    sel_resp.set_error(err);
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok((Some(resp), true));
                }
                Err(e) => return Err(e),
            }
        }
        let mut output_counts = vec![];
        self.exec.collect_output_counts(&mut output_counts);
        resp.into_stream_response(
            self.exec.take_eval_warnings(),
            output_counts,
            self.exec.stop_scan(),
        )
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.exec.collect_metrics_into(metrics);
    }
}
