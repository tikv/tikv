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

use std::mem;

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message, RepeatedField};
use rand::{thread_rng, Rng, ThreadRng};
use tipb::analyze::{self, AnalyzeColumnsReq, AnalyzeIndexReq, AnalyzeReq, AnalyzeType};
use tipb::executor::TableScan;
use tipb::schema::ColumnInfo;

use storage::{Snapshot, SnapshotStore};

use coprocessor::codec::datum;
use coprocessor::dag::executor::{Executor, ExecutorMetrics, IndexScanExecutor, TableScanExecutor};
use coprocessor::*;

use super::cmsketch::CMSketch;
use super::fmsketch::FMSketch;
use super::histogram::Histogram;

// `AnalyzeContext` is used to handle `AnalyzeReq`
pub struct AnalyzeContext {
    req: AnalyzeReq,
    snap: Option<SnapshotStore>,
    ranges: Vec<KeyRange>,
    metrics: ExecutorMetrics,
}

impl AnalyzeContext {
    pub fn new(
        req: AnalyzeReq,
        ranges: Vec<KeyRange>,
        snap: Box<Snapshot>,
        req_ctx: &ReqContext,
    ) -> Result<AnalyzeContext> {
        let snap = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
        );
        Ok(AnalyzeContext {
            req,
            snap: Some(snap),
            ranges,
            metrics: ExecutorMetrics::default(),
        })
    }

    // handle_column is used to process `AnalyzeColumnsReq`
    // it would build a histogram for the primary key(if needed) and
    // collectors for each column value.
    fn handle_column(builder: &mut SampleBuilder) -> Result<Vec<u8>> {
        let (collectors, pk_builder) = builder.collect_columns_stats()?;

        let pk_hist = pk_builder.into_proto();
        let cols: Vec<analyze::SampleCollector> =
            collectors.into_iter().map(|col| col.into_proto()).collect();

        let res_data = {
            let mut res = analyze::AnalyzeColumnsResp::new();
            res.set_collectors(RepeatedField::from_vec(cols));
            res.set_pk_hist(pk_hist);
            box_try!(res.write_to_bytes())
        };
        Ok(res_data)
    }

    // handle_index is used to handle `AnalyzeIndexReq`,
    // it would build a histogram and count-min sketch of index values.
    fn handle_index(req: AnalyzeIndexReq, scanner: &mut IndexScanExecutor) -> Result<Vec<u8>> {
        let mut hist = Histogram::new(req.get_bucket_size() as usize);
        let mut cms = CMSketch::new(
            req.get_cmsketch_depth() as usize,
            req.get_cmsketch_width() as usize,
        );
        while let Some(row) = scanner.next()? {
            let (bytes, end_offsets) = row.data.get_column_values_and_end_offsets();
            hist.append(bytes);
            if let Some(c) = cms.as_mut() {
                for end_offset in end_offsets {
                    c.insert(&bytes[..end_offset])
                }
            }
        }
        let mut res = analyze::AnalyzeIndexResp::new();
        res.set_hist(hist.into_proto());
        if let Some(c) = cms {
            res.set_cms(c.into_proto());
        }
        let dt = box_try!(res.write_to_bytes());
        Ok(dt)
    }
}

impl RequestHandler for AnalyzeContext {
    fn handle_request(&mut self) -> Result<Response> {
        let ret = match self.req.get_tp() {
            AnalyzeType::TypeIndex => {
                let req = self.req.take_idx_req();
                let mut scanner = IndexScanExecutor::new_with_cols_len(
                    i64::from(req.get_num_columns()),
                    mem::replace(&mut self.ranges, Vec::new()),
                    self.snap.take().unwrap(),
                )?;
                let res = AnalyzeContext::handle_index(req, &mut scanner);
                scanner.collect_metrics_into(&mut self.metrics);
                res
            }

            AnalyzeType::TypeColumn => {
                let col_req = self.req.take_col_req();
                let snap = self.snap.take().unwrap();
                let ranges = mem::replace(&mut self.ranges, Vec::new());
                let mut builder = SampleBuilder::new(col_req, snap, ranges)?;
                let res = AnalyzeContext::handle_column(&mut builder);
                builder.data.collect_metrics_into(&mut self.metrics);
                res
            }
        };
        match ret {
            Ok(data) => {
                let mut resp = Response::new();
                resp.set_data(data);
                Ok(resp)
            }
            Err(Error::Other(e)) => {
                let mut resp = Response::new();
                resp.set_other_error(format!("{}", e));
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        metrics.merge(&mut self.metrics);
    }
}

struct SampleBuilder {
    data: TableScanExecutor,
    cols: Vec<ColumnInfo>,
    // the number of columns need to be sampled. It equals to cols.len()
    // if cols[0] is not pk handle, or it should be cols.len() - 1.
    col_len: usize,
    max_bucket_size: usize,
    max_sample_size: usize,
    max_fm_sketch_size: usize,
    cm_sketch_depth: usize,
    cm_sketch_width: usize,
}

/// `SampleBuilder` is used to analyze columns. It collects sample from
/// the result set using Reservoir Sampling algorithm, estimates NDVs
/// using FM Sketch during the collecting process, and builds count-min sketch.
impl SampleBuilder {
    fn new(
        mut req: AnalyzeColumnsReq,
        snap: SnapshotStore,
        ranges: Vec<KeyRange>,
    ) -> Result<SampleBuilder> {
        let cols_info = req.take_columns_info();
        if cols_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }

        let mut col_len = cols_info.len();
        if cols_info[0].get_pk_handle() {
            col_len -= 1;
        }

        let mut meta = TableScan::new();
        meta.set_columns(cols_info);
        let table_scanner = TableScanExecutor::new(&meta, ranges, snap, false)?;
        Ok(SampleBuilder {
            data: table_scanner,
            cols: meta.take_columns().to_vec(),
            col_len,
            max_bucket_size: req.get_bucket_size() as usize,
            max_fm_sketch_size: req.get_sketch_size() as usize,
            max_sample_size: req.get_sample_size() as usize,
            cm_sketch_depth: req.get_cmsketch_depth() as usize,
            cm_sketch_width: req.get_cmsketch_width() as usize,
        })
    }

    // `collect_columns_stats` returns the sample collectors which contain total count,
    // null count, distinct values count and count-min sketch. And it also returns the statistic
    // builder for PK which contains the histogram.
    // See https://en.wikipedia.org/wiki/Reservoir_sampling
    fn collect_columns_stats(&mut self) -> Result<(Vec<SampleCollector>, Histogram)> {
        let mut pk_builder = Histogram::new(self.max_bucket_size);
        let mut collectors = vec![
            SampleCollector::new(
                self.max_sample_size,
                self.max_fm_sketch_size,
                self.cm_sketch_depth,
                self.cm_sketch_width
            );
            self.col_len
        ];
        while let Some(row) = self.data.next()? {
            let cols = row.get_binary_cols(&self.cols)?;
            let retrieve_len = cols.len();
            let mut cols_iter = cols.into_iter();
            if self.col_len != retrieve_len {
                if let Some(v) = cols_iter.next() {
                    pk_builder.append(&v);
                }
            }
            for (collector, val) in collectors.iter_mut().zip(cols_iter) {
                collector.collect(val);
            }
        }
        Ok((collectors, pk_builder))
    }
}

/// `SampleCollector` will collect Samples and calculate the count, ndv and total size of an attribute.
#[derive(Clone)]
struct SampleCollector {
    samples: Vec<Vec<u8>>,
    null_count: u64,
    count: u64,
    max_sample_size: usize,
    fm_sketch: FMSketch,
    cm_sketch: Option<CMSketch>,
    rng: ThreadRng,
    total_size: u64,
}

impl SampleCollector {
    fn new(
        max_sample_size: usize,
        max_fm_sketch_size: usize,
        cm_sketch_depth: usize,
        cm_sketch_width: usize,
    ) -> SampleCollector {
        SampleCollector {
            samples: Default::default(),
            null_count: 0,
            count: 0,
            max_sample_size,
            fm_sketch: FMSketch::new(max_fm_sketch_size),
            cm_sketch: CMSketch::new(cm_sketch_depth, cm_sketch_width),
            rng: thread_rng(),
            total_size: 0,
        }
    }

    fn into_proto(self) -> analyze::SampleCollector {
        let mut s = analyze::SampleCollector::new();
        s.set_null_count(self.null_count as i64);
        s.set_count(self.count as i64);
        s.set_fm_sketch(self.fm_sketch.into_proto());
        s.set_samples(RepeatedField::from_vec(self.samples));
        if let Some(c) = self.cm_sketch {
            s.set_cm_sketch(c.into_proto())
        }
        s.set_total_size(self.total_size as i64);
        s
    }

    pub fn collect(&mut self, data: Vec<u8>) {
        if data[0] == datum::NIL_FLAG {
            self.null_count += 1;
            return;
        }
        self.count += 1;
        self.fm_sketch.insert(&data);
        if let Some(c) = self.cm_sketch.as_mut() {
            c.insert(&data)
        }
        self.total_size += data.len() as u64;
        if self.samples.len() < self.max_sample_size {
            self.samples.push(data);
            return;
        }
        if self.rng.gen_range(0, self.count) < self.max_sample_size as u64 {
            let idx = self.rng.gen_range(0, self.max_sample_size);
            self.samples[idx] = data;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use coprocessor::codec::datum;
    use coprocessor::codec::datum::Datum;

    #[test]
    fn test_sample_collector() {
        let max_sample_size = 3;
        let max_fm_sketch_size = 10;
        let cm_sketch_depth = 2;
        let cm_sketch_width = 16;
        let mut sample = SampleCollector::new(
            max_sample_size,
            max_fm_sketch_size,
            cm_sketch_depth,
            cm_sketch_width,
        );
        let cases = vec![Datum::I64(1), Datum::Null, Datum::I64(2), Datum::I64(5)];

        for data in cases {
            sample.collect(datum::encode_value(&[data]).unwrap());
        }
        assert_eq!(sample.samples.len(), max_sample_size);
        assert_eq!(sample.null_count, 1);
        assert_eq!(sample.count, 3);
        assert_eq!(sample.cm_sketch.unwrap().count(), 3);
        assert_eq!(sample.total_size, 6)
    }
}
