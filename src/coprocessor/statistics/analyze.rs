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

use rand::{thread_rng, Rng, ThreadRng};
use protobuf::{Message, RepeatedField};
use kvproto::coprocessor::{KeyRange, Response};
use tipb::analyze::{self, AnalyzeColumnsReq, AnalyzeReq, AnalyzeType};
use tipb::schema::ColumnInfo;
use tipb::executor::TableScan;

use coprocessor::dag::executor::{Executor, IndexScanExecutor, TableScanExecutor};
use coprocessor::endpoint::ReqContext;
use coprocessor::codec::datum;
use coprocessor::{Error, Result};
use storage::{Snapshot, SnapshotStore, Statistics};
use super::fmsketch::FMSketch;
use super::histogram::Histogram;

// `AnalyzeContext` is used to handle `AnalyzeReq`
pub struct AnalyzeContext<'a> {
    req: AnalyzeReq,
    snap: SnapshotStore<'a>,
    statistics: &'a mut Statistics,
    ranges: Vec<KeyRange>,
}

impl<'a> AnalyzeContext<'a> {
    pub fn new(
        req: AnalyzeReq,
        ranges: Vec<KeyRange>,
        snap: &'a Snapshot,
        statistics: &'a mut Statistics,
        req_ctx: &'a ReqContext,
    ) -> AnalyzeContext<'a> {
        let snap = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.isolation_level,
            req_ctx.fill_cache,
        );
        AnalyzeContext {
            req: req,
            snap: snap,
            statistics: statistics,
            ranges: ranges,
        }
    }

    pub fn handle_request(self) -> Result<Response> {
        let ret = match self.req.get_tp() {
            AnalyzeType::TypeIndex => self.handle_index(),
            AnalyzeType::TypeColumn => self.handle_column(),
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

    // handle_index is used to handle `AnalyzeIndexReq`,
    // it would build a histogram of index values.
    fn handle_index(mut self) -> Result<Vec<u8>> {
        let req = self.req.take_idx_req();
        let mut scanner = IndexScanExecutor::new_with_cols_len(
            req.get_num_columns() as i64,
            self.ranges,
            self.snap,
            self.statistics,
        );
        let mut hist = Histogram::new(req.get_bucket_size() as usize);
        while let Some(row) = scanner.next()? {
            let bytes = row.data.get_column_values();
            hist.append(bytes);
        }
        let mut res = analyze::AnalyzeIndexResp::new();
        res.set_hist(hist.into_proto());
        let dt = box_try!(res.write_to_bytes());
        Ok(dt)
    }

    // handle_column is used to process `AnalyzeColumnsReq`
    // it would build a histogram for the primary key(if needed) and
    // collectors for each column value.
    fn handle_column(mut self) -> Result<Vec<u8>> {
        let col_req = self.req.take_col_req();
        let builder = SampleBuilder::new(col_req, self.snap, self.ranges, &mut self.statistics)?;

        let (collectors, pk_builder) = builder.collect_samples_and_estimate_ndvs()?;
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
}

struct SampleBuilder<'a> {
    data: TableScanExecutor<'a>,
    cols: Vec<ColumnInfo>,
    // the number of columns need to be sampled. It equals to cols.len()
    // if cols[0] is not pk handle, or it should be cols.len() - 1.
    col_len: usize,
    max_bucket_size: usize,
    max_sample_size: usize,
    max_sketch_size: usize,
}

/// `SampleBuilder` is used to analyze columns. It collects sample from
/// the result set using Reservoir Sampling algorithm, and estimates NDVs
/// using FM Sketch during the collecting process.
impl<'a> SampleBuilder<'a> {
    fn new(
        mut req: AnalyzeColumnsReq,
        snap: SnapshotStore<'a>,
        ranges: Vec<KeyRange>,
        statistics: &'a mut Statistics,
    ) -> Result<SampleBuilder<'a>> {
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
        let table_scanner = TableScanExecutor::new(&meta, ranges, snap, statistics);
        Ok(SampleBuilder {
            data: table_scanner,
            cols: meta.take_columns().to_vec(),
            col_len: col_len,
            max_bucket_size: req.get_bucket_size() as usize,
            max_sketch_size: req.get_sketch_size() as usize,
            max_sample_size: req.get_sample_size() as usize,
        })
    }

    // `collect_samples_and_estimate_ndvs` returns the sample collectors which contain total count,
    // null count and distinct values count. And it also returns the statistic builder for PK
    // which contains the histogram. See https://en.wikipedia.org/wiki/Reservoir_sampling
    fn collect_samples_and_estimate_ndvs(mut self) -> Result<(Vec<SampleCollector>, Histogram)> {
        let mut pk_builder = Histogram::new(self.max_bucket_size);
        let mut collectors =
            vec![SampleCollector::new(self.max_sample_size, self.max_sketch_size); self.col_len];
        while let Some(row) = self.data.next()? {
            let cols = row.get_binary_cols(&self.cols)?;
            let retreive_len = cols.len();
            let mut cols_iter = cols.into_iter();
            if self.col_len != retreive_len {
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

/// `SampleCollector` will collect Samples and calculate the count and ndv of an attribute.
#[derive(Clone)]
struct SampleCollector {
    samples: Vec<Vec<u8>>,
    null_count: u64,
    count: u64,
    max_sample_size: usize,
    sketch: FMSketch,
    rng: ThreadRng,
}

impl SampleCollector {
    fn new(max_sample_size: usize, max_sketch_size: usize) -> SampleCollector {
        SampleCollector {
            samples: Default::default(),
            null_count: 0,
            count: 0,
            max_sample_size: max_sample_size,
            sketch: FMSketch::new(max_sketch_size),
            rng: thread_rng(),
        }
    }

    fn into_proto(self) -> analyze::SampleCollector {
        let mut s = analyze::SampleCollector::new();
        s.set_null_count(self.null_count as i64);
        s.set_count(self.count as i64);
        s.set_sketch(self.sketch.into_proto());
        s.set_samples(RepeatedField::from_vec(self.samples));
        s
    }

    pub fn collect(&mut self, data: Vec<u8>) {
        if data[0] == datum::NIL_FLAG {
            self.null_count += 1;
            return;
        }
        self.count += 1;
        self.sketch.insert(&data);
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
    use coprocessor::codec::datum;
    use coprocessor::codec::datum::Datum;
    use super::*;

    #[test]
    fn test_sample_collector() {
        let max_sample_size = 3;
        let max_sketch_size = 10;
        let mut sample = SampleCollector::new(max_sample_size, max_sketch_size);
        let cases = vec![Datum::I64(1), Datum::Null, Datum::I64(2), Datum::I64(5)];

        for data in cases {
            sample.collect(datum::encode_value(&[data]).unwrap());
        }
        assert_eq!(sample.samples.len(), max_sample_size);
        assert_eq!(sample.null_count, 1);
        assert_eq!(sample.count, 3);
    }
}
