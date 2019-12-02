// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query::codec::datum;
use tidb_query::executor::{Executor, IndexScanExecutor, ScanExecutor, TableScanExecutor};
use tidb_query::expr::EvalContext;
use tipb::{self, AnalyzeColumnsReq, AnalyzeIndexReq, AnalyzeReq, AnalyzeType, TableScan};

use super::cmsketch::CmSketch;
use super::fmsketch::FmSketch;
use super::histogram::Histogram;
use crate::coprocessor::dag::TiKVStorage;
use crate::coprocessor::*;
use crate::storage::{Snapshot, SnapshotStore, Statistics};

// `AnalyzeContext` is used to handle `AnalyzeReq`
pub struct AnalyzeContext<S: Snapshot> {
    req: AnalyzeReq,
    storage: Option<TiKVStorage<SnapshotStore<S>>>,
    ranges: Vec<KeyRange>,
    storage_stats: Statistics,
}

impl<S: Snapshot> AnalyzeContext<S> {
    pub fn new(
        req: AnalyzeReq,
        ranges: Vec<KeyRange>,
        start_ts: u64,
        snap: S,
        req_ctx: &ReqContext,
    ) -> Result<Self> {
        let store = SnapshotStore::new(
            snap,
            start_ts.into(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
            req_ctx.bypass_locks.clone(),
        );
        Ok(Self {
            req,
            storage: Some(store.into()),
            ranges,
            storage_stats: Statistics::default(),
        })
    }

    // handle_column is used to process `AnalyzeColumnsReq`
    // it would build a histogram for the primary key(if needed) and
    // collectors for each column value.
    fn handle_column(builder: &mut SampleBuilder<S>) -> Result<Vec<u8>> {
        let (collectors, pk_builder) = builder.collect_columns_stats()?;

        let pk_hist = pk_builder.into_proto();
        let cols: Vec<tipb::SampleCollector> =
            collectors.into_iter().map(|col| col.into_proto()).collect();

        let res_data = {
            let mut res = tipb::AnalyzeColumnsResp::default();
            res.set_collectors(cols.into());
            res.set_pk_hist(pk_hist);
            box_try!(res.write_to_bytes())
        };
        Ok(res_data)
    }

    // handle_index is used to handle `AnalyzeIndexReq`,
    // it would build a histogram and count-min sketch of index values.
    fn handle_index(
        req: AnalyzeIndexReq,
        scanner: &mut IndexScanExecutor<TiKVStorage<SnapshotStore<S>>>,
    ) -> Result<Vec<u8>> {
        let mut hist = Histogram::new(req.get_bucket_size() as usize);
        let mut cms = CmSketch::new(
            req.get_cmsketch_depth() as usize,
            req.get_cmsketch_width() as usize,
        );
        while let Some(row) = scanner.next()? {
            let row = row.take_origin()?;
            let (bytes, end_offsets) = row.data.get_column_values_and_end_offsets();
            hist.append(bytes);
            if let Some(c) = cms.as_mut() {
                for end_offset in end_offsets {
                    c.insert(&bytes[..end_offset])
                }
            }
        }
        let mut res = tipb::AnalyzeIndexResp::default();
        res.set_hist(hist.into_proto());
        if let Some(c) = cms {
            res.set_cms(c.into_proto());
        }
        let dt = box_try!(res.write_to_bytes());
        Ok(dt)
    }
}

impl<S: Snapshot> RequestHandler for AnalyzeContext<S> {
    fn handle_request(&mut self) -> Result<Response> {
        let ret = match self.req.get_tp() {
            AnalyzeType::TypeIndex => {
                let req = self.req.take_idx_req();
                let mut scanner = ScanExecutor::index_scan_with_cols_len(
                    EvalContext::default(),
                    i64::from(req.get_num_columns()),
                    mem::replace(&mut self.ranges, Vec::new()),
                    self.storage.take().unwrap(),
                )?;
                let res = AnalyzeContext::handle_index(req, &mut scanner);
                scanner.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeColumn => {
                let col_req = self.req.take_col_req();
                let storage = self.storage.take().unwrap();
                let ranges = mem::replace(&mut self.ranges, Vec::new());
                let mut builder = SampleBuilder::new(col_req, storage, ranges)?;
                let res = AnalyzeContext::handle_column(&mut builder);
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }
        };
        match ret {
            Ok(data) => {
                let mut resp = Response::default();
                resp.set_data(data);
                Ok(resp)
            }
            Err(Error::Other(e)) => {
                let mut resp = Response::default();
                resp.set_other_error(e.to_string());
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        dest.add(&self.storage_stats);
        self.storage_stats = Statistics::default();
    }
}

struct SampleBuilder<S: Snapshot> {
    data: TableScanExecutor<TiKVStorage<SnapshotStore<S>>>,
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
impl<S: Snapshot> SampleBuilder<S> {
    fn new(
        mut req: AnalyzeColumnsReq,
        storage: TiKVStorage<SnapshotStore<S>>,
        ranges: Vec<KeyRange>,
    ) -> Result<Self> {
        let cols_info = req.take_columns_info();
        if cols_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }

        let mut col_len = cols_info.len();
        if cols_info[0].get_pk_handle() {
            col_len -= 1;
        }

        let mut meta = TableScan::default();
        meta.set_columns(cols_info);
        let table_scanner =
            ScanExecutor::table_scan(meta, EvalContext::default(), ranges, storage, false)?;
        Ok(Self {
            data: table_scanner,
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
            let row = row.take_origin()?;
            let cols = row.get_binary_cols(&mut EvalContext::default())?;
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
    fm_sketch: FmSketch,
    cm_sketch: Option<CmSketch>,
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
            fm_sketch: FmSketch::new(max_fm_sketch_size),
            cm_sketch: CmSketch::new(cm_sketch_depth, cm_sketch_width),
            rng: thread_rng(),
            total_size: 0,
        }
    }

    fn into_proto(self) -> tipb::SampleCollector {
        let mut s = tipb::SampleCollector::default();
        s.set_null_count(self.null_count as i64);
        s.set_count(self.count as i64);
        s.set_fm_sketch(self.fm_sketch.into_proto());
        s.set_samples(self.samples.into());
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
            // https://github.com/pingcap/tidb/blob/master/statistics/sample.go#L173
            self.samples.remove(idx);
            self.samples.push(data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query::codec::datum;
    use tidb_query::codec::datum::Datum;

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
            sample.collect(datum::encode_value(&mut EvalContext::default(), &[data]).unwrap());
        }
        assert_eq!(sample.samples.len(), max_sample_size);
        assert_eq!(sample.null_count, 1);
        assert_eq!(sample.count, 3);
        assert_eq!(sample.cm_sketch.unwrap().count(), 3);
        assert_eq!(sample.total_size, 6)
    }
}
