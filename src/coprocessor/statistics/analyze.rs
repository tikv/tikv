// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

<<<<<<< HEAD
use std::mem;
=======
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use std::sync::Arc;
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)

use async_trait::async_trait;
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use tidb_query::codec::datum::{
    encode_value, Datum, DatumDecoder, DURATION_FLAG, INT_FLAG, NIL_FLAG, UINT_FLAG,
};
<<<<<<< HEAD
use tidb_query::codec::table;
use tidb_query::executor::{Executor, IndexScanExecutor, ScanExecutor, TableScanExecutor};
use tidb_query::expr::EvalContext;
use tidb_query_datatype::def::field_type::FieldTypeAccessor;
use tidb_query_datatype::Collation;
use tipb::{self, AnalyzeColumnsReq, AnalyzeIndexReq, AnalyzeReq, AnalyzeType, TableScan};
=======
use tidb_query_expr::BATCH_MAX_SIZE;
use tikv_util::time::Instant;
use tipb::{self, AnalyzeColumnsReq, AnalyzeIndexReq, AnalyzeReq, AnalyzeType};
use yatp::task::future::reschedule;
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)

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
            false,
        );
        Ok(Self {
            req,
            storage: Some(TiKVStorage::new(store, false)),
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
<<<<<<< HEAD
        while let Some(row) = scanner.next()? {
            let row = row.take_origin()?;
            let (bytes, end_offsets) = row.data.get_column_values_and_end_offsets();
            hist.append(bytes);
=======
        let mut fms = FmSketch::new(req.get_sketch_size() as usize);
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        let mut topn_heap = BinaryHeap::new();
        // cur_val recording the current value's data and its counts when iterating index's rows.
        // Once we met a new value, the old value will be pushed into the topn_heap to maintain the
        // top-n information.
        let mut cur_val: (u32, Vec<u8>) = (0, vec![]);
        let top_n_size = req.get_top_n_size() as usize;
        let stats_version = if req.has_version() {
            req.get_version()
        } else {
            ANALYZE_VERSION_V1
        };
        while let Some((key, _)) = scanner.next()? {
            row_count += 1;
            if row_count >= BATCH_MAX_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            let mut key = &key[..];
            if is_common_handle {
                table::check_record_key(key)?;
                key = &key[table::PREFIX_LEN..];
            } else {
                table::check_index_key(key)?;
                key = &key[table::PREFIX_LEN + table::ID_LEN..];
            }
            let mut datums = key;
            let mut data = Vec::with_capacity(key.len());
            for i in 0..req.get_num_columns() as usize {
                if datums.is_empty() {
                    return Err(box_err!(
                        "{}th column is missing in datum buffer: {}",
                        i,
                        log_wrappers::Value::key(key)
                    ));
                }
                let (column, remaining) = split_datum(datums, false)?;
                datums = remaining;
                data.extend_from_slice(column);
                if let Some(cms) = cms.as_mut() {
                    cms.insert(&data);
                }
            }
            fms.insert(&data);
            if stats_version == ANALYZE_VERSION_V2 {
                hist.append(&data, true);
                if cur_val.1 == data {
                    cur_val.0 += 1;
                } else {
                    if cur_val.0 > 0 {
                        topn_heap.push(Reverse(cur_val));
                    }
                    if topn_heap.len() > top_n_size {
                        topn_heap.pop();
                    }
                    cur_val = (1, data);
                }
            } else {
                hist.append(&data, false);
            }
        }

        if stats_version == ANALYZE_VERSION_V2 {
            if cur_val.0 > 0 {
                topn_heap.push(Reverse(cur_val));
                if topn_heap.len() > top_n_size {
                    topn_heap.pop();
                }
            }
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
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

#[async_trait]
impl<S: Snapshot> RequestHandler for AnalyzeContext<S> {
    async fn handle_request(&mut self) -> Result<Response> {
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
                resp.set_other_error(e);
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

<<<<<<< HEAD
=======
struct RowSampleBuilder<S: Snapshot> {
    data: BatchTableScanExecutor<TiKVStorage<SnapshotStore<S>>>,

    max_sample_size: usize,
    max_fm_sketch_size: usize,
    columns_info: Vec<tipb::ColumnInfo>,
    column_groups: Vec<tipb::AnalyzeColumnGroup>,
}

impl<S: Snapshot> RowSampleBuilder<S> {
    fn new(
        mut req: AnalyzeColumnsReq,
        storage: TiKVStorage<SnapshotStore<S>>,
        ranges: Vec<KeyRange>,
    ) -> Result<Self> {
        let columns_info: Vec<_> = req.take_columns_info().into();
        if columns_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }
        let common_handle_ids = req.take_primary_column_ids();
        let table_scanner = BatchTableScanExecutor::new(
            storage,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            ranges,
            common_handle_ids,
            false,
            false, // Streaming mode is not supported in Analyze request, always false here
            req.take_primary_prefix_column_ids(),
        )?;
        Ok(Self {
            data: table_scanner,
            max_sample_size: req.get_sample_size() as usize,
            max_fm_sketch_size: req.get_sketch_size() as usize,
            columns_info,
            column_groups: req.take_column_groups().into(),
        })
    }

    async fn collect_column_stats(&mut self) -> Result<AnalyzeSamplingResult> {
        use tidb_query_datatype::{codec::collation::Collator, match_template_collator};

        let mut is_drained = false;
        let mut time_slice_start = Instant::now();
        let mut collector = RowSampleCollector::new(
            self.max_sample_size,
            self.max_fm_sketch_size,
            self.columns_info.len() + self.column_groups.len(),
        );
        while !is_drained {
            let time_slice_elapsed = time_slice_start.saturating_elapsed();
            if time_slice_elapsed > MAX_TIME_SLICE {
                reschedule().await;
                time_slice_start = Instant::now();
            }
            let result = self.data.next_batch(BATCH_MAX_SIZE);
            is_drained = result.is_drained?;

            let columns_slice = result.physical_columns.as_slice();

            for logical_row in &result.logical_rows {
                let mut column_vals: Vec<Vec<u8>> = Vec::new();
                let mut collation_key_vals: Vec<Vec<u8>> = Vec::new();
                for i in 0..self.columns_info.len() {
                    let mut val = vec![];
                    columns_slice[i].encode(
                        *logical_row,
                        &self.columns_info[i],
                        &mut EvalContext::default(),
                        &mut val,
                    )?;
                    if self.columns_info[i].as_accessor().is_string_like() {
                        let sorted_val = match_template_collator! {
                            TT, match self.columns_info[i].as_accessor().collation()? {
                                Collation::TT => {
                                    let mut mut_val = &val[..];
                                    let decoded_val = table::decode_col_value(&mut mut_val, &mut EvalContext::default(), &self.columns_info[i])?;
                                    if decoded_val == Datum::Null {
                                        val.clone()
                                    } else {
                                        // Only if the `decoded_val` is Datum::Null, `decoded_val` is a Ok(None).
                                        // So it is safe the unwrap the Ok value.
                                        let decoded_sorted_val = TT::sort_key(&decoded_val.as_string()?.unwrap().into_owned())?;
                                        decoded_sorted_val
                                    }
                                }
                            }
                        };
                        collation_key_vals.push(sorted_val);
                    } else {
                        collation_key_vals.push(Vec::new());
                    }
                    column_vals.push(val);
                }
                collector.count += 1;
                collector.collect_column_group(
                    &column_vals,
                    &collation_key_vals,
                    &self.columns_info,
                    &self.column_groups,
                );
                collector.collect_column(column_vals, collation_key_vals, &self.columns_info);
            }
        }
        Ok(AnalyzeSamplingResult::new(collector))
    }
}

#[derive(Clone)]
struct RowSampleCollector {
    samples: BinaryHeap<Reverse<(i64, Vec<Vec<u8>>)>>,
    null_count: Vec<i64>,
    count: u64,
    max_sample_size: usize,
    fm_sketches: Vec<FmSketch>,
    rng: StdRng,
    total_sizes: Vec<i64>,
    row_buf: Vec<u8>,
}

impl Default for RowSampleCollector {
    fn default() -> Self {
        RowSampleCollector {
            samples: BinaryHeap::new(),
            null_count: vec![],
            count: 0,
            max_sample_size: 0,
            fm_sketches: vec![],
            rng: StdRng::from_entropy(),
            total_sizes: vec![],
            row_buf: Vec::new(),
        }
    }
}

impl RowSampleCollector {
    fn new(
        max_sample_size: usize,
        max_fm_sketch_size: usize,
        col_and_group_len: usize,
    ) -> RowSampleCollector {
        RowSampleCollector {
            samples: BinaryHeap::new(),
            null_count: vec![0; col_and_group_len],
            count: 0,
            max_sample_size,
            fm_sketches: vec![FmSketch::new(max_fm_sketch_size); col_and_group_len],
            rng: StdRng::from_entropy(),
            total_sizes: vec![0; col_and_group_len],
            row_buf: Vec::new(),
        }
    }

    pub fn collect_column_group(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
        column_groups: &[tipb::AnalyzeColumnGroup],
    ) {
        let col_len = columns_val.len();
        for i in 0..column_groups.len() {
            self.row_buf.clear();
            let offsets = column_groups[i].get_column_offsets();
            let mut has_null = true;
            for j in offsets {
                if columns_val[*j as usize][0] == NIL_FLAG {
                    continue;
                }
                has_null = false;
                self.total_sizes[col_len + i] += columns_val[*j as usize].len() as i64
            }
            // We only maintain the null count for single column case.
            if has_null && offsets.len() == 1 {
                self.null_count[col_len + i] += 1;
                continue;
            }
            // Use a in place murmur3 to replace this memory copy.
            for j in offsets {
                if columns_info[*j as usize].as_accessor().is_string_like() {
                    self.row_buf
                        .extend_from_slice(&collation_keys_val[*j as usize]);
                } else {
                    self.row_buf.extend_from_slice(&columns_val[*j as usize]);
                }
            }
            self.fm_sketches[col_len + i].insert(&self.row_buf);
        }
    }

    pub fn collect_column(
        &mut self,
        columns_val: Vec<Vec<u8>>,
        collation_keys_val: Vec<Vec<u8>>,
        columns_info: &[tipb::ColumnInfo],
    ) {
        for i in 0..columns_val.len() {
            if columns_val[i][0] == NIL_FLAG {
                self.null_count[i] += 1;
                continue;
            }
            if columns_info[i].as_accessor().is_string_like() {
                self.fm_sketches[i].insert(&collation_keys_val[i]);
            } else {
                self.fm_sketches[i].insert(&columns_val[i]);
            }
            self.total_sizes[i] += columns_val[i].len() as i64;
        }
        self.sampling(columns_val);
    }

    pub fn sampling(&mut self, data: Vec<Vec<u8>>) {
        let cur_rng = self.rng.gen_range(0, i64::MAX);
        if self.samples.len() < self.max_sample_size {
            self.samples.push(Reverse((cur_rng, data)));
            return;
        }
        if self.samples.len() == self.max_sample_size && self.samples.peek().unwrap().0.0 < cur_rng
        {
            self.samples.pop();
            self.samples.push(Reverse((cur_rng, data)));
        }
    }

    pub fn into_proto(self) -> tipb::RowSampleCollector {
        let mut s = tipb::RowSampleCollector::default();
        let samples = self
            .samples
            .into_iter()
            .map(|r_tuple| {
                let mut pb_sample = tipb::RowSample::default();
                pb_sample.set_row(r_tuple.0.1.into());
                pb_sample.set_weight(r_tuple.0.0);
                pb_sample
            })
            .collect();
        s.set_samples(samples);
        s.set_null_counts(self.null_count);
        s.set_count(self.count as i64);
        let pb_fm_sketches = self
            .fm_sketches
            .into_iter()
            .map(|fm_sketch| fm_sketch.into_proto())
            .collect();
        s.set_fm_sketch(pb_fm_sketches);
        s.set_total_size(self.total_sizes);
        s
    }
}

>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
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
    cols_info: Vec<::tipb::ColumnInfo>,
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
        let vec_cols_info = if cols_info[0].get_pk_handle() {
            col_len -= 1;
            cols_info.to_vec()[1..].to_vec()
        } else {
            cols_info.to_vec()
        };

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
            cols_info: vec_cols_info,
        })
    }

    // `collect_columns_stats` returns the sample collectors which contain total count,
    // null count, distinct values count and count-min sketch. And it also returns the statistic
    // builder for PK which contains the histogram.
    // See https://en.wikipedia.org/wiki/Reservoir_sampling
    fn collect_columns_stats(&mut self) -> Result<(Vec<SampleCollector>, Histogram)> {
        use tidb_query::codec::collation::{match_template_collator, Collator};
        let mut pk_builder = Histogram::new(self.max_bucket_size);
        let mut collectors = vec![
            SampleCollector::new(
                self.max_sample_size,
                self.max_fm_sketch_size,
                self.cm_sketch_depth,
                self.cm_sketch_width,
            );
            self.col_len
        ];
<<<<<<< HEAD
        while let Some(row) = self.data.next()? {
            let row = row.take_origin()?;
            let cols = row.get_binary_cols(&mut EvalContext::default())?;
            let retrieve_len = cols.len();
            let mut cols_iter = cols.into_iter();
            if self.col_len != retrieve_len {
                if let Some(v) = cols_iter.next() {
                    pk_builder.append(&v);
=======
        let mut is_drained = false;
        let mut time_slice_start = Instant::now();
        let mut common_handle_hist = Histogram::new(self.max_bucket_size);
        let mut common_handle_cms = CmSketch::new(self.cm_sketch_depth, self.cm_sketch_width);
        let mut common_handle_fms = FmSketch::new(self.max_fm_sketch_size);
        while !is_drained {
            let time_slice_elapsed = time_slice_start.saturating_elapsed();
            if time_slice_elapsed > MAX_TIME_SLICE {
                reschedule().await;
                time_slice_start = Instant::now();
            }
            let result = self.data.next_batch(BATCH_MAX_SIZE);
            is_drained = result.is_drained?;

            let mut columns_slice = result.physical_columns.as_slice();
            let mut columns_info = &self.columns_info[..];
            if columns_without_handle_len + 1 == columns_slice.len() {
                for logical_row in &result.logical_rows {
                    let mut data = vec![];
                    columns_slice[0].encode(
                        *logical_row,
                        &columns_info[0],
                        &mut EvalContext::default(),
                        &mut data,
                    )?;
                    pk_builder.append(&data, false);
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
                }
            }

            for (i, (collector, val)) in collectors.iter_mut().zip(cols_iter).enumerate() {
                // This is a workaround for different encoding methods used by TiDB and TiKV for CM Sketch.
                // We need this because we must ensure we are using the same encoding method when we are querying values from
                //   CM Sketch (in TiDB) and inserting values into CM Sketch (here).
                // We are inserting raw bytes from TableScanExecutor into CM Sketch here and query CM Sketch using bytes
                //   encoded by tablecodec.EncodeValue() in TiDB. Their results are different after row format becomes ver 2.
                //
                // Here we (1) convert INT bytes to VAR_INT bytes, (2) convert UINT bytes to VAR_UINT bytes,
                //   and (3) "flatten" the duration value from DURATION bytes into i64 value, then convert it to VAR_INT bytes.
                // These are the only 3 cases we need to care about according to TiDB's tablecodec.EncodeValue() and
                //   TiKV's V1CompatibleEncoder::write_v2_as_datum().
                let val = match val[0] {
                    INT_FLAG | UINT_FLAG | DURATION_FLAG => {
                        let mut mut_val = &val[..];
                        let decoded_val = mut_val.read_datum()?;
                        let flattened = table::flatten(&mut EvalContext::default(), decoded_val)?;
                        encode_value(&mut EvalContext::default(), &[flattened])?
                    }
                    _ => val,
                };

                if self.cols_info[i].as_accessor().is_string_like() {
                    let sorted_val = match_template_collator! {
                        TT, match self.cols_info[i].as_accessor().collation()? {
                            Collation::TT => {
                                let mut mut_val = val.as_slice();
                                let decoded_val = table::decode_col_value(&mut mut_val, &mut EvalContext::default(), &self.cols_info[i])?;
                                if decoded_val == Datum::Null {
                                    val
                                } else {
                                    let decoded_sorted_val = TT::sort_key(&decoded_val.as_string()?.unwrap().into_owned())?;
                                    encode_value(&mut EvalContext::default(), &[Datum::Bytes(decoded_sorted_val)]).unwrap()
                                }
                            }
                        }
                    };
                    collector.collect(sorted_val);
                    continue;
                }
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
        if data[0] == NIL_FLAG {
            self.null_count += 1;
            return;
        }
        self.count += 1;
        self.fm_sketch.insert(&data);
        if let Some(c) = self.cm_sketch.as_mut() {
            c.insert(&data);
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
