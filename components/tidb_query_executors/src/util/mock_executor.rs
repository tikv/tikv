// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use async_trait::async_trait;
use kvproto::{coprocessor::KeyRange, metapb::Region};
use tidb_query_common::{
    Result,
    error::StorageError,
    storage::{
        FindRegionResult, IntervalRange, OwnedKvPair, PointRange, RegionStorageAccessor,
        Result as StorageResult, StateRole, Storage,
    },
};
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::VectorValue},
    expr::EvalWarnings,
};
use tikv_util::{box_err, store::check_key_in_region};
use tipb::FieldType;

use crate::interface::*;

/// A simple mock executor that will return batch data according to a fixture
/// without any modification.
///
/// Normally this should be only used in tests.
pub struct MockExecutor {
    pub schema: Vec<FieldType>,
    pub results: std::vec::IntoIter<BatchExecuteResult>,
    pub intermediate_schema: Option<(usize, Vec<FieldType>)>,
    pub intermediate_results: std::vec::IntoIter<Vec<BatchExecuteResult>>,
    pub child: Option<Box<MockExecutor>>,
    pub scanned_range: Option<IntervalRange>,
}

impl MockExecutor {
    pub fn new(schema: Vec<FieldType>, results: Vec<BatchExecuteResult>) -> Self {
        assert!(!results.is_empty());
        Self {
            schema,
            results: results.into_iter(),
            intermediate_schema: None,
            intermediate_results: std::vec::IntoIter::default(),
            child: None,
            scanned_range: None,
        }
    }

    pub fn new_with_child(child: MockExecutor) -> Self {
        Self {
            schema: child.schema.clone(),
            results: vec![].into_iter(),
            intermediate_schema: None,
            intermediate_results: std::vec::IntoIter::default(),
            child: Some(Box::new(child)),
            scanned_range: None,
        }
    }

    pub fn set_next_intermediate_results(&mut self, results: Vec<BatchExecuteResult>) {
        self.intermediate_results = vec![results].into_iter();
    }
}

#[async_trait]
impl BatchExecutor for MockExecutor {
    type StorageStats = ();

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn intermediate_schema(&self, index: usize) -> Result<&[FieldType]> {
        if let Some((idx, schema)) = &self.intermediate_schema {
            if *idx == index {
                return Ok(schema);
            }
        }
        if let Some(child) = &self.child {
            return child.intermediate_schema(index);
        }
        Err(box_err!("no intermediate schema for index {}", index))
    }

    fn consume_and_fill_intermediate_results(
        &mut self,
        results: &mut [Vec<BatchExecuteResult>],
    ) -> Result<()> {
        if let Some((idx, _)) = &self.intermediate_schema {
            if let Some(mut next) = self.intermediate_results.next() {
                results[*idx].append(&mut next);
            }
        }
        if let Some(child) = &mut self.child {
            child.consume_and_fill_intermediate_results(results)?
        }
        Ok(())
    }

    async fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        match &mut self.child {
            Some(child) => child.next_batch(_scan_rows).await,
            None => self.results.next().unwrap(),
        }
    }

    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    fn collect_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        match &mut self.child {
            Some(child) => child.take_scanned_range(),
            None => self.scanned_range.take().unwrap(),
        }
    }

    fn can_be_cached(&self) -> bool {
        false
    }
}

pub struct MockScanExecutor {
    pub rows: Vec<i64>,
    pub pos: usize,
    schema: Vec<FieldType>,
}

impl MockScanExecutor {
    pub fn new(rows: Vec<i64>, schema: Vec<FieldType>) -> Self {
        MockScanExecutor {
            rows,
            pos: 0,
            schema,
        }
    }
}

#[async_trait]
impl BatchExecutor for MockScanExecutor {
    type StorageStats = ();

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn intermediate_schema(&self, _index: usize) -> Result<&[FieldType]> {
        unreachable!()
    }

    fn consume_and_fill_intermediate_results(
        &mut self,
        _results: &mut [Vec<BatchExecuteResult>],
    ) -> Result<()> {
        // Do nothing
        Ok(())
    }

    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let real_scan_rows = std::cmp::min(scan_rows, self.rows.len());
        // just one column
        let mut res_col = Vec::new();
        let mut res_logical_rows = Vec::new();
        let mut cur_row_idx = 0;
        while self.pos < self.rows.len() && cur_row_idx < real_scan_rows {
            res_col.push(Some(self.rows[self.pos]));
            res_logical_rows.push(cur_row_idx);
            self.pos += 1;
            cur_row_idx += 1;
        }
        let is_drained = if self.pos >= self.rows.len() {
            BatchExecIsDrain::Drain
        } else {
            BatchExecIsDrain::Remain
        };
        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(res_col.into())]),
            logical_rows: res_logical_rows,
            warnings: EvalWarnings::default(),
            is_drained: Ok(is_drained),
        }
    }

    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    fn collect_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        // Do nothing
        unreachable!()
    }

    fn can_be_cached(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MockStorage(pub Region, pub Vec<KeyRange>);

impl Storage for MockStorage {
    type Statistics = ();

    fn begin_scan(
        &mut self,
        _is_backward_scan: bool,
        _is_key_only: bool,
        _range: IntervalRange,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    fn scan_next(&mut self) -> StorageResult<Option<OwnedKvPair>> {
        unimplemented!()
    }

    fn get(
        &mut self,
        _is_key_only: bool,
        _range: PointRange,
    ) -> StorageResult<Option<OwnedKvPair>> {
        unimplemented!()
    }

    fn met_uncacheable_data(&self) -> Option<bool> {
        unimplemented!()
    }

    fn collect_statistics(&mut self, _dest: &mut Self::Statistics) {
        unimplemented!()
    }
}

#[derive(Default, Debug, Clone)]
pub struct MockAccessorExpect {
    find_region: Option<(Vec<u8>, Option<FindRegionResult>)>,
    expect_get_local_region_storage: Option<bool>,
}

#[derive(Clone)]
pub enum MockRegionStorageAccessor {
    Expect(Arc<Mutex<MockAccessorExpect>>),
    Data(Vec<(Region, StateRole)>),
}

impl MockRegionStorageAccessor {
    pub fn with_expect_mode() -> Self {
        Self::Expect(Arc::new(Mutex::new(MockAccessorExpect {
            ..Default::default()
        })))
    }

    pub fn with_regions_data(data: Vec<(Region, StateRole)>) -> Self {
        assert!(data.is_sorted_by(|a, b| { a.0.start_key < b.0.start_key }));
        Self::Data(data)
    }

    pub fn with_expect<T>(&self, f: impl Fn(&mut MockAccessorExpect) -> T) -> T {
        match self {
            Self::Expect(e) => {
                let mut expect = e.lock().unwrap();
                f(expect.deref_mut())
            }
            _ => panic!("not in expect mode"),
        }
    }

    pub fn expect_find_region(&self, key: Vec<u8>, region: Region, role: StateRole) {
        self.with_expect(|expect| {
            assert!(expect.find_region.is_none());
            expect.find_region = Some((
                key.clone(),
                Some(FindRegionResult::Found {
                    region: region.clone(),
                    role,
                }),
            ));
        });
    }

    pub fn expect_region_not_found(&self, key: Vec<u8>, next_region_start: Option<Vec<u8>>) {
        self.with_expect(|expect| {
            assert!(expect.find_region.is_none());
            expect.find_region = Some((
                key.clone(),
                Some(FindRegionResult::NotFound {
                    next_region_start: next_region_start.clone(),
                }),
            ));
        });
    }

    pub fn expect_find_region_error(&self, key: Vec<u8>) {
        self.with_expect(|expect| {
            assert!(expect.find_region.is_none());
            expect.find_region = Some((key.clone(), None));
        });
    }

    pub fn expect_get_local_region_storage(&self, success: bool) {
        self.with_expect(|expect| {
            assert!(expect.expect_get_local_region_storage.is_none());
            expect.expect_get_local_region_storage = Some(success);
        });
    }

    pub fn assert_no_exceptions(&self) {
        self.with_expect(|expect| {
            assert!(expect.find_region.is_none());
            assert!(expect.expect_get_local_region_storage.is_none());
        });
    }
}

#[async_trait]
impl RegionStorageAccessor for MockRegionStorageAccessor {
    type Storage = MockStorage;

    async fn find_region_by_key(&self, key: &[u8]) -> StorageResult<FindRegionResult> {
        match self {
            Self::Expect(_) => {
                let find_result = self.with_expect(|expect| -> Option<FindRegionResult> {
                    let (expect_key, find_result) = expect.find_region.take().unwrap();
                    assert_eq!(expect_key, key.to_vec());
                    find_result
                });

                if let Some(result) = find_result {
                    Ok(result)
                } else {
                    Err(StorageError::from(anyhow!("mock find region error")))
                }
            }
            Self::Data(data) => {
                let mut result = FindRegionResult::NotFound {
                    next_region_start: None,
                };
                for (region, role) in data.iter() {
                    if region.get_end_key().is_empty() || region.get_end_key() >= key {
                        if check_key_in_region(key, region) {
                            result = FindRegionResult::Found {
                                region: region.clone(),
                                role: *role,
                            };
                        } else {
                            result = FindRegionResult::NotFound {
                                next_region_start: Some(region.get_start_key().to_vec()),
                            };
                        }
                        break;
                    }
                }
                Ok(result)
            }
        }
    }

    async fn get_local_region_storage(
        &self,
        region: &Region,
        key_ranges: &[KeyRange],
    ) -> StorageResult<Self::Storage> {
        match self {
            Self::Expect(_) => {
                let success = self.with_expect(|expect| -> bool {
                    expect.expect_get_local_region_storage.take().unwrap()
                });

                if success {
                    Ok(MockStorage(region.clone(), key_ranges.to_vec()))
                } else {
                    Err(StorageError::from(anyhow!("mock get storage error")))
                }
            }
            Self::Data(_) => Ok(MockStorage(region.clone(), key_ranges.to_vec())),
        }
    }
}
