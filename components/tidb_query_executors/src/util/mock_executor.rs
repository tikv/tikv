// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use kvproto::{coprocessor::KeyRange, metapb::Region};
use tidb_query_common::{
    error::StorageError,
    storage::{
        FindRegionResult, IntervalRange, OwnedKvPairEntry, PointRange, RegionStorageAccessor,
        Result as StorageResult, StateRole, Storage,
    },
    Result,
};
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::VectorValue},
    expr::EvalWarnings,
};
use tipb::FieldType;

use crate::interface::*;

/// A simple mock executor that will return batch data according to a fixture
/// without any modification.
///
/// Normally this should be only used in tests.
pub struct MockExecutor {
    schema: Vec<FieldType>,
    results: std::vec::IntoIter<BatchExecuteResult>,
}

impl MockExecutor {
    pub fn new(schema: Vec<FieldType>, results: Vec<BatchExecuteResult>) -> Self {
        assert!(!results.is_empty());
        Self {
            schema,
            results: results.into_iter(),
        }
    }
}

#[async_trait]
impl BatchExecutor for MockExecutor {
    type StorageStats = ();

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    async fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        self.results.next().unwrap()
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
        _load_commit_ts: bool,
        _range: IntervalRange,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    fn scan_next_entry(&mut self) -> StorageResult<Option<OwnedKvPairEntry>> {
        unimplemented!()
    }

    fn get_entry(
        &mut self,
        _is_key_only: bool,
        _load_commit_ts: bool,
        _range: PointRange,
    ) -> StorageResult<Option<OwnedKvPairEntry>> {
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
        if data.len() > 1 {
            for i in 1..data.len() {
                let start1 = data[i - 1].0.start_key.clone();
                let start2 = data[i].0.start_key.clone();
                assert!(
                    start1 < start2,
                    "regions are not sorted {}, {:?} vs {:?}",
                    i,
                    start1,
                    start2
                );
            }
        }
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
