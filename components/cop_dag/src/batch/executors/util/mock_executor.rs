// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::expression::FieldType;

use crate::batch::interface::*;

/// A simple mock executor that will return batch data according to a fixture without any
/// modification.
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

impl BatchExecutor for MockExecutor {
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        self.results.next().unwrap()
    }

    fn collect_statistics(&mut self, _destination: &mut BatchExecuteStatistics) {
        // Do nothing
    }
}
