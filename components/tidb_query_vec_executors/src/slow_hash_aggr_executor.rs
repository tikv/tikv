// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::Arc;

use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::collections::HashMap;
use tikv_util::collections::HashMapEntry;
use tipb::Aggregation;
use tipb::{Expr, FieldType};

use crate::interface::*;
use crate::util::aggr_executor::*;
use crate::util::hash_aggr_helper::HashAggregationHelper;
use crate::util::*;
use tidb_query_common::storage::IntervalRange;
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use tidb_query_datatype::expr::{EvalConfig, EvalContext};
use tidb_query_vec_aggr::*;
use tidb_query_vec_expr::RpnStackNode;
use tidb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// Slow Hash Aggregation Executor supports multiple groups but uses less efficient ways to
/// store group keys in hash tables.
///
/// FIXME: It is not correct to just store the serialized data as the group key.
/// See pingcap/tidb#10467.
pub struct BatchSlowHashAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, SlowHashAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchSlowHashAggregationExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

// We assign a dummy type `Box<dyn BatchExecutor<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchSlowHashAggregationExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(!group_by_definitions.is_empty());
        for def in group_by_definitions {
            RpnExpressionBuilder::check_expr_tree_supported(def)?;
        }

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchSlowHashAggregationExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        group_by_exps: Vec<RpnExpression>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            Arc::new(EvalConfig::default()),
            src,
            group_by_exps,
            aggr_defs,
            aggr_def_parser,
        )
        .unwrap()
    }

    pub fn new(
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp_defs: Vec<Expr>,
        aggr_defs: Vec<Expr>,
    ) -> Result<Self> {
        let schema_len = src.schema().len();
        let mut ctx = EvalContext::new(config.clone());
        let mut group_by_exps = Vec::with_capacity(group_by_exp_defs.len());
        for def in group_by_exp_defs {
            group_by_exps.push(RpnExpressionBuilder::build_from_expr_tree(
                def, &mut ctx, schema_len,
            )?);
        }

        Self::new_impl(
            config,
            src,
            group_by_exps,
            aggr_defs,
            AllAggrDefinitionParser,
        )
    }

    #[inline]
    fn new_impl(
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exps: Vec<RpnExpression>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let mut group_key_offsets = Vec::with_capacity(1024);
        group_key_offsets.push(0);
        let group_by_exprs_field_type: Vec<&FieldType> = group_by_exps
            .iter()
            .map(|expr| expr.ret_field_type(src.schema()))
            .collect();
        let extra_group_by_col_index: Vec<usize> = group_by_exprs_field_type
            .iter()
            .enumerate()
            .filter(|(_, field_type)| {
                // The unwrap is fine because aggregate function parser should never return an
                // eval type that we cannot process later. If we made a mistake there, then we
                // should panic.
                EvalType::try_from(field_type.as_accessor().tp()).unwrap() == EvalType::Bytes
            })
            .map(|(col_index, _)| col_index)
            .collect();
        let mut original_group_by_col_index: Vec<usize> = (0..group_by_exps.len()).collect();
        for (i, extra_col_index) in extra_group_by_col_index.iter().enumerate() {
            original_group_by_col_index[*extra_col_index] = group_by_exps.len() + i;
        }
        let group_by_col_len = group_by_exps.len() + extra_group_by_col_index.len();
        let aggr_impl = SlowHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups: HashMap::default(),
            group_by_exps,
            extra_group_by_col_index,
            original_group_by_col_index,
            group_key_buffer: Box::new(Vec::with_capacity(8192)),
            group_key_offsets,
            states_offset_each_logical_row: Vec::with_capacity(crate::runner::BATCH_MAX_SIZE),
            group_by_results_unsafe: Vec::with_capacity(group_by_col_len),
            cached_encoded_result: vec![None; group_by_col_len],
        };

        Ok(Self(AggregationExecutor::new(
            aggr_impl,
            src,
            config,
            aggr_defs,
            aggr_def_parser,
        )?))
    }
}

pub struct SlowHashAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,

    /// The value is the group index. `states` and `group_key_offsets` are stored in
    /// the order of group index.
    groups: HashMap<GroupKeyRefUnsafe, usize>,
    group_by_exps: Vec<RpnExpression>,

    /// Extra group by columns store the bytes columns in original data form while
    /// default columns store them in sortkey form.
    /// The sortkey form is used to aggr on while the original form is to be returned
    /// as results.
    ///
    /// For example, the bytes column at index i will be stored in sortkey form at column i
    /// and in original data form at column `extra_group_by_col_index[i]`.
    extra_group_by_col_index: Vec<usize>,

    /// The sequence of group by column index which are in original form and are in the
    /// same order as group_by_exps by substituting bytes columns index for extra group by column index.
    original_group_by_col_index: Vec<usize>,

    /// Encoded group keys are stored in this buffer sequentially. Offsets of each encoded
    /// element are stored in `group_key_offsets`.
    ///
    /// `GroupKeyRefUnsafe` contains a raw pointer to this buffer.
    #[allow(clippy::box_vec)]
    group_key_buffer: Box<Vec<u8>>,

    /// The offsets of encoded keys in `group_key_buffer`. This `Vec` always has a leading `0`
    /// element. Then, the begin and end offsets of the "i"-th column of the group key whose group
    /// index is "j" are `group_key_offsets[j * group_by_col_len + i]` and
    /// `group_key_offsets[j * group_by_col_len + i + 1]`.
    ///
    /// group_by_col_len = group_by_exps.len() + extra_group_by_col_index.len()
    group_key_offsets: Vec<usize>,

    states_offset_each_logical_row: Vec<usize>,

    /// Stores evaluation results of group by expressions.
    /// It is just used to reduce allocations. The lifetime is not really 'static. The elements
    /// are only valid in the same batch where they are added.
    group_by_results_unsafe: Vec<RpnStackNode<'static>>,

    /// Cached encoded results for calculated Scalar results
    cached_encoded_result: Vec<Option<Vec<u8>>>,
}

unsafe impl Send for SlowHashAggregationImpl {}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for SlowHashAggregationImpl {
    #[inline]
    fn prepare_entities(&mut self, entities: &mut Entities<Src>) {
        let src_schema = entities.src.schema();
        for group_by_exp in &self.group_by_exps {
            entities
                .schema
                .push(group_by_exp.ret_field_type(src_schema).clone());
        }
    }

    #[inline]
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        mut input_physical_columns: LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()> {
        // 1. Calculate which group each src row belongs to.
        self.states_offset_each_logical_row.clear();

        let context = &mut entities.context;
        let src_schema = entities.src.schema();
        let logical_rows_len = input_logical_rows.len();
        let aggr_fn_len = entities.each_aggr_fn.len();

        // Decode columns with mutable input first, so subsequent access to input can be immutable
        // (and the borrow checker will be happy)
        ensure_columns_decoded(
            context,
            &self.group_by_exps,
            src_schema,
            &mut input_physical_columns,
            input_logical_rows,
        )?;
        assert!(self.group_by_results_unsafe.is_empty());
        unsafe {
            eval_exprs_decoded_no_lifetime(
                context,
                &self.group_by_exps,
                src_schema,
                &input_physical_columns,
                input_logical_rows,
                &mut self.group_by_results_unsafe,
            )?;
        }

        for logical_row_idx in 0..logical_rows_len {
            let offset_begin = self.group_key_buffer.len();

            // Always encode group keys to the buffer first
            // we'll then remove them if the group already exists
            for (i, group_by_result) in self.group_by_results_unsafe.iter().enumerate() {
                match group_by_result {
                    RpnStackNode::Vector { value, field_type } => {
                        value.as_ref().encode_sort_key(
                            value.logical_rows()[logical_row_idx],
                            field_type,
                            context,
                            &mut self.group_key_buffer,
                        )?;
                        self.group_key_offsets.push(self.group_key_buffer.len());
                    }
                    RpnStackNode::Scalar { value, field_type } => {
                        match self.cached_encoded_result[i].as_ref() {
                            Some(b) => {
                                self.group_key_buffer.extend_from_slice(b);
                            }
                            None => {
                                let mut cache_result = vec![];
                                value.as_scalar_value_ref().encode_sort_key(
                                    field_type,
                                    context,
                                    &mut cache_result,
                                )?;

                                self.group_key_buffer.extend_from_slice(&cache_result);
                                self.cached_encoded_result[i] = Some(cache_result);
                            }
                        }

                        self.group_key_offsets.push(self.group_key_buffer.len());
                    }
                }
            }

            // End of the sortkey columns
            let group_key_ref_end = self.group_key_buffer.len();

            // Encode bytes column in original form to extra group by columns, which is to be returned
            // as group by results
            for (i, col_index) in self.extra_group_by_col_index.iter().enumerate() {
                let group_by_result = &self.group_by_results_unsafe[*col_index];
                match group_by_result {
                    RpnStackNode::Vector { value, field_type } => {
                        debug_assert!(value.as_ref().eval_type() == EvalType::Bytes);
                        value.as_ref().encode(
                            value.logical_rows()[logical_row_idx],
                            field_type,
                            context,
                            &mut self.group_key_buffer,
                        )?;
                        self.group_key_offsets.push(self.group_key_buffer.len());
                    }
                    RpnStackNode::Scalar { value, field_type } => {
                        debug_assert!(value.eval_type() == EvalType::Bytes);

                        let i = i + self.group_by_exps.len();
                        match self.cached_encoded_result[i].as_ref() {
                            Some(b) => {
                                self.group_key_buffer.extend_from_slice(b);
                            }
                            None => {
                                let mut cache_result = vec![];
                                value.as_scalar_value_ref().encode(
                                    field_type,
                                    context,
                                    &mut cache_result,
                                )?;

                                self.group_key_buffer.extend_from_slice(&cache_result);
                                self.cached_encoded_result[i] = Some(cache_result);
                            }
                        }

                        self.group_key_offsets.push(self.group_key_buffer.len());
                    }
                }
            }

            let buffer_ptr = (&*self.group_key_buffer).into();
            // Extra column is not included in `GroupKeyRefUnsafe` to avoid being aggr on.
            let group_key_ref_unsafe = GroupKeyRefUnsafe {
                buffer_ptr,
                begin: offset_begin,
                end: group_key_ref_end,
            };

            let group_len = self.groups.len();
            let group_index = match self.groups.entry(group_key_ref_unsafe) {
                HashMapEntry::Vacant(entry) => {
                    // if it's a new group, the group index is the current group count
                    entry.insert(group_len);
                    for aggr_fn in &entities.each_aggr_fn {
                        self.states.push(aggr_fn.create_state());
                    }
                    group_len
                }
                HashMapEntry::Occupied(entry) => {
                    // remove the duplicated group key
                    self.group_key_buffer.truncate(offset_begin);
                    self.group_key_offsets.truncate(
                        self.group_key_offsets.len()
                            - self.group_by_exps.len()
                            - self.extra_group_by_col_index.len(),
                    );
                    *entry.get()
                }
            };
            self.states_offset_each_logical_row
                .push(group_index * aggr_fn_len);
        }

        // 2. Update states according to the group.
        HashAggregationHelper::update_each_row_states_by_offset(
            entities,
            &mut input_physical_columns,
            input_logical_rows,
            &mut self.states,
            &self.states_offset_each_logical_row,
        )?;

        // Remember to remove expression results of the current batch. They are invalid
        // in the next batch.
        self.group_by_results_unsafe.clear();

        Ok(())
    }

    #[inline]
    fn groups_len(&self) -> usize {
        self.groups.len()
    }

    #[inline]
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        assert!(src_is_drained);

        let number_of_groups = self.groups.len();
        let mut group_by_columns: Vec<_> = self
            .group_by_exps
            .iter()
            .map(|_| LazyBatchColumn::raw_with_capacity(number_of_groups))
            .collect();
        let aggr_fns_len = entities.each_aggr_fn.len();

        let groups = std::mem::replace(&mut self.groups, HashMap::default());
        for (_, group_index) in groups {
            let states_start_offset = group_index * aggr_fns_len;
            iteratee(
                entities,
                &self.states[states_start_offset..states_start_offset + aggr_fns_len],
            )?;

            // Extract group column from group key for each group
            let group_key_offsets = &self.group_key_offsets
                [group_index * (self.group_by_exps.len() + self.extra_group_by_col_index.len())..];

            for group_index in 0..self.group_by_exps.len() {
                // Read from extra column if it's a bytes column
                let buffer_group_index = self.original_group_by_col_index[group_index];
                let offset_begin = group_key_offsets[buffer_group_index];
                let offset_end = group_key_offsets[buffer_group_index + 1];
                group_by_columns[group_index]
                    .mut_raw()
                    .push(&self.group_key_buffer[offset_begin..offset_end]);
            }
        }

        Ok(group_by_columns)
    }

    /// Slow hash aggregation can output aggregate results only if the source is drained.
    #[inline]
    fn is_partial_results_ready(&self) -> bool {
        false
    }
}

/// A reference to a group key slice in the `group_key_buffer` of `SlowHashAggregationImpl`.
///
/// It is safe as soon as it doesn't outlive the `SlowHashAggregationImpl` that creates this
/// reference.
struct GroupKeyRefUnsafe {
    /// Points to the `group_key_buffer` of `SlowHashAggregationImpl`
    buffer_ptr: NonNull<Vec<u8>>,
    begin: usize,
    end: usize,
}

impl Hash for GroupKeyRefUnsafe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { self.buffer_ptr.as_ref()[self.begin..self.end].hash(state) }
    }
}

impl PartialEq for GroupKeyRefUnsafe {
    fn eq(&self, other: &GroupKeyRefUnsafe) -> bool {
        unsafe {
            self.buffer_ptr.as_ref()[self.begin..self.end]
                == other.buffer_ptr.as_ref()[other.begin..other.end]
        }
    }
}

impl Eq for GroupKeyRefUnsafe {}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_datatype::FieldTypeTp;
    use tipb::ScalarFuncSig;

    use crate::util::aggr_executor::tests::*;
    use tidb_query_datatype::codec::data_type::*;
    use tidb_query_vec_expr::impl_arithmetic::{arithmetic_fn_meta, RealPlus};
    use tidb_query_vec_expr::RpnExpressionBuilder;

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_it_works_integration() {
        use tipb::ExprType;
        use tipb_helper::ExprDefBuilder;

        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - AVG(col_0 + 5.0)
        // And group by:
        // - col_4
        // - 1 (Constant)
        // - col_0 + 1

        let group_by_exps = vec![
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(4)
                .build_for_test(),
            RpnExpressionBuilder::new_for_test()
                .push_constant_for_test(1)
                .build_for_test(),
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(0)
                .push_constant_for_test(1.0)
                .push_fn_call_for_test(arithmetic_fn_meta::<RealPlus>(), 2, FieldTypeTp::Double)
                .build_for_test(),
        ];

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::PlusReal, FieldTypeTp::Double)
                        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
                        .push_child(ExprDefBuilder::constant_real(5.0)),
                )
                .build(),
        ];

        let src_exec = make_src_executor_1();
        let mut exec = BatchSlowHashAggregationExecutor::new_for_test(
            src_exec,
            group_by_exps,
            aggr_definitions,
            AllAggrDefinitionParser,
        );

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let mut r = exec.next_batch(1);
        // col_4 (sort_key),    col_0 + 1 can result in:
        // NULL,                NULL
        // aa,                  NULL
        // aaa,                 8
        // ááá,                 2.5
        assert_eq!(&r.logical_rows, &[0, 1, 2, 3]);
        assert_eq!(r.physical_columns.rows_len(), 4);
        assert_eq!(r.physical_columns.columns_len(), 6); // 3 result column, 3 group by column

        let mut ctx = EvalContext::default();
        // Let's check the three group by column first.
        r.physical_columns[3]
            .ensure_all_decoded_for_test(&mut ctx, &exec.schema()[3])
            .unwrap();
        r.physical_columns[4]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schema()[4])
            .unwrap();
        r.physical_columns[5]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &exec.schema()[5])
            .unwrap();

        // The row order is not defined. Let's sort it by the group by column before asserting.
        let mut sort_column: Vec<(usize, _)> = r.physical_columns[3]
            .decoded()
            .as_bytes_slice()
            .iter()
            .enumerate()
            .collect();
        sort_column.sort_by(|a, b| a.1.cmp(&b.1));

        // Use the order of the sorted column to sort other columns
        let ordered_column: Vec<_> = sort_column
            .iter()
            .map(|(idx, _)| r.physical_columns[3].decoded().as_bytes_slice()[*idx].clone())
            .collect();
        assert_eq!(
            &ordered_column,
            &[
                None,
                Some(b"aa".to_vec()),
                Some(b"aaa".to_vec()),
                Some("ááá".as_bytes().to_vec())
            ]
        );
        assert_eq!(
            r.physical_columns[4].decoded().as_int_slice(),
            &[Some(1), Some(1), Some(1), Some(1)]
        );
        let ordered_column: Vec<_> = sort_column
            .iter()
            .map(|(idx, _)| r.physical_columns[5].decoded().as_real_slice()[*idx])
            .collect();
        assert_eq!(
            &ordered_column,
            &[None, None, Real::new(8.0).ok(), Real::new(2.5).ok()]
        );

        let ordered_column: Vec<_> = sort_column
            .iter()
            .map(|(idx, _)| r.physical_columns[0].decoded().as_int_slice()[*idx])
            .collect();
        assert_eq!(&ordered_column, &[Some(1), Some(2), Some(1), Some(1)]);
        let ordered_column: Vec<_> = sort_column
            .iter()
            .map(|(idx, _)| r.physical_columns[1].decoded().as_int_slice()[*idx])
            .collect();
        assert_eq!(&ordered_column, &[Some(0), Some(0), Some(1), Some(1)]);
        let ordered_column: Vec<_> = sort_column
            .iter()
            .map(|(idx, _)| r.physical_columns[2].decoded().as_real_slice()[*idx])
            .collect();
        assert_eq!(
            &ordered_column,
            &[None, None, Real::new(12.0).ok(), Real::new(6.5).ok()]
        );
    }
}
