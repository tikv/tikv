// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use hashbrown::hash_map::Entry;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::exec_summary::ExecSummaryCollectorDisabled;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::{Error, Result};

fn aggregate_single_group(
    aggr_fn_len: usize,
    states: &[Box<dyn AggrFunctionState>],
    states_offset: usize,
    aggr_fn_output_cardinality: &[usize],
    ordered_aggr_fn_output_types: &[EvalType],
    ctx: &mut EvalContext,
    aggr_output_columns: &mut [VectorValue],
) -> Result<()> {
    let mut output_column_offset = 0;
    for index in 0..aggr_fn_len {
        let aggr_fn_state = &states[states_offset + index];
        let output_cardinality = aggr_fn_output_cardinality[index];
        assert!(output_cardinality > 0);

        if output_cardinality == 1 {
            // Single output column, we use `Vec<Option<T>>` as container.
            let output_type = ordered_aggr_fn_output_types[output_column_offset];
            match_template_evaluable! {
                TT, match output_type {
                    EvalType::TT => {
                        let concrete_output_column = AsMut::<Vec<Option<TT>>>::as_mut(&mut aggr_output_columns[output_column_offset]);
                        aggr_fn_state.push_result(ctx, concrete_output_column)?;
                    }
                }
            }
        }

        output_column_offset += output_cardinality;
    }
    Ok(())
}

enum SingleGroupLookup {
    // 1. The value of each hash table is the start index in `states`. When there are new groups
    //    (i.e. new entry in the hash table), the states of the groups will be appended to `states`.
    Int(HashMap<Option<Int>, usize>),
    Real(HashMap<Option<Real>, usize>),
    Decimal(HashMap<Option<Decimal>, usize>),
    Bytes(HashMap<Option<Bytes>, usize>),
    DateTime(HashMap<Option<DateTime>, usize>),
    Duration(HashMap<Option<Duration>, usize>),
    Json(HashMap<Option<Json>, usize>),
}

struct MultiGroupInfo {
    /// The start index in `states`. It serves a same purpose as the hash table values in
    /// `SingleGroupLookup` fields.
    states_start_index: usize,

    /// The length of each GROUP BY column in the group key.
    ///
    /// For example, if the group key `AaaFfffff` is composed by two group column `[Aaa, Ffffff]`,
    /// then the value of this field is `[3, 6]`.
    ///
    /// We want aggregation executor to provide each GROUP BY column independently, which will be
    /// useful in future executors (like Projection executor). However, currently this segmentation
    /// is not useful because we return data by row and columns will be always pasted together.
    /// Thus segmentation or not doesn't make any differences.
    group_key_segments: Vec<usize>,
}

pub struct BatchHashAggregationExecutor<C: ExecSummaryCollector, Src: BatchExecutor> {
    summary_collector: C,
    context: EvalContext,
    src: Src,

    is_ended: bool,

    group_bys: Vec<RpnExpression>,

    /// The field type of each group by expression.
    group_by_types: Vec<EvalType>,

    /// Aggregate function states.
    aggr_fns: Vec<Box<dyn AggrFunction>>,

    aggr_fn_output_cardinality: Vec<usize>, // One slot each aggregate function

    /// The schema of aggregation output is not given directly, so we need to compute one.
    ordered_schema: Vec<FieldType>, // maybe multiple slot each aggregate function

    // Maybe multiple slot each aggregate function
    ordered_aggr_fn_output_types: Vec<EvalType>,

    // Maybe multiple slot each aggregate function. However currently we only support 1 parameter
    // aggregate function so it accidentally becomes one slot each aggregate function.
    ordered_aggr_fn_input_types: Vec<EvalType>,

    // Maybe multiple slot each aggregate function. However currently we only support 1 parameter
    // aggregate function so it accidentally becomes one slot each aggregate function.
    ordered_aggr_fn_input_exprs: Vec<RpnExpression>,

    number_of_groups: usize,
    states: Vec<Box<dyn AggrFunctionState>>,
    multi_group_lookup: HashMap<Vec<u8>, MultiGroupInfo>,
    single_group_lookup: SingleGroupLookup,
}

impl BatchHashAggregationExecutor<ExecSummaryCollectorDisabled, Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(!group_by_definitions.is_empty());
        for def in group_by_definitions {
            RpnExpressionBuilder::check_expr_tree_supported(def).map_err(|e| {
                Error::Other(box_err!(
                    "Unable to use BatchHashAggregationExecutor: {}",
                    e
                ))
            })?;
        }

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AggrDefinitionParser::check_supported(def).map_err(|e| {
                Error::Other(box_err!(
                    "Unable to use BatchHashAggregationExecutor: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchHashAggregationExecutor<ExecSummaryCollectorDisabled, Src> {
    #[cfg(test)]
    pub fn new_for_test<F>(
        src: Src,
        group_bys: Vec<RpnExpression>,
        aggr_definitions: Vec<Expr>,
        parse_aggr_definition: F,
    ) -> Self
    where
        F: Fn(
            Expr,
            &Tz,
            usize,
            &[FieldType],
            &mut Vec<FieldType>,
            &mut Vec<RpnExpression>,
        ) -> Result<Box<dyn AggrFunction>>,
    {
        Self::new_impl(
            ExecSummaryCollectorDisabled,
            Arc::new(EvalConfig::default()),
            src,
            group_bys,
            aggr_definitions,
            parse_aggr_definition,
        )
        .unwrap()
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchHashAggregationExecutor<C, Src> {
    pub fn new(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        group_by_definitions: Vec<Expr>,
        aggr_definitions: Vec<Expr>,
    ) -> Result<Self> {
        let mut group_bys = Vec::with_capacity(group_by_definitions.len());
        for def in group_by_definitions {
            group_bys.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &config.tz,
                src.schema().len(),
            )?);
        }

        Self::new_impl(
            summary_collector,
            config,
            src,
            group_bys,
            aggr_definitions,
            AggrDefinitionParser::parse,
        )
    }

    fn new_impl<F>(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        group_bys: Vec<RpnExpression>,
        aggr_definitions: Vec<Expr>,
        parse_aggr_definition: F,
    ) -> Result<Self>
    where
        F: Fn(
            Expr,
            &Tz,
            usize,
            &[FieldType],
            &mut Vec<FieldType>,
            &mut Vec<RpnExpression>,
        ) -> Result<Box<dyn AggrFunction>>,
    {
        assert!(!group_bys.is_empty());

        let aggr_len = aggr_definitions.len();
        if aggr_len == 0 {
            return Err(box_err!("There should be at least one aggregate function"));
        }

        let schema = src.schema();
        let schema_len = schema.len();

        let group_by_types: Vec<_> = group_bys
            .iter()
            .map(|exp| EvalType::try_from(exp.ret_field_type(schema).tp()).unwrap())
            .collect();

        let single_group_lookup = if group_bys.len() == 1 {
            match_template_evaluable! {
                TT, match &group_by_types[0] {
                    EvalType::TT => SingleGroupLookup::TT(HashMap::default()),
                }
            }
        } else {
            // Not used
            SingleGroupLookup::Int(HashMap::default())
        };

        let mut aggr_fns = Vec::with_capacity(aggr_len);
        let mut aggr_fn_output_cardinality = Vec::with_capacity(aggr_len);
        let mut ordered_schema = Vec::with_capacity(aggr_len * 2);
        let mut ordered_aggr_fn_input_exprs = Vec::with_capacity(aggr_len * 2);

        for def in aggr_definitions {
            let aggr_output_len = ordered_schema.len();
            let aggr_input_len = ordered_aggr_fn_input_exprs.len();

            let aggr_fn = parse_aggr_definition(
                def,
                &config.tz,
                schema_len,
                schema,
                &mut ordered_schema,
                &mut ordered_aggr_fn_input_exprs,
            )?;

            assert!(ordered_schema.len() > aggr_output_len);
            let this_aggr_output_len = ordered_schema.len() - aggr_output_len;

            // We only support 1 parameter aggregate functions, so let's simply assert it.
            assert_eq!(ordered_aggr_fn_input_exprs.len(), aggr_input_len + 1);

            aggr_fns.push(aggr_fn);
            aggr_fn_output_cardinality.push(this_aggr_output_len);
        }

        let ordered_aggr_fn_output_types = ordered_schema
            .iter()
            .map(|ft| {
                // The unwrap is fine because aggregate function parser should never return an
                // eval type that we cannot process later. If we made a mistake there, then we
                // should panic.
                EvalType::try_from(ft.tp()).unwrap()
            })
            .collect();

        let ordered_aggr_fn_input_types = ordered_aggr_fn_input_exprs
            .iter()
            .map(|expr| {
                // The eval type of the aggr input is the return type of the aggr input expression.
                let ft = expr.ret_field_type(schema);
                // The unwrap is also fine because the expression must be valid, otherwise it is
                // unexpected behaviour and should panic.
                EvalType::try_from(ft.tp()).unwrap()
            })
            .collect();

        // Finally append group columns to the schema
        for group_by in &group_bys {
            ordered_schema.push(group_by.ret_field_type(schema).clone());
        }

        Ok(Self {
            summary_collector,
            context: EvalContext::new(config),
            src,
            is_ended: false,
            group_bys,
            group_by_types,
            aggr_fns,
            aggr_fn_output_cardinality,
            ordered_schema,
            ordered_aggr_fn_output_types,
            ordered_aggr_fn_input_types,
            ordered_aggr_fn_input_exprs,
            number_of_groups: 0,
            states: Vec::with_capacity(1024),
            multi_group_lookup: HashMap::default(),
            single_group_lookup,
        })
    }

    #[inline]
    fn process_src_data(&mut self, mut data: LazyBatchColumnVec) -> Result<()> {
        let rows_len = data.rows_len();
        if rows_len == 0 {
            return Ok(());
        }

        // TODO: Reuse this vector to avoid allocation.
        let mut states_offset_each_row = Vec::with_capacity(rows_len);

        // 1. Calculate the group of each row by evaluating the GROUP BY expression.
        // TODO: Maybe use different type for different group by cardinality.
        let group_by_cardinality = self.group_bys.len();
        if group_by_cardinality == 1 {
            // Fast path: group by only one column. Zero copy & allocate when group exists.
            self.calc_single_group_column_offset(&mut data, &mut states_offset_each_row)?;
        } else if group_by_cardinality > 1 {
            // Slow path: group by multiple columns.
            self.calc_multi_group_column_offset(&mut data, &mut states_offset_each_row)?;
        } else {
            unreachable!();
        }

        // 2. Aggregate each function
        for aggr_fn_index in 0..self.aggr_fns.len() {
            // First, evaluates the expression to get the data to aggregate.
            let eval_output = self.ordered_aggr_fn_input_exprs[aggr_fn_index].eval(
                &mut self.context,
                rows_len,
                self.src.schema(),
                &mut data,
            )?;

            // Next, feed the evaluation result to the aggregate function.
            let input_type = self.ordered_aggr_fn_input_types[aggr_fn_index];
            match eval_output {
                RpnStackNode::Scalar { value, field_type } => {
                    // TODO: We should be able to know `update_type()` from parser.
                    assert_eq!(input_type, EvalType::try_from(field_type.tp()).unwrap());
                    match_template_evaluable! {
                        TT, match input_type {
                            EvalType::TT => {
                                let concrete_value: &Option<TT> = value.as_ref();
                                for offset in &states_offset_each_row {
                                    let aggr_fn_state = &mut self.states[*offset + aggr_fn_index];
                                    aggr_fn_state.update(&mut self.context, concrete_value)?;
                                }
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, field_type } => {
                    assert_eq!(input_type, EvalType::try_from(field_type.tp()).unwrap());
                    match_template_evaluable! {
                        TT, match input_type {
                            EvalType::TT => {
                                let vector_value: &[Option<TT>] = value.as_ref();
                                for (row_index, offset) in states_offset_each_row.iter().enumerate() {
                                    let aggr_fn_state = &mut self.states[*offset + aggr_fn_index];
                                    aggr_fn_state.update(&mut self.context, &vector_value[row_index])?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn calc_single_group_column_offset(
        &mut self,
        data: &mut LazyBatchColumnVec,
        output_offsets: &mut Vec<usize>,
    ) -> Result<()> {
        let eval_result =
            self.group_bys[0].eval(&mut self.context, data.rows_len(), self.src.schema(), data)?;
        // Unwrap is fine because we have verified the group by expression before.
        let group_by_vector = eval_result.vector_value().unwrap();
        match_template_evaluable! {
            TT, match group_by_vector {
                VectorValue::TT(v) => {
                    if let SingleGroupLookup::TT(group) = &mut self.single_group_lookup {
                        Self::calc_single_group_column_offset_impl(
                            v,
                            &self.aggr_fns,
                            group,
                            &mut self.states,
                            output_offsets,
                            &mut self.number_of_groups,
                        )
                    } else {
                        panic!();
                    }
                },
            }
        }
        Ok(())
    }

    fn calc_single_group_column_offset_impl<T: Evaluable>(
        rows: &[Option<T>],
        aggr_fns: &[Box<dyn AggrFunction>],
        group: &mut HashMap<Option<T>, usize>,
        states: &mut Vec<Box<dyn AggrFunctionState>>,
        output_offsets: &mut Vec<usize>,
        number_of_groups: &mut usize,
    ) {
        for val in rows {
            // Not using the entry API so that when entry exists there is no clone.
            match group.get(val) {
                Some(states_index) => {
                    output_offsets.push(*states_index);
                }
                None => {
                    // Group does not exist, prepare groups.
                    let index = states.len();
                    output_offsets.push(index);
                    group.insert(val.clone(), index);
                    *number_of_groups += 1;
                    for aggr_fn in aggr_fns {
                        states.push(aggr_fn.create_state());
                    }
                }
            }
        }
    }

    fn calc_multi_group_column_offset(
        &mut self,
        data: &mut LazyBatchColumnVec,
        output_offsets: &mut Vec<usize>,
    ) -> Result<()> {
        let rows_len = data.rows_len();
        let mut group_keys = vec![Vec::new(); rows_len];
        let mut group_key_segments = vec![Vec::new(); rows_len];

        for group_by in &self.group_bys {
            let eval_result =
                group_by.eval(&mut self.context, rows_len, self.src.schema(), data)?;
            for row_index in 0..rows_len {
                // Unwrap is fine because we have verified the group by expression before.
                let group_column = eval_result.vector_value().unwrap();
                let group_key_previous_length = group_keys[row_index].len();
                group_column.encode(
                    row_index,
                    eval_result.field_type(),
                    &mut group_keys[row_index],
                )?;
                // Only put encoded length of this column (instead of all columns) in the array.
                let group_column_len = group_keys[row_index].len() - group_key_previous_length;
                group_key_segments[row_index].push(group_column_len);
            }
        }

        for (group_key, segments) in group_keys.into_iter().zip(group_key_segments) {
            match self.multi_group_lookup.entry(group_key) {
                Entry::Vacant(entry) => {
                    let index = self.states.len();
                    output_offsets.push(index);
                    entry.insert(MultiGroupInfo {
                        states_start_index: index,
                        group_key_segments: segments,
                    });
                    self.number_of_groups += 1;
                    for aggr_fn in &self.aggr_fns {
                        self.states.push(aggr_fn.create_state());
                    }
                }
                Entry::Occupied(entry) => {
                    output_offsets.push(entry.get().states_start_index);
                }
            }
        }

        Ok(())
    }

    // Don't inline this function to reduce hot code size.
    #[inline(never)]
    fn aggregate_results(&mut self) -> Result<LazyBatchColumnVec> {
        // Output columns consist of aggregate function output columns and GROUP BY columns.
        // - Aggregate function output columns are always decoded.
        // - When there is only one GROUP BY column, the GROUP BY column is decoded.
        // - When there are multiple GROUP BY columns, these GROUP BY columns are **not** decoded:
        //   We already serialized these GROUP BY columns when building the group key, so no need
        //   to serialize them again. Just keeping the serialized data is enough.
        assert_eq!(self.group_by_types.len(), self.group_bys.len());

        let mut aggr_output_columns: Vec<_> = self
            .ordered_aggr_fn_output_types
            .iter()
            .map(|eval_type| VectorValue::with_capacity(1, *eval_type))
            .collect();

        let group_by_cardinality = self.group_by_types.len();
        let mut group_by_columns = Vec::with_capacity(group_by_cardinality);
        if group_by_cardinality == 1 {
            group_by_columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                self.number_of_groups,
                self.group_by_types[0],
            ));
        } else if group_by_cardinality > 1 {
            for _ in 0..group_by_cardinality {
                group_by_columns.push(LazyBatchColumn::raw_with_capacity(self.number_of_groups));
            }
        } else {
            unreachable!();
        }

        if group_by_cardinality == 1 {
            match_template_evaluable! {
                TT, match &mut self.single_group_lookup {
                    SingleGroupLookup::TT(group) => {
                        // Single group column
                        let map = std::mem::replace(group, HashMap::default());
                        for (group_key, states_start_index) in map {
                            // Output aggregate function columns first.
                            aggregate_single_group(
                                self.aggr_fns.len(),
                                &self.states,
                                states_start_index,
                                &self.aggr_fn_output_cardinality,
                                &self.ordered_aggr_fn_output_types,
                                &mut self.context,
                                &mut aggr_output_columns,
                            )?;
                            // Output group columns next.
                            group_by_columns[0].mut_decoded().push(group_key);
                        }
                    }
                }
            }
        } else if group_by_cardinality > 1 {
            // Multi group column
            // Take the map out, and then use `HashMap::into_iter()`. This should be faster
            // then using `HashMap::drain()`.
            // TODO: Verify performance difference.
            let map = std::mem::replace(&mut self.multi_group_lookup, HashMap::default());
            for (group_key, group_info) in map {
                // Output aggregate function columns first.
                aggregate_single_group(
                    self.aggr_fns.len(),
                    &self.states,
                    group_info.states_start_index,
                    &self.aggr_fn_output_cardinality,
                    &self.ordered_aggr_fn_output_types,
                    &mut self.context,
                    &mut aggr_output_columns,
                )?;

                // Output group columns next.
                let mut group_key_start_offset = 0;
                for (group_index, group_segment_len) in
                    group_info.group_key_segments.into_iter().enumerate()
                {
                    group_by_columns[group_index].push_raw(
                        &group_key
                            [group_key_start_offset..group_key_start_offset + group_segment_len],
                    );
                    group_key_start_offset += group_segment_len;
                }
            }
        } else {
            unreachable!();
        }

        let mut columns = Vec::with_capacity(aggr_output_columns.len() + group_by_columns.len());
        for aggr_output_column in aggr_output_columns {
            columns.push(LazyBatchColumn::Decoded(aggr_output_column));
        }
        for group_by_column in group_by_columns {
            columns.push(group_by_column);
        }

        let ret = LazyBatchColumnVec::from(columns);
        assert_eq!(ret.rows_len(), 1);
        ret.assert_columns_equal_length();

        Ok(ret)
    }

    #[inline]
    fn handle_next_batch(&mut self) -> Result<Option<LazyBatchColumnVec>> {
        // Use max batch size from the beginning because aggregation executor always scans all data
        let src_result = self
            .src
            .next_batch(crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE);

        self.context.warnings = src_result.warnings;

        // When there are errors in the underlying executor, there must be no aggregate output.
        // Thus we even don't need to update the aggregate function state and can return directly.
        let src_is_drained = src_result.is_drained?;

        // Consume all data from the underlying executor. We directly return when there are errors
        // for the same reason as above.
        self.process_src_data(src_result.data)?;

        // Aggregate a result if source executor is drained, otherwise just return nothing.
        if src_is_drained {
            Ok(Some(self.aggregate_results()?))
        } else {
            Ok(None)
        }
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchExecutor
    for BatchHashAggregationExecutor<C, Src>
{
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.ordered_schema.as_slice()
    }

    #[inline]
    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let timer = self.summary_collector.on_start_iterate();
        let result = self.handle_next_batch();

        let ret = match result {
            Err(e) => {
                // When there are error, we can just return empty data.
                self.is_ended = true;
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok(None) => {
                // When there is no error and is not drained, we also return empty data.
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(false),
                }
            }
            Ok(Some(data)) => {
                // When there is no error and aggregate finished, we return it as data.
                self.is_ended = true;
                BatchExecuteResult {
                    data,
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(true),
                }
            }
        };

        self.summary_collector
            .on_finish_iterate(timer, ret.data.rows_len());
        ret
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
        self.summary_collector
            .collect_into(&mut destination.summary_per_executor);
    }
}
