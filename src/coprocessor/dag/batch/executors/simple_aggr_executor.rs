// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Simple aggregation is an aggregation that do not have `GROUP BY`s. It is more even more simpler
//! than stream aggregation.

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::executor::Aggregation;
use tipb::expression::Expr;
use tipb::expression::FieldType;

use super::super::interface::*;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::RpnExpression;
use crate::coprocessor::{Error, Result};

pub struct BatchSimpleAggregationExecutor<C: ExecSummaryCollector, Src: BatchExecutor> {
    summary_collector: C,
    context: EvalContext,
    src: Src,

    is_ended: bool,

    /// Aggregate function states.
    aggr_fn_states: Vec<Box<dyn AggrFunctionState>>,

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
}

impl
    BatchSimpleAggregationExecutor<
        crate::coprocessor::dag::batch::statistics::ExecSummaryCollectorDisabled,
        Box<dyn BatchExecutor>,
    >
{
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        assert_eq!(descriptor.get_group_by().len(), 0);
        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AggrDefinitionParser::check_supported(def).map_err(|e| {
                Error::Other(box_err!(
                    "Unable to use BatchSimpleAggregateExecutor: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}

// TODO: This macro may be able to be reused elsewhere.
macro_rules! match_each_eval_type_and_call {
    (match $target:expr => $fn_name:ident<T>($($parameters:expr),*)) => {{
        match $target {
            EvalType::Int => $fn_name::<Int>($($parameters),*),
            EvalType::Real => $fn_name::<Real>($($parameters),*),
            EvalType::Decimal => $fn_name::<Decimal>($($parameters),*),
            EvalType::Bytes => $fn_name::<Bytes>($($parameters),*),
            EvalType::DateTime => $fn_name::<DateTime>($($parameters),*),
            EvalType::Duration => $fn_name::<Duration>($($parameters),*),
            EvalType::Json => $fn_name::<Json>($($parameters),*),
        }
    }};
}

#[inline]
// The `Box<>` wrapper cannot be remove actually. Thus disable the clippy rule. Additionally this
// function will be inlined so that performance will not be affected.
#[allow(clippy::borrowed_box)]
fn update_from_scalar_eval_output<T>(
    aggr_fn_state: &mut Box<dyn AggrFunctionState>,
    ctx: &mut EvalContext,
    value: &ScalarValue,
    rows: usize,
) -> Result<()>
where
    T: Evaluable,
    ScalarValue: AsRef<Option<T>>,
    (dyn AggrFunctionState + 'static): AggrFunctionStateUpdatePartial<T>,
{
    let concrete_value = AsRef::<Option<T>>::as_ref(value);
    aggr_fn_state.update_repeat(ctx, concrete_value, rows)
}

#[inline]
#[allow(clippy::borrowed_box)]
fn update_from_vector_eval_output<T>(
    aggr_fn_state: &mut Box<dyn AggrFunctionState>,
    ctx: &mut EvalContext,
    value: &VectorValue,
) -> Result<()>
where
    T: Evaluable,
    VectorValue: AsRef<[Option<T>]>,
    (dyn AggrFunctionState + 'static): AggrFunctionStateUpdatePartial<T>,
{
    let vector_value = AsRef::<[Option<T>]>::as_ref(value.as_ref());
    aggr_fn_state.update_vector(ctx, vector_value)
}

#[inline]
#[allow(clippy::borrowed_box)]
fn push_result_to_column<T>(
    aggr_fn_state: &mut Box<dyn AggrFunctionState>,
    ctx: &mut EvalContext,
    output_column: &mut VectorValue,
) -> Result<()>
where
    T: Evaluable,
    VectorValue: AsMut<Vec<Option<T>>>,
    dyn AggrFunctionState: AggrFunctionStateResultPartial<Vec<Option<T>>>,
{
    let concrete_output_column = AsMut::<Vec<Option<T>>>::as_mut(output_column);
    aggr_fn_state.push_result(ctx, concrete_output_column)
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchSimpleAggregationExecutor<C, Src> {
    pub fn new(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        aggr_definitions: Vec<Expr>,
    ) -> Result<Self> {
        Self::new_impl(
            summary_collector,
            config,
            src,
            aggr_definitions,
            AggrDefinitionParser::parse,
        )
    }

    /// Provides ability to customize `AggrDefinitionParser::parse`. Useful in tests.
    fn new_impl<F>(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        aggr_definitions: Vec<Expr>,
        parse_aggr_definition: F,
    ) -> Result<Self>
    where
        F: Fn(
            Expr,
            &Tz,
            usize,
            &mut Vec<FieldType>,
            &mut Vec<RpnExpression>,
        ) -> Result<Box<dyn AggrFunction>>,
    {
        let aggr_len = aggr_definitions.len();
        let mut aggr_fn_states = Vec::with_capacity(aggr_len);
        let mut aggr_fn_output_cardinality = Vec::with_capacity(aggr_len);
        let mut ordered_schema = Vec::with_capacity(aggr_len * 2);
        let mut ordered_aggr_fn_input_exprs = Vec::with_capacity(aggr_len * 2);

        for def in aggr_definitions {
            let aggr_output_len = ordered_schema.len();
            let aggr_input_len = ordered_aggr_fn_input_exprs.len();

            let aggr_fn = parse_aggr_definition(
                def,
                &config.tz,
                src.schema().len(),
                &mut ordered_schema,
                &mut ordered_aggr_fn_input_exprs,
            )?;

            assert!(ordered_schema.len() > aggr_output_len);
            let this_aggr_output_len = ordered_schema.len() - aggr_output_len;

            // We only support 1 parameter aggregate functions, so let's simply assert it.
            assert_eq!(ordered_aggr_fn_input_exprs.len(), aggr_input_len + 1);

            aggr_fn_states.push(aggr_fn.create_state());
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

        let src_schema = src.schema();
        let ordered_aggr_fn_input_types = ordered_aggr_fn_input_exprs
            .iter()
            .map(|expr| {
                // The eval type of the aggr input is the return type of the aggr input expression.
                let ft = expr.ret_field_type(src_schema);
                // The unwrap is also fine because the expression must be valid, otherwise it is
                // unexpected behaviour and should panic.
                EvalType::try_from(ft.tp()).unwrap()
            })
            .collect();

        Ok(Self {
            summary_collector,
            context: EvalContext::new(config),
            src,
            is_ended: false,
            aggr_fn_states,
            aggr_fn_output_cardinality,
            ordered_schema,
            ordered_aggr_fn_output_types,
            ordered_aggr_fn_input_types,
            ordered_aggr_fn_input_exprs,
        })
    }

    #[inline]
    fn process_src_data(&mut self, mut data: LazyBatchColumnVec) -> Result<()> {
        let rows_len = data.rows_len();
        if rows_len == 0 {
            return Ok(());
        }

        // There are multiple aggregate functions to calculate, so let's do it one by one.
        for (index, aggr_fn_state) in &mut self.aggr_fn_states.iter_mut().enumerate() {
            // First, evaluates the expression to get the data to aggregate.
            let eval_output = self.ordered_aggr_fn_input_exprs[index].eval(
                &mut self.context,
                rows_len,
                self.src.schema(),
                &mut data,
            )?;

            // Next, feed the evaluation result to the aggregate function.
            let input_type = self.ordered_aggr_fn_input_types[index];
            match eval_output {
                RpnStackNode::Scalar { value, field_type } => {
                    // TODO: We should be able to know `update_type()` from parser.
                    assert_eq!(input_type, EvalType::try_from(field_type.tp()).unwrap());
                    match_each_eval_type_and_call!(
                        match input_type => update_from_scalar_eval_output<T>(
                            aggr_fn_state,
                            &mut self.context,
                            value,
                            rows_len
                        )
                    )?;
                }
                RpnStackNode::Vector { value, field_type } => {
                    assert_eq!(input_type, EvalType::try_from(field_type.tp()).unwrap());
                    match_each_eval_type_and_call!(
                        match input_type => update_from_vector_eval_output<T>(
                            aggr_fn_state,
                            &mut self.context,
                            value.as_ref()
                        )
                    )?;
                }
            }
        }

        Ok(())
    }

    // Don't inline this function to reduce hot code size.
    #[inline(never)]
    fn aggregate_results(&mut self) -> Result<LazyBatchColumnVec> {
        // Construct empty columns. All columns are decoded.
        let mut output_columns: Vec<_> = self
            .ordered_aggr_fn_output_types
            .iter()
            .map(|eval_type| VectorValue::with_capacity(1, *eval_type))
            .collect();

        let mut output_offset = 0;
        for (index, aggr_fn_state) in &mut self.aggr_fn_states.iter_mut().enumerate() {
            let output_cardinality = self.aggr_fn_output_cardinality[index];
            assert!(output_cardinality > 0);

            if output_cardinality == 1 {
                // Single output column, we use `Vec<Option<T>>` as container.
                let output_type = self.ordered_aggr_fn_output_types[output_offset];
                match_each_eval_type_and_call!(
                    match output_type => push_result_to_column<T>(
                        aggr_fn_state,
                        &mut self.context,
                        &mut output_columns[output_offset]
                    )
                )?;
            } else {
                // Multi output column, we use `[VectorValue]` as container.
                aggr_fn_state.push_result(
                    &mut self.context,
                    &mut output_columns[output_offset..output_offset + output_cardinality],
                )?;
            }

            output_offset += output_cardinality;
        }

        Ok(LazyBatchColumnVec::from(output_columns))
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
    for BatchSimpleAggregationExecutor<C, Src>
{
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.ordered_schema.as_slice()
    }

    #[inline]
    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let timer = self.summary_collector.on_start_batch();
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
            .on_finish_batch(timer, ret.data.rows_len());
        ret
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
        self.summary_collector
            .collect_into(&mut destination.summary_per_executor);
    }
}
