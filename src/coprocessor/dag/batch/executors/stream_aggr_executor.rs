// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

pub struct BatchStreamAggregationExecutor<Src: BatchExecutor> {
    // states
    entities: Entities<Src>,
    group_by_exps: Vec<RpnExpression>,
    drained_groups: Vec<Box<dyn AggrFunctionState>>,
    current_group_key: Vec<u8>,
    current_group_state: Vec<Box<dyn AggrFunctionState>>,
}

impl<Src: BatchExecutor> BatchStreamAggregationExecutor<Src> {
    fn new(
        src: Src,
        config: Arc<EvalConfig>,
        group_by_exp_defs: Vec<Expr>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let mut group_by_exps = Vec::with_capacity(group_by_exp_defs.len());
        for def in group_by_exp_defs {
            group_by_exps.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &config.tz,
                src.schema().len(),
            )?);
        }

        let mut entities = Entities::new(src, config, aggr_defs, aggr_def_parser)?;
        for group_by_exp in &group_by_exps {
            entities
                .schema
                .push(group_by_exp.ret_field_type(src_schema).clone());
        }

        Ok(Self {
            entities,
            group_by_exps,
            drained_groups: Vec::new(),
            current_group_key: Vec::new(),
            current_group_state: Vec::new(),
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchStreamAggregationExecutor<Src> {
    fn schema(&self) -> &[FieldType] {
        &self.entities.schema
    }

    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        unimplemented!()
    }

    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.entities.src.collect_statistics(destination);
    }
}
