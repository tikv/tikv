// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{any::Any, sync::OnceLock};

use tidb_query_datatype::codec::data_type::ScalarValue;
use tipb::FieldType;

use super::super::function::RpnFnMeta;
use crate::ShortCircuitFnMeta;

/// A type for each node in the RPN expression list.
#[derive(Debug)]
pub enum RpnExpressionNode {
    /// Represents a function call that decides which arguments and rows need to
    /// be evaluated. Each argument remains an independent RPN expression.
    ShortCircuitFnCall {
        func_meta: ShortCircuitFnMeta,
        args: Box<[RpnExpression]>,
        field_type: FieldType,
    },

    /// Represents a function call.
    FnCall {
        func_meta: RpnFnMeta,
        args_len: usize,
        field_type: FieldType,
        metadata: Box<dyn Any + Send>,
    },

    /// Represents a scalar constant value.
    Constant {
        value: ScalarValue,
        field_type: FieldType,
    },

    /// Represents a reference to a column in the columns specified in
    /// evaluation.
    ColumnRef { offset: usize },
}

impl RpnExpressionNode {
    /// Gets the field type.
    #[cfg(test)]
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnExpressionNode::ShortCircuitFnCall { field_type, .. } => field_type,
            RpnExpressionNode::FnCall { field_type, .. } => field_type,
            RpnExpressionNode::Constant { field_type, .. } => field_type,
            RpnExpressionNode::ColumnRef { .. } => panic!(),
        }
    }

    #[cfg(test)]
    pub fn expr_tp(&self) -> tipb::ExprType {
        use tidb_query_datatype::EvalType;
        use tipb::ExprType;

        match self {
            RpnExpressionNode::ShortCircuitFnCall { .. } => ExprType::ScalarFunc,
            RpnExpressionNode::FnCall { .. } => ExprType::ScalarFunc,
            RpnExpressionNode::Constant { value, .. } => match value.eval_type() {
                EvalType::Bytes => ExprType::Bytes,
                EvalType::DateTime => ExprType::MysqlTime,
                EvalType::Decimal => ExprType::MysqlDecimal,
                EvalType::Duration => ExprType::MysqlDuration,
                EvalType::Int => ExprType::Int64,
                EvalType::Json => ExprType::MysqlJson,
                EvalType::Real => ExprType::Float64,
                EvalType::Enum => ExprType::MysqlEnum,
                EvalType::Set => ExprType::MysqlSet,
                EvalType::VectorFloat32 => ExprType::TiDbVectorFloat32,
            },
            RpnExpressionNode::ColumnRef { .. } => ExprType::ColumnRef,
        }
    }

    /// Borrows the function instance for `FnCall` variant.
    #[cfg(test)]
    pub fn fn_call_func(&self) -> RpnFnMeta {
        match self {
            RpnExpressionNode::FnCall { func_meta, .. } => *func_meta,
            _ => panic!(),
        }
    }

    /// Borrows the constant value for `Constant` variant.
    #[cfg(test)]
    pub fn constant_value(&self) -> &ScalarValue {
        match self {
            RpnExpressionNode::Constant { value, .. } => value,
            _ => panic!(),
        }
    }

    fn collect_metadata(&self, metadata: &mut RpnExpressionMetadata) {
        metadata.node_count += 1;
        match self {
            RpnExpressionNode::ShortCircuitFnCall { args, .. } => {
                for arg in args {
                    arg.collect_metadata(metadata);
                }
            }
            RpnExpressionNode::ColumnRef { offset } => {
                metadata.column_ref_count += 1;
                metadata.referenced_column_offsets.push(*offset);
            }
            _ => {}
        }
    }
}

#[derive(Debug, Default)]
struct RpnExpressionMetadata {
    node_count: usize,
    column_ref_count: usize,
    referenced_column_offsets: Vec<usize>,
}

/// An expression in Reverse Polish notation, which is simply a list of RPN
/// expression nodes.
///
/// You may want to build it using `RpnExpressionBuilder`.
#[derive(Debug)]
pub struct RpnExpression {
    nodes: Vec<RpnExpressionNode>,
    metadata: OnceLock<Box<RpnExpressionMetadata>>,
}

impl std::ops::Deref for RpnExpression {
    type Target = Vec<RpnExpressionNode>;

    fn deref(&self) -> &Vec<RpnExpressionNode> {
        &self.nodes
    }
}

impl std::ops::DerefMut for RpnExpression {
    fn deref_mut(&mut self) -> &mut Vec<RpnExpressionNode> {
        // Any mutable access may change the expression tree, so cached metadata
        // must be rebuilt the next time it is requested.
        self.metadata = OnceLock::new();
        &mut self.nodes
    }
}

impl From<Vec<RpnExpressionNode>> for RpnExpression {
    fn from(v: Vec<RpnExpressionNode>) -> Self {
        Self {
            nodes: v,
            metadata: OnceLock::new(),
        }
    }
}

impl AsRef<[RpnExpressionNode]> for RpnExpression {
    fn as_ref(&self) -> &[RpnExpressionNode] {
        self.nodes.as_ref()
    }
}

impl AsMut<[RpnExpressionNode]> for RpnExpression {
    fn as_mut(&mut self) -> &mut [RpnExpressionNode] {
        self.metadata = OnceLock::new();
        self.nodes.as_mut()
    }
}

impl RpnExpression {
    fn metadata(&self) -> &RpnExpressionMetadata {
        self.metadata
            .get_or_init(|| {
                let mut metadata = RpnExpressionMetadata::default();
                self.collect_metadata(&mut metadata);
                metadata.referenced_column_offsets.sort_unstable();
                metadata.referenced_column_offsets.dedup();
                Box::new(metadata)
            })
            .as_ref()
    }

    fn collect_metadata(&self, metadata: &mut RpnExpressionMetadata) {
        for node in &self.nodes {
            node.collect_metadata(metadata);
        }
    }

    /// Gets the field type of the return value.
    pub fn ret_field_type<'a>(&'a self, schema: &'a [FieldType]) -> &'a FieldType {
        assert!(!self.nodes.is_empty());
        let last_node = self.nodes.last().unwrap();
        match last_node {
            RpnExpressionNode::FnCall { field_type, .. } => field_type,
            RpnExpressionNode::Constant { field_type, .. } => field_type,
            RpnExpressionNode::ColumnRef { offset } => &schema[*offset],
            RpnExpressionNode::ShortCircuitFnCall { field_type, .. } => field_type,
        }
    }

    /// Unwraps into the underlying expression node vector.
    pub fn into_inner(self) -> Vec<RpnExpressionNode> {
        self.nodes
    }

    /// Returns true if the last element of expression is a `Constant` variant.
    pub fn is_last_constant(&self) -> bool {
        assert!(!self.nodes.is_empty());
        matches!(
            self.nodes.last().unwrap(),
            RpnExpressionNode::Constant { .. }
        )
    }

    /// Returns the number of nodes, including nodes nested in short-circuit
    /// arguments.
    pub fn node_count(&self) -> usize {
        self.metadata().node_count
    }

    /// Returns the number of column references, including references nested in
    /// short-circuit arguments.
    pub fn column_ref_count(&self) -> usize {
        self.metadata().column_ref_count
    }

    /// Returns sorted, deduplicated offsets of all referenced columns,
    /// including references nested in short-circuit arguments.
    pub(crate) fn referenced_column_offsets(&self) -> &[usize] {
        &self.metadata().referenced_column_offsets
    }
}

// For `RpnExpression::eval`, see `expr_eval` file.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_metadata_is_deduplicated_and_invalidated_by_mutation() {
        let mut expr = RpnExpression::from(vec![
            RpnExpressionNode::ColumnRef { offset: 2 },
            RpnExpressionNode::ColumnRef { offset: 0 },
            RpnExpressionNode::ColumnRef { offset: 2 },
        ]);

        assert_eq!(expr.node_count(), 3);
        assert_eq!(expr.column_ref_count(), 3);
        assert_eq!(expr.referenced_column_offsets(), &[0, 2]);

        expr.push(RpnExpressionNode::ColumnRef { offset: 1 });

        assert_eq!(expr.node_count(), 4);
        assert_eq!(expr.column_ref_count(), 4);
        assert_eq!(expr.referenced_column_offsets(), &[0, 1, 2]);
    }
}
