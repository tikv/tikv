// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

use protobuf::Message;
use protobuf::RepeatedField;

use kvproto::coprocessor::{KeyRange, Request};
use kvproto::kvrpcpb::Context;
use tipb::executor::{
    Aggregation, ExecType, Executor, IndexScan, Limit, Selection, TableScan, TopN,
};
use tipb::expression::{ByItem, Expr, ExprType};
use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest};

use tikv::coprocessor::codec::{datum, table, Datum};
use tikv::coprocessor::REQ_TYPE_DAG;
use tikv::util::codec::number::NumberEncoder;

pub struct DAGSelect {
    pub execs: Vec<Executor>,
    pub cols: Vec<ColumnInfo>,
    pub order_by: Vec<ByItem>,
    pub limit: Option<u64>,
    pub aggregate: Vec<Expr>,
    pub group_by: Vec<Expr>,
    pub key_range: KeyRange,
    pub output_offsets: Option<Vec<u32>>,
}

impl DAGSelect {
    pub fn from(table: &Table) -> DAGSelect {
        let mut exec = Executor::new();
        exec.set_tp(ExecType::TypeTableScan);
        let mut tbl_scan = TableScan::new();
        let mut table_info = table.get_table_info();
        tbl_scan.set_table_id(table_info.get_table_id());
        let columns_info = table_info.take_columns();
        tbl_scan.set_columns(columns_info);
        exec.set_tbl_scan(tbl_scan);

        let mut range = KeyRange::new();
        range.set_start(table::encode_row_key(table.id, ::std::i64::MIN));
        range.set_end(table::encode_row_key(table.id, ::std::i64::MAX));

        DAGSelect {
            execs: vec![exec],
            cols: table.get_table_columns(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_range: range,
            output_offsets: None,
        }
    }

    pub fn from_index(table: &Table, index: &Column) -> DAGSelect {
        let idx = index.index;
        let mut exec = Executor::new();
        exec.set_tp(ExecType::TypeIndexScan);
        let mut scan = IndexScan::new();
        let mut index_info = table.get_index_info(idx, true);
        scan.set_table_id(index_info.get_table_id());
        scan.set_index_id(idx);

        let columns_info = index_info.take_columns();
        scan.set_columns(columns_info.clone());
        exec.set_idx_scan(scan);

        let range = table.get_index_range(idx);
        DAGSelect {
            execs: vec![exec],
            cols: columns_info.to_vec(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_range: range,
            output_offsets: None,
        }
    }

    pub fn limit(mut self, n: u64) -> DAGSelect {
        self.limit = Some(n);
        self
    }

    pub fn order_by(mut self, col: &Column, desc: bool) -> DAGSelect {
        let col_offset = offset_for_column(&self.cols, col.id);
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col_offset).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        self.order_by.push(item);
        self
    }

    pub fn count(mut self) -> DAGSelect {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::Count);
        self.aggregate.push(expr);
        self
    }

    pub fn aggr_col(mut self, col: &Column, aggr_t: ExprType) -> DAGSelect {
        let col_offset = offset_for_column(&self.cols, col.id);
        let mut col_expr = Expr::new();
        col_expr.set_tp(ExprType::ColumnRef);
        col_expr.mut_val().encode_i64(col_offset).unwrap();
        let mut expr = Expr::new();
        expr.set_tp(aggr_t);
        expr.mut_children().push(col_expr);
        self.aggregate.push(expr);
        self
    }

    pub fn first(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::First)
    }

    pub fn sum(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Sum)
    }

    pub fn avg(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Avg)
    }

    pub fn max(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Max)
    }

    pub fn min(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Min)
    }

    pub fn bit_and(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Agg_BitAnd)
    }

    pub fn bit_or(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Agg_BitOr)
    }

    pub fn bit_xor(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Agg_BitXor)
    }

    pub fn group_by(mut self, cols: &[&Column]) -> DAGSelect {
        for col in cols {
            let offset = offset_for_column(&self.cols, col.id);
            let mut expr = Expr::new();
            expr.set_tp(ExprType::ColumnRef);
            expr.mut_val().encode_i64(offset).unwrap();
            self.group_by.push(expr);
        }
        self
    }

    pub fn output_offsets(mut self, output_offsets: Option<Vec<u32>>) -> DAGSelect {
        self.output_offsets = output_offsets;
        self
    }

    pub fn where_expr(mut self, expr: Expr) -> DAGSelect {
        let mut exec = Executor::new();
        exec.set_tp(ExecType::TypeSelection);
        let mut selection = Selection::new();
        selection.mut_conditions().push(expr);
        exec.set_selection(selection);
        self.execs.push(exec);
        self
    }

    pub fn build(self) -> Request {
        self.build_with(Context::new(), &[0])
    }

    pub fn build_with(mut self, ctx: Context, flags: &[u64]) -> Request {
        if !self.aggregate.is_empty() || !self.group_by.is_empty() {
            let mut exec = Executor::new();
            exec.set_tp(ExecType::TypeAggregation);
            let mut aggr = Aggregation::new();
            if !self.aggregate.is_empty() {
                aggr.set_agg_func(RepeatedField::from_vec(self.aggregate));
            }

            if !self.group_by.is_empty() {
                aggr.set_group_by(RepeatedField::from_vec(self.group_by));
            }
            exec.set_aggregation(aggr);
            self.execs.push(exec);
        }

        if !self.order_by.is_empty() {
            let mut exec = Executor::new();
            exec.set_tp(ExecType::TypeTopN);
            let mut topn = TopN::new();
            topn.set_order_by(RepeatedField::from_vec(self.order_by));
            if let Some(limit) = self.limit.take() {
                topn.set_limit(limit);
            }
            exec.set_topN(topn);
            self.execs.push(exec);
        }

        if let Some(l) = self.limit.take() {
            let mut exec = Executor::new();
            exec.set_tp(ExecType::TypeLimit);
            let mut limit = Limit::new();
            limit.set_limit(l);
            exec.set_limit(limit);
            self.execs.push(exec);
        }

        let mut dag = DAGRequest::new();
        dag.set_executors(RepeatedField::from_vec(self.execs));
        dag.set_start_ts(next_id() as u64);
        dag.set_flags(flags.iter().fold(0, |acc, f| acc | *f));
        dag.set_collect_range_counts(true);

        let output_offsets = if self.output_offsets.is_some() {
            self.output_offsets.take().unwrap()
        } else {
            (0..self.cols.len() as u32).collect()
        };
        dag.set_output_offsets(output_offsets);

        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());
        req.set_ranges(RepeatedField::from_vec(vec![self.key_range]));
        req.set_context(ctx);
        req
    }
}

pub struct DAGChunkSpliter {
    chunks: Vec<Chunk>,
    datums: Vec<Datum>,
    col_cnt: usize,
}

impl DAGChunkSpliter {
    pub fn new(chunks: Vec<Chunk>, col_cnt: usize) -> DAGChunkSpliter {
        DAGChunkSpliter {
            chunks,
            col_cnt,
            datums: Vec::with_capacity(0),
        }
    }
}

impl Iterator for DAGChunkSpliter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Vec<Datum>> {
        loop {
            if self.chunks.is_empty() && self.datums.is_empty() {
                return None;
            } else if self.datums.is_empty() {
                let chunk = self.chunks.remove(0);
                let mut data = chunk.get_rows_data();
                self.datums = datum::decode(&mut data).unwrap();
                continue;
            }
            assert_eq!(self.datums.len() >= self.col_cnt, true);
            let mut cols = self.datums.split_off(self.col_cnt);
            ::std::mem::swap(&mut self.datums, &mut cols);
            return Some(cols);
        }
    }
}
