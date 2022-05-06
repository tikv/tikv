// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    coprocessor::{KeyRange, Request},
    kvrpcpb::Context,
};
use protobuf::Message;
use tidb_query_datatype::codec::{datum, Datum};
use tikv::coprocessor::REQ_TYPE_DAG;
use tikv_util::codec::number::NumberEncoder;
use tipb::{
    Aggregation, ByItem, Chunk, ColumnInfo, DagRequest, ExecType, Executor, Expr, ExprType,
    IndexScan, Limit, Selection, TableScan, TopN,
};

use super::*;

pub struct DAGSelect {
    pub execs: Vec<Executor>,
    pub cols: Vec<ColumnInfo>,
    pub order_by: Vec<ByItem>,
    pub limit: Option<u64>,
    pub aggregate: Vec<Expr>,
    pub group_by: Vec<Expr>,
    pub key_ranges: Vec<KeyRange>,
    pub output_offsets: Option<Vec<u32>>,
    pub paging_size: Option<u64>,
}

impl DAGSelect {
    pub fn from(table: &Table) -> DAGSelect {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeTableScan);
        let mut tbl_scan = TableScan::default();
        let mut table_info = table.table_info();
        tbl_scan.set_table_id(table_info.get_table_id());
        let columns_info = table_info.take_columns();
        tbl_scan.set_columns(columns_info);
        exec.set_tbl_scan(tbl_scan);

        DAGSelect {
            execs: vec![exec],
            cols: table.columns_info(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_ranges: vec![table.get_record_range_all()],
            output_offsets: None,
            paging_size: None,
        }
    }

    pub fn from_index(table: &Table, index: &Column) -> DAGSelect {
        let idx = index.index;
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeIndexScan);
        let mut scan = IndexScan::default();
        let mut index_info = table.index_info(idx, true);
        scan.set_table_id(index_info.get_table_id());
        scan.set_index_id(idx);

        let columns_info = index_info.take_columns();
        scan.set_columns(columns_info.clone());
        exec.set_idx_scan(scan);

        let range = table.get_index_range_all(idx);
        DAGSelect {
            execs: vec![exec],
            cols: columns_info.to_vec(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_ranges: vec![range],
            output_offsets: None,
            paging_size: None,
        }
    }

    #[must_use]
    pub fn limit(mut self, n: u64) -> DAGSelect {
        self.limit = Some(n);
        self
    }

    #[must_use]
    pub fn order_by(mut self, col: &Column, desc: bool) -> DAGSelect {
        let col_offset = offset_for_column(&self.cols, col.id);
        let mut item = ByItem::default();
        let mut expr = Expr::default();
        expr.set_field_type(col.as_field_type());
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col_offset).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        self.order_by.push(item);
        self
    }

    #[must_use]
    pub fn count(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Count)
    }

    #[must_use]
    pub fn aggr_col(mut self, col: &Column, aggr_t: ExprType) -> DAGSelect {
        let col_offset = offset_for_column(&self.cols, col.id);
        let mut col_expr = Expr::default();
        col_expr.set_field_type(col.as_field_type());
        col_expr.set_tp(ExprType::ColumnRef);
        col_expr.mut_val().encode_i64(col_offset).unwrap();
        let mut expr = Expr::default();
        let mut expr_ft = col.as_field_type();
        // Avg will contains two auxiliary columns (sum, count) and the sum should be a `Decimal`
        if aggr_t == ExprType::Avg || aggr_t == ExprType::Sum {
            expr_ft.set_tp(0xf6); // FieldTypeTp::NewDecimal
        }
        expr.set_field_type(expr_ft);
        expr.set_tp(aggr_t);
        expr.mut_children().push(col_expr);
        self.aggregate.push(expr);
        self
    }

    #[must_use]
    pub fn first(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::First)
    }

    #[must_use]
    pub fn sum(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Sum)
    }

    #[must_use]
    pub fn avg(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Avg)
    }

    #[must_use]
    pub fn max(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Max)
    }

    #[must_use]
    pub fn min(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::Min)
    }

    #[must_use]
    pub fn bit_and(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::AggBitAnd)
    }

    #[must_use]
    pub fn bit_or(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::AggBitOr)
    }

    #[must_use]
    pub fn bit_xor(self, col: &Column) -> DAGSelect {
        self.aggr_col(col, ExprType::AggBitXor)
    }

    #[must_use]
    pub fn group_by(mut self, cols: &[&Column]) -> DAGSelect {
        for col in cols {
            let offset = offset_for_column(&self.cols, col.id);
            let mut expr = Expr::default();
            expr.set_field_type(col.as_field_type());
            expr.set_tp(ExprType::ColumnRef);
            expr.mut_val().encode_i64(offset).unwrap();
            self.group_by.push(expr);
        }
        self
    }

    #[must_use]
    pub fn output_offsets(mut self, output_offsets: Option<Vec<u32>>) -> DAGSelect {
        self.output_offsets = output_offsets;
        self
    }

    #[must_use]
    pub fn where_expr(mut self, expr: Expr) -> DAGSelect {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeSelection);
        let mut selection = Selection::default();
        selection.mut_conditions().push(expr);
        exec.set_selection(selection);
        self.execs.push(exec);
        self
    }

    #[must_use]
    pub fn desc(mut self, desc: bool) -> DAGSelect {
        self.execs[0].mut_tbl_scan().set_desc(desc);
        self
    }

    #[must_use]
    pub fn paging_size(mut self, paging_size: u64) -> DAGSelect {
        assert_ne!(paging_size, 0);
        self.paging_size = Some(paging_size);
        self
    }

    #[must_use]
    pub fn key_ranges(mut self, key_ranges: Vec<KeyRange>) -> DAGSelect {
        self.key_ranges = key_ranges;
        self
    }

    pub fn build(self) -> Request {
        self.build_with(Context::default(), &[0])
    }

    pub fn build_with(mut self, ctx: Context, flags: &[u64]) -> Request {
        if !self.aggregate.is_empty() || !self.group_by.is_empty() {
            let mut exec = Executor::default();
            exec.set_tp(ExecType::TypeAggregation);
            let mut aggr = Aggregation::default();
            if !self.aggregate.is_empty() {
                aggr.set_agg_func(self.aggregate.into());
            }

            if !self.group_by.is_empty() {
                aggr.set_group_by(self.group_by.into());
            }
            exec.set_aggregation(aggr);
            self.execs.push(exec);
        }

        if !self.order_by.is_empty() {
            let mut exec = Executor::default();
            exec.set_tp(ExecType::TypeTopN);
            let mut topn = TopN::default();
            topn.set_order_by(self.order_by.into());
            if let Some(limit) = self.limit.take() {
                topn.set_limit(limit);
            }
            exec.set_top_n(topn);
            self.execs.push(exec);
        }

        if let Some(l) = self.limit.take() {
            let mut exec = Executor::default();
            exec.set_tp(ExecType::TypeLimit);
            let mut limit = Limit::default();
            limit.set_limit(l);
            exec.set_limit(limit);
            self.execs.push(exec);
        }

        let mut dag = DagRequest::default();
        dag.set_executors(self.execs.into());
        dag.set_flags(flags.iter().fold(0, |acc, f| acc | *f));
        dag.set_collect_range_counts(true);

        let output_offsets = if self.output_offsets.is_some() {
            self.output_offsets.take().unwrap()
        } else {
            (0..self.cols.len() as u32).collect()
        };
        dag.set_output_offsets(output_offsets);

        let mut req = Request::default();
        req.set_start_ts(next_id() as u64);
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());
        req.set_ranges(self.key_ranges.into());
        req.set_paging_size(self.paging_size.unwrap_or(0));
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
            std::mem::swap(&mut self.datums, &mut cols);
            return Some(cols);
        }
    }
}
