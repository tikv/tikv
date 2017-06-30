// Copyright 2017 PingCAP, Inc.
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

use std::rc::Rc;

use tipb::executor::{Executor, ExecType};
use tipb::schema::ColumnInfo;
use tipb::select::{DAGRequest, Chunk};
use kvproto::coprocessor::KeyRange;
use kvproto::kvrpcpb::IsolationLevel;

use storage::{Snapshot, Statistics};
use util::xeval::EvalContext;
use super::{Result, Error};
use super::executor::Executor as DAGExecutor;
use super::executor::table_scan::TableScanExecutor;
use super::executor::index_scan::IndexScanExecutor;
use super::executor::selection::SelectionExecutor;
// use super::executor::aggregation::AggregationExecutor;
use super::executor::topn::TopNExecutor;
use super::executor::limit::LimitExecutor;

pub struct DAGContext<'s> {
    req: DAGRequest,
    pub columns: Vec<ColumnInfo>,
    ranges: Vec<KeyRange>,
    snap: &'s Snapshot,
    pub has_aggr: bool,
    eval_ctx: Rc<EvalContext>,
    pub chunks: Vec<Chunk>,
}

impl<'s> DAGContext<'s> {
    pub fn new(req: DAGRequest,
               ranges: Vec<KeyRange>,
               snap: &'s Snapshot,
               eval_ctx: Rc<EvalContext>)
               -> DAGContext<'s> {
        DAGContext {
            req: req,
            columns: vec![],
            ranges: ranges,
            snap: snap,
            has_aggr: false,
            eval_ctx: eval_ctx,
            chunks: vec![],
        }
    }

    pub fn validate_dag(&mut self) -> Result<()> {
        let execs = self.req.get_executors();
        let first = try!(execs.first()
            .ok_or_else(|| Error::Other(box_err!("dag copr has not executor"))));
        // check whether first exec is *scan and get the column info
        match first.get_tp() {
            ExecType::TypeTableScan => {
                self.columns = first.get_tbl_scan().get_columns().to_vec();
            }
            ExecType::TypeIndexScan => {
                self.columns = first.get_idx_scan().get_columns().to_vec();
            }
            _ => {
                return Err(box_err!("first exec type should be *Scan, but get {:?}",
                                    first.get_tp()))
            }
        }
        // check whether dag has a aggregation action and take a flag
        if execs.iter().rev().find(|&exec| exec.get_tp() == ExecType::TypeAggregation).is_some() {
            self.has_aggr = true;
        }
        Ok(())
    }

    // FIXME seperate first exec build action from `build_dag`
    // since it will generte some mutable conflict when putting together
    pub fn build_first(&'s self,
                       mut first: Executor,
                       statistics: &'s mut Statistics)
                       -> Box<DAGExecutor + 's> {
        match first.get_tp() {
            ExecType::TypeTableScan => {
                Box::new(TableScanExecutor::new(first.take_tbl_scan(),
                                                self.ranges.clone(),
                                                self.snap,
                                                statistics,
                                                self.req.get_start_ts(),
                                                IsolationLevel::SI))
            }
            ExecType::TypeIndexScan => {
                Box::new(IndexScanExecutor::new(first.take_idx_scan(),
                                                self.ranges.clone(),
                                                self.snap,
                                                statistics,
                                                self.req.get_start_ts(),
                                                IsolationLevel::SI))
            }
            _ => unreachable!(),
        }
    }

    pub fn build_dag(&'s self, statistics: &'s mut Statistics) -> Result<Box<DAGExecutor + 's>> {
        let mut execs = self.req.get_executors().to_vec().into_iter();
        let mut src = self.build_first(execs.next().unwrap(), statistics);
        for mut exec in execs {
            let curr: Box<DAGExecutor> = match exec.get_tp() {
                ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                    return Err(box_err!("got too much *scan exec, should be only one"))
                }
                ExecType::TypeSelection => {
                    Box::new(try!(SelectionExecutor::new(exec.take_selection(),
                                                         self.eval_ctx.clone(),
                                                         &self.columns,
                                                         src)))
                }
                ExecType::TypeAggregation => {
                    Box::new(try!(SelectionExecutor::new(exec.take_selection(),
                                                         self.eval_ctx.clone(),
                                                         &self.columns,
                                                         src)))
                }
                ExecType::TypeTopN => {
                    Box::new(try!(TopNExecutor::new(exec.take_topN(),
                                                    self.eval_ctx.clone(),
                                                    &self.columns,
                                                    src)))
                }
                ExecType::TypeLimit => Box::new(LimitExecutor::new(exec.take_limit(), src)),
            };
            src = curr;
        }
        Ok(src)
    }
}
