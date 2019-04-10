// Copyright 2019 PingCAP, Inc.
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

pub mod aggr;
pub mod bencher;
pub mod fixture_executor;

use criterion::black_box;

use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::Executor as PbExecutor;

use test_coprocessor::*;
use tikv::coprocessor::RequestHandler;
use tikv::storage::RocksEngine;

/// A simple helper function to build the DAG handler.
pub fn build_dag_handler(
    executors: &[PbExecutor],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
    enable_batch: bool,
) -> Box<dyn RequestHandler> {
    use tikv::coprocessor::dag::DAGRequestHandler;
    use tikv::coprocessor::Deadline;
    use tipb::select::DAGRequest;

    let mut dag = DAGRequest::new();
    dag.set_executors(RepeatedField::from_vec(executors.to_vec()));

    DAGRequestHandler::build(
        black_box(dag),
        black_box(ranges.to_vec()),
        black_box(store.to_fixture_store()),
        Deadline::from_now("", std::time::Duration::from_secs(10)),
        64,
        false,
        enable_batch,
    )
    .unwrap()
}
