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

pub struct ErrorRequestHandler {

}

impl RequestHandler for ErrorRequestHandler {
    fn handle_request(&mut self, batch_row_limit: usize) -> Result<coppb::Response> {
        unimplemented!()
    }

    fn handle_streaming_request(&mut self, batch_row_limit: usize) -> Result<(Option<coppb::Response>, bool)> {
        unimplemented!()
    }

    fn collect_metrics_into(&mut self, metrics: &mut self::dag::executor::ExecutorMetrics) {
        unimplemented!()
    }
}