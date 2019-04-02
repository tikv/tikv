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

#[macro_use]
mod function;
mod types;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use tipb::expression::ScalarFuncSig;

use crate::coprocessor::Error;

// TODO: We should not expose this function as `pub` in future once all executors are batch
// executors.
pub fn map_pb_sig_to_rpn_func(value: ScalarFuncSig) -> Result<Box<dyn RpnFunction>, Error> {
    match value {
        v => Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            v
        )),
    }
}
