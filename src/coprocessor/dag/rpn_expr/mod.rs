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

#[macro_use]
mod function;
mod types;

mod impl_compare;
mod impl_dummy;
mod impl_op;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpressionNodeVec, RpnRuntimeContext};

use tipb::expression::ScalarFuncSig;

use crate::coprocessor::Error;

// TODO: We should not expose this function as `pub` in future once all executors are batch
// executors.
pub fn map_pb_sig_to_rpn_func(value: ScalarFuncSig) -> Result<Box<dyn RpnFunction>, Error> {
    match value {
        ScalarFuncSig::EQReal => Ok(Box::new(impl_compare::RpnFnEQReal)),
        ScalarFuncSig::EQInt => Ok(Box::new(impl_compare::RpnFnEQInt)),
        ScalarFuncSig::GTInt => Ok(Box::new(impl_compare::RpnFnGTInt)),
        ScalarFuncSig::LTInt => Ok(Box::new(impl_compare::RpnFnLTInt)),
        ScalarFuncSig::LogicalAnd => Ok(Box::new(impl_op::RpnFnLogicalAnd)),
        ScalarFuncSig::LogicalOr => Ok(Box::new(impl_op::RpnFnLogicalOr)),
        v => Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            v
        )),
    }
}
