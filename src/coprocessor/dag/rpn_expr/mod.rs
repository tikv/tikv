// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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
