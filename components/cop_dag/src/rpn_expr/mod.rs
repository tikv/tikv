// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
pub mod function;
pub mod types;

mod impl_op;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use tipb::expression::ScalarFuncSig;

use crate::Error;

// TODO: We should not expose this function as `pub` in future once all executors are batch
// executors.
pub fn map_pb_sig_to_rpn_func(value: ScalarFuncSig) -> Result<Box<dyn RpnFunction>, Error> {
    match value {
        ScalarFuncSig::LogicalAnd => Ok(Box::new(impl_op::RpnFnLogicalAnd)),
        v => Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            v
        )),
    }
}
