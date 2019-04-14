// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
mod function;
pub mod types;

mod impl_compare;
mod impl_dummy;
mod impl_op;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use tipb::expression::ScalarFuncSig;

use crate::coprocessor::Error;

// TODO: We should not expose this function as `pub` in future once all executors are batch
// executors.
fn map_pb_sig_to_rpn_func(value: ScalarFuncSig) -> Result<Box<dyn RpnFunction>, Error> {
    match value {
        ScalarFuncSig::EQReal => Ok(Box::new(impl_compare::RpnFnEQReal)),
        ScalarFuncSig::EQInt => Ok(Box::new(impl_compare::RpnFnEQInt)),
        ScalarFuncSig::GTInt => Ok(Box::new(impl_compare::RpnFnGTInt)),
        ScalarFuncSig::LTInt => Ok(Box::new(impl_compare::RpnFnLTInt)),
        ScalarFuncSig::LogicalAnd => Ok(Box::new(impl_op::RpnFnLogicalAnd)),
        ScalarFuncSig::LogicalOr => Ok(Box::new(impl_op::RpnFnLogicalOr)),
        ScalarFuncSig::IntIsNull => Ok(Box::new(impl_op::RpnFnIntIsNull)),
        ScalarFuncSig::UnaryNot => Ok(Box::new(impl_op::RpnFnUnaryNot)),
        v => Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            v
        )),
    }
}
