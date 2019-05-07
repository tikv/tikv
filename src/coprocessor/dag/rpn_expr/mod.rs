// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
pub mod function;
pub mod types;

mod impl_compare;
mod impl_op;

pub use self::function::RpnFunction;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use tipb::expression::ScalarFuncSig;

use crate::coprocessor::Result;

use self::impl_compare::*;
use self::impl_op::*;

fn map_pb_sig_to_rpn_func(value: ScalarFuncSig) -> Result<Box<dyn RpnFunction>> {
    Ok(match value {
        ScalarFuncSig::LTReal => Box::new(RpnFnCompare::<RealComparer<CmpOpLT>>::new()),
        ScalarFuncSig::LEReal => Box::new(RpnFnCompare::<RealComparer<CmpOpLE>>::new()),
        ScalarFuncSig::GTReal => Box::new(RpnFnCompare::<RealComparer<CmpOpGT>>::new()),
        ScalarFuncSig::GEReal => Box::new(RpnFnCompare::<RealComparer<CmpOpGE>>::new()),
        ScalarFuncSig::NEReal => Box::new(RpnFnCompare::<RealComparer<CmpOpNE>>::new()),
        ScalarFuncSig::EQReal => Box::new(RpnFnCompare::<RealComparer<CmpOpEQ>>::new()),
        ScalarFuncSig::NullEQReal => Box::new(RpnFnCompare::<RealComparer<CmpOpNullEQ>>::new()),
        ScalarFuncSig::LogicalAnd => Box::new(RpnFnLogicalAnd),
        ScalarFuncSig::LogicalOr => Box::new(RpnFnLogicalOr),
        v => {
            return Err(box_err!(
                "ScalarFunction {:?} is not supported in batch mode",
                v
            ))
        }
    })
}
