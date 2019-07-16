use std::borrow::Cow;

use cop_codegen::rpn_fn;

use super::super::expr::EvalContext;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::{self, Error};
use crate::coprocessor::Result;

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn date_format(
    ctx: &mut EvalContext,
    t: &Option<DateTime>,
    layout: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_date_format() {
        unimplemented!()
    }
}
