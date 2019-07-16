use cop_codegen::rpn_fn;

use super::super::expr::EvalContext;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::Error;
use crate::coprocessor::Result;

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn date_format(
    ctx: &mut EvalContext,
    t: &Option<DateTime>,
    layout: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    use std::str::from_utf8;

    if t.is_none() || layout.is_none() {
        return Ok(None);
    }
    let (t, layout) = (t.as_ref().unwrap(), layout.as_ref().unwrap());
    if t.invalid_zero() {
        return ctx
            .handle_invalid_time_error(Error::incorrect_datetime_value(&format!("{}", t)))
            .map(|_| Ok(None))?;
    }

    let t = t.date_format(from_utf8(layout.as_slice()).map_err(Error::Encoding)?);
    if let Err(err) = t {
        return ctx.handle_invalid_time_error(err).map(|_| Ok(None))?;
    }

    Ok(Some(t.unwrap().into_bytes()))
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
