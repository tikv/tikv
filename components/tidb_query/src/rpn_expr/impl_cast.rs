// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::*;
use tipb::{Expr, FieldType};

use crate::codec::convert::*;
use crate::codec::data_type::*;
use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpressionNode, RpnFnCallExtra, RpnFnMeta};
use crate::Result;

fn get_cast_fn_rpn_meta(
    from_field_type: &FieldType,
    to_field_type: &FieldType,
) -> Result<RpnFnMeta> {
    let from = box_try!(EvalType::try_from(from_field_type.as_accessor().tp()));
    let to = box_try!(EvalType::try_from(to_field_type.as_accessor().tp()));
    let func_meta = match (from, to) {
        // any as duration
        (EvalType::Int, EvalType::Duration) => cast_int_as_duration_fn_meta(),
        (EvalType::Real, EvalType::Duration) => cast_real_as_duration_fn_meta(),
        (EvalType::Bytes, EvalType::Duration) => cast_bytes_as_duration_fn_meta(),
        (EvalType::Decimal, EvalType::Duration) => cast_decimal_as_duration_fn_meta(),
        (EvalType::DateTime, EvalType::Duration) => cast_time_as_duration_fn_meta(),
        (EvalType::Duration, EvalType::Duration) => cast_duration_as_duration_fn_meta(),
        (EvalType::Json, EvalType::Duration) => cast_json_as_duration_fn_meta(),

        (EvalType::Int, EvalType::Int) => {
            match (from_field_type.is_unsigned(), to_field_type.is_unsigned()) {
                (false, false) => cast_any_as_any_fn_meta::<Int, Int>(),
                (false, true) => cast_int_as_uint_fn_meta(),
                (true, false) => cast_uint_as_int_fn_meta(),
                (true, true) => cast_uint_as_uint_fn_meta(),
            }
        }
        (EvalType::Real, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Real, Int>()
            } else {
                cast_float_as_uint_fn_meta()
            }
        }
        (EvalType::Bytes, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Bytes, Int>()
            } else {
                cast_bytes_as_uint_fn_meta()
            }
        }
        (EvalType::Decimal, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Decimal, Int>()
            } else {
                cast_decimal_as_uint_fn_meta()
            }
        }
        (EvalType::DateTime, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<DateTime, Int>()
            } else {
                cast_datetime_as_uint_fn_meta()
            }
        }
        (EvalType::Duration, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Duration, Int>()
            } else {
                cast_duration_as_uint_fn_meta()
            }
        }
        (EvalType::Json, EvalType::Int) => {
            if !to_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Json, Int>()
            } else {
                cast_json_as_uint_fn_meta()
            }
        }
        (EvalType::Int, EvalType::Json) => {
            if from_field_type
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::IS_BOOLEAN)
            {
                cast_int_as_json_boolean_fn_meta()
            } else if !from_field_type.is_unsigned() {
                cast_any_as_any_fn_meta::<Int, Json>()
            } else {
                cast_uint_as_json_fn_meta()
            }
        }
        (EvalType::Real, EvalType::Json) => cast_any_as_any_fn_meta::<Real, Json>(),
        (EvalType::Bytes, EvalType::Json) => cast_string_as_json_fn_meta(),
        (EvalType::Decimal, EvalType::Json) => cast_any_as_any_fn_meta::<Decimal, Json>(),
        (EvalType::DateTime, EvalType::Json) => cast_any_as_any_fn_meta::<DateTime, Json>(),
        (EvalType::Duration, EvalType::Json) => cast_any_as_any_fn_meta::<Duration, Json>(),

        _ => return Err(other_err!("Unsupported cast from {} to {}", from, to)),
    };
    Ok(func_meta)
}

/// Gets the cast function between specified data types.
///
/// TODO: This function supports some internal casts performed by TiKV. However it would be better
/// to be done in TiDB.
pub fn get_cast_fn_rpn_node(
    from_field_type: &FieldType,
    to_field_type: FieldType,
) -> Result<RpnExpressionNode> {
    let func_meta = get_cast_fn_rpn_meta(from_field_type, &to_field_type)?;
    // This cast function is inserted by `Coprocessor` automatically,
    // the `inUnion` flag always false in this situation. Ideally,
    // the cast function should be inserted by TiDB and pushed down
    // with all implicit arguments.
    Ok(RpnExpressionNode::FnCall {
        func_meta,
        args_len: 1,
        field_type: to_field_type,
        implicit_args: Vec::new(),
    })
}

/// Gets the RPN function meta
pub fn map_cast_func(expr: &Expr) -> Result<RpnFnMeta> {
    let children = expr.get_children();
    if children.len() != 1 {
        return Err(other_err!(
            "Unexpected arguments: sig {:?} with {} args",
            expr.get_sig(),
            children.len()
        ));
    }
    get_cast_fn_rpn_meta(children[0].get_field_type(), expr.get_field_type())
}

/// Indicates whether the current expression is evaluated in union statement
///
/// Note: The TiDB will push down the `inUnion` flag by implicit constant arguments,
/// but some CAST expressions inserted by TiKV coprocessor use an empty vector to represent
/// the `inUnion` flag is false.
/// See: https://github.com/pingcap/tidb/blob/1e403873d905b2d0ad3be06bd8cd261203d84638/expression/builtin.go#L260
fn in_union(implicit_args: &[ScalarValue]) -> bool {
    implicit_args.get(0) == Some(&ScalarValue::Int(Some(1)))
}

// cast any as duration, no cast functions reuse `cast_any_as_any`

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_int_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Int>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let fsp = extra.ret_field_type.get_decimal() as u8;
            let (dur, err) = Duration::from_i64_without_ctx(*val, fsp);
            match err {
                // in TiDB, if there is overflow err and overflow as warning,
                // then it will return isNull==true
                Some(err) => {
                    if err.is_overflow() {
                        ctx.handle_overflow_err(err)?;
                        Ok(None)
                    } else {
                        Err(err.into())
                    }
                }
                None => {
                    if dur.is_none() {
                        Err(other_err!("Expect a not none result here, this is a bug"))
                    } else {
                        Ok(Some(dur.unwrap()))
                    }
                }
            }
        }
    }
}

#[rpn_fn(capture = [ctx, extra])]
#[inline]
fn cast_time_as_duration(
    ctx: &mut EvalContext,
    extra: &RpnFnCallExtra,
    val: &Option<DateTime>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let dur: Duration = val.convert(ctx)?;
            Ok(Some(dur.round_frac(extra.ret_field_type.decimal() as i8)?))
        }
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
fn cast_duration_as_duration(
    extra: &RpnFnCallExtra,
    val: &Option<Duration>,
) -> Result<Option<Duration>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(val.round_frac(extra.ret_field_type.decimal() as i8)?)),
    }
}

macro_rules! cast_as_duration {
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<Duration>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    let result = Duration::parse($extra, extra.ret_field_type.get_decimal() as i8);
                    match result {
                        Ok(dur) => Ok(Some(dur)),
                        Err(e) => match e.code() {
                            ERR_DATA_OUT_OF_RANGE => {
                                ctx.handle_overflow_err(e)?;
                                Ok(Some(Duration::zero()))
                            }
                            WARN_DATA_TRUNCATED => {
                                ctx.handle_truncate_err(e)?;
                                Ok(Some(Duration::zero()))
                            }
                            _ => Err(e.into()),
                        },
                    }
                }
            }
        }
    };
}

cast_as_duration!(
    Real,
    cast_real_as_duration,
    val.into_inner().to_string().as_bytes()
);
cast_as_duration!(Bytes, cast_bytes_as_duration, val);
cast_as_duration!(
    Decimal,
    cast_decimal_as_duration,
    val.to_string().as_bytes()
);
cast_as_duration!(Json, cast_json_as_duration, val.unquote()?.as_bytes());

#[rpn_fn(capture = [ctx])]
#[inline]
fn cast_any_as_any<From: ConvertTo<To> + Evaluable, To: Evaluable>(
    ctx: &mut EvalContext,
    val: &Option<From>,
) -> Result<Option<To>> {
    match val {
        None => Ok(None),
        Some(val) => {
            let val = val.convert(ctx)?;
            Ok(Some(val))
        }
    }
}

#[rpn_fn]
#[inline]
fn cast_uint_as_int(val: &Option<Int>) -> Result<Option<i64>> {
    match val {
        None => Ok(None),
        Some(val) => {
            // the val is uint, so it will never < 0,
            // then we needn't to check whether in_union.
            //
            // needn't to call convert_uint_as_int
            Ok(Some(*val as i64))
        }
    }
}

macro_rules! cast_as_unsigned_integer {
    ($ty:ty, $as_uint_fn:ident) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, val,);
    };
    ($ty:ty, $as_uint_fn:ident, $extra:expr) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $extra,);
    };
    ($ty:ty, $as_uint_fn:ident, $extra:expr, $($hook:tt)*) => {
        cast_as_unsigned_integer!(_inner, $ty, $as_uint_fn, $extra, $($hook)*);
    };
    (_inner, $ty:ty, $as_uint_fn:ident, $extra:expr, $($hook:tt)*) => {
        #[rpn_fn(capture = [ctx, extra])]
        #[inline]
        #[allow(unused)]
        pub fn $as_uint_fn(
            ctx: &mut EvalContext,
            extra: &RpnFnCallExtra<'_>,
            val: &Option<$ty>,
        ) -> Result<Option<i64>> {
            match val {
                None => Ok(None),
                Some(val) => {
                    $($hook)*;
                    let val = ($extra).to_uint(ctx, FieldTypeTp::LongLong)?;
                    Ok(Some(val as i64))
                }
            }
        }
    };
}

cast_as_unsigned_integer!(
    Int,
    cast_int_as_uint,
    *val,
    if *val < 0 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Int, cast_uint_as_uint, *val as u64);
cast_as_unsigned_integer!(
    Real,
    cast_float_as_uint,
    val.into_inner(),
    if val.into_inner() < 0f64 && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(Bytes, cast_bytes_as_uint);
cast_as_unsigned_integer!(
    Decimal,
    cast_decimal_as_uint,
    val,
    if val.is_negative() && in_union(extra.implicit_args) {
        return Ok(Some(0));
    }
);
cast_as_unsigned_integer!(DateTime, cast_datetime_as_uint);
cast_as_unsigned_integer!(Duration, cast_duration_as_uint);
cast_as_unsigned_integer!(Json, cast_json_as_uint);

/// The implementation for push down signature `CastIntAsJson` from unsigned integer.
#[rpn_fn]
#[inline]
pub fn cast_uint_as_json(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::U64(*val as u64))),
    }
}

#[rpn_fn]
#[inline]
pub fn cast_int_as_json_boolean(val: &Option<Int>) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => Ok(Some(Json::Boolean(*val != 0))),
    }
}

#[rpn_fn(capture = [extra])]
#[inline]
pub fn cast_string_as_json(
    extra: &RpnFnCallExtra<'_>,
    val: &Option<Bytes>,
) -> Result<Option<Json>> {
    match val {
        None => Ok(None),
        Some(val) => {
            if extra
                .ret_field_type
                .flag()
                .contains(FieldTypeFlag::PARSE_TO_JSON)
            {
                let s = box_try!(String::from_utf8(val.to_owned()));
                let val: Json = s.parse()?;
                Ok(Some(val))
            } else {
                // FIXME: port `JSONBinary` from TiDB to adapt if the bytes is not a valid utf8 string
                let val = unsafe { String::from_utf8_unchecked(val.to_owned()) };
                Ok(Some(Json::String(val)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::data_type::{Bytes, Real, ScalarValue};
    use crate::codec::error::{ERR_DATA_OUT_OF_RANGE, WARN_DATA_TRUNCATED};
    use crate::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP, MIN_FSP};
    use crate::codec::Error;
    use crate::error::Result;
    use crate::expr::Flag;
    use crate::expr::{EvalConfig, EvalContext};
    use crate::rpn_expr::impl_cast::*;
    use crate::rpn_expr::RpnFnCallExtra;
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Display, Formatter};
    use std::sync::Arc;
    use std::{f32, f64, i64, u64};
    use tidb_query_datatype::FieldTypeAccessor;

    #[test]
    fn test_in_union() {
        use super::*;

        assert_eq!(in_union(&[]), false);
        assert_eq!(in_union(&[ScalarValue::Int(None)]), false);
        assert_eq!(in_union(&[ScalarValue::Int(Some(0))]), false);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(0)), ScalarValue::Int(Some(1))]),
            false
        );
        assert_eq!(in_union(&[ScalarValue::Int(Some(1))]), true);
        assert_eq!(
            in_union(&[ScalarValue::Int(Some(1)), ScalarValue::Int(Some(0))]),
            true
        );
    }

    fn test_none_with_ctx_and_extra<F, Input, Ret>(func: F)
    where
        F: Fn(&mut EvalContext, &RpnFnCallExtra, &Option<Input>) -> Result<Option<Ret>>,
    {
        let mut ctx = EvalContext::default();
        let implicit_args = [ScalarValue::Int(Some(1))];
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
            implicit_args: &implicit_args,
        };
        let r = func(&mut ctx, &extra, &None).unwrap();
        assert!(r.is_none());
    }

    fn test_none_with_extra<F, Input, Ret>(func: F)
    where
        F: Fn(&RpnFnCallExtra, &Option<Input>) -> Result<Option<Ret>>,
    {
        let implicit_args = [ScalarValue::Int(Some(1))];
        let ret_field_type: FieldType = FieldType::default();
        let extra = RpnFnCallExtra {
            ret_field_type: &ret_field_type,
            implicit_args: &implicit_args,
        };
        let r = func(&extra, &None).unwrap();
        assert!(r.is_none());
    }

    fn make_ctx(
        overflow_as_warning: bool,
        truncate_as_warning: bool,
        should_clip_to_zero: bool,
    ) -> EvalContext {
        let mut flag: Flag = Flag::default();
        if overflow_as_warning {
            flag |= Flag::OVERFLOW_AS_WARNING;
        }
        if truncate_as_warning {
            flag |= Flag::TRUNCATE_AS_WARNING;
        }
        if should_clip_to_zero {
            flag |= Flag::IN_INSERT_STMT;
        }
        let cfg = Arc::new(EvalConfig::from_flag(flag));
        EvalContext::new(cfg)
    }

    fn make_implicit_args(in_union: bool) -> [ScalarValue; 1] {
        if in_union {
            [ScalarValue::Int(Some(1))]
        } else {
            [ScalarValue::Int(Some(0))]
        }
    }

    fn make_ret_field_type_6(decimal: isize) -> FieldType {
        let mut ft = FieldType::default();
        let fta = ft.as_mut_accessor();
        fta.set_decimal(decimal);
        ft
    }

    fn make_extra<'a>(
        ret_field_type: &'a FieldType,
        implicit_args: &'a [ScalarValue],
    ) -> RpnFnCallExtra<'a> {
        RpnFnCallExtra {
            ret_field_type,
            implicit_args,
        }
    }

    fn check_overflow(ctx: &EvalContext, overflow: bool, log: &str) {
        if overflow {
            // if ctx.warnings.warning_cnt != 1 {
            //     println!(
            //         "ctx.warnings.cnt!=1, it is: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warning_cnt, log, ctx.warnings.warnings
            //     );
            // }
            // if ctx.warnings.warning_cnt == 0 {
            //     return;
            // }
            // if ctx.warnings.warnings[0].get_code() != ERR_DATA_OUT_OF_RANGE {
            //     println!(
            //         "warn[0].code()!=expect, it is: {}, expect: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warnings[0].get_code(),
            //         ERR_DATA_OUT_OF_RANGE,
            //         log,
            //         ctx.warnings.warnings
            //     );
            // }

            assert_eq!(ctx.warnings.warning_cnt, 1, "{}", log);
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                ERR_DATA_OUT_OF_RANGE,
                "{}",
                log
            );
        } else {
            // if ctx.warnings.warning_cnt != 0 {
            //     println!(
            //         "expect warn_cnt==0, it is: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warning_cnt, log, ctx.warnings.warnings
            //     );
            // }
            assert_eq!(ctx.warnings.warning_cnt, 0, "{}", log);
        }
    }

    fn check_warning(ctx: &EvalContext, err_code: Option<i32>, log: &str) {
        if let Some(x) = err_code {
            // if ctx.warnings.warning_cnt != 1 {
            //     println!(
            //         "ctx.warnings.cnt!=1, it is: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warning_cnt, log, ctx.warnings.warnings
            //     );
            // }
            // if ctx.warnings.warning_cnt == 0 {
            //     return;
            // }
            // if ctx.warnings.warnings[0].get_code() != x {
            //     println!(
            //         "warn[0].code()!=expect, it is: {}, expect: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warnings[0].get_code(),
            //         x,
            //         log,
            //         ctx.warnings.warnings
            //     );
            // }
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "log: {}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                x,
                "log: {}, warnings: {:?}",
                log,
                ctx.warnings.warnings
            );
        } else {
            // if ctx.warnings.warning_cnt != 0 {
            //     println!(
            //         "expect warn_cnt==0, it is: {}, log: {}, warnings: {:?}",
            //         ctx.warnings.warning_cnt, log, ctx.warnings.warnings
            //     );
            // }
            assert_eq!(
                ctx.warnings.warning_cnt, 0,
                "log: {}, warnings: {:?}",
                log, ctx.warnings.warnings
            );
        }
    }

    fn check_result<R: Debug + PartialEq>(expect: Option<&R>, res: &Result<Option<R>>, log: &str) {
        assert!(res.is_ok(), "{}", log);
        let res = res.as_ref().unwrap();
        if res.is_none() {
            // if expect.is_some() {
            //     println!("expect is some, but res is none, log: {}", log);
            // }
            assert!(expect.is_none(), "{}", log);
        } else {
            let res = res.as_ref().unwrap();
            // if expect.unwrap() != res {
            //     println!(
            //         "expect.unwrap()==res: {}, log: {}",
            //         expect.unwrap() == res,
            //         log
            //     );
            // }
            assert_eq!(res, expect.unwrap(), "{}", log);
        }
    }

    impl Display for Json {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "{}", self.to_string())
        }
    }

    // comment for all test below:
    // if there should not be any overflow/truncate,
    // then should not set ctx with overflow_as_warning/truncated_as_warning flag,
    // and then if there is unexpected overflow/truncate,
    // then we will find them in `unwrap`

    #[test]
    fn test_int_as_duration() {
        test_none_with_ctx_and_extra(cast_int_as_duration);

        // This case copy from Duration.rs::tests::test_from_i64
        let cs: Vec<(i64, isize, crate::codec::Result<Option<Duration>>, bool)> = vec![
            // (input, fsp, expect, overflow)
            (
                101010,
                0,
                Ok(Some(Duration::parse(b"10:10:10", 0).unwrap())),
                false,
            ),
            (
                101010,
                5,
                Ok(Some(Duration::parse(b"10:10:10", 5).unwrap())),
                false,
            ),
            (
                8385959,
                0,
                Ok(Some(Duration::parse(b"838:59:59", 0).unwrap())),
                false,
            ),
            (
                8385959,
                6,
                Ok(Some(Duration::parse(b"838:59:59", 6).unwrap())),
                false,
            ),
            (
                -101010,
                0,
                Ok(Some(Duration::parse(b"-10:10:10", 0).unwrap())),
                false,
            ),
            (
                -101010,
                5,
                Ok(Some(Duration::parse(b"-10:10:10", 5).unwrap())),
                false,
            ),
            (
                -8385959,
                0,
                Ok(Some(Duration::parse(b"-838:59:59", 0).unwrap())),
                false,
            ),
            (
                -8385959,
                6,
                Ok(Some(Duration::parse(b"-838:59:59", 6).unwrap())),
                false,
            ),
            // will overflow
            (8385960, 0, Ok(None), true),
            (8385960, 1, Ok(None), true),
            (8385960, 5, Ok(None), true),
            (8385960, 6, Ok(None), true),
            (-8385960, 0, Ok(None), true),
            (-8385960, 1, Ok(None), true),
            (-8385960, 5, Ok(None), true),
            (-8385960, 6, Ok(None), true),
            // will truncated
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8375960, 0, Err(Error::truncated_wrong_val("", "")), false),
            (8376049, 0, Err(Error::truncated_wrong_val("", "")), false),
            // TODO, add test for num>=10000000000
            //  after Duration::from_f64 had impl logic for num>=10000000000
            // (10000000000, 0, Ok(Duration::parse(b"0:0:0", 0).unwrap())),
            // (10000235959, 0, Ok(Duration::parse(b"23:59:59", 0).unwrap())),
            // (10000000000, 0, Ok(Duration::parse(b"0:0:0", 0).unwrap())),
        ];

        for (input, fsp, expect, overflow) in cs {
            let mut ctx = make_ctx(overflow, false, false);
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_6(fsp);
            let extra = make_extra(&rft, &ia);

            let result = cast_int_as_duration(&mut ctx, &extra, &Some(input));

            // make log
            let expect_str = match expect.as_ref() {
                Ok(x) => format!("{:?}", x.map(|x| x.to_string())),
                Err(e) => format!("{:?}", e),
            };
            let result_str = match result {
                Ok(Some(x)) => x.to_string(),
                _ => format!("{:?}", result),
            };
            let log = format!(
                "input: {}, fsp: {}, expect: {}, result: {:?}",
                input, fsp, expect_str, result_str
            );

            match expect {
                Ok(expect) => {
                    check_result(expect.as_ref(), &result, log.as_str());
                    check_overflow(&ctx, overflow, log.as_str());
                }
                Err(e) => {
                    // if !result.is_err() {
                    //     println!("result.is_err==false, log: {}, output_err: {}", log, e);
                    // }
                    assert!(result.is_err(), "log: {}, output_err: {}", log, e);
                }
            }
        }
    }

    fn test_as_duration_helper<T: Clone, FnToCastStr, FnToDebugStr, FnCast>(
        base_cs: Vec<T>,
        to_cast_str_func: FnToCastStr,
        to_debug_str_func: FnToDebugStr,
        cast_func: FnCast,
        func_name: &str,
    ) where
        FnToCastStr: Fn(&T) -> String,
        FnToDebugStr: Fn(&T) -> String,
        FnCast: Fn(&mut EvalContext, &RpnFnCallExtra, &Option<T>) -> Result<Option<Duration>>,
    {
        // cast_real_as_duration call Duration::parse directly,
        // and Duration::parse is test in duration.rs.
        // Our test here is to make sure that the result is same as calling Duration::parse
        // no matter whether call_real_as_duration call Duration::parse directly.
        for val in base_cs {
            for fsp in MIN_FSP..=MAX_FSP {
                let mut ctx = make_ctx(true, true, false);
                let ia = make_implicit_args(false);
                let rft = make_ret_field_type_6(fsp as isize);
                let extra = make_extra(&rft, &ia);

                let result = cast_func(&mut ctx, &extra, &Some(val.clone()));

                let val_str = to_cast_str_func(&val);
                let base_expect = Duration::parse(val_str.as_bytes(), fsp);

                // make log
                let result_str = result.as_ref().map(|x| x.map(|x| x.to_string()));

                match base_expect {
                    Err(e) => match e.code() {
                        ERR_DATA_OUT_OF_RANGE => {
                            let log = format!(
                                "func_name:{}, input: {}, fsp: {}, output: {:?}, expect: {}, expect_warn: {}",
                                func_name, to_debug_str_func(&val), fsp, result_str, Duration::zero(), ERR_DATA_OUT_OF_RANGE
                            );
                            check_overflow(&ctx, true, log.as_str());
                            check_result(Some(&Duration::zero()), &result, log.as_str());
                        }
                        WARN_DATA_TRUNCATED => {
                            let log = format!(
                                "func_name:{}, input: {}, fsp: {}, output: {:?}, output_warn: {:?}, expect: {}, expect_warn: {}",
                                func_name, to_debug_str_func(&val), fsp, result_str, ctx.warnings.warnings, Duration::zero(), WARN_DATA_TRUNCATED
                            );
                            check_warning(&ctx, Some(WARN_DATA_TRUNCATED), log.as_str());
                            check_result(Some(&Duration::zero()), &result, log.as_str());
                        }
                        _ => {
                            let expect_err: crate::error::Error = e.into();
                            let log = format!(
                                "func_name:{}, input: {}, fsp: {}, output: {:?}, output_warn: {:?}, expect: {:?}",
                                func_name, to_debug_str_func(&val), fsp, result_str, ctx.warnings.warnings, expect_err
                            );
                            // if !result.is_err() {
                            //     println!("expect result is err, log: {}", log.as_str());
                            // }
                            assert!(result.is_err(), "log: {}", log)
                        }
                    },
                    Ok(v) => {
                        let log = format!(
                            "func_name:{}, input: {}, fsp: {}, output: {:?}, output_warn: {:?}, expect: {:?}",
                            func_name, to_debug_str_func(&val), fsp, result_str, ctx.warnings.warnings, v
                        );
                        check_result(Some(&v), &result, log.as_str())
                    }
                }
            }
        }
    }

    #[test]
    fn test_real_as_duration() {
        test_none_with_ctx_and_extra(cast_real_as_duration);

        let cs: Vec<f64> = vec![
            101112.0,
            101112.123456,
            1112.0,
            12.0,
            -0.123,
            12345.0,
            -123.0,
            -23.0,
        ];

        test_as_duration_helper(
            cs,
            |x| x.to_string(),
            |x| x.to_string(),
            |ctx, extra, val| {
                let val = val.map(|x| Real::new(x).unwrap());
                cast_real_as_duration(ctx, extra, &val)
            },
            "cast_real_as_duration",
        )
    }

    #[test]
    fn test_bytes_as_duration() {
        test_none_with_ctx_and_extra(cast_bytes_as_duration);

        let cs: Vec<Bytes> = vec![
            b"17:51:04.78".to_vec(),
            b"-17:51:04.78".to_vec(),
            b"17:51:04.78".to_vec(),
            b"-17:51:04.78".to_vec(),
        ];

        test_as_duration_helper(
            cs,
            |x| String::from_utf8_lossy(x).to_string(),
            |x| String::from_utf8_lossy(x).to_string(),
            cast_bytes_as_duration,
            "cast_bytes_as_duration",
        );
    }

    #[test]
    fn test_decimal_as_duration() {
        test_none_with_ctx_and_extra(cast_decimal_as_duration);

        let cs = vec![
            Decimal::from(i64::MIN),
            Decimal::from(i64::MAX),
            Decimal::from(u64::MAX),
            Decimal::zero(),
            Decimal::from_bytes(b"-9223372036854775808")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"9223372036854775808")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-9223372036854775809")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"9223372036854775809")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-18446744073709551615")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"18446744073709551615")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-18446744073709551616")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"18446744073709551616")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-184467440737095516160")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"184467440737095516160")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"-99999999999999999999999999999999")
                .unwrap()
                .unwrap(),
            Decimal::from_bytes(b"99999999999999999999999999999999")
                .unwrap()
                .unwrap(),
        ];
        test_as_duration_helper(
            cs,
            |x| x.to_string(),
            |x| x.to_string(),
            cast_decimal_as_duration,
            "cast_decimal_as_duration",
        );
    }

    #[test]
    fn test_time_as_duration() {
        test_none_with_ctx_and_extra(cast_time_as_duration);

        // copy from test_convert_to_duration
        let cs = vec![
            // (input, input's fsp, output's fsp, output)
            ("2012-12-31 11:30:45.123456", 4, 0, "11:30:45"),
            ("2012-12-31 11:30:45.123456", 4, 1, "11:30:45.1"),
            ("2012-12-31 11:30:45.123456", 4, 2, "11:30:45.12"),
            ("2012-12-31 11:30:45.123456", 4, 3, "11:30:45.124"),
            ("2012-12-31 11:30:45.123456", 4, 4, "11:30:45.1235"),
            ("2012-12-31 11:30:45.123456", 4, 5, "11:30:45.12350"),
            ("2012-12-31 11:30:45.123456", 4, 6, "11:30:45.123500"),
            ("2012-12-31 11:30:45.123456", 6, 0, "11:30:45"),
            ("2012-12-31 11:30:45.123456", 6, 1, "11:30:45.1"),
            ("2012-12-31 11:30:45.123456", 6, 2, "11:30:45.12"),
            ("2012-12-31 11:30:45.123456", 6, 3, "11:30:45.123"),
            ("2012-12-31 11:30:45.123456", 6, 4, "11:30:45.1235"),
            ("2012-12-31 11:30:45.123456", 6, 5, "11:30:45.12346"),
            ("2012-12-31 11:30:45.123456", 6, 6, "11:30:45.123456"),
            ("2012-12-31 11:30:45.123456", 0, 0, "11:30:45"),
            ("2012-12-31 11:30:45.123456", 0, 1, "11:30:45.0"),
            ("2012-12-31 11:30:45.123456", 0, 2, "11:30:45.00"),
            ("2012-12-31 11:30:45.123456", 0, 3, "11:30:45.000"),
            ("2012-12-31 11:30:45.123456", 0, 4, "11:30:45.0000"),
            ("2012-12-31 11:30:45.123456", 0, 5, "11:30:45.00000"),
            ("2012-12-31 11:30:45.123456", 0, 6, "11:30:45.000000"),
            ("0000-00-00 00:00:00", 6, 0, "00:00:00"),
            ("0000-00-00 00:00:00", 6, 1, "00:00:00.0"),
            ("0000-00-00 00:00:00", 6, 2, "00:00:00.00"),
            ("0000-00-00 00:00:00", 6, 3, "00:00:00.000"),
            ("0000-00-00 00:00:00", 6, 4, "00:00:00.0000"),
            ("0000-00-00 00:00:00", 6, 5, "00:00:00.00000"),
            ("0000-00-00 00:00:00", 6, 6, "00:00:00.000000"),
        ];
        for (s, fsp, expect_fsp, expect) in cs {
            let mut ctx = make_ctx(false, false, false);

            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_6(expect_fsp);
            let extra = make_extra(&rft, &ia);

            let input_time = Time::parse_utc_datetime(s, fsp).unwrap();
            let expect_time = Duration::parse(expect.as_bytes(), expect_fsp as i8).unwrap();
            let result = cast_time_as_duration(&mut ctx, &extra, &Some(input_time));
            let result_str = result.as_ref().map(|x| x.as_ref().map(|x| x.to_string()));
            let log = format!(
                "input: {}, fsp: {}, expect_fsp: {}, expect: {}, output: {:?}",
                s, fsp, expect_fsp, expect, result_str,
            );
            check_result(Some(&expect_time), &result, log.as_str());
        }
    }

    #[test]
    fn test_duration_as_duration() {
        test_none_with_extra(cast_duration_as_duration);

        let cs = vec![
            ("11:30:45.123456", 6, 0, "11:30:45"),
            ("11:30:45.123456", 6, 1, "11:30:45.1"),
            ("11:30:45.123456", 6, 2, "11:30:45.12"),
            ("11:30:45.123456", 6, 3, "11:30:45.123"),
            ("11:30:45.123456", 6, 4, "11:30:45.1235"),
            ("11:30:45.123456", 6, 5, "11:30:45.12346"),
            ("11:30:45.123456", 6, 6, "11:30:45.123456"),
        ];

        for (input, input_fsp, output_fsp, expect) in cs {
            let ia = make_implicit_args(false);
            let rft = make_ret_field_type_6(output_fsp as isize);
            let extra = make_extra(&rft, &ia);

            let dur = Duration::parse(input.as_bytes(), input_fsp).unwrap();
            let expect = Duration::parse(expect.as_bytes(), output_fsp).unwrap();
            let r = cast_duration_as_duration(&extra, &Some(dur));

            let result_str = r.as_ref().map(|x| x.map(|x| x.to_string()));
            let log = format!(
                "input: {}, input_fsp: {}, output_fsp: {}, expect: {}, output: {:?}",
                input, input_fsp, output_fsp, expect, result_str
            );
            check_result(Some(&expect), &r, log.as_str());
        }
    }

    #[test]
    fn test_json_as_duration() {
        test_none_with_ctx_and_extra(cast_json_as_duration);

        // the case that Json::unquote failed had be tested by test_json_unquote

        let cs = vec![
            Json::Object(BTreeMap::default()),
            Json::Array(vec![]),
            Json::I64(10),
            Json::I64(i64::MAX),
            Json::I64(i64::MIN),
            Json::U64(0),
            Json::U64(u64::MAX),
            Json::Double(10.5),
            Json::Double(10.4),
            Json::Double(-10.4),
            Json::Double(-10.5),
            Json::Double(i64::MIN as u64 as f64),
            Json::Double(i64::MAX as u64 as f64),
            Json::Double(i64::MIN as u64 as f64),
            Json::Double(i64::MIN as f64),
            Json::Double(((1u64 << 63) + (1u64 << 62)) as u64 as f64),
            Json::Double(-((1u64 << 63) as f64 + (1u64 << 62) as f64)),
            Json::Double(f64::from(f32::MIN)),
            Json::Double(f64::from(f32::MAX)),
            Json::Double(f64::MAX),
            Json::Double(f64::MAX),
            Json::String(String::from("10.0")),
            Json::String(String::from(
                "999999999999999999999999999999999999999999999999",
            )),
            Json::String(String::from(
                "-999999999999999999999999999999999999999999999999",
            )),
            Json::String(String::from(
                "99999999999999999999999999999999999999999999999aabcde9",
            )),
            Json::String(String::from(
                "-99999999999999999999999999999999999999999999999aabcde9",
            )),
            Json::Boolean(true),
            Json::Boolean(false),
            Json::None,
        ];
        test_as_duration_helper(
            cs,
            |x| x.unquote().unwrap(),
            |x| format!("{:?}", x),
            cast_json_as_duration,
            "cast_json_as_duration",
        );
    }
}
