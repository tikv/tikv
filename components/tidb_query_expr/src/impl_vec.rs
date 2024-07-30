// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;

#[rpn_fn(writer)]
#[inline]
fn vec_as_text(a: VectorFloat32Ref, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(Bytes::from(a.to_string()))))
}

#[rpn_fn]
#[inline]
fn vec_dims(arg: VectorFloat32Ref) -> Result<Option<Int>> {
    Ok(Some(arg.len() as Int))
}

#[rpn_fn]
#[inline]
fn vec_l1_distance(a: VectorFloat32Ref, b: VectorFloat32Ref) -> Result<Option<Real>> {
    // TiKV does not support NaN. This turns NaN into null
    Ok(Real::new(a.l1_distance(b)?).ok())
}

#[rpn_fn]
#[inline]
fn vec_l2_distance(a: VectorFloat32Ref, b: VectorFloat32Ref) -> Result<Option<Real>> {
    // TiKV does not support NaN. This turns NaN into null
    Ok(Real::new(a.l2_distance(b)?).ok())
}

#[rpn_fn]
#[inline]
fn vec_negative_inner_product(a: VectorFloat32Ref, b: VectorFloat32Ref) -> Result<Option<Real>> {
    // TiKV does not support NaN. This turns NaN into null
    Ok(Real::new(a.inner_product(b)? * -1.0).ok())
}

#[rpn_fn]
#[inline]
fn vec_cosine_distance(a: VectorFloat32Ref, b: VectorFloat32Ref) -> Result<Option<Real>> {
    // TiKV does not support NaN. This turns NaN into null
    Ok(Real::new(a.cosine_distance(b)?).ok())
}

#[rpn_fn]
#[inline]
fn vec_l2_norm(a: VectorFloat32Ref) -> Result<Option<Real>> {
    // TiKV does not support NaN. This turns NaN into null
    Ok(Real::new(a.l2_norm()).ok())
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::types::test_util::RpnFnScalarEvaluator;

    // Test cases are ported from pgvector: https://github.com/pgvector/pgvector/blob/master/test/expected/functions.out
    // Copyright (c) 1996-2023, PostgreSQL Global Development Group

    #[test]
    fn test_dims() {
        let cases = vec![
            (vec![], Some(0)),
            (vec![1.0, 2.0], Some(2)),
            (vec![1.0, 2.0, 3.0], Some(3)),
        ];
        for (arg, expected_output) in cases {
            let arg = VectorFloat32::new(arg).unwrap();
            let output: Option<Int> = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::VecDimsSig)
                .unwrap();
            assert_eq!(output, expected_output);
        }
    }

    #[test]
    fn test_l2_norm() {
        let cases = vec![
            (vec![], Some(0.0)),
            (vec![3.0, 4.0], Some(5.0)),
            (vec![0.0, 1.0], Some(1.0)),
        ];

        for (arg, expected_output) in cases {
            let arg = VectorFloat32::new(arg).unwrap();
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::VecL2NormSig)
                .unwrap();
            assert_eq!(output, expected_output.map(|x| Real::new(x).unwrap()));
        }
    }

    #[test]
    fn test_l2_distance() {
        let ok_cases = vec![
            (vec![0.0, 0.0], vec![3.0, 4.0], Some(5.0)),
            (vec![0.0, 0.0], vec![0.0, 1.0], Some(1.0)),
            (vec![3e38], vec![-3e38], Some(f64::INFINITY)),
        ];
        for (arg1, arg2, expected_output) in ok_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecL2DistanceSig)
                .unwrap();
            assert_eq!(output, expected_output.map(|x| Real::new(x).unwrap()));
        }

        let err_cases = vec![(vec![1.0, 2.0], vec![3.0])];
        for (arg1, arg2) in err_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecL2DistanceSig);
            assert!(output.is_err(), "expected error, got {:?}", output);
        }
    }

    #[test]
    fn test_negative_inner_product() {
        let ok_cases = vec![
            (vec![1.0, 2.0], vec![3.0, 4.0], Some(-11.0)),
            (vec![3e38], vec![3e38], Some(f64::NEG_INFINITY)),
        ];
        for (arg1, arg2, expected_output) in ok_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecNegativeInnerProductSig)
                .unwrap();
            assert_eq!(output, expected_output.map(|x| Real::new(x).unwrap()));
        }

        let err_cases = vec![(vec![1.0, 2.0], vec![3.0])];
        for (arg1, arg2) in err_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecNegativeInnerProductSig);
            assert!(output.is_err(), "expected error, got {:?}", output);
        }
    }

    #[test]
    fn test_cosine_distance() {
        let ok_cases = vec![
            (vec![1.0, 2.0], vec![2.0, 4.0], Some(0.0)),
            (vec![1.0, 2.0], vec![0.0, 0.0], None), // NaN turns to NULL
            (vec![1.0, 1.0], vec![1.0, 1.0], Some(0.0)),
            (vec![1.0, 0.0], vec![0.0, 2.0], Some(1.0)),
            (vec![1.0, 1.0], vec![-1.0, -1.0], Some(2.0)),
            (vec![1.0, 1.0], vec![1.1, 1.1], Some(0.0)),
            (vec![1.0, 1.0], vec![-1.1, -1.1], Some(2.0)),
            (vec![3e38], vec![3e38], None), // NaN turns to NULL
        ];
        for (arg1, arg2, expected_output) in ok_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecCosineDistanceSig)
                .unwrap();
            assert_eq!(output, expected_output.map(|x| Real::new(x).unwrap()));
        }

        let err_cases = vec![(vec![1.0, 2.0], vec![3.0])];
        for (arg1, arg2) in err_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecCosineDistanceSig);
            assert!(output.is_err(), "expected error, got {:?}", output);
        }
    }

    #[test]
    fn test_l1_distance() {
        let ok_cases = vec![
            (vec![0.0, 0.0], vec![3.0, 4.0], Some(7.0)),
            (vec![0.0, 0.0], vec![0.0, 1.0], Some(1.0)),
            (vec![3e38], vec![-3e38], Some(f64::INFINITY)),
        ];
        for (arg1, arg2, expected_output) in ok_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecL1DistanceSig)
                .unwrap();
            assert_eq!(output, expected_output.map(|x| Real::new(x).unwrap()));
        }

        let err_cases = vec![(vec![1.0, 2.0], vec![3.0])];
        for (arg1, arg2) in err_cases {
            let arg1 = VectorFloat32::new(arg1).unwrap();
            let arg2 = VectorFloat32::new(arg2).unwrap();
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .push_param(arg1)
                .push_param(arg2)
                .evaluate(ScalarFuncSig::VecL1DistanceSig);
            assert!(output.is_err(), "expected error, got {:?}", output);
        }
    }
}
