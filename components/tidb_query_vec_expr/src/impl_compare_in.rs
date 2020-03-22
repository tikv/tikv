// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::hash::Hash;
use std::marker::{PhantomData, Send, Sized};

use codec::prelude::NumberDecoder;
use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::EvalType;
use tipb::{Expr, ExprType};

use tidb_query_common::{Error, Result};
use tidb_query_datatype::codec::collation::*;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::codec::mysql::{Decimal, MAX_FSP};

pub trait InByHash {
    type Key: Evaluable + Extract + Eq;
    type StoreKey: 'static + Hash + Eq + Sized + Send;

    fn map(key: Self::Key) -> Result<Self::StoreKey>;
    fn map_ref(key: &Self::Key) -> Result<&Self::StoreKey>;
}

pub struct NormalInByHash<K: Evaluable + Extract + Hash + Eq + Sized + Send>(PhantomData<K>);

impl<K: Evaluable + Extract + Hash + Eq + Sized> InByHash for NormalInByHash<K> {
    type Key = K;
    type StoreKey = K;

    #[inline]
    fn map(key: Self::Key) -> Result<Self::StoreKey> {
        Ok(key)
    }

    #[inline]
    fn map_ref(key: &Self::Key) -> Result<&Self::StoreKey> {
        Ok(key)
    }
}

pub struct CollationAwareBytesInByHash<C: Collator>(PhantomData<C>);

impl<C: Collator> InByHash for CollationAwareBytesInByHash<C> {
    type Key = Bytes;
    type StoreKey = SortKey<Bytes, C>;

    fn map(key: Self::Key) -> Result<Self::StoreKey> {
        SortKey::new(key).map_err(Error::from)
    }

    fn map_ref(key: &Self::Key) -> Result<&Self::StoreKey> {
        SortKey::new_ref(key).map_err(Error::from)
    }
}

pub trait Extract: Sized {
    fn extract(expr_tp: ExprType, val: Vec<u8>) -> Result<Self>;
}

#[inline]
fn type_error(eval_type: EvalType, expr_type: ExprType) -> Error {
    return other_err!(
        "Unexpected ExprType {:?} and EvalType {:?}",
        expr_type,
        eval_type
    );
}

impl Extract for Int {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>) -> Result<Self> {
        if expr_tp == ExprType::Int64 {
            let value = val
                .as_slice()
                .read_i64()
                .map_err(|_| other_err!("Unable to decode int64 from the request"))?;
            Ok(value)
        } else if expr_tp == ExprType::Uint64 {
            let value = val
                .as_slice()
                .read_u64()
                .map_err(|_| other_err!("Unable to decode uint64 from the request"))?;
            Ok(value as i64)
        } else {
            Err(type_error(Int::EVAL_TYPE, expr_tp))
        }
    }
}

impl Extract for Real {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>) -> Result<Self> {
        if expr_tp != ExprType::Float32 && expr_tp != ExprType::Float64 {
            return Err(type_error(Real::EVAL_TYPE, expr_tp));
        }
        let value = val
            .as_slice()
            .read_f64()
            .map_err(|_| other_err!("Unable to decode float from the request"))?;
        Real::new(value).map_err(|_| other_err!("Unable to convert float to real"))
    }
}

impl Extract for Bytes {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>) -> Result<Self> {
        if expr_tp != ExprType::Bytes && expr_tp != ExprType::String {
            return Err(type_error(Bytes::EVAL_TYPE, expr_tp));
        }
        Ok(val)
    }
}

impl Extract for Decimal {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>) -> Result<Self> {
        if expr_tp != ExprType::MysqlDecimal {
            return Err(type_error(Decimal::EVAL_TYPE, expr_tp));
        }
        use tidb_query_datatype::codec::mysql::DecimalDecoder;
        let value = val
            .as_slice()
            .read_decimal()
            .map_err(|_| other_err!("Unable to decode decimal from the request"))?;
        Ok(value)
    }
}

impl Extract for Duration {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>) -> Result<Self> {
        if expr_tp != ExprType::MysqlDuration {
            return Err(type_error(Duration::EVAL_TYPE, expr_tp));
        }
        let n = val
            .as_slice()
            .read_i64()
            .map_err(|_| other_err!("Unable to decode duration from the request"))?;
        let value = Duration::from_nanos(n, MAX_FSP)
            .map_err(|_| other_err!("Unable to decode duration from the request"))?;
        Ok(value)
    }
}

pub trait InByCompare: Evaluable + Eq {}

impl InByCompare for Int {}
impl InByCompare for Real {}
impl InByCompare for Bytes {}
impl InByCompare for Decimal {}
impl InByCompare for Duration {}
// DateTime requires TZInfo in context, and we cannot acquire it during metadata_mapper.
// TODO: implement InByHash for DateTime.
impl InByCompare for DateTime {}
// Implement Hash for Json is impossible, due to equality of Json depends on an epsilon.
impl InByCompare for Json {}

#[derive(Debug)]
pub struct CompareInMeta<T: Eq + Hash> {
    lookup_set: HashSet<T>,
    has_null: bool,
}

#[rpn_fn(varg, capture = [metadata], min_args = 1, metadata_mapper = init_compare_in_data::<T>)]
#[inline]
pub fn compare_in_by_hash<T: InByHash>(
    metadata: &CompareInMeta<T::StoreKey>,
    args: &[&Option<T::Key>],
) -> Result<Option<Int>> {
    assert!(!args.is_empty());
    let base_val = args[0];
    match base_val {
        None => Ok(None),
        Some(base_val) => {
            let base_val = T::map_ref(base_val)?;
            if metadata.lookup_set.contains(base_val) {
                return Ok(Some(1));
            }
            let mut default_ret = if metadata.has_null { None } else { Some(0) };
            for arg in &args[1..] {
                match arg {
                    None => {
                        default_ret = None;
                    }
                    Some(v) => {
                        let v = T::map_ref(v)?;
                        if base_val == v {
                            return Ok(Some(1));
                        }
                    }
                }
            }
            Ok(default_ret)
        }
    }
}

fn init_compare_in_data<T: InByHash>(expr: &mut Expr) -> Result<CompareInMeta<T::StoreKey>> {
    let mut lookup_set = HashSet::new();
    let mut has_null = false;
    let children = expr.mut_children();
    assert!(!children.is_empty());

    let n = children.len();
    let mut tail_index = n - 1;
    // try to evaluate and remove all constant nodes except args[0].
    for i in (1..n).rev() {
        let tree_node = &mut children[i];
        let mut is_constant = true;
        match tree_node.get_tp() {
            ExprType::ScalarFunc | ExprType::ColumnRef => {
                is_constant = false;
            }
            ExprType::Null => {
                has_null = true;
            }
            expr_type => {
                let val = T::Key::extract(expr_type, tree_node.take_val())?;
                let val = T::map(val)?;
                lookup_set.insert(val);
            }
        }
        if is_constant {
            children.as_mut_slice().swap(i, tail_index);
            tail_index -= 1;
        }
    }
    children.truncate(tail_index + 1);

    Ok(CompareInMeta {
        lookup_set,
        has_null,
    })
}

#[rpn_fn(varg, min_args = 1)]
#[inline]
pub fn compare_in_by_compare<T: InByCompare>(args: &[&Option<T>]) -> Result<Option<Int>> {
    assert!(!args.is_empty());
    let base_val = args[0];
    match base_val {
        None => Ok(None),
        Some(base_val) => {
            let mut default_ret = Some(0);
            for arg in &args[1..] {
                match arg {
                    None => {
                        default_ret = None;
                    }
                    Some(v) => {
                        if v == base_val {
                            return Ok(Some(1));
                        }
                    }
                }
            }
            Ok(default_ret)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::map_expr_node_to_rpn_func;
    use super::*;

    use test::{black_box, Bencher};
    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::{Collation, FieldTypeTp};
    use tipb::ScalarFuncSig;
    use tipb_helper::ExprDefBuilder;

    use crate::types::RpnFnMeta;
    use crate::{RpnExpressionBuilder, RpnExpressionNode};
    use tidb_query_datatype::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
    use tidb_query_datatype::expr::EvalContext;

    #[test]
    fn test_in_constant() {
        // mapper to test compare_in_by_compare.
        fn by_compare_mapper(expr: &Expr) -> Result<RpnFnMeta> {
            match expr.get_sig() {
                ScalarFuncSig::InInt => Ok(compare_in_by_compare_fn_meta::<Int>()),
                _ => map_expr_node_to_rpn_func(expr),
            }
        }

        fn test_with_mapper<F>(mapper: F, by_hash: bool)
        where
            F: Fn(&Expr) -> Result<RpnFnMeta> + Copy,
        {
            let cases = vec![
                (vec![Some(1)], Some(0)),
                (vec![Some(1), Some(2)], Some(0)),
                (vec![Some(1), Some(2), Some(1)], Some(1)),
                (vec![Some(1), Some(2), None], None),
                (vec![Some(1), Some(2), None, Some(1)], Some(1)),
                (vec![None, Some(2), Some(1)], None),
            ];
            for (args, expected) in cases {
                let mut builder =
                    ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong);
                for arg in args {
                    builder = builder.push_child(match arg {
                        Some(v) => ExprDefBuilder::constant_int(v),
                        None => ExprDefBuilder::constant_null(FieldTypeTp::LongLong),
                    });
                }
                let node = builder.build();
                let exp =
                    RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, mapper, 1)
                        .unwrap();
                if by_hash {
                    if let RpnExpressionNode::FnCall { args_len, .. } = exp[0] {
                        // all constant args except base_val should be removed.
                        assert_eq!(args_len, 1);
                    }
                }
                let mut ctx = EvalContext::default();
                let schema = &[FieldTypeTp::LongLong.into()];
                let mut columns = LazyBatchColumnVec::empty();
                let result = exp.eval(&mut ctx, schema, &mut columns, &[], 1);
                let val = result.unwrap();
                assert!(val.is_vector());
                assert_eq!(
                    val.vector_value().unwrap().as_ref().as_int_slice(),
                    &[expected]
                );
            }
        }

        test_with_mapper(map_expr_node_to_rpn_func, true);
        test_with_mapper(by_compare_mapper, false);
    }

    #[test]
    fn test_in_string() {
        let cases = vec![
            (
                vec![Some("times naive"), Some("young"), Some("simple"), None],
                Collation::Binary,
                None,
            ),
            (
                vec![
                    Some("times naive"),
                    Some("young"),
                    Some("simple"),
                    Some("times naive"),
                ],
                Collation::Binary,
                Some(1),
            ),
            (
                vec![
                    Some("pINGcap"),
                    Some("PINGCAP"),
                    Some("PingCAP"),
                    Some("pingcap"),
                    Some("pingCap"),
                ],
                Collation::Utf8Mb4Bin,
                Some(0),
            ),
            (
                vec![
                    Some("pINGcap"),
                    Some("PINGCAP"),
                    Some("PingCAP"),
                    Some("pingcap"),
                    Some("pingCap"),
                ],
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            (
                vec![Some("breeswish"), Some("breezewish")],
                Collation::Utf8Mb4GeneralCi,
                Some(0),
            ),
            (
                vec![Some("breeswish"), Some("breezewish")],
                Collation::Utf8Mb4GeneralCi,
                Some(0),
            ),
            (
                vec![Some("codeworm96"), Some("CODEWORM96"), Some("CoDeWorm99")],
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            (
                vec![
                    Some("扩散性百万甜🍞"),
                    Some("扩散性百万辣🍞"),
                    Some("扩散性百万咸🍞"),
                ],
                Collation::Utf8Mb4GeneralCi,
                Some(0),
            ),
            (
                vec![Some("🐰"), Some("🐇"), Some("🐻"), Some("🐰")],
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
        ];

        for (args, collation, expected) in cases {
            let ft = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .collation(collation)
                .build();
            let mut builder = ExprDefBuilder::scalar_func(ScalarFuncSig::InString, ft);
            for arg in args {
                builder = builder.push_child(match arg {
                    Some(v) => ExprDefBuilder::constant_bytes(String::from(v).into_bytes()),
                    None => ExprDefBuilder::constant_null(FieldTypeTp::String),
                });
            }
            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
                node,
                map_expr_node_to_rpn_func,
                1,
            )
            .unwrap();
            if let RpnExpressionNode::FnCall { args_len, .. } = exp[0] {
                // all constant args except base_val should be removed.
                assert_eq!(args_len, 1);
            }
            let mut ctx = EvalContext::default();
            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();
            let result = exp.eval(&mut ctx, schema, &mut columns, &[], 1);
            let val = result.unwrap();
            assert!(val.is_vector());
            assert_eq!(
                val.vector_value().unwrap().as_ref().as_int_slice(),
                &[expected]
            );
        }
    }

    #[test]
    fn test_in_complex() {
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(11))
            .push_child(ExprDefBuilder::constant_int(22))
            .push_child(
                ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(6))
                    .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)),
            )
            .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        if let RpnExpressionNode::FnCall { args_len, .. } = exp[0] {
            assert_eq!(args_len, 3);
        }
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];
        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(5)); // row 1, 11 in [(5 + 6), ...]
                col.mut_decoded().push_int(Some(1)); // row 0
                col.mut_decoded().push_int(Some(1)); // row 2
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(8)); // row 1
                col.mut_decoded().push_int(Some(11)); // row 0, 11 in [11, ...]
                col.mut_decoded().push_int(Some(1)); // row 2
                col
            },
        ]);
        let result = exp.eval(&mut ctx, schema, &mut columns, &[1, 0, 2], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            &[Some(1), Some(1), Some(0)],
        );
    }

    #[bench]
    fn bench_compare_in(b: &mut Bencher) {
        let mut builder = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong));
        for i in 0..1024 {
            builder = builder.push_child(ExprDefBuilder::constant_int(i));
        }
        let node = builder.build();

        profiler::start("./bench_compare_in.profile");
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            1,
        )
        .unwrap();

        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::LongLong.into()];
        let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1024, EvalType::Int);
        for i in 0..1024 {
            col.mut_decoded().push_int(Some(i));
        }
        let mut columns = LazyBatchColumnVec::from(vec![col]);
        let logical_rows: &[usize] = &(0..1024).collect::<Vec<usize>>();
        profiler::start("./bench_compare_in.profile");
        b.iter(|| {
            let result = black_box(&exp).eval(
                black_box(&mut ctx),
                black_box(schema),
                black_box(&mut columns),
                black_box(&logical_rows),
                black_box(1024),
            );
            assert!(result.is_ok());
        });
        profiler::stop();
    }
}
