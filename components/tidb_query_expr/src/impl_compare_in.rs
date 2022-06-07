// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    hash::Hash,
    marker::{PhantomData, Send, Sized},
};

use codec::prelude::NumberDecoder;
use tidb_query_codegen::rpn_fn;
use tidb_query_common::{Error, Result};
use tidb_query_datatype::{
    codec::{
        collation::*,
        data_type::*,
        mysql::{Decimal, EnumDecoder, MAX_FSP},
    },
    EvalType, FieldTypeAccessor, FieldTypeFlag,
};
use tipb::{Expr, ExprType, FieldType};

pub trait InByHash {
    type Key: EvaluableRet + Extract + Eq;
    type StoreKey: 'static + Hash + Eq + Sized + Send;

    fn map(key: Self::Key) -> Result<Self::StoreKey>;
    fn map_ref(key: &Self::Key) -> Result<&Self::StoreKey>;
}

pub struct NormalInByHash<K: EvaluableRet + Extract + Hash + Eq + Sized + Send>(PhantomData<K>);

impl<K: EvaluableRet + Extract + Hash + Eq + Sized> InByHash for NormalInByHash<K> {
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
    fn extract(expr_tp: ExprType, val: Vec<u8>, field_type: &FieldType) -> Result<Self>;
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
    fn extract(expr_tp: ExprType, val: Vec<u8>, _field_type: &FieldType) -> Result<Self> {
        if expr_tp == ExprType::Int64 {
            let value = val
                .as_slice()
                .read_i64()
                .map_err(|_| other_err!("Unable to decode int64 from the request"))?;
            Ok(value)
        } else if expr_tp == ExprType::Uint64 || expr_tp == ExprType::MysqlEnum {
            let value = val
                .as_slice()
                .read_u64()
                .map_err(|_| other_err!("Unable to decode uint64 from the request"))?;
            Ok(value as i64)
        } else {
            Err(type_error(<Int as Evaluable>::EVAL_TYPE, expr_tp))
        }
    }
}

impl Extract for Real {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>, _field_type: &FieldType) -> Result<Self> {
        if expr_tp != ExprType::Float32 && expr_tp != ExprType::Float64 {
            return Err(type_error(<Real as Evaluable>::EVAL_TYPE, expr_tp));
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
    fn extract(expr_tp: ExprType, val: Vec<u8>, field_type: &FieldType) -> Result<Self> {
        match expr_tp {
            ExprType::Bytes | ExprType::String => Ok(val),
            ExprType::MysqlEnum => {
                let value = val
                    .as_slice()
                    .read_enum_uint(field_type)
                    .map_err(|_| other_err!("Unable to decode enum from the request"))?;
                Ok(value.name().into())
            }
            _ => Err(type_error(Bytes::EVAL_TYPE, expr_tp)),
        }
    }
}

impl Extract for Decimal {
    #[inline]
    fn extract(expr_tp: ExprType, val: Vec<u8>, _field_type: &FieldType) -> Result<Self> {
        if expr_tp != ExprType::MysqlDecimal {
            return Err(type_error(<Decimal as Evaluable>::EVAL_TYPE, expr_tp));
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
    fn extract(expr_tp: ExprType, val: Vec<u8>, _field_type: &FieldType) -> Result<Self> {
        if expr_tp != ExprType::MysqlDuration {
            return Err(type_error(<Duration as Evaluable>::EVAL_TYPE, expr_tp));
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

pub trait InByCompare: Evaluable + EvaluableRet + Eq {}

impl InByCompare for Int {}
impl InByCompare for Real {}
impl InByCompare for Decimal {}
impl InByCompare for Duration {}
// DateTime requires TZInfo in context, and we cannot acquire it during metadata_mapper.
// TODO: implement InByHash for DateTime.
impl InByCompare for DateTime {}

#[derive(Debug)]
pub struct CompareInMeta<T: Eq + Hash> {
    lookup_map: HashMap<T, bool>,
    has_null: bool,
    // only used when arg0 is int type.
    unsigned_flags: Vec<bool>,
}

#[rpn_fn(nullable, varg, capture = [metadata], min_args = 1, metadata_mapper = init_compare_in_data::<T>)]
#[inline]
pub fn compare_in_by_hash<T: InByHash>(
    metadata: &CompareInMeta<T::StoreKey>,
    args: &[Option<&T::Key>],
) -> Result<Option<Int>>
where
    T::Key: Evaluable,
{
    assert!(!args.is_empty());
    let base_val = args[0];
    match base_val {
        None => Ok(None),
        Some(base_val) => {
            let base_val = T::map_ref(base_val)?;
            if metadata.lookup_map.contains_key(base_val) {
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

#[rpn_fn(nullable, varg, capture = [metadata], min_args = 1, metadata_mapper = init_compare_in_data::<NormalInByHash::<Int>>)]
#[inline]
pub fn compare_in_int_type_by_hash(
    metadata: &CompareInMeta<Int>,
    args: &[Option<&Int>],
) -> Result<Option<Int>> {
    assert!(!args.is_empty());
    let base_val = args[0];
    let arg0_unsigned = metadata.unsigned_flags[0];
    match base_val {
        None => Ok(None),
        Some(base_val) => {
            if let Some(&argi_unsigned) = metadata.lookup_map.get(base_val) {
                // Why check 'base_val >= 0', in the following expamle:
                // eg: col_int_signed in (col_int_unsigned)
                // if both col_int_signed and col_int_unsigned are 1, the result should be true,
                // even though their signed flags are different.
                if (*base_val >= 0)
                    || (arg0_unsigned && argi_unsigned)
                    || (!arg0_unsigned && !argi_unsigned)
                {
                    return Ok(Some(1));
                }
            }
            let mut default_ret = if metadata.has_null { None } else { Some(0) };
            for (i, arg) in args[1..].iter().enumerate() {
                let argi_unsigned = metadata.unsigned_flags[i + 1];
                match arg {
                    None => {
                        default_ret = None;
                    }
                    Some(&v) => {
                        if (*base_val == v)
                            && ((*base_val >= 0)
                                || (arg0_unsigned && argi_unsigned)
                                || (!arg0_unsigned && !argi_unsigned))
                        {
                            return Ok(Some(1));
                        }
                    }
                }
            }
            Ok(default_ret)
        }
    }
}

#[rpn_fn(nullable, varg, capture = [metadata], min_args = 1, metadata_mapper = init_compare_in_data::<CollationAwareBytesInByHash::<C>>)]
#[inline]
pub fn compare_in_by_hash_bytes<C: Collator>(
    metadata: &CompareInMeta<SortKey<Bytes, C>>,
    args: &[Option<BytesRef>],
) -> Result<Option<Int>> {
    assert!(!args.is_empty());
    let base_val = args[0];
    match base_val {
        None => Ok(None),
        Some(base_val) => {
            let base_val = CollationAwareBytesInByHash::<C>::map(base_val.to_vec())?;
            if metadata.lookup_map.contains_key(&base_val) {
                return Ok(Some(1));
            }
            let mut default_ret = if metadata.has_null { None } else { Some(0) };
            for arg in &args[1..] {
                match arg {
                    None => {
                        default_ret = None;
                    }
                    Some(v) => {
                        let v = CollationAwareBytesInByHash::<C>::map(v.to_vec())?;
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
    let mut lookup_map = HashMap::new();
    let mut has_null = false;
    let children = expr.mut_children();
    assert!(!children.is_empty());
    let mut unsigned_flags = vec![false; children.len()];
    unsigned_flags[0] = children[0]
        .get_field_type()
        .as_accessor()
        .flag()
        .contains(FieldTypeFlag::UNSIGNED);

    let n = children.len();
    let mut tail_index = n - 1;
    // try to evaluate and remove all constant nodes except args[0].
    for i in (1..n).rev() {
        let tree_node = &mut children[i];
        let mut is_constant = true;
        let is_unsigned = tree_node
            .get_field_type()
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);
        unsigned_flags[i] = is_unsigned;
        match tree_node.get_tp() {
            ExprType::ScalarFunc | ExprType::ColumnRef => {
                is_constant = false;
            }
            ExprType::Null => {
                has_null = true;
            }
            expr_type => {
                let val =
                    T::Key::extract(expr_type, tree_node.take_val(), tree_node.get_field_type())?;
                let val = T::map(val)?;
                lookup_map.insert(val, is_unsigned);
            }
        }
        if is_constant {
            children.as_mut_slice().swap(i, tail_index);
            unsigned_flags.swap(i, tail_index);
            tail_index -= 1;
        }
    }
    children.truncate(tail_index + 1);
    unsigned_flags.truncate(tail_index + 1);

    Ok(CompareInMeta {
        lookup_map,
        has_null,
        unsigned_flags,
    })
}

#[rpn_fn(nullable, varg, min_args = 1)]
#[inline]
pub fn compare_in_by_compare<T: InByCompare>(args: &[Option<&T>]) -> Result<Option<Int>> {
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
                        if *v == base_val {
                            return Ok(Some(1));
                        }
                    }
                }
            }
            Ok(default_ret)
        }
    }
}

#[rpn_fn(nullable, varg, min_args = 1)]
#[inline]
pub fn compare_in_by_compare_json(args: &[Option<JsonRef>]) -> Result<Option<Int>> {
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
                        if v == &base_val {
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
    use test::{black_box, Bencher};
    use tidb_query_datatype::{
        builder::FieldTypeBuilder,
        codec::batch::{LazyBatchColumn, LazyBatchColumnVec},
        expr::EvalContext,
        Collation, FieldTypeTp,
    };
    use tipb::{FieldType, ScalarFuncSig};
    use tipb_helper::ExprDefBuilder;

    use super::{super::map_expr_node_to_rpn_func, *};
    use crate::{types::RpnFnMeta, RpnExpressionBuilder, RpnExpressionNode};

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
                    val.vector_value().unwrap().as_ref().to_int_vec(),
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
            (
                vec![Some("🐰"), Some("🐇"), Some("🐻"), Some("🐰")],
                Collation::Utf8Mb4UnicodeCi,
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
                val.vector_value().unwrap().as_ref().to_int_vec(),
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
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1), Some(1), Some(0)],
        );
    }

    #[test]
    fn test_unsigned_signed_int() {
        // -1(col int signed) in (max_u64, 1)
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Long))
            .push_child(ExprDefBuilder::constant_uint(18446744073709551615))
            .push_child(ExprDefBuilder::constant_int(1))
            .build();
        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::Long.into()];
        let log_rows = vec![0];
        // each vec represents a column
        let mut phy_rows = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Int);
            col.mut_decoded().push_int(Some(-1));
            col
        }]);
        let result = expr.eval(&mut ctx, schema, &mut phy_rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(0)]
        );

        // max_u64(col bigint unsigned) in (-1, 1)
        let mut bigint_unsigned_col: FieldType = FieldTypeTp::LongLong.into();
        bigint_unsigned_col
            .as_mut_accessor()
            .set_flag(FieldTypeFlag::UNSIGNED);
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, bigint_unsigned_col))
            .push_child(ExprDefBuilder::constant_int(-1))
            .push_child(ExprDefBuilder::constant_int(1))
            .build();
        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::Long.into()];
        let log_rows = vec![0];
        // each vec represents a column
        let mut phy_rows = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Int);
            col.mut_decoded().push_int(Some(-1));
            col
        }]);
        let result = expr.eval(&mut ctx, schema, &mut phy_rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(0)]
        );

        // -1 in (max_u64_col, 1)
        let mut bigint_unsigned_col: FieldType = FieldTypeTp::LongLong.into();
        bigint_unsigned_col
            .as_mut_accessor()
            .set_flag(FieldTypeFlag::UNSIGNED);
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(-1))
            .push_child(ExprDefBuilder::column_ref(0, bigint_unsigned_col))
            .push_child(ExprDefBuilder::constant_int(1))
            .build();
        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::Long.into()];
        let log_rows = vec![0];
        // each vec represents a column
        let mut phy_rows = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Int);
            col.mut_decoded().push_int(Some(-1));
            col
        }]);
        let result = expr.eval(&mut ctx, schema, &mut phy_rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(0)]
        );

        // -1 in (max_u64_col, -1)
        let mut bigint_unsigned_col: FieldType = FieldTypeTp::LongLong.into();
        bigint_unsigned_col
            .as_mut_accessor()
            .set_flag(FieldTypeFlag::UNSIGNED);
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(-1))
            .push_child(ExprDefBuilder::column_ref(0, bigint_unsigned_col))
            .push_child(ExprDefBuilder::constant_int(-1))
            .build();
        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::Long.into()];
        let log_rows = vec![0];
        // each vec represents a column
        let mut phy_rows = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Int);
            col.mut_decoded().push_int(Some(-1));
            col
        }]);
        let result = expr.eval(&mut ctx, schema, &mut phy_rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1)]
        );

        // max_u64 in (max_u64_col, -1)
        let mut bigint_unsigned_col: FieldType = FieldTypeTp::LongLong.into();
        bigint_unsigned_col
            .as_mut_accessor()
            .set_flag(FieldTypeFlag::UNSIGNED);
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_uint(18446744073709551615))
            .push_child(ExprDefBuilder::column_ref(0, bigint_unsigned_col))
            .push_child(ExprDefBuilder::constant_int(-1))
            .build();
        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::Long.into()];
        let log_rows = vec![0];
        // each vec represents a column
        let mut phy_rows = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Int);
            col.mut_decoded().push_int(Some(-1));
            col
        }]);
        let result = expr.eval(&mut ctx, schema, &mut phy_rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1)]
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
                black_box(logical_rows),
                black_box(1024),
            );
            assert!(result.is_ok());
        });
        profiler::stop();
    }

    use codec::prelude::NumberEncoder;
    #[test]
    fn test_enum() {
        let mut enum_expr = Expr::default();
        enum_expr.set_tp(ExprType::MysqlEnum);
        enum_expr.mut_val().write_u64(2u64).unwrap();
        let mut field_type = FieldType::new();
        field_type.set_tp(FieldTypeTp::Enum.to_u8().unwrap() as i32);
        let elems = protobuf::RepeatedField::from_slice(&[
            String::from("c"),
            String::from("b"),
            String::from("a"),
        ]);
        field_type.set_elems(elems);
        enum_expr.set_field_type(field_type);

        // string in(enum,...)
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InString, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_bytes("b".into()))
            .push_child(enum_expr.clone())
            .build();

        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::LongLong.into()];
        let log_rows = vec![0];
        let mut rows = LazyBatchColumnVec::empty();

        let result = expr.eval(&mut ctx, schema, &mut rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1)]
        );

        // int in(enum,...)
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_uint(2))
            .push_child(enum_expr.clone())
            .build();

        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::LongLong.into()];
        let log_rows = vec![0];
        let mut rows = LazyBatchColumnVec::empty();

        let result = expr.eval(&mut ctx, schema, &mut rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1)]
        );

        // enum in(string, enum,...)
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InString, FieldTypeTp::LongLong)
            .push_child(enum_expr.clone())
            .push_child(ExprDefBuilder::constant_bytes("a".into()))
            .push_child(enum_expr.clone())
            .build();

        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::LongLong.into()];
        let log_rows = vec![0];
        let mut rows = LazyBatchColumnVec::empty();

        let result = expr.eval(&mut ctx, schema, &mut rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1)]
        );

        // enum in(int, enum,...)
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::InInt, FieldTypeTp::LongLong)
            .push_child(enum_expr.clone())
            .push_child(ExprDefBuilder::constant_uint(1))
            .push_child(enum_expr)
            .build();

        let expr = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node,
            map_expr_node_to_rpn_func,
            2,
        )
        .unwrap();
        let mut ctx = EvalContext::default();
        let schema = &[FieldTypeTp::LongLong.into()];
        let log_rows = vec![0];
        let mut rows = LazyBatchColumnVec::empty();

        let result = expr.eval(&mut ctx, schema, &mut rows, &log_rows, 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().to_int_vec(),
            &[Some(1)]
        );
    }
}
