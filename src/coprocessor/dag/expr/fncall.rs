// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::usize;

use tipb::expression::ScalarFuncSig;

use coprocessor::codec::Datum;
use coprocessor::codec::mysql::{self, Decimal, Duration, Json, Time};
use super::{Error, FnCall, Result, StatementContext};
use super::compare::CmpOp;

impl FnCall {
    pub fn check_args(sig: ScalarFuncSig, args: usize) -> Result<()> {
        let (min_args, max_args) = match sig {
            ScalarFuncSig::LTInt |
            ScalarFuncSig::LEInt |
            ScalarFuncSig::GTInt |
            ScalarFuncSig::GEInt |
            ScalarFuncSig::EQInt |
            ScalarFuncSig::NEInt |
            ScalarFuncSig::NullEQInt |
            ScalarFuncSig::LTReal |
            ScalarFuncSig::LEReal |
            ScalarFuncSig::GTReal |
            ScalarFuncSig::GEReal |
            ScalarFuncSig::EQReal |
            ScalarFuncSig::NEReal |
            ScalarFuncSig::NullEQReal |
            ScalarFuncSig::LTDecimal |
            ScalarFuncSig::LEDecimal |
            ScalarFuncSig::GTDecimal |
            ScalarFuncSig::GEDecimal |
            ScalarFuncSig::EQDecimal |
            ScalarFuncSig::NEDecimal |
            ScalarFuncSig::NullEQDecimal |
            ScalarFuncSig::LTString |
            ScalarFuncSig::LEString |
            ScalarFuncSig::GTString |
            ScalarFuncSig::GEString |
            ScalarFuncSig::EQString |
            ScalarFuncSig::NEString |
            ScalarFuncSig::NullEQString |
            ScalarFuncSig::LTTime |
            ScalarFuncSig::LETime |
            ScalarFuncSig::GTTime |
            ScalarFuncSig::GETime |
            ScalarFuncSig::EQTime |
            ScalarFuncSig::NETime |
            ScalarFuncSig::NullEQTime |
            ScalarFuncSig::LTDuration |
            ScalarFuncSig::LEDuration |
            ScalarFuncSig::GTDuration |
            ScalarFuncSig::GEDuration |
            ScalarFuncSig::EQDuration |
            ScalarFuncSig::NEDuration |
            ScalarFuncSig::NullEQDuration |
            ScalarFuncSig::LTJson |
            ScalarFuncSig::LEJson |
            ScalarFuncSig::GTJson |
            ScalarFuncSig::GEJson |
            ScalarFuncSig::EQJson |
            ScalarFuncSig::NEJson |
            ScalarFuncSig::NullEQJson |
            ScalarFuncSig::PlusReal |
            ScalarFuncSig::PlusDecimal |
            ScalarFuncSig::PlusInt |
            ScalarFuncSig::MinusReal |
            ScalarFuncSig::MinusDecimal |
            ScalarFuncSig::MinusInt |
            ScalarFuncSig::MultiplyReal |
            ScalarFuncSig::MultiplyDecimal |
            ScalarFuncSig::MultiplyInt |
            ScalarFuncSig::IfNullInt |
            ScalarFuncSig::IfNullReal |
            ScalarFuncSig::IfNullString |
            ScalarFuncSig::IfNullDecimal |
            ScalarFuncSig::IfNullTime |
            ScalarFuncSig::IfNullDuration |
            ScalarFuncSig::IfNullJson |
            ScalarFuncSig::LogicalAnd |
            ScalarFuncSig::LogicalOr |
            ScalarFuncSig::LogicalXor |
            ScalarFuncSig::DivideDecimal |
            ScalarFuncSig::DivideReal |
            ScalarFuncSig::BitAndSig |
            ScalarFuncSig::BitOrSig |
            ScalarFuncSig::BitXorSig => (2, 2),

            ScalarFuncSig::CastIntAsInt |
            ScalarFuncSig::CastIntAsReal |
            ScalarFuncSig::CastIntAsString |
            ScalarFuncSig::CastIntAsDecimal |
            ScalarFuncSig::CastIntAsTime |
            ScalarFuncSig::CastIntAsDuration |
            ScalarFuncSig::CastIntAsJson |
            ScalarFuncSig::CastRealAsInt |
            ScalarFuncSig::CastRealAsReal |
            ScalarFuncSig::CastRealAsString |
            ScalarFuncSig::CastRealAsDecimal |
            ScalarFuncSig::CastRealAsTime |
            ScalarFuncSig::CastRealAsDuration |
            ScalarFuncSig::CastRealAsJson |
            ScalarFuncSig::CastDecimalAsInt |
            ScalarFuncSig::CastDecimalAsReal |
            ScalarFuncSig::CastDecimalAsString |
            ScalarFuncSig::CastDecimalAsDecimal |
            ScalarFuncSig::CastDecimalAsTime |
            ScalarFuncSig::CastDecimalAsDuration |
            ScalarFuncSig::CastDecimalAsJson |
            ScalarFuncSig::CastStringAsInt |
            ScalarFuncSig::CastStringAsReal |
            ScalarFuncSig::CastStringAsString |
            ScalarFuncSig::CastStringAsDecimal |
            ScalarFuncSig::CastStringAsTime |
            ScalarFuncSig::CastStringAsDuration |
            ScalarFuncSig::CastStringAsJson |
            ScalarFuncSig::CastTimeAsInt |
            ScalarFuncSig::CastTimeAsReal |
            ScalarFuncSig::CastTimeAsString |
            ScalarFuncSig::CastTimeAsDecimal |
            ScalarFuncSig::CastTimeAsTime |
            ScalarFuncSig::CastTimeAsDuration |
            ScalarFuncSig::CastTimeAsJson |
            ScalarFuncSig::CastDurationAsInt |
            ScalarFuncSig::CastDurationAsReal |
            ScalarFuncSig::CastDurationAsString |
            ScalarFuncSig::CastDurationAsDecimal |
            ScalarFuncSig::CastDurationAsTime |
            ScalarFuncSig::CastDurationAsDuration |
            ScalarFuncSig::CastDurationAsJson |
            ScalarFuncSig::CastJsonAsInt |
            ScalarFuncSig::CastJsonAsReal |
            ScalarFuncSig::CastJsonAsString |
            ScalarFuncSig::CastJsonAsDecimal |
            ScalarFuncSig::CastJsonAsTime |
            ScalarFuncSig::CastJsonAsDuration |
            ScalarFuncSig::CastJsonAsJson |
            ScalarFuncSig::UnaryNot |
            ScalarFuncSig::UnaryMinusInt |
            ScalarFuncSig::UnaryMinusReal |
            ScalarFuncSig::UnaryMinusDecimal |
            ScalarFuncSig::IntIsTrue |
            ScalarFuncSig::IntIsFalse |
            ScalarFuncSig::IntIsNull |
            ScalarFuncSig::RealIsTrue |
            ScalarFuncSig::RealIsFalse |
            ScalarFuncSig::RealIsNull |
            ScalarFuncSig::DecimalIsTrue |
            ScalarFuncSig::DecimalIsFalse |
            ScalarFuncSig::DecimalIsNull |
            ScalarFuncSig::StringIsNull |
            ScalarFuncSig::TimeIsNull |
            ScalarFuncSig::DurationIsNull |
            ScalarFuncSig::JsonIsNull |
            ScalarFuncSig::AbsInt |
            ScalarFuncSig::AbsUInt |
            ScalarFuncSig::AbsReal |
            ScalarFuncSig::AbsDecimal |
            ScalarFuncSig::CeilReal |
            ScalarFuncSig::CeilIntToInt |
            ScalarFuncSig::CeilIntToDec |
            ScalarFuncSig::CeilDecToDec |
            ScalarFuncSig::CeilDecToInt |
            ScalarFuncSig::FloorReal |
            ScalarFuncSig::FloorIntToInt |
            ScalarFuncSig::FloorIntToDec |
            ScalarFuncSig::FloorDecToDec |
            ScalarFuncSig::FloorDecToInt |
            ScalarFuncSig::JsonTypeSig |
            ScalarFuncSig::JsonUnquoteSig |
            ScalarFuncSig::BitNegSig => (1, 1),

            ScalarFuncSig::IfInt |
            ScalarFuncSig::IfReal |
            ScalarFuncSig::IfString |
            ScalarFuncSig::IfDecimal |
            ScalarFuncSig::IfTime |
            ScalarFuncSig::IfDuration |
            ScalarFuncSig::IfJson |
            ScalarFuncSig::LikeSig => (3, 3),

            ScalarFuncSig::JsonArraySig | ScalarFuncSig::JsonObjectSig => (0, usize::MAX),

            ScalarFuncSig::CoalesceDecimal |
            ScalarFuncSig::CoalesceDuration |
            ScalarFuncSig::CoalesceInt |
            ScalarFuncSig::CoalesceJson |
            ScalarFuncSig::CoalesceReal |
            ScalarFuncSig::CoalesceString |
            ScalarFuncSig::CoalesceTime |
            ScalarFuncSig::CaseWhenDecimal |
            ScalarFuncSig::CaseWhenDuration |
            ScalarFuncSig::CaseWhenInt |
            ScalarFuncSig::CaseWhenJson |
            ScalarFuncSig::CaseWhenReal |
            ScalarFuncSig::CaseWhenString |
            ScalarFuncSig::CaseWhenTime => (1, usize::MAX),

            ScalarFuncSig::JsonExtractSig |
            ScalarFuncSig::JsonRemoveSig |
            ScalarFuncSig::JsonMergeSig => (2, usize::MAX),

            ScalarFuncSig::JsonSetSig |
            ScalarFuncSig::JsonInsertSig |
            ScalarFuncSig::JsonReplaceSig => (3, usize::MAX),
        };
        if args < min_args || args > max_args {
            return Err(box_err!("unexpected arguments"));
        }
        let other_checks = match sig {
            ScalarFuncSig::JsonObjectSig => args & 1 == 0,
            ScalarFuncSig::JsonSetSig |
            ScalarFuncSig::JsonInsertSig |
            ScalarFuncSig::JsonReplaceSig => args & 1 == 1,
            _ => true,
        };
        if !other_checks {
            return Err(box_err!("unexpected arguments"));
        }
        Ok(())
    }
}

macro_rules! dispatch_call {
    (
        INT_CALLS {$($i_sig:ident => $i_func:ident $($i_arg:expr)*,)*}
        REAL_CALLS {$($r_sig:ident => $r_func:ident $($r_arg:expr)*,)*}
        DEC_CALLS {$($d_sig:ident => $d_func:ident $($d_arg:expr)*,)*}
        BYTES_CALLS {$($b_sig:ident => $b_func:ident $($b_arg:expr)*,)*}
        TIME_CALLS {$($t_sig:ident => $t_func:ident $($t_arg:expr)*,)*}
        DUR_CALLS {$($u_sig:ident => $u_func:ident $($u_arg:expr)*,)*}
        JSON_CALLS {$($j_sig:ident => $j_func:ident $($j_arg:expr)*,)*}
    ) => {
        impl FnCall {
            pub fn eval_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
                match self.sig {
                    $(ScalarFuncSig::$i_sig => self.$i_func(ctx, row, $($i_arg),*)),*,
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
                match self.sig {
                    $(ScalarFuncSig::$r_sig => self.$r_func(ctx, row, $($r_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_decimal<'a, 'b: 'a>(
                &'b self, ctx: &StatementContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Decimal>>> {
                match self.sig {
                    $(ScalarFuncSig::$d_sig => self.$d_func(ctx, row, $($d_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_bytes<'a, 'b: 'a>(
                &'b self,
                ctx: &StatementContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, [u8]>>> {
                match self.sig {
                    $(ScalarFuncSig::$b_sig => self.$b_func(ctx, row, $($b_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_time<'a, 'b: 'a>(
                &'b self,
                ctx: &StatementContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Time>>> {
                match self.sig {
                    $(ScalarFuncSig::$t_sig => self.$t_func(ctx, row, $($t_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_duration<'a, 'b: 'a>(
                &'b self,
                ctx: &StatementContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Duration>>> {
                match self.sig {
                    $(ScalarFuncSig::$u_sig => self.$u_func(ctx, row, $($u_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_json<'a, 'b: 'a>(
                &'b self,
                ctx: &StatementContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Json>>> {
                match self.sig {
                    $(ScalarFuncSig::$j_sig => self.$j_func(ctx, row, $($j_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Datum> {
                match self.sig {
                    $(ScalarFuncSig::$i_sig => {
                        match self.$i_func(ctx, row, $($i_arg)*) {
                            Ok(Some(i)) => {
                                if mysql::has_unsigned_flag(self.tp.get_flag() as u64) {
                                    Ok(Datum::U64(i as u64))
                                } else {
                                    Ok(Datum::I64(i))
                                }
                            }
                            Ok(None) => Ok(Datum::Null),
                            Err(e) => Err(e),
                        }
                    },)*
                    $(ScalarFuncSig::$r_sig => {
                        self.$r_func(ctx, row, $($r_arg)*).map(Datum::from)
                    })*
                    $(ScalarFuncSig::$d_sig => {
                        self.$d_func(ctx, row, $($d_arg)*).map(Datum::from)
                    })*
                    $(ScalarFuncSig::$b_sig => {
                        self.$b_func(ctx, row, $($b_arg)*).map(Datum::from)
                    })*
                    $(ScalarFuncSig::$t_sig => {
                        self.$t_func(ctx, row, $($t_arg)*).map(Datum::from)
                    })*
                    $(ScalarFuncSig::$u_sig => {
                        self.$u_func(ctx, row, $($u_arg)*).map(Datum::from)
                    })*
                    $(ScalarFuncSig::$j_sig => {
                        self.$j_func(ctx, row, $($j_arg)*).map(Datum::from)
                    })*
                }
            }
        }
    };
}

dispatch_call! {
    INT_CALLS {
        LTInt => compare_int CmpOp::LT,
        LEInt => compare_int CmpOp::LE,
        GTInt => compare_int CmpOp::GT,
        GEInt => compare_int CmpOp::GE,
        EQInt => compare_int CmpOp::EQ,
        NEInt => compare_int CmpOp::NE,
        NullEQInt => compare_int CmpOp::NullEQ,

        LTReal => compare_real CmpOp::LT,
        LEReal => compare_real CmpOp::LE,
        GTReal => compare_real CmpOp::GT,
        GEReal => compare_real CmpOp::GE,
        EQReal => compare_real CmpOp::EQ,
        NEReal => compare_real CmpOp::NE,
        NullEQReal => compare_real CmpOp::NullEQ,

        LTDecimal => compare_decimal CmpOp::LT,
        LEDecimal => compare_decimal CmpOp::LE,
        GTDecimal => compare_decimal CmpOp::GT,
        GEDecimal => compare_decimal CmpOp::GE,
        EQDecimal => compare_decimal CmpOp::EQ,
        NEDecimal => compare_decimal CmpOp::NE,
        NullEQDecimal => compare_decimal CmpOp::NullEQ,

        LTString => compare_string CmpOp::LT,
        LEString => compare_string CmpOp::LE,
        GTString => compare_string CmpOp::GT,
        GEString => compare_string CmpOp::GE,
        EQString => compare_string CmpOp::EQ,
        NEString => compare_string CmpOp::NE,
        NullEQString => compare_string CmpOp::NullEQ,

        LTTime => compare_time CmpOp::LT,
        LETime => compare_time CmpOp::LE,
        GTTime => compare_time CmpOp::GT,
        GETime => compare_time CmpOp::GE,
        EQTime => compare_time CmpOp::EQ,
        NETime => compare_time CmpOp::NE,
        NullEQTime => compare_time CmpOp::NullEQ,

        LTDuration => compare_duration CmpOp::LT,
        LEDuration => compare_duration CmpOp::LE,
        GTDuration => compare_duration CmpOp::GT,
        GEDuration => compare_duration CmpOp::GE,
        EQDuration => compare_duration CmpOp::EQ,
        NEDuration => compare_duration CmpOp::NE,
        NullEQDuration => compare_duration CmpOp::NullEQ,

        LTJson => compare_json CmpOp::LT,
        LEJson => compare_json CmpOp::LE,
        GTJson => compare_json CmpOp::GT,
        GEJson => compare_json CmpOp::GE,
        EQJson => compare_json CmpOp::EQ,
        NEJson => compare_json CmpOp::NE,
        NullEQJson => compare_json CmpOp::NullEQ,

        CastIntAsInt => cast_int_as_int,
        CastRealAsInt => cast_real_as_int,
        CastDecimalAsInt => cast_decimal_as_int,
        CastStringAsInt => cast_str_as_int,
        CastTimeAsInt => cast_time_as_int,
        CastDurationAsInt => cast_duration_as_int,
        CastJsonAsInt => cast_json_as_int,

        PlusInt => plus_int,
        MinusInt => minus_int,
        MultiplyInt => multiply_int,

        LogicalAnd => logical_and,
        LogicalOr => logical_or,
        LogicalXor => logical_xor,

        UnaryNot => unary_not,
        UnaryMinusInt => unary_minus_int,
        IntIsNull => int_is_null,
        IntIsFalse => int_is_false,
        IntIsTrue => int_is_true,
        RealIsTrue => real_is_true,
        RealIsFalse => real_is_false,
        RealIsNull => real_is_null,
        DecimalIsNull => decimal_is_null,
        DecimalIsTrue => decimal_is_true,
        DecimalIsFalse => decimal_is_false,
        StringIsNull => string_is_null,
        TimeIsNull => time_is_null,
        DurationIsNull => duration_is_null,
        JsonIsNull => json_is_null,

        AbsInt => abs_int,
        AbsUInt => abs_uint,
        CeilIntToInt => ceil_int_to_int,
        CeilDecToInt => ceil_dec_to_int,
        FloorIntToInt => floor_int_to_int,
        FloorDecToInt => floor_dec_to_int,

        IfNullInt => if_null_int,
        IfInt => if_int,

        CoalesceInt => coalesce_int,
        CaseWhenInt => case_when_int,

        LikeSig => like,

        BitAndSig => bit_and,
        BitNegSig => bit_neg,
        BitOrSig => bit_or,
        BitXorSig => bit_xor,
    }
    REAL_CALLS {
        CastIntAsReal => cast_int_as_real,
        CastRealAsReal => cast_real_as_real,
        CastDecimalAsReal => cast_decimal_as_real,
        CastStringAsReal => cast_str_as_real,
        CastTimeAsReal => cast_time_as_real,
        CastDurationAsReal => cast_duration_as_real,
        CastJsonAsReal => cast_json_as_real,
        UnaryMinusReal => unary_minus_real,

        PlusReal => plus_real,
        MinusReal => minus_real,
        MultiplyReal => multiply_real,

        AbsReal => abs_real,
        CeilReal => ceil_real,
        FloorReal => floor_real,

        IfNullReal => if_null_real,
        IfReal => if_real,

        CoalesceReal => coalesce_real,
        CaseWhenReal => case_when_real,
        DivideReal => divide_real,
    }
    DEC_CALLS {
        CastIntAsDecimal => cast_int_as_decimal,
        CastRealAsDecimal => cast_real_as_decimal,
        CastDecimalAsDecimal => cast_decimal_as_decimal,
        CastStringAsDecimal => cast_str_as_decimal,
        CastTimeAsDecimal => cast_time_as_decimal,
        CastDurationAsDecimal => cast_duration_as_decimal,
        CastJsonAsDecimal => cast_json_as_decimal,
        UnaryMinusDecimal => unary_minus_decimal,

        PlusDecimal => plus_decimal,
        MinusDecimal => minus_decimal,
        MultiplyDecimal => multiply_decimal,

        AbsDecimal => abs_decimal,
        CeilDecToDec => ceil_dec_to_dec,
        CeilIntToDec => cast_int_as_decimal,
        FloorDecToDec => floor_dec_to_dec,
        FloorIntToDec => cast_int_as_decimal,

        IfNullDecimal => if_null_decimal,
        IfDecimal => if_decimal,

        CoalesceDecimal => coalesce_decimal,
        CaseWhenDecimal => case_when_decimal,
        DivideDecimal => divide_decimal,
    }
    BYTES_CALLS {
        CastIntAsString => cast_int_as_str,
        CastRealAsString => cast_real_as_str,
        CastDecimalAsString => cast_decimal_as_str,
        CastStringAsString => cast_str_as_str,
        CastTimeAsString => cast_time_as_str,
        CastDurationAsString => cast_duration_as_str,
        CastJsonAsString => cast_json_as_str,

        IfNullString => if_null_string,
        IfString => if_string,

        CoalesceString => coalesce_string,
        CaseWhenString => case_when_string,
        JsonTypeSig => json_type,
        JsonUnquoteSig => json_unquote,
    }
    TIME_CALLS {
        CastIntAsTime => cast_int_as_time,
        CastRealAsTime => cast_real_as_time,
        CastDecimalAsTime => cast_decimal_as_time,
        CastStringAsTime => cast_str_as_time,
        CastTimeAsTime => cast_time_as_time,
        CastDurationAsTime => cast_duration_as_time,
        CastJsonAsTime => cast_json_as_time,

        IfNullTime => if_null_time,
        IfTime => if_time,

        CoalesceTime => coalesce_time,
        CaseWhenTime => case_when_time,
    }
    DUR_CALLS {
        CastIntAsDuration => cast_int_as_duration,
        CastRealAsDuration => cast_real_as_duration,
        CastDecimalAsDuration => cast_decimal_as_duration,
        CastStringAsDuration => cast_str_as_duration,
        CastTimeAsDuration => cast_time_as_duration,
        CastDurationAsDuration => cast_duration_as_duration,
        CastJsonAsDuration => cast_json_as_duration,

        IfNullDuration => if_null_duration,
        IfDuration => if_duration,

        CoalesceDuration => coalesce_duration,
        CaseWhenDuration => case_when_duration,
    }
    JSON_CALLS {
        CastIntAsJson => cast_int_as_json,
        CastRealAsJson => cast_real_as_json,
        CastDecimalAsJson => cast_decimal_as_json,
        CastStringAsJson => cast_str_as_json,
        CastTimeAsJson => cast_time_as_json,
        CastDurationAsJson => cast_duration_as_json,
        CastJsonAsJson => cast_json_as_json,

        CoalesceJson => coalesce_json,
        CaseWhenJson => case_when_json,

        IfJson => if_json,
        IfNullJson => if_null_json,

        JsonExtractSig => json_extract,
        JsonSetSig => json_set,
        JsonInsertSig => json_insert,
        JsonReplaceSig => json_replace,
        JsonRemoveSig => json_remove,
        JsonMergeSig => json_merge,
        JsonArraySig => json_array,
        JsonObjectSig => json_object,
    }
}
