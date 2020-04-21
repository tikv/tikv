// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::usize;

use tidb_query_datatype::prelude::*;
use tidb_query_datatype::FieldTypeFlag;
use tipb::ScalarFuncSig;

use super::builtin_compare::CmpOp;
use super::{Error, EvalContext, Result};
use crate::ScalarFunc;
use tidb_query_datatype::codec::mysql::{Decimal, Duration, Json, Time};
use tidb_query_datatype::codec::Datum;

impl ScalarFunc {
    pub fn check_args(sig: ScalarFuncSig, args: usize) -> Result<()> {
        let (min_args, max_args) = match sig {
            ScalarFuncSig::LtInt
            | ScalarFuncSig::LeInt
            | ScalarFuncSig::GtInt
            | ScalarFuncSig::GeInt
            | ScalarFuncSig::EqInt
            | ScalarFuncSig::NeInt
            | ScalarFuncSig::NullEqInt
            | ScalarFuncSig::LtReal
            | ScalarFuncSig::LeReal
            | ScalarFuncSig::GtReal
            | ScalarFuncSig::GeReal
            | ScalarFuncSig::EqReal
            | ScalarFuncSig::NeReal
            | ScalarFuncSig::NullEqReal
            | ScalarFuncSig::LtDecimal
            | ScalarFuncSig::LeDecimal
            | ScalarFuncSig::GtDecimal
            | ScalarFuncSig::GeDecimal
            | ScalarFuncSig::EqDecimal
            | ScalarFuncSig::NeDecimal
            | ScalarFuncSig::NullEqDecimal
            | ScalarFuncSig::LtString
            | ScalarFuncSig::LeString
            | ScalarFuncSig::GtString
            | ScalarFuncSig::GeString
            | ScalarFuncSig::EqString
            | ScalarFuncSig::NeString
            | ScalarFuncSig::NullEqString
            | ScalarFuncSig::LtTime
            | ScalarFuncSig::LeTime
            | ScalarFuncSig::GtTime
            | ScalarFuncSig::GeTime
            | ScalarFuncSig::EqTime
            | ScalarFuncSig::NeTime
            | ScalarFuncSig::NullEqTime
            | ScalarFuncSig::LtDuration
            | ScalarFuncSig::LeDuration
            | ScalarFuncSig::GtDuration
            | ScalarFuncSig::GeDuration
            | ScalarFuncSig::EqDuration
            | ScalarFuncSig::NeDuration
            | ScalarFuncSig::NullEqDuration
            | ScalarFuncSig::LtJson
            | ScalarFuncSig::LeJson
            | ScalarFuncSig::GtJson
            | ScalarFuncSig::GeJson
            | ScalarFuncSig::EqJson
            | ScalarFuncSig::NeJson
            | ScalarFuncSig::NullEqJson
            | ScalarFuncSig::PlusReal
            | ScalarFuncSig::PlusDecimal
            | ScalarFuncSig::PlusInt
            | ScalarFuncSig::MinusReal
            | ScalarFuncSig::MinusDecimal
            | ScalarFuncSig::MinusInt
            | ScalarFuncSig::MultiplyReal
            | ScalarFuncSig::MultiplyDecimal
            | ScalarFuncSig::MultiplyInt
            | ScalarFuncSig::MultiplyIntUnsigned
            | ScalarFuncSig::PlusIntUnsignedUnsigned
            | ScalarFuncSig::PlusIntUnsignedSigned
            | ScalarFuncSig::PlusIntSignedUnsigned
            | ScalarFuncSig::PlusIntSignedSigned
            | ScalarFuncSig::IfNullInt
            | ScalarFuncSig::IfNullReal
            | ScalarFuncSig::IfNullString
            | ScalarFuncSig::IfNullDecimal
            | ScalarFuncSig::IfNullTime
            | ScalarFuncSig::IfNullDuration
            | ScalarFuncSig::IfNullJson
            | ScalarFuncSig::LeftUtf8
            | ScalarFuncSig::Left
            | ScalarFuncSig::RightUtf8
            | ScalarFuncSig::Right
            | ScalarFuncSig::LogicalAnd
            | ScalarFuncSig::LogicalOr
            | ScalarFuncSig::LogicalXor
            | ScalarFuncSig::DivideDecimal
            | ScalarFuncSig::DivideReal
            | ScalarFuncSig::IntDivideInt
            | ScalarFuncSig::IntDivideDecimal
            | ScalarFuncSig::ModReal
            | ScalarFuncSig::ModDecimal
            | ScalarFuncSig::ModInt
            | ScalarFuncSig::BitAndSig
            | ScalarFuncSig::BitOrSig
            | ScalarFuncSig::BitXorSig
            | ScalarFuncSig::LeftShift
            | ScalarFuncSig::RightShift
            | ScalarFuncSig::Pow
            | ScalarFuncSig::Atan2Args
            | ScalarFuncSig::Log2Args
            | ScalarFuncSig::RegexpUtf8Sig
            | ScalarFuncSig::RegexpSig
            | ScalarFuncSig::RoundWithFracDec
            | ScalarFuncSig::RoundWithFracInt
            | ScalarFuncSig::RoundWithFracReal
            | ScalarFuncSig::DateFormatSig
            | ScalarFuncSig::Sha2
            | ScalarFuncSig::TruncateInt
            | ScalarFuncSig::TruncateUint
            | ScalarFuncSig::WeekWithMode
            | ScalarFuncSig::YearWeekWithMode
            | ScalarFuncSig::TruncateReal
            | ScalarFuncSig::TruncateDecimal
            | ScalarFuncSig::Trim2Args
            | ScalarFuncSig::Substring2ArgsUtf8
            | ScalarFuncSig::Substring2Args
            | ScalarFuncSig::Repeat
            | ScalarFuncSig::DateDiff
            | ScalarFuncSig::AddDatetimeAndDuration
            | ScalarFuncSig::AddDatetimeAndString
            | ScalarFuncSig::AddDurationAndDuration
            | ScalarFuncSig::AddDurationAndString
            | ScalarFuncSig::SubDatetimeAndDuration
            | ScalarFuncSig::SubDatetimeAndString
            | ScalarFuncSig::SubDurationAndDuration
            | ScalarFuncSig::SubDurationAndString
            | ScalarFuncSig::PeriodAdd
            | ScalarFuncSig::PeriodDiff
            | ScalarFuncSig::Strcmp
            | ScalarFuncSig::Instr
            | ScalarFuncSig::Locate2ArgsUtf8
            | ScalarFuncSig::InstrUtf8
            | ScalarFuncSig::Locate2Args
            | ScalarFuncSig::FindInSet => (2, 2),

            ScalarFuncSig::CastIntAsInt
            | ScalarFuncSig::CastIntAsReal
            | ScalarFuncSig::CastIntAsString
            | ScalarFuncSig::CastIntAsDecimal
            | ScalarFuncSig::CastIntAsTime
            | ScalarFuncSig::CastIntAsDuration
            | ScalarFuncSig::CastIntAsJson
            | ScalarFuncSig::CastRealAsInt
            | ScalarFuncSig::CastRealAsReal
            | ScalarFuncSig::CastRealAsString
            | ScalarFuncSig::CastRealAsDecimal
            | ScalarFuncSig::CastRealAsTime
            | ScalarFuncSig::CastRealAsDuration
            | ScalarFuncSig::CastRealAsJson
            | ScalarFuncSig::CastDecimalAsInt
            | ScalarFuncSig::CastDecimalAsReal
            | ScalarFuncSig::CastDecimalAsString
            | ScalarFuncSig::CastDecimalAsDecimal
            | ScalarFuncSig::CastDecimalAsTime
            | ScalarFuncSig::CastDecimalAsDuration
            | ScalarFuncSig::CastDecimalAsJson
            | ScalarFuncSig::CastStringAsInt
            | ScalarFuncSig::CastStringAsReal
            | ScalarFuncSig::CastStringAsString
            | ScalarFuncSig::CastStringAsDecimal
            | ScalarFuncSig::CastStringAsTime
            | ScalarFuncSig::CastStringAsDuration
            | ScalarFuncSig::CastStringAsJson
            | ScalarFuncSig::CastTimeAsInt
            | ScalarFuncSig::CastTimeAsReal
            | ScalarFuncSig::CastTimeAsString
            | ScalarFuncSig::CastTimeAsDecimal
            | ScalarFuncSig::CastTimeAsTime
            | ScalarFuncSig::CastTimeAsDuration
            | ScalarFuncSig::CastTimeAsJson
            | ScalarFuncSig::CastDurationAsInt
            | ScalarFuncSig::CastDurationAsReal
            | ScalarFuncSig::CastDurationAsString
            | ScalarFuncSig::CastDurationAsDecimal
            | ScalarFuncSig::CastDurationAsTime
            | ScalarFuncSig::CastDurationAsDuration
            | ScalarFuncSig::CastDurationAsJson
            | ScalarFuncSig::CastJsonAsInt
            | ScalarFuncSig::CastJsonAsReal
            | ScalarFuncSig::CastJsonAsString
            | ScalarFuncSig::CastJsonAsDecimal
            | ScalarFuncSig::CastJsonAsTime
            | ScalarFuncSig::CastJsonAsDuration
            | ScalarFuncSig::CastJsonAsJson
            | ScalarFuncSig::Date
            | ScalarFuncSig::LastDay
            | ScalarFuncSig::Hour
            | ScalarFuncSig::Minute
            | ScalarFuncSig::Second
            | ScalarFuncSig::MicroSecond
            | ScalarFuncSig::Month
            | ScalarFuncSig::MonthName
            | ScalarFuncSig::DayName
            | ScalarFuncSig::DayOfMonth
            | ScalarFuncSig::DayOfWeek
            | ScalarFuncSig::DayOfYear
            | ScalarFuncSig::WeekDay
            | ScalarFuncSig::WeekOfYear
            | ScalarFuncSig::Year
            | ScalarFuncSig::UnaryNotInt
            | ScalarFuncSig::UnaryNotReal
            | ScalarFuncSig::UnaryNotDecimal
            | ScalarFuncSig::UnaryMinusInt
            | ScalarFuncSig::UnaryMinusReal
            | ScalarFuncSig::UnaryMinusDecimal
            | ScalarFuncSig::IntIsTrue
            | ScalarFuncSig::IntIsTrueWithNull
            | ScalarFuncSig::IntIsFalse
            | ScalarFuncSig::IntIsFalseWithNull
            | ScalarFuncSig::RealIsTrue
            | ScalarFuncSig::RealIsTrueWithNull
            | ScalarFuncSig::RealIsFalse
            | ScalarFuncSig::RealIsFalseWithNull
            | ScalarFuncSig::DecimalIsTrue
            | ScalarFuncSig::DecimalIsTrueWithNull
            | ScalarFuncSig::DecimalIsFalse
            | ScalarFuncSig::DecimalIsFalseWithNull
            | ScalarFuncSig::IntIsNull
            | ScalarFuncSig::RealIsNull
            | ScalarFuncSig::DecimalIsNull
            | ScalarFuncSig::StringIsNull
            | ScalarFuncSig::TimeIsNull
            | ScalarFuncSig::DurationIsNull
            | ScalarFuncSig::JsonIsNull
            | ScalarFuncSig::AbsInt
            | ScalarFuncSig::AbsUInt
            | ScalarFuncSig::AbsReal
            | ScalarFuncSig::AbsDecimal
            | ScalarFuncSig::CeilReal
            | ScalarFuncSig::CeilIntToInt
            | ScalarFuncSig::CeilIntToDec
            | ScalarFuncSig::CeilDecToDec
            | ScalarFuncSig::CeilDecToInt
            | ScalarFuncSig::FloorReal
            | ScalarFuncSig::FloorIntToInt
            | ScalarFuncSig::FloorIntToDec
            | ScalarFuncSig::FloorDecToDec
            | ScalarFuncSig::FloorDecToInt
            | ScalarFuncSig::Rand
            | ScalarFuncSig::RandWithSeedFirstGen
            | ScalarFuncSig::Crc32
            | ScalarFuncSig::Sign
            | ScalarFuncSig::Sqrt
            | ScalarFuncSig::Atan1Arg
            | ScalarFuncSig::Acos
            | ScalarFuncSig::Asin
            | ScalarFuncSig::Cos
            | ScalarFuncSig::Tan
            | ScalarFuncSig::Sin
            | ScalarFuncSig::JsonTypeSig
            | ScalarFuncSig::JsonUnquoteSig
            | ScalarFuncSig::Log10
            | ScalarFuncSig::Log1Arg
            | ScalarFuncSig::Log2
            | ScalarFuncSig::Ascii
            | ScalarFuncSig::CharLengthUtf8
            | ScalarFuncSig::CharLength
            | ScalarFuncSig::ReverseUtf8
            | ScalarFuncSig::Reverse
            | ScalarFuncSig::Quote
            | ScalarFuncSig::UpperUtf8
            | ScalarFuncSig::Upper
            | ScalarFuncSig::Lower
            | ScalarFuncSig::Length
            | ScalarFuncSig::Bin
            | ScalarFuncSig::LTrim
            | ScalarFuncSig::RTrim
            | ScalarFuncSig::BitCount
            | ScalarFuncSig::BitLength
            | ScalarFuncSig::RoundReal
            | ScalarFuncSig::RoundDec
            | ScalarFuncSig::RoundInt
            | ScalarFuncSig::BitNegSig
            | ScalarFuncSig::IsIPv4
            | ScalarFuncSig::IsIPv4Compat
            | ScalarFuncSig::IsIPv4Mapped
            | ScalarFuncSig::IsIPv6
            | ScalarFuncSig::InetAton
            | ScalarFuncSig::InetNtoa
            | ScalarFuncSig::Inet6Aton
            | ScalarFuncSig::Inet6Ntoa
            | ScalarFuncSig::HexIntArg
            | ScalarFuncSig::HexStrArg
            | ScalarFuncSig::UnHex
            | ScalarFuncSig::Cot
            | ScalarFuncSig::Degrees
            | ScalarFuncSig::Sha1
            | ScalarFuncSig::Md5
            | ScalarFuncSig::Radians
            | ScalarFuncSig::Exp
            | ScalarFuncSig::Trim1Arg
            | ScalarFuncSig::FromBase64
            | ScalarFuncSig::ToBase64
            | ScalarFuncSig::WeekWithoutMode
            | ScalarFuncSig::YearWeekWithoutMode
            | ScalarFuncSig::Space
            | ScalarFuncSig::Compress
            | ScalarFuncSig::Uncompress
            | ScalarFuncSig::UncompressedLength
            | ScalarFuncSig::ToDays
            | ScalarFuncSig::ToSeconds
            | ScalarFuncSig::FromDays
            | ScalarFuncSig::Ord
            | ScalarFuncSig::OctInt
            | ScalarFuncSig::JsonDepthSig
            | ScalarFuncSig::RandomBytes
            | ScalarFuncSig::JsonKeysSig => (1, 1),

            ScalarFuncSig::JsonKeys2ArgsSig | ScalarFuncSig::JsonLengthSig => (1, 2),

            ScalarFuncSig::IfInt
            | ScalarFuncSig::IfReal
            | ScalarFuncSig::IfString
            | ScalarFuncSig::IfDecimal
            | ScalarFuncSig::IfTime
            | ScalarFuncSig::IfDuration
            | ScalarFuncSig::IfJson
            | ScalarFuncSig::LikeSig
            | ScalarFuncSig::Conv
            | ScalarFuncSig::Trim3Args
            | ScalarFuncSig::SubstringIndex
            | ScalarFuncSig::Substring3ArgsUtf8
            | ScalarFuncSig::Substring3Args
            | ScalarFuncSig::LpadUtf8
            | ScalarFuncSig::Lpad
            | ScalarFuncSig::RpadUtf8
            | ScalarFuncSig::Rpad
            | ScalarFuncSig::Locate3ArgsUtf8
            | ScalarFuncSig::Locate3Args
            | ScalarFuncSig::Replace => (3, 3),

            ScalarFuncSig::JsonArraySig
            | ScalarFuncSig::IntAnyValue
            | ScalarFuncSig::RealAnyValue
            | ScalarFuncSig::StringAnyValue
            | ScalarFuncSig::TimeAnyValue
            | ScalarFuncSig::DecimalAnyValue
            | ScalarFuncSig::JsonAnyValue
            | ScalarFuncSig::DurationAnyValue
            | ScalarFuncSig::JsonObjectSig => (0, usize::MAX),

            ScalarFuncSig::CoalesceDecimal
            | ScalarFuncSig::CoalesceDuration
            | ScalarFuncSig::CoalesceInt
            | ScalarFuncSig::CoalesceJson
            | ScalarFuncSig::CoalesceReal
            | ScalarFuncSig::CoalesceString
            | ScalarFuncSig::CoalesceTime
            | ScalarFuncSig::CaseWhenDecimal
            | ScalarFuncSig::CaseWhenDuration
            | ScalarFuncSig::CaseWhenInt
            | ScalarFuncSig::CaseWhenJson
            | ScalarFuncSig::CaseWhenReal
            | ScalarFuncSig::CaseWhenString
            | ScalarFuncSig::Concat
            | ScalarFuncSig::ConcatWs
            | ScalarFuncSig::FieldInt
            | ScalarFuncSig::FieldReal
            | ScalarFuncSig::FieldString
            | ScalarFuncSig::CaseWhenTime => (1, usize::MAX),

            ScalarFuncSig::JsonExtractSig
            | ScalarFuncSig::JsonRemoveSig
            | ScalarFuncSig::JsonMergeSig
            | ScalarFuncSig::InInt
            | ScalarFuncSig::InReal
            | ScalarFuncSig::InString
            | ScalarFuncSig::InDecimal
            | ScalarFuncSig::InTime
            | ScalarFuncSig::InDuration
            | ScalarFuncSig::InJson
            | ScalarFuncSig::GreatestInt
            | ScalarFuncSig::GreatestReal
            | ScalarFuncSig::GreatestDecimal
            | ScalarFuncSig::GreatestString
            | ScalarFuncSig::GreatestTime
            | ScalarFuncSig::LeastInt
            | ScalarFuncSig::LeastReal
            | ScalarFuncSig::LeastDecimal
            | ScalarFuncSig::LeastString
            | ScalarFuncSig::LeastTime
            | ScalarFuncSig::IntervalInt
            | ScalarFuncSig::Elt
            | ScalarFuncSig::IntervalReal => (2, usize::MAX),

            ScalarFuncSig::JsonSetSig
            | ScalarFuncSig::JsonInsertSig
            | ScalarFuncSig::JsonReplaceSig => (3, usize::MAX),

            ScalarFuncSig::AddTimeDateTimeNull
            | ScalarFuncSig::AddTimeDurationNull
            | ScalarFuncSig::AddTimeStringNull
            | ScalarFuncSig::SubTimeDateTimeNull
            | ScalarFuncSig::SubTimeDurationNull
            | ScalarFuncSig::Uuid
            | ScalarFuncSig::Pi => (0, 0),

            // PbCode is unspecified
            ScalarFuncSig::Unspecified => {
                return Err(box_err!("TiDB internal error (unspecified PbCode)"));
            }

            // unimplemented signature
            _ => return Err(Error::UnknownSignature(sig)),
        };
        if args < min_args || args > max_args {
            return Err(box_err!(
                "unexpected arguments: sig {:?} with {} args",
                sig,
                args
            ));
        }
        let other_checks = match sig {
            ScalarFuncSig::JsonObjectSig => args & 1 == 0,
            ScalarFuncSig::JsonSetSig
            | ScalarFuncSig::JsonInsertSig
            | ScalarFuncSig::JsonReplaceSig => args & 1 == 1,
            _ => true,
        };
        if !other_checks {
            return Err(box_err!(
                "unexpected arguments: sig {:?} with {} args",
                sig,
                args
            ));
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
        impl ScalarFunc {
            pub fn eval_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
                match self.sig {
                    $(ScalarFuncSig::$i_sig => self.$i_func(ctx, row, $($i_arg),*)),*,
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
                match self.sig {
                    $(ScalarFuncSig::$r_sig => self.$r_func(ctx, row, $($r_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_decimal<'a, 'b: 'a>(
                &'b self, ctx: &mut EvalContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Decimal>>> {
                match self.sig {
                    $(ScalarFuncSig::$d_sig => self.$d_func(ctx, row, $($d_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_bytes<'a, 'b: 'a>(
                &'b self,
                ctx: &mut EvalContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, [u8]>>> {
                match self.sig {
                    $(ScalarFuncSig::$b_sig => self.$b_func(ctx, row, $($b_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_time<'a, 'b: 'a>(
                &'b self,
                ctx: &mut EvalContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Time>>> {
                match self.sig {
                    $(ScalarFuncSig::$t_sig => self.$t_func(ctx, row, $($t_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_duration<'a, 'b: 'a>(
                &'b self,
                ctx: &mut EvalContext,
                row: &'a [Datum]
            ) -> Result<Option<Duration>> {
                match self.sig {
                    $(ScalarFuncSig::$u_sig => self.$u_func(ctx, row, $($u_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval_json<'a, 'b: 'a>(
                &'b self,
                ctx: &mut EvalContext,
                row: &'a [Datum]
            ) -> Result<Option<Cow<'a, Json>>> {
                match self.sig {
                    $(ScalarFuncSig::$j_sig => self.$j_func(ctx, row, $($j_arg),*),)*
                    _ => Err(Error::UnknownSignature(self.sig))
                }
            }

            pub fn eval(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Datum> {
                match self.sig {
                    $(ScalarFuncSig::$i_sig => {
                        match self.$i_func(ctx, row, $($i_arg)*) {
                            Ok(Some(i)) => {
                                if self.field_type.as_accessor().flag().contains(FieldTypeFlag::UNSIGNED) {
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
                    _ => unimplemented!(),
                }
            }
        }
    };
}

dispatch_call! {
    INT_CALLS {
        LtInt => compare_int CmpOp::LT,
        LeInt => compare_int CmpOp::LE,
        GtInt => compare_int CmpOp::GT,
        GeInt => compare_int CmpOp::GE,
        EqInt => compare_int CmpOp::EQ,
        NeInt => compare_int CmpOp::NE,
        NullEqInt => compare_int CmpOp::NullEQ,

        LtReal => compare_real CmpOp::LT,
        LeReal => compare_real CmpOp::LE,
        GtReal => compare_real CmpOp::GT,
        GeReal => compare_real CmpOp::GE,
        EqReal => compare_real CmpOp::EQ,
        NeReal => compare_real CmpOp::NE,
        NullEqReal => compare_real CmpOp::NullEQ,

        LtDecimal => compare_decimal CmpOp::LT,
        LeDecimal => compare_decimal CmpOp::LE,
        GtDecimal => compare_decimal CmpOp::GT,
        GeDecimal => compare_decimal CmpOp::GE,
        EqDecimal => compare_decimal CmpOp::EQ,
        NeDecimal => compare_decimal CmpOp::NE,
        NullEqDecimal => compare_decimal CmpOp::NullEQ,

        LtString => compare_string CmpOp::LT,
        LeString => compare_string CmpOp::LE,
        GtString => compare_string CmpOp::GT,
        GeString => compare_string CmpOp::GE,
        EqString => compare_string CmpOp::EQ,
        NeString => compare_string CmpOp::NE,
        NullEqString => compare_string CmpOp::NullEQ,

        LtTime => compare_time CmpOp::LT,
        LeTime => compare_time CmpOp::LE,
        GtTime => compare_time CmpOp::GT,
        GeTime => compare_time CmpOp::GE,
        EqTime => compare_time CmpOp::EQ,
        NeTime => compare_time CmpOp::NE,
        NullEqTime => compare_time CmpOp::NullEQ,

        LtDuration => compare_duration CmpOp::LT,
        LeDuration => compare_duration CmpOp::LE,
        GtDuration => compare_duration CmpOp::GT,
        GeDuration => compare_duration CmpOp::GE,
        EqDuration => compare_duration CmpOp::EQ,
        NeDuration => compare_duration CmpOp::NE,
        NullEqDuration => compare_duration CmpOp::NullEQ,

        LtJson => compare_json CmpOp::LT,
        LeJson => compare_json CmpOp::LE,
        GtJson => compare_json CmpOp::GT,
        GeJson => compare_json CmpOp::GE,
        EqJson => compare_json CmpOp::EQ,
        NeJson => compare_json CmpOp::NE,
        NullEqJson => compare_json CmpOp::NullEQ,

        CastIntAsInt => cast_int_as_int,
        CastRealAsInt => cast_real_as_int,
        CastDecimalAsInt => cast_decimal_as_int,
        CastStringAsInt => cast_str_as_int,
        CastTimeAsInt => cast_time_as_int,
        CastDurationAsInt => cast_duration_as_int,
        CastJsonAsInt => cast_json_as_int,

        InInt => in_int,
        InReal => in_real,
        InDecimal => in_decimal,
        InString => in_string,
        InTime => in_time,
        InDuration => in_duration,
        InJson => in_json,
        IntervalInt => interval_int,
        IntervalReal => interval_real,
        IntAnyValue => int_any_value,

        PlusInt => plus_int,
        PlusIntUnsignedUnsigned => plus_int,
        PlusIntUnsignedSigned => plus_int,
        PlusIntSignedUnsigned => plus_int,
        PlusIntSignedSigned => plus_int,
        MinusInt => minus_int,
        MultiplyInt => multiply_int,
        MultiplyIntUnsigned => multiply_int_unsigned,
        IntDivideInt => int_divide_int,
        IntDivideDecimal => int_divide_decimal,
        ModInt => mod_int,

        Hour => hour,
        Minute => minute,
        Second => second,
        MicroSecond => micro_second,
        Month => month,
        DayOfMonth => day_of_month,
        DayOfWeek => day_of_week,
        DayOfYear => day_of_year,
        WeekWithMode => week_with_mode,
        WeekWithoutMode => week_without_mode,
        YearWeekWithMode => year_week_with_mode,
        YearWeekWithoutMode => year_week_without_mode,
        WeekDay => week_day,
        WeekOfYear => week_of_year,
        Year => year,
        ToDays => to_days,
        ToSeconds => to_seconds,
        DateDiff => date_diff,
        PeriodAdd => period_add,
        PeriodDiff => period_diff,

        LogicalAnd => logical_and,
        LogicalOr => logical_or,
        LogicalXor => logical_xor,

        UnaryNotInt => unary_not_int,
        UnaryNotReal => unary_not_real,
        UnaryNotDecimal => unary_not_decimal,
        UnaryMinusInt => unary_minus_int,
        IntIsTrue => int_is_true false,
        IntIsTrueWithNull => int_is_true true,
        IntIsFalse => int_is_false false,
        IntIsFalseWithNull => int_is_false true,
        RealIsTrue => real_is_true false,
        RealIsTrueWithNull => real_is_true true,
        RealIsFalse => real_is_false false,
        RealIsFalseWithNull => real_is_false true,
        DecimalIsTrue => decimal_is_true false,
        DecimalIsTrueWithNull => decimal_is_true true,
        DecimalIsFalse => decimal_is_false false,
        DecimalIsFalseWithNull => decimal_is_false true,
        IntIsNull => int_is_null,
        RealIsNull => real_is_null,
        DecimalIsNull => decimal_is_null,
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
        Crc32 => crc32,
        Sign => sign,

        RoundInt => round_int,
        RoundWithFracInt => round_with_frac_int,

        TruncateInt => truncate_int,
        TruncateUint => truncate_uint,


        IfNullInt => if_null_int,
        IfInt => if_int,

        CoalesceInt => coalesce_int,
        CaseWhenInt => case_when_int,
        GreatestInt => greatest_int,
        LeastInt => least_int,

        LikeSig => like,
        RegexpUtf8Sig => regexp_utf8,
        RegexpSig => regexp,

        BitAndSig => bit_and,
        BitNegSig => bit_neg,
        BitOrSig => bit_or,
        BitXorSig => bit_xor,

        Length => length,
        Locate2ArgsUtf8 => locate_2_args_utf8,
        Locate3ArgsUtf8 => locate_3_args_utf8,
        Locate2Args => locate_2_args,
        Locate3Args => locate_3_args,
        BitCount => bit_count,
        FieldInt => field_int,
        FieldReal => field_real,
        FieldString => field_string,
        CharLengthUtf8 => char_length_utf8,
        CharLength => char_length,
        BitLength => bit_length,
        LeftShift => left_shift,
        RightShift => right_shift,
        Ascii => ascii,
        IsIPv4 => is_ipv4,
        IsIPv4Compat => is_ipv4_compat,
        IsIPv4Mapped => is_ipv4_mapped,
        IsIPv6 => is_ipv6,
        InetAton => inet_aton,

        UncompressedLength => uncompressed_length,
        Strcmp => strcmp,
        Instr => instr,
        JsonLengthSig => json_length,
        Ord => ord,
        InstrUtf8 => instr_utf8,
        JsonDepthSig => json_depth,
        FindInSet => find_in_set,
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
        DivideReal => divide_real,
        ModReal => mod_real,

        AbsReal => abs_real,
        CeilReal => ceil_real,
        FloorReal => floor_real,
        RoundReal => round_real,
        RoundWithFracReal => round_with_frac_real,
        Pi => pi,
        Rand => rand,
        RandWithSeedFirstGen => rand_with_seed_first_gen,
        TruncateReal => truncate_real,
        Radians => radians,
        Exp => exp,

        RealAnyValue => real_any_value,

        IfNullReal => if_null_real,
        IfReal => if_real,

        CoalesceReal => coalesce_real,
        CaseWhenReal => case_when_real,
        Log2 => log2,
        Log10 => log10,
        Log1Arg => log_1_arg,
        Log2Args => log_2_args,
        GreatestReal => greatest_real,
        LeastReal => least_real,
        Sqrt => sqrt,
        Atan1Arg => atan_1_arg,
        Atan2Args => atan_2_args,
        Acos => acos,
        Asin => asin,
        Cos => cos,
        Tan => tan,
        Sin => sin,
        Pow => pow,
        Cot => cot,
        Degrees => degrees,
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
        DivideDecimal => divide_decimal,
        ModDecimal => mod_decimal,

        AbsDecimal => abs_decimal,
        CeilDecToDec => ceil_dec_to_dec,
        CeilIntToDec => cast_int_as_decimal,
        FloorDecToDec => floor_dec_to_dec,
        FloorIntToDec => cast_int_as_decimal,
        RoundDec => round_dec,
        RoundWithFracDec => round_with_frac_dec,

        DecimalAnyValue => decimal_any_value,

        TruncateDecimal => truncate_decimal,

        IfNullDecimal => if_null_decimal,
        IfDecimal => if_decimal,

        CoalesceDecimal => coalesce_decimal,
        CaseWhenDecimal => case_when_decimal,
        GreatestDecimal => greatest_decimal,
        LeastDecimal => least_decimal,
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
        GreatestString => greatest_string,
        LeastString => least_string,
        GreatestTime => greatest_time,
        LeastTime => least_time,
        JsonTypeSig => json_type,
        JsonUnquoteSig => json_unquote,

        LeftUtf8 => left_utf8,
        RightUtf8 => right_utf8,
        Left => left,
        Right => right,
        UpperUtf8 => upper_utf8,
        Upper => upper,
        Lower => lower,
        DateFormatSig => date_format,
        MonthName => month_name,
        DayName => day_name,
        Bin => bin,
        Concat => concat,
        Replace => replace,
        ConcatWs => concat_ws,
        LTrim => ltrim,
        RTrim => rtrim,
        ReverseUtf8 => reverse_utf8,
        Repeat => repeat,
        Reverse => reverse,
        HexIntArg => hex_int_arg,
        HexStrArg => hex_str_arg,
        UnHex => un_hex,
        InetNtoa => inet_ntoa,
        Inet6Aton => inet6_aton,
        Inet6Ntoa => inet6_ntoa,
        Md5 => md5,
        Uuid => uuid,
        Sha1 => sha1,
        Sha2 => sha2,
        Elt => elt,
        FromBase64 => from_base64,
        ToBase64 => to_base64,
        Compress => compress,
        Uncompress => uncompress,
        Quote => quote,
        OctInt => oct_int,
        RandomBytes => random_bytes,

        Conv => conv,
        Trim1Arg => trim_1_arg,
        Trim2Args => trim_2_args,
        Trim3Args => trim_3_args,
        SubstringIndex => substring_index,
        Substring2ArgsUtf8 => substring_2_args_utf8,
        Substring3ArgsUtf8 => substring_3_args_utf8,
        Substring2Args => substring_2_args,
        Substring3Args => substring_3_args,
        Space => space,
        LpadUtf8 => lpad_utf8,
        Lpad => lpad,
        RpadUtf8 => rpad_utf8,
        Rpad => rpad,

        StringAnyValue => string_any_value,
        AddTimeStringNull => add_time_string_null,
    }
    TIME_CALLS {
        CastIntAsTime => cast_int_as_time,
        CastRealAsTime => cast_real_as_time,
        CastDecimalAsTime => cast_decimal_as_time,
        CastStringAsTime => cast_str_as_time,
        CastTimeAsTime => cast_time_as_time,
        CastDurationAsTime => cast_duration_as_time,
        CastJsonAsTime => cast_json_as_time,

        TimeAnyValue => time_any_value,

        Date => date,
        LastDay => last_day,
        AddDatetimeAndDuration => add_datetime_and_duration,
        AddDatetimeAndString => add_datetime_and_string,
        AddTimeDateTimeNull => add_time_datetime_null,
        SubDatetimeAndDuration => sub_datetime_and_duration,
        SubDatetimeAndString => sub_datetime_and_string,
        SubTimeDateTimeNull => sub_time_datetime_null,
        FromDays => from_days,

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

        DurationAnyValue => duration_any_value,

        IfNullDuration => if_null_duration,
        IfDuration => if_duration,

        CoalesceDuration => coalesce_duration,
        CaseWhenDuration => case_when_duration,

        AddDurationAndDuration => add_duration_and_duration,
        AddDurationAndString => add_duration_and_string,
        AddTimeDurationNull => add_time_duration_null,

        SubDurationAndDuration => sub_duration_and_duration,
        SubDurationAndString => sub_duration_and_string,
        SubTimeDurationNull => sub_time_duration_null,
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


        JsonKeysSig => json_keys,
        JsonKeys2ArgsSig => json_keys,
        JsonExtractSig => json_extract,
        JsonSetSig => json_set,
        JsonInsertSig => json_insert,
        JsonReplaceSig => json_replace,
        JsonRemoveSig => json_remove,
        JsonMergeSig => json_merge,
        JsonArraySig => json_array,
        JsonObjectSig => json_object,
        JsonAnyValue => json_any_value,
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, ScalarFunc};
    use tipb::ScalarFuncSig;

    #[test]
    fn test_check_args() {
        assert!(ScalarFunc::check_args(ScalarFuncSig::LtInt, 2).is_ok());
        assert!(ScalarFunc::check_args(ScalarFuncSig::LtInt, 1).is_err());
        assert!(ScalarFunc::check_args(ScalarFuncSig::LtInt, 3).is_err());

        let err = format!("{:?}", Error::UnknownSignature(ScalarFuncSig::Version));
        assert_eq!(
            format!(
                "{:?}",
                ScalarFunc::check_args(ScalarFuncSig::Version, 1).unwrap_err()
            ),
            err
        );
    }
}
