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

use super::builtin_compare::CmpOp;
use super::{Error, EvalContext, Result, ScalarFunc};
use coprocessor::codec::mysql::{self, Decimal, Duration, Json, Time};
use coprocessor::codec::Datum;

impl ScalarFunc {
    pub fn check_args(sig: ScalarFuncSig, args: usize) -> Result<()> {
        let (min_args, max_args) = match sig {
            ScalarFuncSig::LTInt
            | ScalarFuncSig::LEInt
            | ScalarFuncSig::GTInt
            | ScalarFuncSig::GEInt
            | ScalarFuncSig::EQInt
            | ScalarFuncSig::NEInt
            | ScalarFuncSig::NullEQInt
            | ScalarFuncSig::LTReal
            | ScalarFuncSig::LEReal
            | ScalarFuncSig::GTReal
            | ScalarFuncSig::GEReal
            | ScalarFuncSig::EQReal
            | ScalarFuncSig::NEReal
            | ScalarFuncSig::NullEQReal
            | ScalarFuncSig::LTDecimal
            | ScalarFuncSig::LEDecimal
            | ScalarFuncSig::GTDecimal
            | ScalarFuncSig::GEDecimal
            | ScalarFuncSig::EQDecimal
            | ScalarFuncSig::NEDecimal
            | ScalarFuncSig::NullEQDecimal
            | ScalarFuncSig::LTString
            | ScalarFuncSig::LEString
            | ScalarFuncSig::GTString
            | ScalarFuncSig::GEString
            | ScalarFuncSig::EQString
            | ScalarFuncSig::NEString
            | ScalarFuncSig::NullEQString
            | ScalarFuncSig::LTTime
            | ScalarFuncSig::LETime
            | ScalarFuncSig::GTTime
            | ScalarFuncSig::GETime
            | ScalarFuncSig::EQTime
            | ScalarFuncSig::NETime
            | ScalarFuncSig::NullEQTime
            | ScalarFuncSig::LTDuration
            | ScalarFuncSig::LEDuration
            | ScalarFuncSig::GTDuration
            | ScalarFuncSig::GEDuration
            | ScalarFuncSig::EQDuration
            | ScalarFuncSig::NEDuration
            | ScalarFuncSig::NullEQDuration
            | ScalarFuncSig::LTJson
            | ScalarFuncSig::LEJson
            | ScalarFuncSig::GTJson
            | ScalarFuncSig::GEJson
            | ScalarFuncSig::EQJson
            | ScalarFuncSig::NEJson
            | ScalarFuncSig::NullEQJson
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
            | ScalarFuncSig::IfNullInt
            | ScalarFuncSig::IfNullReal
            | ScalarFuncSig::IfNullString
            | ScalarFuncSig::IfNullDecimal
            | ScalarFuncSig::IfNullTime
            | ScalarFuncSig::IfNullDuration
            | ScalarFuncSig::IfNullJson
            | ScalarFuncSig::Left
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
            | ScalarFuncSig::RegexpSig
            | ScalarFuncSig::RegexpBinarySig
            | ScalarFuncSig::LeftShift
            | ScalarFuncSig::RightShift
            | ScalarFuncSig::Pow
            | ScalarFuncSig::Atan2Args
            | ScalarFuncSig::DateFormatSig => (2, 2),

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
            | ScalarFuncSig::UnaryNot
            | ScalarFuncSig::UnaryMinusInt
            | ScalarFuncSig::UnaryMinusReal
            | ScalarFuncSig::UnaryMinusDecimal
            | ScalarFuncSig::IntIsTrue
            | ScalarFuncSig::IntIsFalse
            | ScalarFuncSig::IntIsNull
            | ScalarFuncSig::RealIsTrue
            | ScalarFuncSig::RealIsFalse
            | ScalarFuncSig::RealIsNull
            | ScalarFuncSig::DecimalIsTrue
            | ScalarFuncSig::DecimalIsFalse
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
            | ScalarFuncSig::CRC32
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
            | ScalarFuncSig::ASCII
            | ScalarFuncSig::CharLength
            | ScalarFuncSig::Reverse
            | ScalarFuncSig::ReverseBinary
            | ScalarFuncSig::Upper
            | ScalarFuncSig::Lower
            | ScalarFuncSig::Length
            | ScalarFuncSig::Bin
            | ScalarFuncSig::BitLength
            | ScalarFuncSig::BitNegSig
            | ScalarFuncSig::IsIPv4
            | ScalarFuncSig::IsIPv6
            | ScalarFuncSig::Inet6Aton
            | ScalarFuncSig::Inet6Ntoa
            | ScalarFuncSig::UnHex
            | ScalarFuncSig::MD5 => (1, 1),

            ScalarFuncSig::IfInt
            | ScalarFuncSig::IfReal
            | ScalarFuncSig::IfString
            | ScalarFuncSig::IfDecimal
            | ScalarFuncSig::IfTime
            | ScalarFuncSig::IfDuration
            | ScalarFuncSig::IfJson
            | ScalarFuncSig::LikeSig => (3, 3),

            ScalarFuncSig::JsonArraySig | ScalarFuncSig::JsonObjectSig => (0, usize::MAX),

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
            | ScalarFuncSig::IntervalInt
            | ScalarFuncSig::Elt
            | ScalarFuncSig::IntervalReal => (2, usize::MAX),

            ScalarFuncSig::JsonSetSig
            | ScalarFuncSig::JsonInsertSig
            | ScalarFuncSig::JsonReplaceSig => (3, usize::MAX),

            ScalarFuncSig::PI => (0, 0),

            // unimplement signature
            ScalarFuncSig::AddDateAndDuration
            | ScalarFuncSig::AddDateAndString
            | ScalarFuncSig::AddDateDatetimeInt
            | ScalarFuncSig::AddDateDatetimeString
            | ScalarFuncSig::AddDateIntInt
            | ScalarFuncSig::AddDateIntString
            | ScalarFuncSig::AddDateStringDecimal
            | ScalarFuncSig::AddDateStringInt
            | ScalarFuncSig::AddDateStringString
            | ScalarFuncSig::AddDatetimeAndDuration
            | ScalarFuncSig::AddDatetimeAndString
            | ScalarFuncSig::AddDurationAndDuration
            | ScalarFuncSig::AddDurationAndString
            | ScalarFuncSig::AddStringAndDuration
            | ScalarFuncSig::AddStringAndString
            | ScalarFuncSig::AddTimeDateTimeNull
            | ScalarFuncSig::AddTimeDurationNull
            | ScalarFuncSig::AddTimeStringNull
            | ScalarFuncSig::AesDecrypt
            | ScalarFuncSig::AesEncrypt
            | ScalarFuncSig::BitCount
            | ScalarFuncSig::Char
            | ScalarFuncSig::Compress
            | ScalarFuncSig::Concat
            | ScalarFuncSig::ConcatWS
            | ScalarFuncSig::ConnectionID
            | ScalarFuncSig::Conv
            | ScalarFuncSig::Convert
            | ScalarFuncSig::ConvertTz
            | ScalarFuncSig::Cot
            | ScalarFuncSig::CurrentDate
            | ScalarFuncSig::CurrentTime0Arg
            | ScalarFuncSig::CurrentTime1Arg
            | ScalarFuncSig::CurrentUser
            | ScalarFuncSig::Database
            | ScalarFuncSig::DateDiff
            | ScalarFuncSig::DateLiteral
            | ScalarFuncSig::DayName
            | ScalarFuncSig::DayOfMonth
            | ScalarFuncSig::DayOfWeek
            | ScalarFuncSig::DayOfYear
            | ScalarFuncSig::DecimalAnyValue
            | ScalarFuncSig::Degrees
            | ScalarFuncSig::DurationAnyValue
            | ScalarFuncSig::DurationDurationTimeDiff
            | ScalarFuncSig::DurationStringTimeDiff
            | ScalarFuncSig::Exp
            | ScalarFuncSig::ExportSet3Arg
            | ScalarFuncSig::ExportSet4Arg
            | ScalarFuncSig::ExportSet5Arg
            | ScalarFuncSig::ExtractDatetime
            | ScalarFuncSig::ExtractDuration
            | ScalarFuncSig::FieldInt
            | ScalarFuncSig::FieldReal
            | ScalarFuncSig::FieldString
            | ScalarFuncSig::FindInSet
            | ScalarFuncSig::Format
            | ScalarFuncSig::FormatWithLocale
            | ScalarFuncSig::FoundRows
            | ScalarFuncSig::FromBase64
            | ScalarFuncSig::FromDays
            | ScalarFuncSig::FromUnixTime1Arg
            | ScalarFuncSig::FromUnixTime2Arg
            | ScalarFuncSig::GetFormat
            | ScalarFuncSig::GetParamString
            | ScalarFuncSig::GetVar
            | ScalarFuncSig::GreatestDecimal
            | ScalarFuncSig::GreatestInt
            | ScalarFuncSig::GreatestReal
            | ScalarFuncSig::GreatestString
            | ScalarFuncSig::GreatestTime
            | ScalarFuncSig::HexIntArg
            | ScalarFuncSig::HexStrArg
            | ScalarFuncSig::Hour
            | ScalarFuncSig::InetAton
            | ScalarFuncSig::InetNtoa
            | ScalarFuncSig::Insert
            | ScalarFuncSig::InsertBinary
            | ScalarFuncSig::Instr
            | ScalarFuncSig::InstrBinary
            | ScalarFuncSig::IntAnyValue
            | ScalarFuncSig::IsIPv4Compat
            | ScalarFuncSig::IsIPv4Mapped
            | ScalarFuncSig::JSONAnyValue
            | ScalarFuncSig::LastInsertID
            | ScalarFuncSig::LastInsertIDWithID
            | ScalarFuncSig::LeastDecimal
            | ScalarFuncSig::LeastInt
            | ScalarFuncSig::LeastReal
            | ScalarFuncSig::LeastString
            | ScalarFuncSig::LeastTime
            | ScalarFuncSig::LeftBinary
            | ScalarFuncSig::Locate2Args
            | ScalarFuncSig::Locate3Args
            | ScalarFuncSig::LocateBinary2Args
            | ScalarFuncSig::LocateBinary3Args
            | ScalarFuncSig::Lock
            | ScalarFuncSig::Log10
            | ScalarFuncSig::Log1Arg
            | ScalarFuncSig::Log2
            | ScalarFuncSig::Log2Args
            | ScalarFuncSig::Lpad
            | ScalarFuncSig::LpadBinary
            | ScalarFuncSig::LTrim
            | ScalarFuncSig::MakeDate
            | ScalarFuncSig::MakeSet
            | ScalarFuncSig::MakeTime
            | ScalarFuncSig::MicroSecond
            | ScalarFuncSig::Minute
            | ScalarFuncSig::Month
            | ScalarFuncSig::MonthName
            | ScalarFuncSig::NowWithArg
            | ScalarFuncSig::NowWithoutArg
            | ScalarFuncSig::NullTimeDiff
            | ScalarFuncSig::OctInt
            | ScalarFuncSig::OctString
            | ScalarFuncSig::Ord
            | ScalarFuncSig::Password
            | ScalarFuncSig::PeriodAdd
            | ScalarFuncSig::PeriodDiff
            | ScalarFuncSig::Quarter
            | ScalarFuncSig::Quote
            | ScalarFuncSig::Radians
            | ScalarFuncSig::Rand
            | ScalarFuncSig::RandomBytes
            | ScalarFuncSig::RandWithSeed
            | ScalarFuncSig::RealAnyValue
            | ScalarFuncSig::ReleaseLock
            | ScalarFuncSig::Repeat
            | ScalarFuncSig::Replace
            | ScalarFuncSig::Right
            | ScalarFuncSig::RightBinary
            | ScalarFuncSig::RouldReal
            | ScalarFuncSig::RoundDec
            | ScalarFuncSig::RoundInt
            | ScalarFuncSig::RoundWithFracDec
            | ScalarFuncSig::RoundWithFracInt
            | ScalarFuncSig::RoundWithFracReal
            | ScalarFuncSig::RowCount
            | ScalarFuncSig::RowSig
            | ScalarFuncSig::Rpad
            | ScalarFuncSig::RpadBinary
            | ScalarFuncSig::RTrim
            | ScalarFuncSig::Second
            | ScalarFuncSig::SecToTime
            | ScalarFuncSig::SetVar
            | ScalarFuncSig::SHA1
            | ScalarFuncSig::SHA2
            | ScalarFuncSig::Sleep
            | ScalarFuncSig::Space
            | ScalarFuncSig::Strcmp
            | ScalarFuncSig::StringAnyValue
            | ScalarFuncSig::StringDurationTimeDiff
            | ScalarFuncSig::StringStringTimeDiff
            | ScalarFuncSig::StringTimeTimeDiff
            | ScalarFuncSig::StrToDateDate
            | ScalarFuncSig::StrToDateDatetime
            | ScalarFuncSig::StrToDateDuration
            | ScalarFuncSig::SubDateAndDuration
            | ScalarFuncSig::SubDateAndString
            | ScalarFuncSig::SubDateDatetimeInt
            | ScalarFuncSig::SubDateDatetimeString
            | ScalarFuncSig::SubDateIntInt
            | ScalarFuncSig::SubDateIntString
            | ScalarFuncSig::SubDateStringDecimal
            | ScalarFuncSig::SubDateStringInt
            | ScalarFuncSig::SubDateStringString
            | ScalarFuncSig::SubDatetimeAndDuration
            | ScalarFuncSig::SubDatetimeAndString
            | ScalarFuncSig::SubDurationAndDuration
            | ScalarFuncSig::SubDurationAndString
            | ScalarFuncSig::Substring2Args
            | ScalarFuncSig::Substring3Args
            | ScalarFuncSig::SubStringAndDuration
            | ScalarFuncSig::SubStringAndString
            | ScalarFuncSig::SubstringBinary2Args
            | ScalarFuncSig::SubstringBinary3Args
            | ScalarFuncSig::SubstringIndex
            | ScalarFuncSig::SubTimeDateTimeNull
            | ScalarFuncSig::SubTimeDurationNull
            | ScalarFuncSig::SubTimeStringNull
            | ScalarFuncSig::SysDateWithFsp
            | ScalarFuncSig::SysDateWithoutFsp
            | ScalarFuncSig::TiDBVersion
            | ScalarFuncSig::Time
            | ScalarFuncSig::TimeAnyValue
            | ScalarFuncSig::TimeFormat
            | ScalarFuncSig::TimeLiteral
            | ScalarFuncSig::Timestamp1Arg
            | ScalarFuncSig::Timestamp2Args
            | ScalarFuncSig::TimestampAdd
            | ScalarFuncSig::TimestampDiff
            | ScalarFuncSig::TimestampLiteral
            | ScalarFuncSig::TimeStringTimeDiff
            | ScalarFuncSig::TimeTimeTimeDiff
            | ScalarFuncSig::TimeToSec
            | ScalarFuncSig::ToBase64
            | ScalarFuncSig::ToDays
            | ScalarFuncSig::ToSeconds
            | ScalarFuncSig::Trim1Arg
            | ScalarFuncSig::Trim2Args
            | ScalarFuncSig::Trim3Args
            | ScalarFuncSig::TruncateDecimal
            | ScalarFuncSig::TruncateInt
            | ScalarFuncSig::TruncateReal
            | ScalarFuncSig::Uncompress
            | ScalarFuncSig::UncompressedLength
            | ScalarFuncSig::UnixTimestampCurrent
            | ScalarFuncSig::UnixTimestampDec
            | ScalarFuncSig::UnixTimestampInt
            | ScalarFuncSig::User
            | ScalarFuncSig::UTCDate
            | ScalarFuncSig::UTCTimestampWithArg
            | ScalarFuncSig::UTCTimestampWithoutArg
            | ScalarFuncSig::UTCTimeWithArg
            | ScalarFuncSig::UTCTimeWithoutArg
            | ScalarFuncSig::UUID
            | ScalarFuncSig::ValuesDecimal
            | ScalarFuncSig::ValuesDuration
            | ScalarFuncSig::ValuesInt
            | ScalarFuncSig::ValuesJSON
            | ScalarFuncSig::ValuesReal
            | ScalarFuncSig::ValuesString
            | ScalarFuncSig::ValuesTime
            | ScalarFuncSig::Version
            | ScalarFuncSig::WeekDay
            | ScalarFuncSig::WeekOfYear
            | ScalarFuncSig::WeekWithMode
            | ScalarFuncSig::WeekWithoutMode
            | ScalarFuncSig::Year
            | ScalarFuncSig::YearWeekWithMode
            | ScalarFuncSig::YearWeekWithoutMode => return Err(Error::UnknownSignature(sig)),
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
            ) -> Result<Option<Cow<'a, Duration>>> {
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
                                if mysql::has_unsigned_flag(u64::from(self.tp.get_flag())) {
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

        InInt => in_int,
        InReal => in_real,
        InDecimal => in_decimal,
        InString => in_string,
        InTime => in_time,
        InDuration => in_duration,
        InJson => in_json,
        IntervalInt => interval_int,
        IntervalReal => interval_real,

        PlusInt => plus_int,
        MinusInt => minus_int,
        MultiplyInt => multiply_int,
        MultiplyIntUnsigned => multiply_int_unsigned,
        IntDivideInt => int_divide_int,
        IntDivideDecimal => int_divide_decimal,
        ModInt => mod_int,

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
        CRC32 => crc32,
        Sign => sign,

        IfNullInt => if_null_int,
        IfInt => if_int,

        CoalesceInt => coalesce_int,
        CaseWhenInt => case_when_int,

        LikeSig => like,
        RegexpSig => regexp,
        RegexpBinarySig => regexp_binary,

        BitAndSig => bit_and,
        BitNegSig => bit_neg,
        BitOrSig => bit_or,
        BitXorSig => bit_xor,

        Length => length,
        CharLength => char_length,
        BitLength => bit_length,
        LeftShift => left_shift,
        RightShift => right_shift,
        ASCII => ascii,
        IsIPv4 => is_ipv4,
        IsIPv6 => is_ipv6,
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
        PI => pi,

        IfNullReal => if_null_real,
        IfReal => if_real,

        CoalesceReal => coalesce_real,
        CaseWhenReal => case_when_real,

        Sqrt => sqrt,
        Atan1Arg => atan_1_arg,
        Atan2Args => atan_2_args,
        Acos => acos,
        Asin => asin,
        Cos => cos,
        Tan => tan,
        Sin => sin,
        Pow => pow,
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

        IfNullDecimal => if_null_decimal,
        IfDecimal => if_decimal,

        CoalesceDecimal => coalesce_decimal,
        CaseWhenDecimal => case_when_decimal,
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

        Left => left,
        Upper => upper,
        Lower => lower,
        DateFormatSig => date_format,
        Bin => bin,
        Reverse => reverse,
        ReverseBinary => reverse_binary,
        UnHex => un_hex,
        Inet6Aton => inet6_aton,
        Inet6Ntoa => inet6_ntoa,
        MD5 => md5,
        Elt => elt,
    }
    TIME_CALLS {
        CastIntAsTime => cast_int_as_time,
        CastRealAsTime => cast_real_as_time,
        CastDecimalAsTime => cast_decimal_as_time,
        CastStringAsTime => cast_str_as_time,
        CastTimeAsTime => cast_time_as_time,
        CastDurationAsTime => cast_duration_as_time,
        CastJsonAsTime => cast_json_as_time,

        Date => date,
        LastDay => last_day,

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

#[cfg(test)]
mod test {
    use coprocessor::dag::expr::{Error, ScalarFunc};
    use std::usize;
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_check_args() {
        let cases = vec![
            (
                vec![
                    ScalarFuncSig::LTInt,
                    ScalarFuncSig::LEInt,
                    ScalarFuncSig::GTInt,
                    ScalarFuncSig::GEInt,
                    ScalarFuncSig::EQInt,
                    ScalarFuncSig::NEInt,
                    ScalarFuncSig::NullEQInt,
                    ScalarFuncSig::LTReal,
                    ScalarFuncSig::LEReal,
                    ScalarFuncSig::GTReal,
                    ScalarFuncSig::GEReal,
                    ScalarFuncSig::EQReal,
                    ScalarFuncSig::NEReal,
                    ScalarFuncSig::NullEQReal,
                    ScalarFuncSig::LTDecimal,
                    ScalarFuncSig::LEDecimal,
                    ScalarFuncSig::GTDecimal,
                    ScalarFuncSig::GEDecimal,
                    ScalarFuncSig::EQDecimal,
                    ScalarFuncSig::NEDecimal,
                    ScalarFuncSig::NullEQDecimal,
                    ScalarFuncSig::LTString,
                    ScalarFuncSig::LEString,
                    ScalarFuncSig::GTString,
                    ScalarFuncSig::GEString,
                    ScalarFuncSig::EQString,
                    ScalarFuncSig::NEString,
                    ScalarFuncSig::NullEQString,
                    ScalarFuncSig::LTTime,
                    ScalarFuncSig::LETime,
                    ScalarFuncSig::GTTime,
                    ScalarFuncSig::GETime,
                    ScalarFuncSig::EQTime,
                    ScalarFuncSig::NETime,
                    ScalarFuncSig::NullEQTime,
                    ScalarFuncSig::LTDuration,
                    ScalarFuncSig::LEDuration,
                    ScalarFuncSig::GTDuration,
                    ScalarFuncSig::GEDuration,
                    ScalarFuncSig::EQDuration,
                    ScalarFuncSig::NEDuration,
                    ScalarFuncSig::NullEQDuration,
                    ScalarFuncSig::LTJson,
                    ScalarFuncSig::LEJson,
                    ScalarFuncSig::GTJson,
                    ScalarFuncSig::GEJson,
                    ScalarFuncSig::EQJson,
                    ScalarFuncSig::NEJson,
                    ScalarFuncSig::NullEQJson,
                    ScalarFuncSig::PlusReal,
                    ScalarFuncSig::PlusDecimal,
                    ScalarFuncSig::PlusInt,
                    ScalarFuncSig::MinusReal,
                    ScalarFuncSig::MinusDecimal,
                    ScalarFuncSig::MinusInt,
                    ScalarFuncSig::MultiplyReal,
                    ScalarFuncSig::MultiplyDecimal,
                    ScalarFuncSig::MultiplyInt,
                    ScalarFuncSig::MultiplyIntUnsigned,
                    ScalarFuncSig::IfNullInt,
                    ScalarFuncSig::IfNullReal,
                    ScalarFuncSig::IfNullString,
                    ScalarFuncSig::IfNullDecimal,
                    ScalarFuncSig::IfNullTime,
                    ScalarFuncSig::IfNullDuration,
                    ScalarFuncSig::IfNullJson,
                    ScalarFuncSig::Left,
                    ScalarFuncSig::LogicalAnd,
                    ScalarFuncSig::LogicalOr,
                    ScalarFuncSig::LogicalXor,
                    ScalarFuncSig::DivideDecimal,
                    ScalarFuncSig::DivideReal,
                    ScalarFuncSig::IntDivideInt,
                    ScalarFuncSig::IntDivideDecimal,
                    ScalarFuncSig::ModReal,
                    ScalarFuncSig::ModDecimal,
                    ScalarFuncSig::ModInt,
                    ScalarFuncSig::BitAndSig,
                    ScalarFuncSig::BitOrSig,
                    ScalarFuncSig::BitXorSig,
                    ScalarFuncSig::DateFormatSig,
                    ScalarFuncSig::LeftShift,
                    ScalarFuncSig::RightShift,
                    ScalarFuncSig::Pow,
                    ScalarFuncSig::Atan2Args,
                ],
                2,
                2,
            ),
            (
                vec![
                    ScalarFuncSig::CastIntAsInt,
                    ScalarFuncSig::CastIntAsReal,
                    ScalarFuncSig::CastIntAsString,
                    ScalarFuncSig::CastIntAsDecimal,
                    ScalarFuncSig::CastIntAsTime,
                    ScalarFuncSig::CastIntAsDuration,
                    ScalarFuncSig::CastIntAsJson,
                    ScalarFuncSig::CastRealAsInt,
                    ScalarFuncSig::CastRealAsReal,
                    ScalarFuncSig::CastRealAsString,
                    ScalarFuncSig::CastRealAsDecimal,
                    ScalarFuncSig::CastRealAsTime,
                    ScalarFuncSig::CastRealAsDuration,
                    ScalarFuncSig::CastRealAsJson,
                    ScalarFuncSig::CastDecimalAsInt,
                    ScalarFuncSig::CastDecimalAsReal,
                    ScalarFuncSig::CastDecimalAsString,
                    ScalarFuncSig::CastDecimalAsDecimal,
                    ScalarFuncSig::CastDecimalAsTime,
                    ScalarFuncSig::CastDecimalAsDuration,
                    ScalarFuncSig::CastDecimalAsJson,
                    ScalarFuncSig::CastStringAsInt,
                    ScalarFuncSig::CastStringAsReal,
                    ScalarFuncSig::CastStringAsString,
                    ScalarFuncSig::CastStringAsDecimal,
                    ScalarFuncSig::CastStringAsTime,
                    ScalarFuncSig::CastStringAsDuration,
                    ScalarFuncSig::CastStringAsJson,
                    ScalarFuncSig::CastTimeAsInt,
                    ScalarFuncSig::CastTimeAsReal,
                    ScalarFuncSig::CastTimeAsString,
                    ScalarFuncSig::CastTimeAsDecimal,
                    ScalarFuncSig::CastTimeAsTime,
                    ScalarFuncSig::CastTimeAsDuration,
                    ScalarFuncSig::CastTimeAsJson,
                    ScalarFuncSig::CastDurationAsInt,
                    ScalarFuncSig::CastDurationAsReal,
                    ScalarFuncSig::CastDurationAsString,
                    ScalarFuncSig::CastDurationAsDecimal,
                    ScalarFuncSig::CastDurationAsTime,
                    ScalarFuncSig::CastDurationAsDuration,
                    ScalarFuncSig::CastDurationAsJson,
                    ScalarFuncSig::CastJsonAsInt,
                    ScalarFuncSig::CastJsonAsReal,
                    ScalarFuncSig::CastJsonAsString,
                    ScalarFuncSig::CastJsonAsDecimal,
                    ScalarFuncSig::CastJsonAsTime,
                    ScalarFuncSig::CastJsonAsDuration,
                    ScalarFuncSig::CastJsonAsJson,
                    ScalarFuncSig::Date,
                    ScalarFuncSig::LastDay,
                    ScalarFuncSig::UnaryNot,
                    ScalarFuncSig::UnaryMinusInt,
                    ScalarFuncSig::UnaryMinusReal,
                    ScalarFuncSig::UnaryMinusDecimal,
                    ScalarFuncSig::IntIsTrue,
                    ScalarFuncSig::IntIsFalse,
                    ScalarFuncSig::IntIsNull,
                    ScalarFuncSig::RealIsTrue,
                    ScalarFuncSig::RealIsFalse,
                    ScalarFuncSig::RealIsNull,
                    ScalarFuncSig::DecimalIsTrue,
                    ScalarFuncSig::DecimalIsFalse,
                    ScalarFuncSig::DecimalIsNull,
                    ScalarFuncSig::StringIsNull,
                    ScalarFuncSig::TimeIsNull,
                    ScalarFuncSig::DurationIsNull,
                    ScalarFuncSig::JsonIsNull,
                    ScalarFuncSig::AbsInt,
                    ScalarFuncSig::AbsUInt,
                    ScalarFuncSig::AbsReal,
                    ScalarFuncSig::AbsDecimal,
                    ScalarFuncSig::CeilReal,
                    ScalarFuncSig::CeilIntToInt,
                    ScalarFuncSig::CeilIntToDec,
                    ScalarFuncSig::CeilDecToDec,
                    ScalarFuncSig::CeilDecToInt,
                    ScalarFuncSig::FloorReal,
                    ScalarFuncSig::FloorIntToInt,
                    ScalarFuncSig::FloorIntToDec,
                    ScalarFuncSig::FloorDecToDec,
                    ScalarFuncSig::FloorDecToInt,
                    ScalarFuncSig::CRC32,
                    ScalarFuncSig::Sign,
                    ScalarFuncSig::Sqrt,
                    ScalarFuncSig::Atan1Arg,
                    ScalarFuncSig::Acos,
                    ScalarFuncSig::Asin,
                    ScalarFuncSig::Cos,
                    ScalarFuncSig::Tan,
                    ScalarFuncSig::Sin,
                    ScalarFuncSig::JsonTypeSig,
                    ScalarFuncSig::JsonUnquoteSig,
                    ScalarFuncSig::ASCII,
                    ScalarFuncSig::Bin,
                    ScalarFuncSig::BitNegSig,
                    ScalarFuncSig::BitLength,
                    ScalarFuncSig::CharLength,
                    ScalarFuncSig::Length,
                    ScalarFuncSig::Reverse,
                    ScalarFuncSig::ReverseBinary,
                    ScalarFuncSig::Lower,
                    ScalarFuncSig::Upper,
                    ScalarFuncSig::IsIPv4,
                    ScalarFuncSig::IsIPv6,
                    ScalarFuncSig::MD5,
                ],
                1,
                1,
            ),
            (
                vec![
                    ScalarFuncSig::IfInt,
                    ScalarFuncSig::IfReal,
                    ScalarFuncSig::IfString,
                    ScalarFuncSig::IfDecimal,
                    ScalarFuncSig::IfTime,
                    ScalarFuncSig::IfDuration,
                    ScalarFuncSig::IfJson,
                    ScalarFuncSig::LikeSig,
                ],
                3,
                3,
            ),
            (
                vec![ScalarFuncSig::JsonArraySig, ScalarFuncSig::JsonObjectSig],
                0,
                usize::MAX,
            ),
            (
                vec![
                    ScalarFuncSig::CoalesceDecimal,
                    ScalarFuncSig::CoalesceDuration,
                    ScalarFuncSig::CoalesceInt,
                    ScalarFuncSig::CoalesceJson,
                    ScalarFuncSig::CoalesceReal,
                    ScalarFuncSig::CoalesceString,
                    ScalarFuncSig::CoalesceTime,
                    ScalarFuncSig::CaseWhenDecimal,
                    ScalarFuncSig::CaseWhenDuration,
                    ScalarFuncSig::CaseWhenInt,
                    ScalarFuncSig::CaseWhenJson,
                    ScalarFuncSig::CaseWhenReal,
                    ScalarFuncSig::CaseWhenString,
                    ScalarFuncSig::CaseWhenTime,
                ],
                1,
                usize::MAX,
            ),
            (
                vec![
                    ScalarFuncSig::JsonExtractSig,
                    ScalarFuncSig::JsonRemoveSig,
                    ScalarFuncSig::JsonMergeSig,
                    ScalarFuncSig::InInt,
                    ScalarFuncSig::InReal,
                    ScalarFuncSig::InString,
                    ScalarFuncSig::InDecimal,
                    ScalarFuncSig::InTime,
                    ScalarFuncSig::InDuration,
                    ScalarFuncSig::InJson,
                    ScalarFuncSig::IntervalInt,
                    ScalarFuncSig::IntervalReal,
                    ScalarFuncSig::Elt,
                ],
                2,
                usize::MAX,
            ),
            (
                vec![
                    ScalarFuncSig::JsonSetSig,
                    ScalarFuncSig::JsonInsertSig,
                    ScalarFuncSig::JsonReplaceSig,
                ],
                3,
                usize::MAX,
            ),
            (vec![ScalarFuncSig::PI], 0, 0),
        ];
        for (sigs, min, max) in cases {
            for sig in sigs {
                assert!(ScalarFunc::check_args(sig, min).is_ok());
                match sig {
                    ScalarFuncSig::JsonObjectSig => {
                        assert!(ScalarFunc::check_args(sig, 3).is_err());
                    }
                    ScalarFuncSig::JsonSetSig
                    | ScalarFuncSig::JsonInsertSig
                    | ScalarFuncSig::JsonReplaceSig => {
                        assert!(ScalarFunc::check_args(sig, 4).is_err());
                    }
                    _ => assert!(ScalarFunc::check_args(sig, max).is_ok()),
                }
            }
        }

        // unimplemented signature
        let cases = vec![
            ScalarFuncSig::AddDateAndDuration,
            ScalarFuncSig::AddDateAndString,
            ScalarFuncSig::AddDateDatetimeInt,
            ScalarFuncSig::AddDateDatetimeString,
            ScalarFuncSig::AddDateIntInt,
            ScalarFuncSig::AddDateIntString,
            ScalarFuncSig::AddDateStringDecimal,
            ScalarFuncSig::AddDateStringInt,
            ScalarFuncSig::AddDateStringString,
            ScalarFuncSig::AddDatetimeAndDuration,
            ScalarFuncSig::AddDatetimeAndString,
            ScalarFuncSig::AddDurationAndDuration,
            ScalarFuncSig::AddDurationAndString,
            ScalarFuncSig::AddStringAndDuration,
            ScalarFuncSig::AddStringAndString,
            ScalarFuncSig::AddTimeDateTimeNull,
            ScalarFuncSig::AddTimeDurationNull,
            ScalarFuncSig::AddTimeStringNull,
            ScalarFuncSig::AesDecrypt,
            ScalarFuncSig::AesEncrypt,
            ScalarFuncSig::BitCount,
            ScalarFuncSig::Char,
            ScalarFuncSig::Compress,
            ScalarFuncSig::Concat,
            ScalarFuncSig::ConcatWS,
            ScalarFuncSig::ConnectionID,
            ScalarFuncSig::Conv,
            ScalarFuncSig::Convert,
            ScalarFuncSig::ConvertTz,
            ScalarFuncSig::Cot,
            ScalarFuncSig::CurrentDate,
            ScalarFuncSig::CurrentTime0Arg,
            ScalarFuncSig::CurrentTime1Arg,
            ScalarFuncSig::CurrentUser,
            ScalarFuncSig::Database,
            ScalarFuncSig::DateDiff,
            ScalarFuncSig::DateLiteral,
            ScalarFuncSig::DayName,
            ScalarFuncSig::DayOfMonth,
            ScalarFuncSig::DayOfWeek,
            ScalarFuncSig::DayOfYear,
            ScalarFuncSig::DecimalAnyValue,
            ScalarFuncSig::Degrees,
            ScalarFuncSig::DurationAnyValue,
            ScalarFuncSig::DurationDurationTimeDiff,
            ScalarFuncSig::DurationStringTimeDiff,
            ScalarFuncSig::Exp,
            ScalarFuncSig::ExportSet3Arg,
            ScalarFuncSig::ExportSet4Arg,
            ScalarFuncSig::ExportSet5Arg,
            ScalarFuncSig::ExtractDatetime,
            ScalarFuncSig::ExtractDuration,
            ScalarFuncSig::FieldInt,
            ScalarFuncSig::FieldReal,
            ScalarFuncSig::FieldString,
            ScalarFuncSig::FindInSet,
            ScalarFuncSig::Format,
            ScalarFuncSig::FormatWithLocale,
            ScalarFuncSig::FoundRows,
            ScalarFuncSig::FromBase64,
            ScalarFuncSig::FromDays,
            ScalarFuncSig::FromUnixTime1Arg,
            ScalarFuncSig::FromUnixTime2Arg,
            ScalarFuncSig::GetFormat,
            ScalarFuncSig::GetParamString,
            ScalarFuncSig::GetVar,
            ScalarFuncSig::GreatestDecimal,
            ScalarFuncSig::GreatestInt,
            ScalarFuncSig::GreatestReal,
            ScalarFuncSig::GreatestString,
            ScalarFuncSig::GreatestTime,
            ScalarFuncSig::HexIntArg,
            ScalarFuncSig::HexStrArg,
            ScalarFuncSig::Hour,
            ScalarFuncSig::InetAton,
            ScalarFuncSig::InetNtoa,
            ScalarFuncSig::Insert,
            ScalarFuncSig::InsertBinary,
            ScalarFuncSig::Instr,
            ScalarFuncSig::InstrBinary,
            ScalarFuncSig::IntAnyValue,
            ScalarFuncSig::IsIPv4Compat,
            ScalarFuncSig::IsIPv4Mapped,
            ScalarFuncSig::JSONAnyValue,
            ScalarFuncSig::LastInsertID,
            ScalarFuncSig::LastInsertIDWithID,
            ScalarFuncSig::LeastDecimal,
            ScalarFuncSig::LeastInt,
            ScalarFuncSig::LeastReal,
            ScalarFuncSig::LeastString,
            ScalarFuncSig::LeastTime,
            ScalarFuncSig::LeftBinary,
            ScalarFuncSig::Locate2Args,
            ScalarFuncSig::Locate3Args,
            ScalarFuncSig::LocateBinary2Args,
            ScalarFuncSig::LocateBinary3Args,
            ScalarFuncSig::Lock,
            ScalarFuncSig::Log10,
            ScalarFuncSig::Log1Arg,
            ScalarFuncSig::Log2,
            ScalarFuncSig::Log2Args,
            ScalarFuncSig::Lpad,
            ScalarFuncSig::LpadBinary,
            ScalarFuncSig::LTrim,
            ScalarFuncSig::MakeDate,
            ScalarFuncSig::MakeSet,
            ScalarFuncSig::MakeTime,
            ScalarFuncSig::MicroSecond,
            ScalarFuncSig::Minute,
            ScalarFuncSig::Month,
            ScalarFuncSig::MonthName,
            ScalarFuncSig::NowWithArg,
            ScalarFuncSig::NowWithoutArg,
            ScalarFuncSig::NullTimeDiff,
            ScalarFuncSig::OctInt,
            ScalarFuncSig::OctString,
            ScalarFuncSig::Ord,
            ScalarFuncSig::Password,
            ScalarFuncSig::PeriodAdd,
            ScalarFuncSig::PeriodDiff,
            ScalarFuncSig::Quarter,
            ScalarFuncSig::Quote,
            ScalarFuncSig::Radians,
            ScalarFuncSig::Rand,
            ScalarFuncSig::RandomBytes,
            ScalarFuncSig::RandWithSeed,
            ScalarFuncSig::RealAnyValue,
            ScalarFuncSig::ReleaseLock,
            ScalarFuncSig::Repeat,
            ScalarFuncSig::Replace,
            ScalarFuncSig::Right,
            ScalarFuncSig::RightBinary,
            ScalarFuncSig::RouldReal,
            ScalarFuncSig::RoundDec,
            ScalarFuncSig::RoundInt,
            ScalarFuncSig::RoundWithFracDec,
            ScalarFuncSig::RoundWithFracInt,
            ScalarFuncSig::RoundWithFracReal,
            ScalarFuncSig::RowCount,
            ScalarFuncSig::RowSig,
            ScalarFuncSig::Rpad,
            ScalarFuncSig::RpadBinary,
            ScalarFuncSig::RTrim,
            ScalarFuncSig::Second,
            ScalarFuncSig::SecToTime,
            ScalarFuncSig::SetVar,
            ScalarFuncSig::SHA1,
            ScalarFuncSig::SHA2,
            ScalarFuncSig::Sleep,
            ScalarFuncSig::Space,
            ScalarFuncSig::Strcmp,
            ScalarFuncSig::StringAnyValue,
            ScalarFuncSig::StringDurationTimeDiff,
            ScalarFuncSig::StringStringTimeDiff,
            ScalarFuncSig::StringTimeTimeDiff,
            ScalarFuncSig::StrToDateDate,
            ScalarFuncSig::StrToDateDatetime,
            ScalarFuncSig::StrToDateDuration,
            ScalarFuncSig::SubDateAndDuration,
            ScalarFuncSig::SubDateAndString,
            ScalarFuncSig::SubDateDatetimeInt,
            ScalarFuncSig::SubDateDatetimeString,
            ScalarFuncSig::SubDateIntInt,
            ScalarFuncSig::SubDateIntString,
            ScalarFuncSig::SubDateStringDecimal,
            ScalarFuncSig::SubDateStringInt,
            ScalarFuncSig::SubDateStringString,
            ScalarFuncSig::SubDatetimeAndDuration,
            ScalarFuncSig::SubDatetimeAndString,
            ScalarFuncSig::SubDurationAndDuration,
            ScalarFuncSig::SubDurationAndString,
            ScalarFuncSig::Substring2Args,
            ScalarFuncSig::Substring3Args,
            ScalarFuncSig::SubStringAndDuration,
            ScalarFuncSig::SubStringAndString,
            ScalarFuncSig::SubstringBinary2Args,
            ScalarFuncSig::SubstringBinary3Args,
            ScalarFuncSig::SubstringIndex,
            ScalarFuncSig::SubTimeDateTimeNull,
            ScalarFuncSig::SubTimeDurationNull,
            ScalarFuncSig::SubTimeStringNull,
            ScalarFuncSig::SysDateWithFsp,
            ScalarFuncSig::SysDateWithoutFsp,
            ScalarFuncSig::TiDBVersion,
            ScalarFuncSig::Time,
            ScalarFuncSig::TimeAnyValue,
            ScalarFuncSig::TimeFormat,
            ScalarFuncSig::TimeLiteral,
            ScalarFuncSig::Timestamp1Arg,
            ScalarFuncSig::Timestamp2Args,
            ScalarFuncSig::TimestampAdd,
            ScalarFuncSig::TimestampDiff,
            ScalarFuncSig::TimestampLiteral,
            ScalarFuncSig::TimeStringTimeDiff,
            ScalarFuncSig::TimeTimeTimeDiff,
            ScalarFuncSig::TimeToSec,
            ScalarFuncSig::ToBase64,
            ScalarFuncSig::ToDays,
            ScalarFuncSig::ToSeconds,
            ScalarFuncSig::Trim1Arg,
            ScalarFuncSig::Trim2Args,
            ScalarFuncSig::Trim3Args,
            ScalarFuncSig::TruncateDecimal,
            ScalarFuncSig::TruncateInt,
            ScalarFuncSig::TruncateReal,
            ScalarFuncSig::Uncompress,
            ScalarFuncSig::UncompressedLength,
            ScalarFuncSig::UnixTimestampCurrent,
            ScalarFuncSig::UnixTimestampDec,
            ScalarFuncSig::UnixTimestampInt,
            ScalarFuncSig::User,
            ScalarFuncSig::UTCDate,
            ScalarFuncSig::UTCTimestampWithArg,
            ScalarFuncSig::UTCTimestampWithoutArg,
            ScalarFuncSig::UTCTimeWithArg,
            ScalarFuncSig::UTCTimeWithoutArg,
            ScalarFuncSig::UUID,
            ScalarFuncSig::ValuesDecimal,
            ScalarFuncSig::ValuesDuration,
            ScalarFuncSig::ValuesInt,
            ScalarFuncSig::ValuesJSON,
            ScalarFuncSig::ValuesReal,
            ScalarFuncSig::ValuesString,
            ScalarFuncSig::ValuesTime,
            ScalarFuncSig::Version,
            ScalarFuncSig::WeekDay,
            ScalarFuncSig::WeekOfYear,
            ScalarFuncSig::WeekWithMode,
            ScalarFuncSig::WeekWithoutMode,
            ScalarFuncSig::Year,
            ScalarFuncSig::YearWeekWithMode,
            ScalarFuncSig::YearWeekWithoutMode,
        ];

        for sig in cases {
            let err = format!("{:?}", Error::UnknownSignature(sig));
            assert_eq!(
                format!("{:?}", ScalarFunc::check_args(sig, 1).unwrap_err()),
                err
            );
        }
    }

}
