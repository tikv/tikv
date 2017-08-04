

use std::io;

use chrono::FixedOffset;

use super::codec::mysql::{Duration, Time, Decimal};


use super::codec::Datum;

use super::super::util;

use tipb::expression::{Expr, DataType, ExprType, ScalarFuncSig};
use tipb::expression::FieldType;

use tipb::select::DAGRequest;

use util::codec::number::NumberDecoder;
use coprocessor::codec::mysql::decimal::DecimalDecoder;
use coprocessor::codec::mysql::MAX_FSP;


// FIXME: introduce Duration & Time?
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TypeClass {
    String = 0,
	  Real = 1,
	  Int = 2,
	  Row = 3,
    Decimal = 4,
}


quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        Type { has: TypeClass, expected: TypeClass } {
            description("type error")
            display("type error: cannot get {:?} result from {:?} expression", expected, has)
        }
        Codec(err: util::codec::Error) {
            from()
            description("codec error")
            display("codec error: {}", err)
            cause(err)
        }
        Other(desc: &'static str) {
            description(desc)
            display("error {}", desc)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;


pub enum Nullable<T> {
    Value(T),
    Null,
}

impl<T> Nullable<T> {
    pub fn is_null(&self) -> bool {
        match *self {
            Nullable::Value(_) => false,
            Nullable::Null => true
        }
    }

    pub fn is_value(&self) -> bool {
        match *self {
            Nullable::Value(_) => true,
            Nullable::Null => false
        }
    }

    pub fn map<F, U>(self, f: F) -> Nullable<U>
        where F: FnOnce(T) -> U {
        match self {
            Nullable::Value(val) => Nullable::Value(f(val)),
            Nullable::Null => Nullable::Null
        }
    }

    fn and_then<U, F: FnOnce(T) -> Nullable<U>>(self, f: F) -> Nullable<U> {
        match self {
            Nullable::Value(x) => f(x),
            Nullable::Null => Nullable::Null,
        }
    }
}

impl<T> From<T> for Nullable<T> {
    fn from(v: T) -> Nullable<T> {
        Nullable::Value(v)
    }
}


/// Flags are used by `SelectRequest.flags` to handle execution mode, like how to handle
/// truncate error.
/// `FLAG_IGNORE_TRUNCATE` indicates if truncate error should be ignored.
/// Read-only statements should ignore truncate error, write statements should not ignore
/// truncate error.
pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
/// `FLAG_TRUNCATE_AS_WARNING` indicates if truncate error should be returned as warning.
/// This flag only matters if `FLAG_IGNORE_TRUNCATE` is not set, in strict sql mode, truncate error
/// should be returned as error, in non-strict sql mode, truncate error should be saved as warning.
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;


/// StatementContext contains variables for a statement
pub struct StatementContext {
    // from DAGRequest
    tz_offset: FixedOffset,
    /// FlagIgnoreTruncate indicates if truncate error should be ignored.
	  /// Read-only statements should ignore truncate error, write statements should not ignore truncate error.
    ignore_truncate: bool,
    /// FlagTruncateAsWarning indicates if truncate error should be returned as warning.
	  /// This flag only matters if FlagIgnoreTruncate is not set, in strict sql mode, truncate error should
	  /// be returned as error, in non-strict sql mode, truncate error should be saved as warning.
    truncate_as_warning: bool,
}

impl StatementContext {
    fn from_request(req: &DAGRequest) -> Result<StatementContext> {
        let flags = req.get_flags();
        Ok(StatementContext {
            // FIXME: i64 to i32 truncation ignored
            tz_offset: try!(FixedOffset::east_opt(req.get_time_zone_offset() as i32)
                            .ok_or_else(|| Error::Other("invalid tz offset"))),
            ignore_truncate: flags & FLAG_IGNORE_TRUNCATE != 0,
            truncate_as_warning: flags & FLAG_TRUNCATE_AS_WARNING != 0,
        })
    }
}


#[derive(Default)]
pub struct Evaluator {
    pub row: Vec<Datum>,
}


pub trait ExprExt {
    /// Eval evaluates an expression through a row.
    fn eval(&self, row: &[Datum], ctx: &StatementContext) -> Result<Datum>;

    fn data_type(&self) -> DataType;

    fn type_class(&self) -> TypeClass {
        self.data_type().type_class()
    }

    fn is_hybird_type(&self) -> bool {
        match self.data_type() {
            DataType::TypeEnum | DataType::TypeBit | DataType::TypeSet => true,
            _ => false,
        }
    }

    /// EvalInt returns the int64 representation of expression.
    fn eval_int(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<i64>>;

    /// EvalReal returns the float64 representation of expression.
    fn eval_real(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<f64>>;

    /// EvalString returns the string representation of expression.
    fn eval_string(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<String>>;

    /// EvalDecimal returns the decimal representation of expression.
    fn eval_decimal(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Decimal>>;

    /// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
    fn eval_time(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Time>>;

    /// returns the duration representation of expression.
    fn eval_duration(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Duration>>;

}


impl ExprExt for Expr {
    fn data_type(&self) -> DataType {
        self.get_field_type().get_tp()
    }

    fn eval(&self, row: &[Datum], ctx: &StatementContext) -> Result<Datum> {
        match self.get_tp() {
            // Constant
            ExprType::Null => Ok(Datum::Null),
            ExprType::Int64 => Ok(try!(self.get_val().decode_i64()).into()),
            ExprType::Uint64 => Ok(try!(self.get_val().decode_u64()).into()),
            ExprType::String | ExprType::Bytes =>
                Ok(Datum::Bytes(self.get_val().to_owned())),
            ExprType::Float32 | ExprType::Float64 =>
                Ok(try!(self.get_val().decode_f64()).into()),
            ExprType::MysqlDuration => {
                let n = try!(self.get_val().decode_i64());
                Ok(try!(Duration::from_nanos(n, MAX_FSP)).into())
            },
            ExprType::MysqlDecimal => Ok(try!(self.get_val().decode_decimal()).into()),

            // Column
            // FIXME: ambiguous ColumnRef in tidb:expression/expr_to_pb.go
            ExprType::ColumnRef => {
                let column_offset = try!(self.get_val().decode_i64()) as usize;
                if column_offset < row.len() {
                    Ok(row[column_offset].clone())
                } else {
                    Err(Error::Other("column index out of range"))
                }
            },

            // ScalarFunction
            ExprType::ScalarFunc => {
                let sig = self.get_sig();
                match sig.return_type() {
                    TypeClass::Int => {
                        if let Nullable::Value(val) = try!(self.eval_int(row, ctx)) {
                            Ok(Datum::I64(val))
                        } else {
                            Ok(Datum::Null)
                        }
                    },
                    _ => unimplemented!("TODO: impl other Signature family")
                }
            },
            _ => Err(Error::Other("unexpected expr type in DAG mode")),
        }
    }

    /// EvalInt returns the int64 representation of expression.
    fn eval_int(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<i64>> {
        match self.get_tp() {
            ExprType::ScalarFunc => {
                assert_eq!(self.get_sig().return_type(), TypeClass::Int);
                eval_expr_as_int(self, row, ctx)
            },
            _ => unimplemented!("TODO: implement all INT family Constant/ColumnRef")
        }
    }

    /// EvalReal returns the float64 representation of expression.
    fn eval_real(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<f64>> {
        match self.get_tp() {
            ExprType::ScalarFunc => {
                assert_eq!(self.get_sig().return_type(), TypeClass::Real);
                eval_expr_as_real(self, row, ctx)
            },
            _ => unimplemented!("TODO: implement all REAL family Constant/ColumnRef")
        }
    }

    /// EvalString returns the string representation of expression.
    fn eval_string(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<String>> {
        match self.get_tp() {
            ExprType::ScalarFunc => {
                assert_eq!(self.get_sig().return_type(), TypeClass::String);
                eval_expr_as_string(self, row, ctx)
            },
            _ => unimplemented!("TODO: implement all STR family Constant/ColumnRef")
        }
    }

    /// EvalDecimal returns the decimal representation of expression.
    fn eval_decimal(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Decimal>> {
        match self.get_tp() {
            ExprType::ScalarFunc => {
                assert_eq!(self.get_sig().return_type(), TypeClass::Decimal);
                eval_expr_as_decimal(self, row, ctx)
            },
            _ => unimplemented!("TODO: implement all DECIMAL family Constant/ColumnRef")
        }
    }

    /// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
    fn eval_time(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Time>> {
        match self.get_tp() {
            ExprType::ScalarFunc => {
                assert_eq!(self.get_sig().return_type(), unimplemented!());
                eval_expr_as_time(self, row, ctx)
            },
            _ => unimplemented!("TODO: implement all TIME family Constant/ColumnRef")
        }
    }

    /// returns the duration representation of expression.
    fn eval_duration(&self, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Duration>> {
        match self.get_tp() {
            ExprType::ScalarFunc => {
                assert_eq!(self.get_sig().return_type(), unimplemented!());
                eval_expr_as_duration(self, row, ctx)
            },
            _ => unimplemented!("TODO: implement all DURATION family Constant/ColumnRef")
        }
    }

}


trait DataTypeExt {
    fn type_class(&self) -> TypeClass;
    fn is_blob_type(&self) -> bool;
    fn is_char_type(&self) -> bool;
    fn is_var_char_type(&self) -> bool;
    fn is_prefixable(&self) -> bool;
    fn has_time_fraction(&self) -> bool;
    fn is_time_type(&self) -> bool;
}

impl DataTypeExt for DataType {
    fn type_class(&self) -> TypeClass {
        match *self {
            DataType::TypeTiny | DataType::TypeShort |
            DataType::TypeInt24 | DataType::TypeLong |
            DataType::TypeLongLong | DataType::TypeBit |
            DataType::TypeYear
                => TypeClass::Int,
            DataType::TypeNewDecimal
                => TypeClass::Decimal,
            DataType::TypeFloat | DataType::TypeDouble
                => TypeClass::Real,
            _
                => TypeClass::String,
        }
    }

    fn is_blob_type(&self) -> bool {
        match *self {
            DataType::TypeTinyBlob | DataType::TypeMediumBlob | DataType::TypeBlob | DataType::TypeLongBlob => true,
            _ => false,
        }
    }

    fn is_char_type(&self) -> bool {
        match *self {
            DataType::TypeString | DataType::TypeVarchar => true,
            _ => false,
        }
    }

    fn is_var_char_type(&self) -> bool {
        match *self {
            DataType::TypeVarString | DataType::TypeVarchar => true,
            _ => false,
        }
    }

    fn is_prefixable(&self) -> bool {
        self.is_blob_type() || self.is_char_type()
    }

    fn has_time_fraction(&self) -> bool {
        match *self {
            DataType::TypeDatetime | DataType::TypeDuration | DataType::TypeTimestamp => true,
            _ => false
        }
    }

    fn is_time_type(&self) -> bool {
        match *self {
            DataType::TypeDatetime | DataType::TypeDate | DataType::TypeNewDate | DataType::TypeTimestamp => true,
            _ => false,
        }
    }
}

// FieldType.flags
pub const FLAG_NOT_NULL         : u32 =  0b00000001;
pub const FLAG_PRIMARY_KEY      : u32 =  0b00000010;
pub const FLAG_UNIQUE_KEY       : u32 =  0b00000100;
pub const FLAG_MULTIPLE_KEY     : u32 =  0b00001000;
pub const FLAG_BLOB             : u32 =  0b00010000;
pub const FLAG_UNSIGNED         : u32 =  0b00100000;
pub const FLAG_ZEROFILL         : u32 =  0b01000000;
pub const FLAG_BINARY           : u32 =  0b10000000;

pub const FLAG_ENUM             : u32 =  0b00000001_00000000;
pub const FLAG_AUTO_INCREMENT   : u32 =  0b00000010_00000000;
pub const FLAG_TIMESTAMP        : u32 =  0b00000100_00000000;
pub const FLAG_SET              : u32 =  0b00001000_00000000;
pub const FLAG_NO_DEFAULT_VALUE : u32 =  0b00010000_00000000;
pub const FLAG_ON_UPDATE_NOW    : u32 =  0b00100000_00000000;
pub const FLAG_NUM              : u32 =  0b01000000_00000000;
pub const FLAG_PART_KEY         : u32 =  0b10000000_00000000;

trait FieldTypeExt {
    fn is_binary_string(&self) -> bool;
}

impl FieldTypeExt for FieldType {
    fn is_binary_string(&self) -> bool {
        let flags = self.get_flag();
        let type_ = self.get_tp();
        (flags & FLAG_BINARY != 0) &&
            (type_.is_char_type() || type_.is_blob_type() || type_.is_var_char_type())
    }
}



pub trait ScalarFnSigExt {
    fn return_type(&self) -> TypeClass;

}

impl ScalarFnSigExt for ScalarFuncSig {
    fn return_type(&self) -> TypeClass {
        match *self {
            ScalarFuncSig::CastIntAsInt | ScalarFuncSig::CastRealAsInt | ScalarFuncSig::CastDecimalAsInt |
            ScalarFuncSig::CastStringAsInt | ScalarFuncSig::CastTimeAsInt | ScalarFuncSig::CastDurationAsInt |
            ScalarFuncSig::LTInt | ScalarFuncSig::LEInt | ScalarFuncSig::GTInt | ScalarFuncSig::GEInt |
            ScalarFuncSig::EQInt | ScalarFuncSig::NEInt | ScalarFuncSig::NullEQInt |
            ScalarFuncSig::AbsInt => TypeClass::Int,
            _ => unimplemented!()
        }
    }

}


fn eval_expr_as_int(expr: &Expr, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<i64>> {
    match expr.get_sig() {
        ScalarFuncSig::CastIntAsInt => {
            expr.get_children().get(0)
                .ok_or(Error::Other("empty expr children"))
                .and_then(|e| e.eval_int(row, ctx))
        },
        ScalarFuncSig::CastRealAsInt => {
            expr.get_children().get(0)
                .ok_or(Error::Other("empty expr children"))
                .and_then(|e| e.eval_real(row, ctx))
                .map(|v| v.map(|v| v as i64))
        },
        ScalarFuncSig::CastDecimalAsInt => {
            expr.get_children().get(0)
                .ok_or(Error::Other("empty expr children"))
                .and_then(|e| e.eval_decimal(row, ctx))
                .map(|v| v.map(|v| v.as_i64().unwrap())) // Decimal safe unwrap
        },
        // TODO: CAST family

        // UnOp
        ScalarFuncSig::AbsInt => {
            expr.get_children().get(0)
                .ok_or(Error::Other("empty expr children"))
                .and_then(|e| e.eval_int(row, ctx))
                .map(|v| v.map(i64::abs))
        }
        // ...

        // BinOp
        ScalarFuncSig::LTInt => {
            let lhs = try!(expr.get_children().get(0)
                           .ok_or(Error::Other("LT(int) needs at least 2 children"))
                           .and_then(|e| e.eval_int(row, ctx)));
            let rhs = try!(expr.get_children().get(1)
                           .ok_or(Error::Other("LT(int) needs at least 2 children"))
                           .and_then(|e| e.eval_int(row, ctx)));

            let f = |a, b| {
                (a > b) as i64
            };
            Ok(lhs.and_then(|a| rhs.map(|b| f(a,b))))
        },
        // ...
        _ => unimplemented!()
    }
}


fn eval_expr_as_real(expr: &Expr, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<f64>> {
    unimplemented!()
}

fn eval_expr_as_string(expr: &Expr, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<String>> {
    unimplemented!()
}

fn eval_expr_as_decimal(expr: &Expr, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Decimal>> {
  unimplemented!()
}

fn eval_expr_as_time(expr: &Expr, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Time>> {
    unimplemented!()
}

fn eval_expr_as_duration(expr: &Expr, row: &[Datum], ctx: &StatementContext) -> Result<Nullable<Duration>> {
    unimplemented!()
}

