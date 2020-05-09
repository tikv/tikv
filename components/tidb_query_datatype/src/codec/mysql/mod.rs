// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
/// `UNSPECIFIED_FSP` is the unspecified fractional seconds part.
pub const UNSPECIFIED_FSP: i8 = -1;
/// `MAX_FSP` is the maximum digit of fractional seconds part.
pub const MAX_FSP: i8 = 6;
/// `MIN_FSP` is the minimum digit of fractional seconds part.
pub const MIN_FSP: i8 = 0;
/// `DEFAULT_FSP` is the default digit of fractional seconds part.
/// `MySQL` use 0 as the default Fsp.
pub const DEFAULT_FSP: i8 = 0;

fn check_fsp(fsp: i8) -> Result<u8> {
    if fsp == UNSPECIFIED_FSP {
        return Ok(DEFAULT_FSP as u8);
    }
    if fsp > MAX_FSP || fsp < MIN_FSP {
        return Err(invalid_type!("Invalid fsp {}", fsp));
    }
    Ok(fsp as u8)
}

pub mod binary_literal;
pub mod charset;
pub mod decimal;
pub mod duration;
pub mod json;
pub mod time;

pub use self::decimal::{dec_encoded_len, Decimal, DecimalDecoder, DecimalEncoder, Res, RoundMode};
pub use self::duration::{Duration, DurationDecoder, DurationEncoder};
pub use self::json::{
    parse_json_path_expr, Json, JsonDatumPayloadChunkEncoder, JsonDecoder, JsonEncoder, JsonType,
    ModifyType, PathExpression,
};
pub use self::time::{Time, TimeDecoder, TimeEncoder, TimeType, Tz};
