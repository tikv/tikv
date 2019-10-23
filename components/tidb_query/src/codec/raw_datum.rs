// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Decoders to decode into a concrete datum.

use codec::prelude::*;
use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
use tipb::FieldType;

use super::data_type::*;
use crate::codec::datum;
use crate::codec::mysql::{DecimalDecoder, JsonDecoder, Tz};
use crate::codec::{Error, Result};

#[inline]
fn decode_int(v: &mut &[u8]) -> Result<i64> {
    v.read_i64()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as i64".to_owned()))
}

#[inline]
fn decode_uint(v: &mut &[u8]) -> Result<u64> {
    v.read_u64()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as u64".to_owned()))
}

#[inline]
fn decode_var_int(v: &mut &[u8]) -> Result<i64> {
    v.read_var_i64()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as var_i64".to_owned()))
}

#[inline]
fn decode_var_uint(v: &mut &[u8]) -> Result<u64> {
    v.read_var_u64()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as var_u64".to_owned()))
}

#[inline]
fn decode_float(v: &mut &[u8]) -> Result<f64> {
    v.read_f64()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as f64".to_owned()))
}

#[inline]
fn decode_decimal(v: &mut &[u8]) -> Result<Decimal> {
    v.read_decimal()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as decimal".to_owned()))
}

#[inline]
fn decode_bytes(v: &mut &[u8]) -> Result<Vec<u8>> {
    v.read_comparable_bytes()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as bytes".to_owned()))
}

#[inline]
fn decode_compact_bytes(v: &mut &[u8]) -> Result<Vec<u8>> {
    v.read_compact_bytes()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as compact bytes".to_owned()))
}

#[inline]
fn decode_json(v: &mut &[u8]) -> Result<Json> {
    v.read_json()
        .map_err(|_| Error::InvalidDataType("Failed to decode data as json".to_owned()))
}

#[inline]
fn decode_duration_from_i64(v: i64, field_type: &FieldType) -> Result<Duration> {
    Duration::from_nanos(v, field_type.as_accessor().decimal() as i8)
        .map_err(|_| Error::InvalidDataType("Failed to decode i64 as duration".to_owned()))
}

#[inline]
fn decode_date_time_from_uint(v: u64, time_zone: &Tz, field_type: &FieldType) -> Result<DateTime> {
    use std::convert::TryInto;

    let fsp = field_type.decimal() as i8;
    let time_type = field_type.as_accessor().tp().try_into()?;
    DateTime::from_packed_u64(v, time_type, fsp, time_zone)
}

pub fn decode_int_datum(mut raw_datum: &[u8]) -> Result<Option<Int>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        datum::INT_FLAG => Ok(Some(decode_int(&mut raw_datum)?)),
        datum::UINT_FLAG => Ok(Some(decode_uint(&mut raw_datum)? as i64)),
        datum::VAR_INT_FLAG => Ok(Some(decode_var_int(&mut raw_datum)?)),
        datum::VAR_UINT_FLAG => Ok(Some(decode_var_uint(&mut raw_datum)? as i64)),
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for Int vector",
            flag
        ))),
    }
}

#[allow(clippy::cast_lossless)]
pub fn decode_real_datum(mut raw_datum: &[u8], field_type: &FieldType) -> Result<Option<Real>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        // In both index and record, it's flag is `FLOAT`. See TiDB's `encode()`.
        datum::FLOAT_FLAG => {
            let mut v = decode_float(&mut raw_datum)?;
            if field_type.as_accessor().tp() == FieldTypeTp::Float {
                v = (v as f32) as f64;
            }
            Ok(Real::new(v).ok()) // NaN to None
        }
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for Real vector",
            flag
        ))),
    }
}

pub fn decode_decimal_datum(mut raw_datum: &[u8]) -> Result<Option<Decimal>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        // In both index and record, it's flag is `DECIMAL`. See TiDB's `encode()`.
        datum::DECIMAL_FLAG => Ok(Some(decode_decimal(&mut raw_datum)?)),
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for Decimal vector",
            flag
        ))),
    }
}

pub fn decode_bytes_datum(mut raw_datum: &[u8]) -> Result<Option<Bytes>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        // In index, it's flag is `BYTES`. See TiDB's `encode()`.
        datum::BYTES_FLAG => Ok(Some(decode_bytes(&mut raw_datum)?)),
        // In record, it's flag is `COMPACT_BYTES`. See TiDB's `encode()`.
        datum::COMPACT_BYTES_FLAG => Ok(Some(decode_compact_bytes(&mut raw_datum)?)),
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for Bytes vector",
            flag
        ))),
    }
}

pub fn decode_date_time_datum(
    mut raw_datum: &[u8],
    field_type: &FieldType,
    time_zone: &Tz,
) -> Result<Option<DateTime>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        // In index, it's flag is `UINT`. See TiDB's `encode()`.
        datum::UINT_FLAG => {
            let v = decode_uint(&mut raw_datum)?;
            let v = decode_date_time_from_uint(v, time_zone, field_type)?;
            Ok(Some(v))
        }
        // In record, it's flag is `VAR_UINT`. See TiDB's `flatten()` and `encode()`.
        datum::VAR_UINT_FLAG => {
            let v = decode_var_uint(&mut raw_datum)?;
            let v = decode_date_time_from_uint(v, time_zone, field_type)?;
            Ok(Some(v))
        }
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for DateTime vector",
            flag
        ))),
    }
}

pub fn decode_duration_datum(
    mut raw_datum: &[u8],
    field_type: &FieldType,
) -> Result<Option<Duration>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        // In index, it's flag is `DURATION`. See TiDB's `encode()`.
        datum::DURATION_FLAG => {
            let v = decode_int(&mut raw_datum)?;
            let v = decode_duration_from_i64(v, field_type)?;
            Ok(Some(v))
        }
        // In record, it's flag is `VAR_INT`. See TiDB's `flatten()` and `encode()`.
        datum::VAR_INT_FLAG => {
            let v = decode_var_int(&mut raw_datum)?;
            let v = decode_duration_from_i64(v, field_type)?;
            Ok(Some(v))
        }
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for Duration vector",
            flag
        ))),
    }
}

pub fn decode_json_datum(mut raw_datum: &[u8]) -> Result<Option<Json>> {
    if raw_datum.is_empty() {
        return Err(Error::InvalidDataType(
            "Failed to decode datum flag".to_owned(),
        ));
    }
    let flag = raw_datum[0];
    raw_datum = &raw_datum[1..];
    match flag {
        datum::NIL_FLAG => Ok(None),
        // In both index and record, it's flag is `JSON`. See TiDB's `encode()`.
        datum::JSON_FLAG => Ok(Some(decode_json(&mut raw_datum)?)),
        _ => Err(Error::InvalidDataType(format!(
            "Unsupported datum flag {} for Json vector",
            flag
        ))),
    }
}

pub trait RawDatumDecoder<T> {
    fn decode(self, field_type: &FieldType, time_zone: &Tz) -> Result<Option<T>>;
}

impl<'a> RawDatumDecoder<Int> for &'a [u8] {
    fn decode(self, _field_type: &FieldType, _time_zone: &Tz) -> Result<Option<Int>> {
        decode_int_datum(self)
    }
}

impl<'a> RawDatumDecoder<Real> for &'a [u8] {
    fn decode(self, field_type: &FieldType, _time_zone: &Tz) -> Result<Option<Real>> {
        decode_real_datum(self, field_type)
    }
}

impl<'a> RawDatumDecoder<Decimal> for &'a [u8] {
    fn decode(self, _field_type: &FieldType, _time_zone: &Tz) -> Result<Option<Decimal>> {
        decode_decimal_datum(self)
    }
}

impl<'a> RawDatumDecoder<Bytes> for &'a [u8] {
    fn decode(self, _field_type: &FieldType, _time_zone: &Tz) -> Result<Option<Bytes>> {
        decode_bytes_datum(self)
    }
}

impl<'a> RawDatumDecoder<DateTime> for &'a [u8] {
    fn decode(self, field_type: &FieldType, time_zone: &Tz) -> Result<Option<DateTime>> {
        decode_date_time_datum(self, field_type, time_zone)
    }
}

impl<'a> RawDatumDecoder<Duration> for &'a [u8] {
    fn decode(self, field_type: &FieldType, _time_zone: &Tz) -> Result<Option<Duration>> {
        decode_duration_datum(self, field_type)
    }
}

impl<'a> RawDatumDecoder<Json> for &'a [u8] {
    fn decode(self, _field_type: &FieldType, _time_zone: &Tz) -> Result<Option<Json>> {
        decode_json_datum(self)
    }
}
