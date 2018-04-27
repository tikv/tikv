use super::Result;
use coprocessor::codec::Datum;
use coprocessor::codec::mysql::decimal::DECIMAL_STRUCT_SIZE;
use coprocessor::codec::mysql::{types, Decimal, DecimalDecoder, DecimalEncoder, Duration,
                                DurationDecoder, DurationEncoder, Json, JsonDecoder, JsonEncoder,
                                Time, TimeDecoder, TimeEncoder};
use std::io::Write;
use tipb::expression::FieldType;
use util::codec::number::{NumberDecoder, NumberEncoder};
#[derive(Default)]
pub struct Column {
    length: usize,
    null_cnt: usize,
    null_bitmap: Vec<u8>,
    var_offsets: Vec<usize>,
    data: Vec<u8>,
    // if the data's length is fixed, fixed_len should be bigger than 0
    fixed_len: usize,
}

impl Column {
    pub fn new(tp: &FieldType, init_cap: usize) -> Column {
        match tp.get_tp() as u8 {
            types::TINY
            | types::SHORT
            | types::INT24
            | types::LONG
            | types::LONG_LONG
            | types::YEAR
            | types::FLOAT
            | types::DOUBLE => {
                //TODO:no Datum::F32
                Column::new_fixed_column(8, init_cap)
            }
            types::DURATION | types::DATE | types::DATETIME | types::TIMESTAMP => {
                Column::new_fixed_column(16, init_cap)
            }
            types::NEW_DECIMAL => Column::new_fixed_column(DECIMAL_STRUCT_SIZE, init_cap),
            _ => Column::new_var_len_column(init_cap),
        }
    }

    pub fn get_datum(&self, idx: usize, tp: &FieldType) -> Result<Datum> {
        if self.is_null(idx) {
            return Ok(Datum::Null);
        }
        let d = match tp.get_tp() as u8 {
            types::TINY
            | types::SHORT
            | types::INT24
            | types::LONG
            | types::LONG_LONG
            | types::YEAR => {
                if types::has_unsigned_flag(tp.get_flag()) {
                    Datum::U64(self.get_u64(idx)?)
                } else {
                    Datum::I64(self.get_i64(idx)?)
                }
            }
            types::FLOAT | types::DOUBLE => Datum::F64(self.get_f64(idx)?),
            types::DATE | types::DATETIME | types::TIMESTAMP => Datum::Time(self.get_time(idx)?),
            types::DURATION => Datum::Dur(self.get_duration(idx)?),
            types::NEW_DECIMAL => Datum::Dec(self.get_decimal(idx)?),
            types::JSON => Datum::Json(self.get_json(idx)?),
            types::ENUM | types::BIT | types::SET => {
                return Err(box_err!(
                    "get datum with {:?} is not supported yet.",
                    tp.get_tp()
                ))
            }
            types::VARCHAR
            | types::VAR_STRING
            | types::STRING
            | types::BLOB
            | types::TINY_BLOB
            | types::MEDIUM_BLOB
            | types::LONG_BLOB => Datum::Bytes(self.get_bytes(idx).to_vec()),
            _ => unreachable!(),
        };
        Ok(d)
    }

    pub fn append_datum(&mut self, data: &Datum) -> Result<()> {
        match data {
            Datum::Null => self.append_null(),
            Datum::I64(v) => self.append_i64(*v),
            Datum::U64(v) => self.append_u64(*v),
            Datum::F64(ref v) => self.append_f64(*v),
            Datum::Bytes(ref v) => self.append_bytes(v),
            Datum::Dec(ref v) => self.append_decimal(v),
            Datum::Dur(ref v) => self.append_duration(v),
            Datum::Time(ref v) => self.append_time(v),
            Datum::Json(ref v) => self.append_json(v),
            _ => Err(box_err!("unsupported datum {:?}", data)),
        }
    }

    pub fn new_fixed_column(fixed_len: usize, init_cap: usize) -> Column {
        Column {
            fixed_len,
            data: Vec::with_capacity(fixed_len * init_cap),
            null_bitmap: Vec::with_capacity(init_cap >> 3),
            ..Default::default()
        }
    }

    pub fn new_var_len_column(init_cap: usize) -> Column {
        let mut offsets = Vec::with_capacity(init_cap + 1);
        offsets.push(0);
        Column {
            var_offsets: offsets,
            data: Vec::with_capacity(4 * init_cap),
            null_bitmap: Vec::with_capacity(init_cap >> 3),
            ..Default::default()
        }
    }

    fn is_fixed(&self) -> bool {
        self.fixed_len > 0
    }

    pub fn reset(&mut self) {
        self.length = 0;
        self.null_cnt = 0;
        self.null_bitmap.clear();
        if !self.var_offsets.is_empty() {
            // The first offset is always 0, it makes slicing the data easier, we need to keep it.
            self.var_offsets.truncate(1);
        }
        self.data.clear();
    }

    pub fn is_null(&self, row_idx: usize) -> bool {
        if let Some(null_byte) = self.null_bitmap.get(row_idx >> 3) {
            null_byte & (1 << ((row_idx) & 7)) == 0
        } else {
            false //TODO:tidb would panic here
        }
    }

    fn append_null_bitmap(&mut self, on: bool) {
        let idx = self.length >> 3; // /8
        if idx >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        if on {
            let pos = self.length & 7; // %8
            self.null_bitmap[idx] |= 1 << pos;
        } else {
            self.null_cnt += 1;
        }
    }

    pub fn append_null(&mut self) -> Result<()> {
        self.append_null_bitmap(false);
        if self.is_fixed() {
            let len = self.fixed_len + self.data.len();
            self.data.resize(len, 0);
        } else {
            let offset = self.var_offsets[self.length];
            self.var_offsets.push(offset);
        }
        self.length += 1;
        Ok(())
    }

    fn finish_append_fixed(&mut self) -> Result<()> {
        self.append_null_bitmap(true);
        self.length += 1;
        self.data.resize(self.length * self.fixed_len, 0);
        Ok(())
    }

    pub fn append_i64(&mut self, v: i64) -> Result<()> {
        self.data.encode_i64_le(v)?;
        self.finish_append_fixed()
    }

    pub fn get_i64(&self, idx: usize) -> Result<i64> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.decode_i64_le()
    }

    pub fn append_u64(&mut self, v: u64) -> Result<()> {
        self.data.encode_u64_le(v)?;
        self.finish_append_fixed()
    }

    pub fn get_u64(&self, idx: usize) -> Result<u64> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.decode_u64_le()
    }

    pub fn append_f64(&mut self, v: f64) -> Result<()> {
        self.data.encode_f64_le(v)?;
        self.finish_append_fixed()
    }

    pub fn get_f64(&self, idx: usize) -> Result<f64> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.decode_f64_le()
    }

    fn finished_append_var(&mut self) -> Result<()> {
        self.append_null_bitmap(true);
        let offset = self.data.len();
        self.var_offsets.push(offset);
        self.length += 1;
        Ok(())
    }

    pub fn append_bytes(&mut self, byte: &[u8]) -> Result<()> {
        self.data.write_all(byte)?;
        self.finished_append_var()
    }

    pub fn get_bytes(&self, idx: usize) -> &[u8] {
        let start = self.var_offsets[idx];
        let end = self.var_offsets[idx + 1];
        &self.data[start..end]
    }

    pub fn append_time(&mut self, t: &Time) -> Result<()> {
        self.data.encode_time(t)?;
        self.finish_append_fixed()
    }

    pub fn get_time(&self, idx: usize) -> Result<Time> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.decode_time()
    }

    pub fn append_duration(&mut self, d: &Duration) -> Result<()> {
        self.data.encode_duration(d)?;
        self.finish_append_fixed()
    }

    pub fn get_duration(&self, idx: usize) -> Result<Duration> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.decode_duration()
    }

    pub fn append_decimal(&mut self, d: &Decimal) -> Result<()> {
        self.data.encode_decimal_to_chunk(d)?;
        self.finish_append_fixed()
    }

    pub fn get_decimal(&self, idx: usize) -> Result<Decimal> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.decode_decimal_from_chunk()
    }

    pub fn append_json(&mut self, j: &Json) -> Result<()> {
        self.data.encode_json(j)?;
        self.finished_append_var()
    }

    pub fn get_json(&self, idx: usize) -> Result<Json> {
        let start = self.var_offsets[idx];
        let end = self.var_offsets[idx + 1];
        let mut data = &self.data[start..end];
        data.decode_json()
    }

    pub fn length(&self) -> usize {
        self.length
    }
}
