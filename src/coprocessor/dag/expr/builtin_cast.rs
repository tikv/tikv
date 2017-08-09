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

// FIXME(shirly): remove following later
#![allow(dead_code)]

use std::u64;
use super::{FnCall, StatementContext, Result};
use coprocessor::codec::{Datum, mysql};
use coprocessor::codec::mysql::Decimal;
use coprocessor::codec::convert::convert_int_to_uint;

impl FnCall {
    pub fn cast_int_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(row, ctx)
    }

    pub fn cast_int_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try!(self.children[0].eval_int(row, ctx));
        if val.is_none() {
            return Ok(None);
        }
        let val = val.unwrap();
        if !mysql::has_unsigned_flag(self.children[0].ret_type.get_flag() as u64) {
            Ok(Some(val as f64))
        } else {
            let uval = try!(convert_int_to_uint(val, u64::MAX));
            Ok(Some(uval as f64))
        }
    }

    pub fn cast_int_as_decimal(&self,
                               ctx: &StatementContext,
                               row: &[Datum])
                               -> Result<Option<Decimal>> {
        let val = try!(self.children[0].eval_int(row, ctx));
        if val.is_none() {
            return Ok(None);
        }
        let val = val.unwrap();
        let field_type = &self.children[0].ret_type;
        let res = if !mysql::has_unsigned_flag(field_type.get_flag() as u64) {
            Decimal::from(val)
        } else {
            let uval = try!(convert_int_to_uint(val, u64::MAX));
            Decimal::from(uval)
        };
        let res = try!(res.convert_to(ctx, field_type.get_flen(), field_type.get_decimal()));
        // TODO
        Ok(Some(res))
    }
}
