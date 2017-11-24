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
use coprocessor::codec::Datum;
use super::{FnCall, Result, StatementContext};

impl FnCall {
    #[inline]
    pub fn date_format<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let t = try_opt!(self.children[0].eval_time(ctx, row));
        if t.invalid_zero() {
            return Err(box_err!("Incorrect datetime value: '{}'", t));
        }
        let format_mask = try_opt!(self.children[0].eval_string(ctx, row));
        let format_mask_str = String::from_utf8(format_mask.into_owned())?;
        let res = t.date_format(format_mask_str)?;
        self.produce_str_with_specified_tp(ctx, Cow::Owned(res.into_bytes()))
            .map(Some)
    }
}
