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

use tipb::expression::ScalarFuncSig;
use super::{FnCall, Result};

impl FnCall {
    pub fn check_args(sig: ScalarFuncSig, args: usize) -> Result<()> {
        let (min_args, max_args) = match sig {
            ScalarFuncSig::LTInt => (2, 2),
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
            ScalarFuncSig::CastJsonAsJson => (1, 1),
            _ => unimplemented!(),
        };
        if args < min_args || args > max_args {
            return Err(box_err!("unexpected arguments"));
        }
        Ok(())
    }
}
