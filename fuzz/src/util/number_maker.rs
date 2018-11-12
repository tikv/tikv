// Copyright 2018 PingCAP, Inc.
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

macro_rules! make_numbers {
    ($name:ident, $size:expr, $ret:ty) => {
        pub fn $name<I>(iter: &mut I) -> $ret
        where
            I: Iterator<Item = u8>,
        {
            let mut bytes = [0u8; $size];
            for byte in &mut bytes {
                *byte = iter.next().unwrap();
            }
            unsafe { ::std::mem::transmute(bytes) }
        }
    };
}

make_numbers!{make_u64, 8, u64}
make_numbers!{make_i64, 8, i64}
make_numbers!{make_f64, 8, f64}
make_numbers!{make_u32, 4, u32}
make_numbers!{make_i32, 4, i32}
make_numbers!{make_u16, 2, u16}
make_numbers!{make_i8, 1, i8}

pub fn make_bool<I>(iter: &mut I) -> bool
where
    I: Iterator<Item = u8>,
{
    make_i8(iter) % 2 == 0
}
