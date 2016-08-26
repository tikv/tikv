// Copyright 2016 PingCAP, Inc.
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


use util::codec::Result;

// A word holds 9 digits.
const DIGITS_PER_WORD: u8 = 9;
// A word is 4 bytes i32.
const WORD_SIZE: u8 = 4;
const DIG_2_BYTES: &'static [u8] = &[0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

/// Return the first encoded decimal's length.
pub fn dec_encoded_len(encoded: &[u8]) -> Result<usize> {
    if encoded.len() < 3 {
        return Err(box_err!("decimal too short: {} < 3", encoded.len()));
    }

    let precision = encoded[0];
    let frac_cnt = encoded[1];
    if precision < frac_cnt {
        return Err(box_err!("invalid decimal, precision {} < frac_cnt {}",
                            precision,
                            frac_cnt));
    }
    let int_cnt = precision - frac_cnt;
    let int_word_cnt = int_cnt / DIGITS_PER_WORD;
    let frac_word_cnt = frac_cnt / DIGITS_PER_WORD;
    let int_left = (int_cnt - int_word_cnt * DIGITS_PER_WORD) as usize;
    let frac_left = (frac_cnt - frac_word_cnt * DIGITS_PER_WORD) as usize;
    let int_len = (int_word_cnt * WORD_SIZE + DIG_2_BYTES[int_left]) as usize;
    let frac_len = (frac_word_cnt * WORD_SIZE + DIG_2_BYTES[frac_left]) as usize;
    Ok(int_len + frac_len + 2)
}
