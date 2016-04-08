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


use std;

use super::Result;

/// `bytes_to_int` converts a byte arrays to an i64 in best effort.
/// TODO: handle overflow.
pub fn bytes_to_int(bytes: &[u8]) -> Result<i64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ');
    let mut negative = false;
    let mut r = 0i64;
    if let Some(&c) = trimed.next() {
        if c == b'-' {
            negative = true;
        } else if c >= b'0' && c <= b'9' {
            r = c as i64 - b'0' as i64;
        } else if c != b'+' {
            return Ok(0);
        }

        r = trimed.take_while(|&&c| c >= b'0' && c <= b'9')
                  .fold(r, |l, &r| l * 10 + (r - b'0') as i64);
        if negative {
            r = -r;
        }
    }
    Ok(r)
}

/// `bytes_to_f64` converts a byte array to a float64 in best effort.
pub fn bytes_to_f64(bytes: &[u8]) -> Result<f64> {
    let f = match std::str::from_utf8(bytes) {
        Ok(s) => {
            match s.trim().parse::<f64>() {
                Ok(f) => f,
                Err(e) => {
                    error!("failed to parse float from {}: {}", s, e);
                    0.0
                }
            }
        }
        Err(e) => {
            error!("failed to convert bytes to str: {:?}", e);
            0.0
        }
    };
    Ok(f)
}

#[cfg(test)]
mod test {
    use super::*;

    use std::f64::EPSILON;

    #[test]
    fn test_bytes_to_i64() {
        let tests: Vec<(&'static [u8], i64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"", 0),
        ];
        for (bs, n) in tests {
            let t = bytes_to_int(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }
    }

    #[test]
    fn test_bytes_to_f64() {
        let tests: Vec<(&'static [u8], f64)> = vec![
            (b"", 0.0),
            (b" 23a", 23.0),
            (b"-1", -1.0),
            (b"1.11", 1.11),
            (b"1.11.00", 0.0),
            (b"xx", 0.0),
            (b"0x00", 0.0),
            (b"11.xx", 0.0),
            (b"xx.11", 0.0),
        ];

        for (v, f) in tests {
            let ff = bytes_to_f64(v).unwrap();
            if (ff - f).abs() > EPSILON {
                panic!("{:?} should be decode to {}, but got {}", v, f, ff);
            }
        }
    }
}
