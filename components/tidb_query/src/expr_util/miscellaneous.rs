// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;

pub fn inet_aton(addr: Cow<str>) -> Option<i64> {
    if addr.len() == 0 || addr.ends_with('.') {
        return None;
    }
    let (mut byte_result, mut result, mut dot_count): (u64, u64, usize) = (0, 0, 0);
    for c in addr.chars() {
        if c >= '0' && c <= '9' {
            let digit = c as u64 - '0' as u64;
            byte_result = byte_result * 10 + digit;
            if byte_result > 255 {
                return None;
            }
        } else if c == '.' {
            dot_count += 1;
            if dot_count > 3 {
                return None;
            }
            result = (result << 8) + byte_result;
            byte_result = 0;
        } else {
            return None;
        }
    }
    if dot_count == 1 {
        result <<= 16;
    } else if dot_count == 2 {
        result <<= 8;
    }
    Some(((result << 8) + byte_result) as i64)
}
