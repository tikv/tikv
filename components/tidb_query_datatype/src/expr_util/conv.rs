// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::data_type::*;

#[derive(Copy, Clone)]
struct IntWithSign(u64, bool);

impl IntWithSign {
    fn from_int(num: Int) -> IntWithSign {
        IntWithSign(num.wrapping_abs() as u64, num < 0)
    }

    fn from_signed_uint(num: u64, is_neg: bool) -> IntWithSign {
        IntWithSign(num, is_neg)
    }

    // Shrink num to fit the boundary of i64.
    fn shrink_from_signed_uint(num: u64, is_neg: bool) -> IntWithSign {
        let value = if is_neg {
            num.min(-Int::min_value() as u64)
        } else {
            num.min(Int::max_value() as u64)
        };
        IntWithSign::from_signed_uint(value, is_neg)
    }

    fn format_radix(mut x: u64, radix: u32) -> String {
        let mut r = vec![];
        loop {
            let m = x % u64::from(radix);
            x /= u64::from(radix);
            r.push(
                std::char::from_digit(m as u32, radix)
                    .unwrap()
                    .to_ascii_uppercase(),
            );
            if x == 0 {
                break;
            }
        }
        r.iter().rev().collect::<String>()
    }

    fn format_to_base(self, to_base: IntWithSign) -> String {
        let IntWithSign(value, is_neg) = self;
        let IntWithSign(to_base, should_ignore_sign) = to_base;
        let mut real_val = value as i64;
        if is_neg && !should_ignore_sign {
            real_val = -real_val;
        }
        let mut ret = IntWithSign::format_radix(real_val as u64, to_base as u32);
        if is_neg && should_ignore_sign {
            ret.insert(0, '-');
        }
        ret
    }
}

fn is_valid_base(base: IntWithSign) -> bool {
    let IntWithSign(num, _) = base;
    num >= 2 && num <= 36
}

// Parse

fn extract_num_str(s: &str, from_base: IntWithSign) -> Option<(String, bool)> {
    let mut iter = s.chars().peekable();
    let head = *iter.peek().unwrap();
    let mut is_neg = false;
    if head == '+' || head == '-' {
        is_neg = head == '-';
        iter.next();
    }
    let IntWithSign(base, _) = from_base;
    let s = iter
        .take_while(|x| x.is_digit(base as u32))
        .collect::<String>();
    if s.is_empty() {
        None
    } else {
        Some((s, is_neg))
    }
}

fn extract_num(num_s: &str, is_neg: bool, from_base: IntWithSign) -> IntWithSign {
    let IntWithSign(from_base, signed) = from_base;
    let value = u64::from_str_radix(num_s, from_base as u32).unwrap();
    if signed {
        IntWithSign::shrink_from_signed_uint(value, is_neg)
    } else {
        IntWithSign::from_signed_uint(value, is_neg)
    }
}

pub fn conv(s: &str, from_base: Int, to_base: Int) -> Option<Bytes> {
    let s = s.trim();
    let from_base = IntWithSign::from_int(from_base);
    let to_base = IntWithSign::from_int(to_base);
    if is_valid_base(from_base) && is_valid_base(to_base) {
        if let Some((num_str, is_neg)) = extract_num_str(s, from_base) {
            let num = extract_num(num_str.as_ref(), is_neg, from_base);
            Some(num.format_to_base(to_base).into_bytes())
        } else {
            Some(b"0".to_vec())
        }
    } else {
        None
    }
}
