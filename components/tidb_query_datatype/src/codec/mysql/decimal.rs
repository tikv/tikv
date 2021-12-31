// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::intrinsics::copy_nonoverlapping;
use std::ops::{Add, Deref, DerefMut, Div, Mul, Neg, Rem, Sub};
use std::str::{self, FromStr};
use std::string::ToString;
use std::{cmp, mem};

use codec::prelude::*;
use tikv_util::escape;

use crate::codec::convert::{self, ConvertTo};
use crate::codec::data_type::*;
use crate::codec::{Error, Result, TEN_POW};
use crate::expr::EvalContext;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Res<T> {
    Ok(T),
    Truncated(T),
    Overflow(T),
}

impl<T> Res<T> {
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Res<U> {
        match self {
            Res::Ok(t) => Res::Ok(f(t)),
            Res::Truncated(t) => Res::Truncated(f(t)),
            Res::Overflow(t) => Res::Overflow(f(t)),
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            Res::Ok(t) | Res::Truncated(t) | Res::Overflow(t) => t,
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(*self, Res::Ok(_))
    }

    pub fn is_overflow(&self) -> bool {
        matches!(*self, Res::Overflow(_))
    }

    pub fn is_truncated(&self) -> bool {
        matches!(*self, Res::Truncated(_))
    }

    /// Convert `Res` into `Result` with an `EvalContext` that handling the errors
    /// If `truncated_err` is None, `ctx` will try to handle the default truncated error: `Error::truncated()`,
    /// otherwise handle the specified error inside `truncated_err`.
    /// Same does `overflow_err` means.
    fn into_result_impl(
        self,
        ctx: &mut EvalContext,
        truncated_err: Option<Error>,
        overflow_err: Option<Error>,
    ) -> Result<T> {
        match self {
            Res::Ok(t) => Ok(t),
            Res::Truncated(t) => if let Some(error) = truncated_err {
                ctx.handle_truncate_err(error)
            } else {
                ctx.handle_truncate(true)
            }
            .map(|()| t),

            Res::Overflow(t) => if let Some(error) = overflow_err {
                ctx.handle_overflow_err(error)
            } else {
                ctx.handle_overflow_err(Error::overflow("DECIMAL", ""))
            }
            .map(|()| t),
        }
    }

    pub fn into_result_with_overflow_err(
        self,
        ctx: &mut EvalContext,
        overflow_err: Error,
    ) -> Result<T> {
        self.into_result_impl(ctx, None, Some(overflow_err))
    }

    pub fn into_result(self, ctx: &mut EvalContext) -> Result<T> {
        self.into_result_impl(ctx, None, None)
    }
}

impl<T> From<Res<T>> for Result<T> {
    fn from(r: Res<T>) -> Result<T> {
        match r {
            Res::Ok(t) => Ok(t),
            Res::Truncated(_) => Err(Error::truncated()),
            Res::Overflow(_) => Err(Error::overflow("", "")),
        }
    }
}

impl<T> Deref for Res<T> {
    type Target = T;

    fn deref(&self) -> &T {
        match *self {
            Res::Ok(ref t) | Res::Overflow(ref t) | Res::Truncated(ref t) => t,
        }
    }
}

impl<T> DerefMut for Res<T> {
    fn deref_mut(&mut self) -> &mut T {
        match *self {
            Res::Ok(ref mut t) | Res::Overflow(ref mut t) | Res::Truncated(ref mut t) => t,
        }
    }
}

// A `Decimal` holds 9 words.
const WORD_BUF_LEN: u8 = 9;
// A word holds 9 digits.
const DIGITS_PER_WORD: u8 = 9;
// A word is 4 bytes i32.
const WORD_SIZE: u8 = 4;
const DIG_MASK: u32 = TEN_POW[8];
const WORD_BASE: u32 = TEN_POW[9];
const WORD_MAX: u32 = WORD_BASE - 1;
const MAX_FRACTION: u8 = 30;
const DEFAULT_DIV_FRAC_INCR: u8 = 4;
const DIG_2_BYTES: &[u8] = &[0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
const FRAC_MAX: &[u32] = &[
    900000000, 990000000, 999000000, 999900000, 999990000, 999999000, 999999900, 999999990,
];
const NOT_FIXED_DEC: u8 = 31;

macro_rules! word_cnt {
    ($len:expr) => {
        word_cnt!($len, u8)
    };
    ($len:expr, $t:ty) => {{
        if $len > 0 && $len as usize > (DIGITS_PER_WORD * WORD_BUF_LEN) as usize {
            // process overflow
            (WORD_BUF_LEN + 1) as $t
        } else if $len <= 0 && ($len as $t) > 0 {
            // when $len is negative and $t is unsigned
            0 as $t
        } else {
            ($len as $t + DIGITS_PER_WORD as $t - 1) / (DIGITS_PER_WORD as $t)
        }
    }};
}

/// Return the first encoded decimal's length.
pub fn dec_encoded_len(encoded: &[u8]) -> Result<usize> {
    if encoded.len() < 2 {
        return Err(box_err!("decimal too short: {} < 2", encoded.len()));
    }

    let precision = encoded[0];
    let frac_cnt = encoded[1];
    if precision < frac_cnt {
        return Err(box_err!(
            "invalid decimal, precision {} < frac_cnt {}",
            precision,
            frac_cnt
        ));
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

/// `count_leading_zeroes` returns the number of leading zeroes that can be removed from int.
fn count_leading_zeroes(i: u8, word: u32) -> u8 {
    let (mut c, mut i) = (0, i as usize);
    while TEN_POW[i] > word {
        i -= 1;
        c += 1;
    }
    c
}

/// `count_trailing_zeroes` returns the number of trailing zeroes that can be removed from fraction.
fn count_trailing_zeroes(i: u8, word: u32) -> u8 {
    let (mut c, mut i) = (0, i as usize);
    while word % TEN_POW[i] == 0 {
        i += 1;
        c += 1;
    }
    c
}

/// `add` adds a and b and carry, stores the sum and new carry.
fn add(a: u32, b: u32, carry: &mut u32, res: &mut u32) {
    let sum = a + b + *carry;
    if sum >= WORD_BASE {
        *res = sum - WORD_BASE;
        *carry = 1;
    } else {
        *res = sum;
        *carry = 0;
    }
}

/// `fix_word_cnt_err` limits word count in `word_buf_len`.
fn fix_word_cnt_err(int_word_cnt: u8, frac_word_cnt: u8, word_buf_len: u8) -> Res<(u8, u8)> {
    if int_word_cnt + frac_word_cnt > word_buf_len {
        if int_word_cnt > word_buf_len {
            return Res::Overflow((word_buf_len, 0));
        }
        return Res::Truncated((int_word_cnt, word_buf_len - int_word_cnt));
    }
    Res::Ok((int_word_cnt, frac_word_cnt))
}

/// `sub` subtracts rhs and carry from lhs, store the diff and new carry.
fn sub(lhs: u32, rhs: u32, carry: &mut i32, res: &mut u32) {
    let diff = lhs as i32 - rhs as i32 - *carry;
    if diff < 0 {
        *carry = 1;
        *res = (diff + WORD_BASE as i32) as u32;
    } else {
        *carry = 0;
        *res = diff as u32;
    }
}

/// `sub2` subtracts rhs and carry from lhs, stores the diff and new carry.
/// the new carry may be 2.
fn sub2(lhs: u32, rhs: u32, carry: &mut i32, res: &mut u32) {
    let mut diff = lhs as i32 - rhs as i32 - *carry;
    if diff < -(WORD_BASE as i32) {
        *carry = 2;
        diff += WORD_BASE as i32 + WORD_BASE as i32;
    } else if diff < 0 {
        *carry = 1;
        diff += WORD_BASE as i32;
    } else {
        *carry = 0;
    }
    *res = diff as u32;
}

type SubTmp = (usize, usize, u8);

/// calculate the carry for lhs - rhs, returns the carry and needed temporary results for
/// beginning a subtraction.
///
/// The new carry can be:
///     1. None if lhs is equals to rhs.
///     2. Some(0) if abs(lhs) > abs(rhs),
///     3. Some(1) if abs(lhs) < abs(rhs).
/// l_frac_word_cnt and r_frac_word_cnt do not contain the suffix 0 when r_int_word_cnt == l_int_word_cnt.
#[inline]
fn calc_sub_carry(lhs: &Decimal, rhs: &Decimal) -> (Option<i32>, u8, SubTmp, SubTmp) {
    let (l_int_word_cnt, mut l_frac_word_cnt) = (word_cnt!(lhs.int_cnt), word_cnt!(lhs.frac_cnt));
    let (r_int_word_cnt, mut r_frac_word_cnt) = (word_cnt!(rhs.int_cnt), word_cnt!(rhs.frac_cnt));
    let frac_word_to = cmp::max(l_frac_word_cnt, r_frac_word_cnt);

    let (l_stop, mut l_idx) = (l_int_word_cnt as usize, 0usize);
    while l_idx < l_stop && lhs.word_buf[l_idx] == 0 {
        l_idx += 1;
    }
    let l_start = l_idx;
    let l_int_word_cnt = l_stop - l_idx;

    let (r_stop, mut r_idx) = (r_int_word_cnt as usize, 0usize);
    while r_idx < r_stop && rhs.word_buf[r_idx] == 0 {
        r_idx += 1;
    }
    let r_start = r_idx;
    let r_int_word_cnt = r_stop - r_idx;

    let carry = match r_int_word_cnt.cmp(&l_int_word_cnt) {
        Ordering::Greater => Some(1),
        Ordering::Equal => {
            let mut l_end = (l_stop + l_frac_word_cnt as usize - 1) as isize;
            let mut r_end = (r_stop + r_frac_word_cnt as usize - 1) as isize;
            // trims suffix 0(also trims the suffix 0 before the point
            // when there is no digit after point).
            while l_idx as isize <= l_end && lhs.word_buf[l_end as usize] == 0 {
                l_end -= 1;
            }

            // trims suffix 0(also trims the suffix 0 before the point
            // when there is no digit after point).
            while r_idx as isize <= r_end && rhs.word_buf[r_end as usize] == 0 {
                r_end -= 1;
            }
            // here l_end is the last nonzero index in l.word_buf, attention:it may in the range of (0,l_int_word_cnt)
            l_frac_word_cnt = cmp::max(0, l_end + 1 - l_stop as isize) as u8;
            // here r_end is the last nonzero index in r.word_buf, attention:it may in the range of (0,r_int_word_cnt)
            r_frac_word_cnt = cmp::max(0, r_end + 1 - r_stop as isize) as u8;
            while l_idx as isize <= l_end
                && r_idx as isize <= r_end
                && lhs.word_buf[l_idx] == rhs.word_buf[r_idx]
            {
                l_idx += 1;
                r_idx += 1;
            }
            if l_idx as isize <= l_end {
                if r_idx as isize <= r_end && rhs.word_buf[r_idx] > lhs.word_buf[l_idx] {
                    Some(1)
                } else {
                    Some(0)
                }
            } else if r_idx as isize <= r_end {
                Some(1)
            } else {
                None
            }
        }
        Ordering::Less => Some(0),
    };
    let l_res = (l_start, l_int_word_cnt, l_frac_word_cnt);
    let r_res = (r_start, r_int_word_cnt, r_frac_word_cnt);
    (carry, frac_word_to, l_res, r_res)
}

/// subtract rhs from lhs when lhs.negative=rhs.negative.
fn do_sub<'a>(mut lhs: &'a Decimal, mut rhs: &'a Decimal) -> Res<Decimal> {
    let (carry, mut frac_word_to, l_res, r_res) = calc_sub_carry(lhs, rhs);
    if carry.is_none() {
        return Res::Ok(Decimal::zero());
    }
    let (mut l_start, mut l_int_word_cnt, mut l_frac_word_cnt) = l_res;
    let (mut r_start, mut r_int_word_cnt, mut r_frac_word_cnt) = r_res;

    // determine the res.negative and make the abs(lhs) > abs(rhs).
    let negative = if carry.unwrap() > 0 {
        mem::swap(&mut lhs, &mut rhs);
        mem::swap(&mut l_start, &mut r_start);
        mem::swap(&mut l_int_word_cnt, &mut r_int_word_cnt);
        mem::swap(&mut l_frac_word_cnt, &mut r_frac_word_cnt);
        !rhs.negative
    } else {
        lhs.negative
    };

    let res = fix_word_cnt_err(l_int_word_cnt as u8, frac_word_to, WORD_BUF_LEN);
    l_int_word_cnt = res.0 as usize;
    frac_word_to = res.1;
    let mut idx_to = l_int_word_cnt + frac_word_to as usize;
    let mut frac_cnt = cmp::max(lhs.frac_cnt, rhs.frac_cnt);
    let int_cnt = l_int_word_cnt as u8 * DIGITS_PER_WORD;
    if !res.is_ok() {
        frac_cnt = cmp::min(frac_cnt, frac_word_to * DIGITS_PER_WORD);
        l_frac_word_cnt = cmp::min(l_frac_word_cnt, frac_word_to);
        r_frac_word_cnt = cmp::min(r_frac_word_cnt, frac_word_to);
        r_int_word_cnt = cmp::min(r_int_word_cnt, l_int_word_cnt);
    }
    let mut carry = 0;
    let mut res = res.map(|_| Decimal::new(int_cnt, frac_cnt, negative));
    let mut l_idx = l_start + l_int_word_cnt as usize + l_frac_word_cnt as usize;
    let mut r_idx = r_start + r_int_word_cnt as usize + r_frac_word_cnt as usize;
    // adjust `l_idx` and `r_idx` to the same position of digits after the point.
    if l_frac_word_cnt > r_frac_word_cnt {
        let l_stop = l_start + l_int_word_cnt as usize + r_frac_word_cnt as usize;
        if l_frac_word_cnt < frac_word_to {
            // It happens only when suffix 0 exist(3.10000000000-2.00).
            idx_to -= (frac_word_to - l_frac_word_cnt) as usize;
        }
        while l_idx > l_stop {
            idx_to -= 1;
            l_idx -= 1;
            res.word_buf[idx_to] = lhs.word_buf[l_idx];
        }
    } else {
        let r_stop = r_start + r_int_word_cnt as usize + l_frac_word_cnt as usize;
        if frac_word_to > r_frac_word_cnt {
            // It happens only when suffix 0 exist(3.00-2.00000000000).
            idx_to -= (frac_word_to - r_frac_word_cnt) as usize;
        }
        while r_idx > r_stop {
            idx_to -= 1;
            r_idx -= 1;
            sub(
                0,
                rhs.word_buf[r_idx],
                &mut carry,
                &mut res.word_buf[idx_to],
            );
        }
    }

    while r_idx > r_start {
        idx_to -= 1;
        l_idx -= 1;
        r_idx -= 1;
        sub(
            lhs.word_buf[l_idx],
            rhs.word_buf[r_idx],
            &mut carry,
            &mut res.word_buf[idx_to],
        );
    }

    while carry > 0 && l_idx > l_start {
        idx_to -= 1;
        l_idx -= 1;
        sub(
            lhs.word_buf[l_idx],
            0,
            &mut carry,
            &mut res.word_buf[idx_to],
        );
    }
    while l_idx > l_start {
        idx_to -= 1;
        l_idx -= 1;
        res.word_buf[idx_to] = lhs.word_buf[l_idx];
    }
    res
}

/// Get the max possible decimal with giving precision and fraction digit count.
/// The `prec` should >= `frac_cnt`.
///
/// # Panics
///
/// Will panic if `prec` < `frac_cnt`.
/// The panic is because of `debug_assert`.
pub fn max_decimal(prec: u8, frac_cnt: u8) -> Decimal {
    debug_assert!(prec >= frac_cnt);
    let int_cnt = prec - frac_cnt;
    let mut res = Decimal::new(int_cnt, frac_cnt, false);
    let mut idx = 0;
    if int_cnt > 0 {
        let first_word_cnt = int_cnt % DIGITS_PER_WORD;
        if first_word_cnt > 0 {
            res.word_buf[idx] = TEN_POW[first_word_cnt as usize] - 1;
            idx += 1;
        }
        for _ in 0..int_cnt / DIGITS_PER_WORD {
            res.word_buf[idx] = WORD_MAX;
            idx += 1;
        }
    }
    if frac_cnt > 0 {
        let last_digits = frac_cnt % DIGITS_PER_WORD;
        for _ in 0..frac_cnt / DIGITS_PER_WORD {
            res.word_buf[idx] = WORD_MAX;
            idx += 1;
        }
        if last_digits > 0 {
            res.word_buf[idx] = FRAC_MAX[last_digits as usize - 1];
        }
    }
    res
}

/// `max_or_min_dec`(`NewMaxOrMinDec` in tidb) returns the max or min
/// value decimal for given precision and fraction.
/// The `prec` should >= `frac_cnt`.
///
/// # Panics
///
/// Will panic if `prec` < `frac_cnt`.
/// The panic is because of `debug_assert`.
pub fn max_or_min_dec(negative: bool, prec: u8, frac: u8) -> Decimal {
    let mut ret = max_decimal(prec, frac);
    ret.negative = negative;
    ret
}

/// add lhs to rhs.
fn do_add<'a>(mut lhs: &'a Decimal, mut rhs: &'a Decimal) -> Res<Decimal> {
    let (mut l_int_word_cnt, mut l_frac_word_cnt) =
        (word_cnt!(lhs.int_cnt), word_cnt!(lhs.frac_cnt));
    let (mut r_int_word_cnt, mut r_frac_word_cnt) =
        (word_cnt!(rhs.int_cnt), word_cnt!(rhs.frac_cnt));
    let (mut int_word_to, frac_word_to) = (
        cmp::max(l_int_word_cnt, r_int_word_cnt),
        cmp::max(l_frac_word_cnt, r_frac_word_cnt),
    );
    let x = match l_int_word_cnt.cmp(&r_int_word_cnt) {
        Ordering::Greater => lhs.word_buf[0],
        Ordering::Less => rhs.word_buf[0],
        Ordering::Equal => lhs.word_buf[0] + rhs.word_buf[0],
    };
    if x > WORD_MAX - 1 {
        int_word_to += 1;
    }
    let res = fix_word_cnt_err(int_word_to, frac_word_to, WORD_BUF_LEN);
    if res.is_overflow() {
        return Res::Overflow(max_decimal(WORD_BUF_LEN * DIGITS_PER_WORD, 0));
    }
    let (int_word_to, frac_word_to) = res.unwrap();
    let mut idx_to = (int_word_to + frac_word_to) as usize;
    let mut res = res.map(|_| {
        Decimal::new(
            int_word_to * DIGITS_PER_WORD,
            cmp::max(lhs.frac_cnt, rhs.frac_cnt),
            lhs.negative,
        )
    });
    res.word_buf[0] = 0;
    if !res.is_ok() {
        res.frac_cnt = cmp::min(frac_word_to * DIGITS_PER_WORD, res.frac_cnt);
        l_frac_word_cnt = cmp::min(frac_word_to, l_frac_word_cnt);
        r_frac_word_cnt = cmp::min(r_frac_word_cnt, frac_word_to);
        l_int_word_cnt = cmp::min(l_int_word_cnt, int_word_to);
        r_int_word_cnt = cmp::min(r_int_word_cnt, int_word_to);
    }
    let (mut l_idx, mut r_idx, l_stop, r_stop, exchanged);
    if l_frac_word_cnt > r_frac_word_cnt {
        l_idx = (l_int_word_cnt + l_frac_word_cnt) as usize;
        l_stop = (l_int_word_cnt + r_frac_word_cnt) as usize;
        r_idx = (r_int_word_cnt + r_frac_word_cnt) as usize;
        r_stop = l_int_word_cnt.saturating_sub(r_int_word_cnt) as usize;
        exchanged = false;
    } else {
        l_idx = (r_int_word_cnt + r_frac_word_cnt) as usize;
        l_stop = (r_int_word_cnt + l_frac_word_cnt) as usize;
        r_idx = (l_int_word_cnt + l_frac_word_cnt) as usize;
        r_stop = r_int_word_cnt.saturating_sub(l_int_word_cnt) as usize;
        mem::swap(&mut lhs, &mut rhs);
        exchanged = true;
    }
    while l_idx > l_stop {
        idx_to -= 1;
        l_idx -= 1;
        res.word_buf[idx_to] = lhs.word_buf[l_idx];
    }
    let mut carry = 0;
    while l_idx > r_stop {
        l_idx -= 1;
        r_idx -= 1;
        idx_to -= 1;
        add(
            lhs.word_buf[l_idx],
            rhs.word_buf[r_idx],
            &mut carry,
            &mut res.word_buf[idx_to],
        );
    }
    let l_stop = 0;
    if l_int_word_cnt > r_int_word_cnt {
        l_idx = (l_int_word_cnt - r_int_word_cnt) as usize;
        if exchanged {
            mem::swap(&mut lhs, &mut rhs);
        }
    } else {
        l_idx = (r_int_word_cnt - l_int_word_cnt) as usize;
        if !exchanged {
            mem::swap(&mut lhs, &mut rhs);
        }
    }
    while l_idx > l_stop {
        idx_to -= 1;
        l_idx -= 1;
        add(
            lhs.word_buf[l_idx],
            0,
            &mut carry,
            &mut res.word_buf[idx_to],
        );
    }
    if carry > 0 {
        idx_to -= 1;
        res.word_buf[idx_to] = 1;
    }
    res
}

fn do_div_mod_impl(
    lhs: &Decimal,
    rhs: &Decimal,
    mut frac_incr: u8,
    do_mod: bool,
) -> Option<Res<Decimal>> {
    let r_frac_cnt = word_cnt!(rhs.frac_cnt) * DIGITS_PER_WORD;
    let (r_idx, r_prec) = rhs.remove_leading_zeroes(rhs.int_cnt + r_frac_cnt);
    if r_prec == 0 {
        return None;
    }

    let l_frac_cnt = word_cnt!(lhs.frac_cnt) * DIGITS_PER_WORD;
    let (l_idx, l_prec) = lhs.remove_leading_zeroes(lhs.int_cnt + l_frac_cnt);
    if l_prec == 0 {
        return Some(Res::Ok(Decimal::zero()));
    }

    frac_incr = frac_incr.saturating_sub(l_frac_cnt - lhs.frac_cnt + r_frac_cnt - rhs.frac_cnt);
    let mut int_cnt_to =
        (i32::from(l_prec) - i32::from(l_frac_cnt)) - (i32::from(r_prec) - i32::from(r_frac_cnt));

    if lhs.word_buf[l_idx] >= rhs.word_buf[r_idx] {
        int_cnt_to += 1;
    }
    let mut int_word_to = if int_cnt_to < 0 {
        int_cnt_to /= i32::from(DIGITS_PER_WORD);
        0
    } else {
        word_cnt!(int_cnt_to)
    };

    let mut frac_word_to;
    let mut res = if do_mod {
        frac_word_to = 0;
        let frac_cnt = cmp::max(lhs.frac_cnt, rhs.frac_cnt);
        Res::Ok(Decimal::new(0, frac_cnt, lhs.negative))
    } else {
        frac_word_to = word_cnt!(
            l_frac_cnt
                .saturating_add(r_frac_cnt)
                .saturating_add(frac_incr)
        );
        let res = fix_word_cnt_err(int_word_to, frac_word_to, WORD_BUF_LEN);
        int_word_to = res.0;
        frac_word_to = res.1;
        res.map(|_| {
            Decimal::new(
                int_word_to * DIGITS_PER_WORD,
                frac_word_to * DIGITS_PER_WORD,
                lhs.negative != rhs.negative,
            )
        })
    };
    let mut idx_to = if !do_mod && int_cnt_to < 0 {
        cmp::min((-int_cnt_to) as u8, WORD_BUF_LEN)
    } else {
        0
    };
    let i = word_cnt!(l_prec as usize, usize);
    let l_len = cmp::max(
        3,
        i + word_cnt!(
            r_frac_cnt
                .saturating_mul(2)
                .saturating_add(frac_incr)
                .saturating_add(1)
        ) as usize
            + 1,
    );
    let mut buf = vec![0; l_len];
    (&mut buf[0..i]).copy_from_slice(&lhs.word_buf[l_idx..l_idx + i]);
    let mut l_idx = 0;
    let (r_start, mut r_stop) = (r_idx, r_idx + word_cnt!(r_prec as usize, usize) - 1);
    while rhs.word_buf[r_stop] == 0 && r_stop >= r_start {
        r_stop -= 1;
    }
    let r_len = r_stop - r_start;
    r_stop += 1;

    let norm_factor = i64::from(WORD_BASE / (rhs.word_buf[r_start] + 1));
    let mut r_norm = norm_factor * i64::from(rhs.word_buf[r_start]);
    if r_len > 0 {
        r_norm += norm_factor * i64::from(rhs.word_buf[r_start + 1]) / i64::from(WORD_BASE);
    }
    let mut dcarry = 0;
    if buf[l_idx] < rhs.word_buf[r_start] {
        dcarry = buf[l_idx] as i32;
        l_idx += 1;
    }
    let mut guess;
    for idx_to in idx_to..int_word_to + frac_word_to {
        if dcarry == 0 && buf[l_idx] < rhs.word_buf[r_start] {
            guess = 0;
        } else {
            let x = i64::from(buf[l_idx]) + i64::from(dcarry) * i64::from(WORD_BASE);
            let y = i64::from(buf[l_idx + 1]);
            guess = (norm_factor * x + norm_factor * y / i64::from(WORD_BASE)) / r_norm;
            if guess >= i64::from(WORD_BASE) {
                guess = i64::from(WORD_BASE) - 1;
            }
            if r_len > 0 {
                if i64::from(rhs.word_buf[r_start + 1]) * guess
                    > (x - guess * i64::from(rhs.word_buf[r_start])) * i64::from(WORD_BASE) + y
                {
                    guess -= 1;
                }
                if i64::from(rhs.word_buf[r_start + 1]) * guess
                    > (x - guess * i64::from(rhs.word_buf[r_start])) * i64::from(WORD_BASE) + y
                {
                    guess -= 1;
                }
            }
            let mut carry = 0;
            for (r_idx, l_idx) in (r_start..r_stop).rev().zip((0..=l_idx + r_len).rev()) {
                let x = guess * i64::from(rhs.word_buf[r_idx]);
                let hi = x / i64::from(WORD_BASE);
                let lo = x - hi * i64::from(WORD_BASE);
                sub2(buf[l_idx], lo as u32, &mut carry, &mut buf[l_idx]);
                carry += hi as i32;
            }
            if dcarry < carry {
                carry = 1;
            } else {
                carry = 0;
            }
            if carry > 0 {
                guess -= 1;
                let mut carry = 0;
                for (r_idx, l_idx) in (r_start..r_stop).rev().zip((0..=l_idx + r_len).rev()) {
                    add(buf[l_idx], rhs.word_buf[r_idx], &mut carry, &mut buf[l_idx]);
                }
            }
        }
        if !do_mod {
            res.word_buf[idx_to as usize] = guess as u32;
        }
        dcarry = buf[l_idx] as i32;
        l_idx += 1;
    }
    if do_mod {
        if dcarry != 0 {
            l_idx -= 1;
            buf[l_idx] = dcarry as u32;
        }
        idx_to = 0;

        int_cnt_to =
            i32::from(l_prec) - i32::from(l_frac_cnt) - l_idx as i32 * i32::from(DIGITS_PER_WORD);

        let mut int_word_to = if int_cnt_to < 0 {
            (int_cnt_to / i32::from(DIGITS_PER_WORD)) as i8
        } else {
            word_cnt!(int_cnt_to, i8)
        };

        let mut frac_word_to = word_cnt!(res.frac_cnt);
        if int_word_to == 0 && frac_word_to == 0 {
            return Some(Res::Ok(Decimal::zero()));
        }
        let mut l_stop;
        if int_word_to <= 0 {
            if (-int_word_to) as u8 >= WORD_BUF_LEN {
                return Some(Res::Truncated(Decimal::zero()));
            }
            l_stop = (l_idx as i8 + int_word_to + frac_word_to as i8) as u8;
            frac_word_to = (frac_word_to as i8 + int_word_to) as u8;
            res.int_cnt = 0;
            while int_word_to < 0 {
                res.word_buf[idx_to as usize] = 0;
                idx_to += 1;
                int_word_to += 1;
            }
        } else {
            if int_word_to as u8 > WORD_BUF_LEN {
                res.int_cnt = DIGITS_PER_WORD * WORD_BUF_LEN;
                res.frac_cnt = 0;
                return Some(Res::Overflow(res.unwrap()));
            }
            l_stop = l_idx as u8 + int_word_to as u8 + frac_word_to;
            res.int_cnt = cmp::min(int_word_to as u8 * DIGITS_PER_WORD, rhs.int_cnt);
        }
        if int_word_to as u8 + frac_word_to > WORD_BUF_LEN {
            l_stop -= int_word_to as u8 + frac_word_to - WORD_BUF_LEN;
            frac_word_to = WORD_BUF_LEN - int_word_to as u8;
            res.frac_cnt = frac_word_to - int_word_to as u8;
            res = Res::Truncated(res.unwrap());
        }
        let src = &buf[l_idx..l_stop as usize];
        let idx_to = idx_to as usize;
        let dest = &mut res.word_buf[idx_to..idx_to + src.len()];
        dest.copy_from_slice(src);
    }
    if res.is_zero() {
        res.negative = false
    }
    Some(res)
}

fn do_div_mod(lhs: &Decimal, rhs: &Decimal, frac_incr: u8, do_mod: bool) -> Option<Res<Decimal>> {
    do_div_mod_impl(lhs, rhs, frac_incr, do_mod)
}

/// `do_mul` multiplies two decimals.
fn do_mul(lhs: &Decimal, rhs: &Decimal) -> Res<Decimal> {
    let (l_int_word_cnt, mut l_frac_word_cnt) = (
        i32::from(word_cnt!(lhs.int_cnt)),
        i32::from(word_cnt!(lhs.frac_cnt)),
    );
    let (mut r_int_word_cnt, mut r_frac_word_cnt) = (
        i32::from(word_cnt!(rhs.int_cnt)),
        i32::from(word_cnt!(rhs.frac_cnt)),
    );
    let (int_word_to, frac_word_to) = (
        word_cnt!(lhs.int_cnt + rhs.int_cnt) as usize,
        l_frac_word_cnt + r_frac_word_cnt,
    );
    let (mut old_int_word_to, mut old_frac_word_to) = (int_word_to as i32, frac_word_to as i32);
    let res = fix_word_cnt_err(int_word_to as u8, frac_word_to as u8, WORD_BUF_LEN);
    let (int_word_to, frac_word_to) = (res.0 as usize, res.1 as usize);
    let negative = lhs.negative != rhs.negative;
    let frac_cnt = cmp::min(lhs.frac_cnt + rhs.frac_cnt, NOT_FIXED_DEC);
    let int_cnt = int_word_to as u8 * DIGITS_PER_WORD;
    let mut dec = Decimal::new(int_cnt, frac_cnt, negative);
    dec.result_frac_cnt = cmp::min(lhs.result_frac_cnt + rhs.result_frac_cnt, MAX_FRACTION);
    if res.is_overflow() {
        return Res::Overflow(dec);
    }

    if !res.is_ok() {
        dec.frac_cnt = cmp::min(dec.frac_cnt, frac_word_to as u8 * DIGITS_PER_WORD);
        if old_int_word_to > int_word_to as i32 {
            old_int_word_to -= int_word_to as i32;
            old_frac_word_to = old_int_word_to / 2;
            r_int_word_cnt = old_int_word_to - old_frac_word_to;
            l_frac_word_cnt = 0;
            r_frac_word_cnt = 0;
        } else {
            old_frac_word_to -= int_word_to as i32;
            old_int_word_to = old_frac_word_to / 2;
            if l_frac_word_cnt <= r_frac_word_cnt {
                l_frac_word_cnt -= old_int_word_to;
                r_frac_word_cnt -= old_frac_word_to - old_int_word_to;
            } else {
                r_frac_word_cnt -= old_int_word_to;
                l_frac_word_cnt -= old_frac_word_to - old_int_word_to;
            }
        }
    }

    let mut start_to = int_word_to + frac_word_to;
    let (offset_min, offset_max) = (0, i32::from(WORD_BUF_LEN));
    let r_start = num::clamp(r_int_word_cnt + r_frac_word_cnt, offset_min, offset_max) as usize;
    let left_stop = num::clamp(l_int_word_cnt + l_frac_word_cnt, offset_min, offset_max) as usize;
    for l_idx in (0..left_stop).rev() {
        if start_to < r_start {
            break;
        }
        let (mut carry, mut idx_to) = (0, start_to);
        start_to -= 1;
        for r_idx in (0..r_start).rev() {
            idx_to -= 1;
            let p = u64::from(lhs.word_buf[l_idx]) * u64::from(rhs.word_buf[r_idx]);
            let hi = p / u64::from(WORD_BASE);
            let lo = p - hi * u64::from(WORD_BASE);
            add(
                dec.word_buf[idx_to],
                lo as u32,
                &mut carry,
                &mut dec.word_buf[idx_to],
            );
            carry += hi as u32;
        }
        while carry > 0 {
            if idx_to == 0 {
                return Res::Overflow(dec);
            }
            idx_to -= 1;
            add(
                dec.word_buf[idx_to],
                0,
                &mut carry,
                &mut dec.word_buf[idx_to],
            );
        }
    }

    // Now we have to check for -0.000 case
    if dec.negative {
        let (mut idx, end) = (0, int_word_to + frac_word_to);
        while dec.word_buf[idx] == 0 {
            idx += 1;
            if idx == end {
                // we got decimal zero.
                dec = Decimal::zero();
                break;
            }
        }
    }

    let (mut idx_to, mut d_to_move) = (0, int_word_to + word_cnt!(dec.frac_cnt) as usize);
    while dec.word_buf[idx_to] == 0 && dec.int_cnt > DIGITS_PER_WORD {
        idx_to += 1;
        dec.int_cnt -= DIGITS_PER_WORD;
        d_to_move -= 1;
    }
    if idx_to > 0 {
        for cur_idx in 0..d_to_move {
            dec.word_buf[cur_idx] = dec.word_buf[idx_to];
            idx_to += 1;
        }
    }
    res.map(|_| dec)
}

/// `DECIMAL_STRUCT_SIZE`is the struct size of `Decimal`.
pub const DECIMAL_STRUCT_SIZE: usize = 40;

const_assert_eq!(DECIMAL_STRUCT_SIZE, mem::size_of::<Decimal>());

/// `Decimal` represents a decimal value.
#[repr(C)]
#[derive(Clone, Debug, Copy)]
pub struct Decimal {
    /// The number of *decimal* digits before the point.
    int_cnt: u8,

    /// The number of decimal digits after the point.
    frac_cnt: u8,

    /// The number of calculated or printed result fraction digits.
    result_frac_cnt: u8,

    negative: bool,

    /// An array of u32 words.
    /// A word is an u32 value can hold 9 digits.(0 <= word < wordBase)
    word_buf: [u32; 9],
}

#[derive(Debug, Clone)]
pub enum RoundMode {
    // HalfEven rounds normally.
    HalfEven,
    // Truncate just truncates the decimal.
    Truncate,
    // Ceiling is not supported now.
    Ceiling,
}

impl Decimal {
    /// abs the Decimal into a new Decimal.
    #[inline]
    pub fn abs(mut self) -> Res<Decimal> {
        self.negative = false;
        Res::Ok(self)
    }

    /// ceil the Decimal into a new Decimal.
    pub fn ceil(&self) -> Res<Decimal> {
        if !self.negative {
            self.round(0, RoundMode::Ceiling)
        } else {
            self.round(0, RoundMode::Truncate)
        }
    }

    /// floor the Decimal into a new Decimal.
    pub fn floor(&self) -> Res<Decimal> {
        if !self.negative {
            self.round(0, RoundMode::Truncate)
        } else {
            self.round(0, RoundMode::Ceiling)
        }
    }

    /// create a new decimal for internal usage.
    fn new(int_cnt: u8, frac_cnt: u8, negative: bool) -> Decimal {
        Decimal {
            int_cnt,
            frac_cnt,
            result_frac_cnt: 0,
            negative,
            word_buf: [0; 9],
        }
    }

    pub fn is_negative(&self) -> bool {
        self.negative
    }

    /// Creates a new decimal which is zero.
    pub fn zero() -> Decimal {
        Decimal::new(1, 0, false)
    }

    /// Given a precision count 'prec', get:
    ///  1. the index of first non-zero word in self.word_buf to hold the leading 'prec' number of
    ///     digits
    ///  2. the number of remained digits if we remove all leading zeros for the leading 'prec'
    ///     number of digits
    fn remove_leading_zeroes(&self, prec: u8) -> (usize, u8) {
        let mut cnt = prec;
        let mut i = ((cnt + DIGITS_PER_WORD - 1) % DIGITS_PER_WORD) + 1;
        let mut word_idx = 0;
        while cnt > 0 && self.word_buf[word_idx] == 0 {
            cnt -= i;
            i = DIGITS_PER_WORD;
            word_idx += 1;
        }
        if cnt > 0 {
            cnt -= count_leading_zeroes((cnt - 1) % DIGITS_PER_WORD, self.word_buf[word_idx])
        }
        (word_idx, cnt)
    }

    /// Prepare a buf for string output.
    fn prepare_buf(&self) -> (Vec<u8>, usize, u8, u8, u8) {
        let frac_cnt = self.frac_cnt;
        let (mut word_start_idx, mut int_cnt) = self.remove_leading_zeroes(self.int_cnt);
        if int_cnt + frac_cnt == 0 {
            int_cnt = 1;
            word_start_idx = 0;
        }
        let int_len = cmp::max(1, int_cnt);
        let frac_len = frac_cnt;
        let mut len = int_len + frac_len;
        if self.negative {
            len += 1;
        }
        if frac_cnt > 0 {
            len += 1;
        }
        let buf = Vec::with_capacity(len as usize);
        (buf, word_start_idx, int_len, int_cnt, frac_cnt)
    }

    /// Get the least precision and fraction count to encode this decimal completely.
    pub fn prec_and_frac(&self) -> (u8, u8) {
        let (_, int_cnt) = self.remove_leading_zeroes(self.int_cnt);
        let prec = int_cnt + self.frac_cnt;
        if prec == 0 {
            (1, self.frac_cnt)
        } else {
            (prec, self.frac_cnt)
        }
    }

    /// `digit_bounds` returns bounds of decimal digits in the number.
    fn digit_bounds(&self) -> (u8, u8) {
        let mut buf_beg = 0;
        let buf_len = (word_cnt!(self.int_cnt) + word_cnt!(self.frac_cnt)) as usize;
        let mut buf_end = buf_len - 1;

        while buf_beg < buf_len && self.word_buf[buf_beg] == 0 {
            buf_beg += 1;
        }
        if buf_beg >= buf_len {
            return (0, 0);
        }

        let mut i;
        let mut start = if buf_beg == 0 && self.int_cnt > 0 {
            i = (self.int_cnt - 1) % DIGITS_PER_WORD;
            DIGITS_PER_WORD - i - 1
        } else {
            i = DIGITS_PER_WORD - 1;
            buf_beg as u8 * DIGITS_PER_WORD
        };
        if buf_beg < buf_len {
            start += count_leading_zeroes(i, self.word_buf[buf_beg]);
        }

        while buf_end > buf_beg && self.word_buf[buf_end] == 0 {
            buf_end -= 1;
        }
        let (i, mut end) = if buf_end == buf_len - 1 && self.frac_cnt > 0 {
            i = (self.frac_cnt - 1) % DIGITS_PER_WORD + 1;
            (DIGITS_PER_WORD - i + 1, buf_end as u8 * DIGITS_PER_WORD + i)
        } else {
            (1, (buf_end as u8 + 1) * DIGITS_PER_WORD)
        };
        end -= count_trailing_zeroes(i, self.word_buf[buf_end]);
        (start, end)
    }

    /// `do_mini_left_shift` does left shift for alignment of data in buffer.
    ///
    /// Result fitting in the buffer should be garanted.
    /// 'shift' have to be from 1 to DIGITS_PER_WORD - 1 (inclusive)
    fn do_mini_left_shift(mut self, shift: u8, beg: u8, end: u8) -> Decimal {
        let shift = shift as usize;
        let mut buf_from = (beg / DIGITS_PER_WORD) as usize;
        let buf_end = ((end - 1) / DIGITS_PER_WORD) as usize;
        let c_shift = DIGITS_PER_WORD as usize - shift;
        if beg % DIGITS_PER_WORD < shift as u8 {
            self.word_buf[buf_from - 1] = self.word_buf[buf_from] / TEN_POW[c_shift];
        }
        while buf_from < buf_end {
            self.word_buf[buf_from] = (self.word_buf[buf_from] % TEN_POW[c_shift]) * TEN_POW[shift]
                + self.word_buf[buf_from + 1] / TEN_POW[c_shift];
            buf_from += 1;
        }
        self.word_buf[buf_from] = (self.word_buf[buf_from] % TEN_POW[c_shift]) * TEN_POW[shift];
        self
    }

    /// `do_mini_right_shift` does right shift for alignment of data in buffer.
    ///
    /// Result fitting in the buffer should be garanted.
    /// 'shift' have to be from 1 to DIGITS_PER_WORD - 1 (inclusive)
    fn do_mini_right_shift(mut self, shift: u8, beg: u8, end: u8) -> Decimal {
        let shift = shift as usize;
        let mut buf_from = ((end - 1) / DIGITS_PER_WORD) as usize;
        let buf_end = (beg / DIGITS_PER_WORD) as usize;
        let c_shift = DIGITS_PER_WORD as usize - shift;
        if DIGITS_PER_WORD - ((end - 1) % DIGITS_PER_WORD + 1) < shift as u8 {
            self.word_buf[buf_from + 1] =
                (self.word_buf[buf_from] % TEN_POW[shift]) * TEN_POW[c_shift];
        }
        while buf_from > buf_end {
            self.word_buf[buf_from] = self.word_buf[buf_from] / TEN_POW[shift]
                + (self.word_buf[buf_from - 1] % TEN_POW[shift]) * TEN_POW[c_shift];
            buf_from -= 1;
        }
        self.word_buf[buf_from] /= TEN_POW[shift];
        self
    }

    // TODO: remove this after merge the `refactor ScalarFunc::builtin_cast`
    //
    /// convert_to(ProduceDecWithSpecifiedTp in tidb)
    /// produces a new decimal according to `flen` and `decimal`.
    pub fn convert_to(self, ctx: &mut EvalContext, flen: u8, decimal: u8) -> Result<Decimal> {
        let (prec, frac) = self.prec_and_frac();
        if flen < decimal {
            return Err(Error::m_bigger_than_d(""));
        }
        if !self.is_zero() && prec - frac > flen - decimal {
            return Ok(max_or_min_dec(self.negative, flen, decimal));
            // TODO:select (cast 111 as decimal(1)) causes a warning in MySQL.
        }

        if frac == decimal {
            return Ok(self);
        }

        let tmp = self;
        let ret = self.round(decimal as i8, RoundMode::HalfEven).unwrap();
        // TODO: process over_flow
        if !ret.is_zero() && frac > decimal && ret != tmp {
            // TODO handle InInsertStmt in ctx
            ctx.handle_truncate(true)?;
        }
        Ok(ret)
    }

    /// Round rounds the decimal to "frac" digits.
    ///
    /// NOTES
    ///  scale can be negative !
    ///  one TRUNCATED error (line XXX below) isn't treated very logical :(
    pub fn round(self, frac: i8, round_mode: RoundMode) -> Res<Decimal> {
        self.round_with_word_buf_len(frac, WORD_BUF_LEN, round_mode)
    }

    fn round_with_word_buf_len(
        self,
        mut frac: i8,
        word_buf_len: u8,
        round_mode: RoundMode,
    ) -> Res<Decimal> {
        if frac > MAX_FRACTION as i8 {
            frac = MAX_FRACTION as i8;
        }
        let mut frac_words_to = if frac > 0 {
            word_cnt!(frac, i8)
        } else {
            (frac + 1) / DIGITS_PER_WORD as i8
        };
        let (int_word_cnt, frac_word_cnt) = (word_cnt!(self.int_cnt), word_cnt!(self.frac_cnt));

        let mut res = if int_word_cnt as i8 + frac_words_to > word_buf_len as i8 {
            frac_words_to = word_buf_len as i8 - int_word_cnt as i8;
            frac = frac_words_to * DIGITS_PER_WORD as i8;
            Res::Truncated(self)
        } else if self.int_cnt as i8 + frac < 0 {
            return Res::Ok(Self::zero());
        } else {
            Res::Ok(self)
        };
        res.int_cnt = cmp::min(int_word_cnt, word_buf_len) * DIGITS_PER_WORD;
        if frac_words_to > frac_word_cnt as i8 {
            for idx in int_word_cnt + frac_word_cnt..int_word_cnt + frac_words_to as u8 {
                res.word_buf[idx as usize] = 0;
            }
            res.frac_cnt = frac as u8;
            return res;
        }
        if frac >= res.frac_cnt as i8 {
            res.frac_cnt = frac as u8;
            return res;
        }

        Decimal::handle_incr(
            res,
            int_word_cnt,
            frac_words_to,
            frac,
            frac_word_cnt,
            word_buf_len,
            round_mode,
        )
    }

    fn handle_incr(
        mut res: Res<Decimal>,
        int_word_cnt: u8,
        frac_words_to: i8,
        frac: i8,
        frac_word_cnt: u8,
        word_buf_len: u8,
        round_mode: RoundMode,
    ) -> Res<Decimal> {
        // Do increment
        let mut to_idx = int_word_cnt as i8 + frac_words_to - 1;
        if frac == frac_words_to * DIGITS_PER_WORD as i8 {
            let do_inc = match round_mode {
                // Notice: No support for ceiling mode now.
                RoundMode::Ceiling => {
                    // If any word after scale is not zero, do increment.
                    // e.g ceiling 3.0001 to scale 1, gets 3.1
                    let idx = to_idx + frac_word_cnt as i8 - frac_words_to;
                    if idx > to_idx {
                        res.word_buf[(to_idx + 1) as usize..=(idx as usize)]
                            .iter()
                            .any(|c| *c != 0)
                    } else {
                        false
                    }
                }
                RoundMode::HalfEven => {
                    // If first digit after scale is 5 and round even,
                    // do increment if digit at scale is odd.
                    res.word_buf[(to_idx + 1) as usize] / DIG_MASK >= 5
                }
                RoundMode::Truncate => false,
            };
            if do_inc {
                if to_idx >= 0 {
                    res.word_buf[to_idx as usize] += 1;
                } else {
                    to_idx += 1;
                    res.word_buf[to_idx as usize] = WORD_BASE;
                }
            } else if int_word_cnt as i8 + frac_words_to == 0 {
                return Res::Ok(Self::zero());
            }
        } else {
            // TODO - fix this code as it won't work for CEILING mode
            let pos = (frac_words_to * DIGITS_PER_WORD as i8 - frac - 1) as usize;
            let mut shifted_number = res.word_buf[to_idx as usize] / TEN_POW[pos];
            let dig_after_scale = shifted_number % 10;
            let round_digit = match round_mode {
                RoundMode::Ceiling => 0,
                RoundMode::HalfEven => 5,
                RoundMode::Truncate => 10,
            };
            if dig_after_scale > round_digit || (round_digit == 5 && dig_after_scale == 5) {
                shifted_number += 10;
            }
            res.word_buf[to_idx as usize] = TEN_POW[pos] * (shifted_number - dig_after_scale);
        }

        if frac_words_to < frac_word_cnt as i8 {
            let idx = if frac == 0 && int_word_cnt == 0 {
                1
            } else {
                (int_word_cnt as i8 + frac_words_to) as usize
            };
            for i in idx..word_buf_len as usize {
                res.word_buf[i] = 0;
            }
        }

        Decimal::handle_carry(
            res,
            to_idx as usize,
            frac,
            frac_words_to,
            int_word_cnt,
            word_buf_len,
        )
    }

    fn handle_carry(
        mut dec: Res<Decimal>,
        mut to_idx: usize,
        mut frac: i8,
        mut frac_word_to: i8,
        int_word_cnt: u8,
        word_buf_len: u8,
    ) -> Res<Decimal> {
        if dec.word_buf[to_idx] >= WORD_BASE {
            let mut carry = 1;
            dec.word_buf[to_idx] -= WORD_BASE;
            while carry == 1 && to_idx > 0 {
                to_idx -= 1;
                add(
                    dec.word_buf[to_idx],
                    0,
                    &mut carry,
                    &mut dec.word_buf[to_idx],
                );
            }
            if carry > 0 {
                if int_word_cnt as i8 + frac_word_to >= word_buf_len as i8 {
                    frac_word_to -= 1;
                    frac = frac_word_to * DIGITS_PER_WORD as i8;
                    dec = Res::Truncated(dec.unwrap());
                }
                for i in (0..int_word_cnt as usize + cmp::max(frac_word_to, 0) as usize).rev() {
                    if i + 1 < word_buf_len as usize {
                        dec.word_buf[i + 1] = dec.word_buf[i];
                    } else if !dec.is_overflow() {
                        dec = Res::Overflow(dec.unwrap());
                    }
                }
                to_idx = 0;
                dec.word_buf[0] = 1;
                if dec.int_cnt < DIGITS_PER_WORD * word_buf_len {
                    dec.int_cnt += 1;
                } else {
                    dec = Res::Overflow(dec.unwrap());
                }
            }
        } else {
            while dec.word_buf[to_idx] == 0 {
                if to_idx == 0 {
                    let idx = frac_word_to + 1;
                    dec.int_cnt = 1;
                    dec.negative = false;
                    dec.frac_cnt = cmp::max(0, frac) as u8;
                    for i in 0..idx {
                        dec.word_buf[i as usize] = 0;
                    }
                    return Res::Ok(dec.unwrap());
                }
                to_idx -= 1;
            }
        }
        let first_dig = dec.int_cnt % DIGITS_PER_WORD;
        if first_dig > 0 && dec.word_buf[to_idx] >= TEN_POW[first_dig as usize] {
            dec.int_cnt += 1;
        }
        dec.frac_cnt = cmp::max(0, frac) as u8;
        dec
    }

    /// `shift` shifts decimal digits in given number (with rounding if it need),
    /// shift > 0 means shift to left shift, shift < 0 means right shift.
    ///
    /// In fact it is multiplying on 10^shift.
    pub fn shift(self, shift: isize) -> Res<Decimal> {
        self.shift_with_word_buf_len(shift, WORD_BUF_LEN)
    }

    fn shift_with_word_buf_len(self, shift: isize, word_buf_len: u8) -> Res<Decimal> {
        if shift == 0 {
            return Res::Ok(self);
        }
        let (mut beg, mut end) = self.digit_bounds();
        if beg == end {
            return Res::Ok(Self::zero());
        }

        let upper = (DIGITS_PER_WORD * word_buf_len * 2) as isize;
        if shift > upper {
            // process overflow by shift.
            return Res::Overflow(self);
        } else if shift < -upper {
            // processor truncated by shift.
            return Res::Truncated(Self::zero());
        }

        let point = word_cnt!(self.int_cnt) * DIGITS_PER_WORD;
        let mut new_point = point as isize + shift;
        let int_cnt = if new_point < beg as isize {
            0
        } else {
            new_point - beg as isize
        };
        let mut frac_cnt = if new_point > end as isize {
            0
        } else {
            end as isize - new_point
        };
        let int_word_cnt = word_cnt!(int_cnt, isize);
        let mut frac_word_cnt = word_cnt!(frac_cnt, isize);
        let new_len = int_word_cnt + frac_word_cnt;
        let mut res = if new_len > word_buf_len as isize {
            let lack = new_len - word_buf_len as isize;
            if frac_word_cnt < lack {
                return Res::Overflow(self);
            }
            frac_word_cnt -= lack;
            let diff = frac_cnt - frac_word_cnt * DIGITS_PER_WORD as isize;
            frac_cnt = frac_word_cnt * DIGITS_PER_WORD as isize;
            if end as isize - diff <= beg as isize {
                return Res::Truncated(Self::zero());
            }
            end = (end as isize - diff) as u8;
            Res::Truncated(
                self.round_with_word_buf_len(
                    end as i8 - point as i8,
                    word_buf_len,
                    RoundMode::HalfEven,
                )
                .unwrap(),
            )
        } else {
            Res::Ok(self)
        };

        if shift % DIGITS_PER_WORD as isize != 0 {
            let (l_mini_shift, r_mini_shift, mini_shift, do_left);
            if shift > 0 {
                l_mini_shift = (shift % DIGITS_PER_WORD as isize) as u8;
                r_mini_shift = DIGITS_PER_WORD - l_mini_shift;
                do_left = l_mini_shift <= beg;
            } else {
                r_mini_shift = ((-shift) % DIGITS_PER_WORD as isize) as u8;
                l_mini_shift = DIGITS_PER_WORD - r_mini_shift;
                do_left = (DIGITS_PER_WORD * word_buf_len - end) < r_mini_shift;
            }
            if do_left {
                res = res.map(|d| d.do_mini_left_shift(l_mini_shift, beg, end));
                mini_shift = -(l_mini_shift as i8);
            } else {
                res = res.map(|d| d.do_mini_right_shift(r_mini_shift, beg, end));
                mini_shift = r_mini_shift as i8;
            }
            new_point += mini_shift as isize;
            if shift + mini_shift as isize == 0 && (new_point - int_cnt) < DIGITS_PER_WORD as isize
            {
                res.int_cnt = int_cnt as u8;
                res.frac_cnt = frac_cnt as u8;
                return res;
            }
            beg = (beg as i8 + mini_shift) as u8;
            end = (end as i8 + mini_shift) as u8;
        }

        let new_front = new_point - int_cnt;
        if new_front >= DIGITS_PER_WORD as isize || new_front < 0 {
            let mut word_shift;
            if new_front > 0 {
                word_shift = new_front / DIGITS_PER_WORD as isize;
                let to = ((beg / DIGITS_PER_WORD) as isize - word_shift) as usize;
                let barier = (((end - 1) / DIGITS_PER_WORD) as isize - word_shift) as usize;
                for i in to..=barier {
                    res.word_buf[i] = res.word_buf[i + word_shift as usize];
                }
                for i in barier + 1..=barier + word_shift as usize {
                    res.word_buf[i] = 0;
                }
                word_shift = -word_shift;
            } else {
                word_shift = (1 - new_front) / DIGITS_PER_WORD as isize;
                let to = (((end - 1) / DIGITS_PER_WORD) as isize + word_shift) as usize;
                let barier = ((beg / DIGITS_PER_WORD) as isize + word_shift) as usize;
                for i in (barier..=to).rev() {
                    res.word_buf[i] = res.word_buf[i - word_shift as usize];
                }
                for i in barier - word_shift as usize..barier {
                    res.word_buf[i] = 0;
                }
            }
            let shift_cnt = word_shift * DIGITS_PER_WORD as isize;
            beg = (beg as isize + shift_cnt) as u8;
            end = (end as isize + shift_cnt) as u8;
            new_point += shift_cnt;
        }
        let beg_word = (beg / DIGITS_PER_WORD) as isize;
        let end_word = ((end - 1) / DIGITS_PER_WORD) as isize;
        let new_point_word = if new_point != 0 {
            (new_point - 1) / DIGITS_PER_WORD as isize
        } else {
            0
        };
        if new_point_word > end_word {
            for i in end_word + 1..=new_point_word {
                res.word_buf[i as usize] = 0;
            }
        } else {
            for i in new_point_word..beg_word {
                res.word_buf[i as usize] = 0;
            }
        }
        res.int_cnt = int_cnt as u8;
        res.frac_cnt = frac_cnt as u8;
        res
    }

    /// `as_i64` returns int part of the decimal.
    pub fn as_i64(&self) -> Res<i64> {
        let mut x = 0i64;
        let int_word_cnt = word_cnt!(self.int_cnt) as usize;
        for word_idx in 0..int_word_cnt {
            let y = x;
            x = x
                .wrapping_mul(i64::from(WORD_BASE))
                .wrapping_sub(i64::from(self.word_buf[word_idx]));
            if y < i64::MIN / i64::from(WORD_BASE) || x > y {
                if self.negative {
                    return Res::Overflow(i64::MIN);
                }
                return Res::Overflow(i64::MAX);
            }
        }
        if !self.negative && x == i64::MIN {
            return Res::Overflow(i64::MAX);
        }
        if !self.negative {
            x = -x;
        }
        for word_idx in int_word_cnt..int_word_cnt + word_cnt!(self.frac_cnt) as usize {
            if self.word_buf[word_idx] != 0 {
                return Res::Truncated(x);
            }
        }
        Res::Ok(x)
    }

    /// `as_i64_with_ctx` returns int part of the decimal.
    pub fn as_i64_with_ctx(&self, ctx: &mut EvalContext) -> Result<i64> {
        let res = self.as_i64();
        ctx.handle_truncate(res.is_truncated())?;
        res.into()
    }

    /// `as_u64` returns int part of the decimal
    pub fn as_u64(&self) -> Res<u64> {
        if self.negative {
            return Res::Overflow(0);
        }
        let mut x = 0u64;
        let int_cnt = word_cnt!(self.int_cnt) as usize;
        for word_idx in 0..int_cnt {
            x = match x.overflowing_mul(u64::from(WORD_BASE)) {
                (_, true) => return Res::Overflow(u64::MAX),
                (x, _) => match x.overflowing_add(u64::from(self.word_buf[word_idx])) {
                    (_, true) => return Res::Overflow(u64::MAX),
                    (x, _) => x,
                },
            };
        }
        for word_idx in int_cnt..int_cnt + word_cnt!(self.frac_cnt) as usize {
            if self.word_buf[word_idx] != 0 {
                return Res::Truncated(x);
            }
        }
        Res::Ok(x)
    }

    pub fn from_f64(val: f64) -> Result<Decimal> {
        if val.is_infinite() {
            Err(invalid_type!("{} can't be convert to decimal'", val))
        } else {
            let r = val.to_string();
            Decimal::from_str(r.as_str())
        }
    }

    /// Returns a `Decimal` from a given bytes slice
    ///
    /// # Notes
    ///
    /// An error will be returned if the given input is as follows:
    /// 1. empty string
    /// 2. string which cannot be converted to decimal
    pub fn from_bytes(s: &[u8]) -> Result<Res<Decimal>> {
        Decimal::from_bytes_with_word_buf(s, WORD_BUF_LEN)
    }

    /// Returns a `Decimal` from a given bytes slice buffer and specified buffer length
    ///
    /// # Notes
    ///
    /// An error will be returned if the given input is as follows:
    /// 1. an empty string
    /// 2. a string which cannot be converted to decimal
    fn from_bytes_with_word_buf(s: &[u8], word_buf_len: u8) -> Result<Res<Decimal>> {
        // trim whitespace
        let mut bs = match s.iter().position(|c| !c.is_ascii_whitespace()) {
            //TODO: return badnumber
            None => return Err(box_err!("\"{}\" is empty", escape(s))),
            Some(pos) => &s[pos..],
        };
        let mut negative = false;
        match bs[0] {
            b'-' => {
                negative = true;
                bs = &bs[1..];
            }
            b'+' => bs = &bs[1..],
            _ => {}
        }
        let int_idx = first_non_digit(bs, 0);
        let mut int_cnt = int_idx;
        let mut end_idx = int_idx;
        let mut frac_cnt = if int_idx < bs.len() && bs[int_idx] == b'.' {
            end_idx = first_non_digit(bs, int_idx + 1);
            end_idx - int_idx - 1
        } else {
            0
        };
        if int_cnt + frac_cnt == 0 {
            // TODO: bad number
            return Err(box_err!("\"{}\" is invalid number", escape(s)));
        }
        let int_word_cnt = word_cnt!(int_cnt);
        let frac_word_cnt = word_cnt!(frac_cnt);
        let res = fix_word_cnt_err(int_word_cnt, frac_word_cnt, word_buf_len);
        let (int_word_cnt, frac_word_cnt) = (res.0, res.1);
        if !res.is_ok() {
            frac_cnt = (frac_word_cnt * DIGITS_PER_WORD) as usize;
            if res.is_overflow() {
                int_cnt = (int_word_cnt * DIGITS_PER_WORD) as usize;
            }
        }
        let mut d = res.map(|_| Decimal::new(int_cnt as u8, frac_cnt as u8, negative));
        let mut inner_idx = 0;
        let mut word_idx = int_word_cnt as usize;
        let mut word = 0;
        for c in bs[int_idx - int_cnt as usize..int_idx].iter().rev() {
            word += u32::from(c - b'0') * TEN_POW[inner_idx];
            inner_idx += 1;
            if inner_idx == DIGITS_PER_WORD as usize {
                //TODO overflow
                word_idx -= 1;
                d.word_buf[word_idx] = word;
                word = 0;
                inner_idx = 0;
            }
        }
        if inner_idx != 0 {
            word_idx -= 1;
            d.word_buf[word_idx] = word;
        }

        word_idx = int_word_cnt as usize;
        word = 0;
        inner_idx = 0;
        for &c in bs.iter().skip(int_idx + 1).take(frac_cnt as usize) {
            word = u32::from(c - b'0') + word * 10;
            inner_idx += 1;
            if inner_idx == DIGITS_PER_WORD as usize {
                d.word_buf[word_idx] = word;
                word_idx += 1;
                word = 0;
                inner_idx = 0;
            }
        }
        if inner_idx != 0 {
            d.word_buf[word_idx] = word * TEN_POW[DIGITS_PER_WORD as usize - inner_idx];
        }
        if end_idx < bs.len() && (bs[end_idx] == b'e' || bs[end_idx] == b'E') {
            let exp = convert::bytes_to_int_without_context(&bs[end_idx + 1..])?;
            if exp > i64::from(i32::MAX) / 2 {
                return Ok(Res::Overflow(max_or_min_dec(
                    d.negative,
                    WORD_BUF_LEN * DIGITS_PER_WORD,
                    0,
                )));
            }
            if exp < i64::from(i32::MIN) / 2 && !d.is_overflow() {
                return Ok(Res::Truncated(Self::zero()));
            }
            if !d.is_overflow() {
                let is_truncated = d.is_truncated();
                d = match d.unwrap().shift(exp as isize) {
                    Res::Overflow(v) => Res::Overflow(max_or_min_dec(
                        v.negative,
                        WORD_BUF_LEN * DIGITS_PER_WORD,
                        0,
                    )),
                    Res::Ok(v) => {
                        if is_truncated {
                            Res::Truncated(v)
                        } else {
                            Res::Ok(v)
                        }
                    }
                    res => res,
                };
            }
        }
        if d.word_buf.iter().all(|c| *c == 0) {
            d.negative = false;
        }
        d.result_frac_cnt = d.frac_cnt;
        Ok(d)
    }

    /// Get the approximate needed capacity to encode this decimal.
    ///
    /// see also `encode_decimal`.
    pub fn approximate_encoded_size(&self) -> usize {
        let (prec, frac) = self.prec_and_frac();
        dec_encoded_len(&[prec, frac]).unwrap_or(3)
    }

    fn div(&self, rhs: &Decimal, frac_incr: u8) -> Option<Res<Decimal>> {
        let result_frac_cnt =
            cmp::min(self.result_frac_cnt.saturating_add(frac_incr), MAX_FRACTION);
        let mut res = do_div_mod(self, rhs, frac_incr, false);
        if let Some(ref mut dec) = res {
            dec.result_frac_cnt = result_frac_cnt;
        }
        res
    }

    pub fn is_zero(&self) -> bool {
        let len = word_cnt!(self.int_cnt) + word_cnt!(self.frac_cnt);
        self.word_buf[0..len as usize].iter().all(|&x| x == 0)
    }
}

macro_rules! enable_conv_for_int {
    ($s:ty, $t:ty) => {
        impl From<$s> for Decimal {
            fn from(t: $s) -> Decimal {
                #[allow(clippy::cast_lossless)]
                (t as $t).into()
            }
        }
    };
}

enable_conv_for_int!(u32, u64);
enable_conv_for_int!(u16, u64);
enable_conv_for_int!(u8, u64);
enable_conv_for_int!(i32, i64);
enable_conv_for_int!(i16, i64);
enable_conv_for_int!(i8, i64);
enable_conv_for_int!(usize, u64);
enable_conv_for_int!(isize, i64);

impl ConvertTo<f64> for Decimal {
    /// This function should not return err,
    /// if it return err, then the err because of bug.
    ///
    /// Port from TiDB's MyDecimal::ToFloat64.
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<f64> {
        let r = self.to_string().parse::<f64>();
        debug_assert!(r.is_ok());
        Ok(r?)
    }
}

impl From<i64> for Decimal {
    fn from(i: i64) -> Decimal {
        let (neg, mut d) = if i < 0 {
            (true, Decimal::from(i.overflowing_neg().0 as u64))
        } else {
            (false, Decimal::from(i as u64))
        };
        d.negative = neg;
        d
    }
}

impl From<u64> for Decimal {
    fn from(u: u64) -> Decimal {
        let (mut x, mut word_idx) = (u, 1);
        while x >= u64::from(WORD_BASE) {
            word_idx += 1;
            x /= u64::from(WORD_BASE);
        }
        let mut d = Decimal::new(word_idx * DIGITS_PER_WORD, 0, false);
        x = u;
        while word_idx > 0 {
            word_idx -= 1;
            d.word_buf[word_idx as usize] = (x % u64::from(WORD_BASE)) as u32;
            x /= u64::from(WORD_BASE);
        }
        d
    }
}

impl ConvertTo<Decimal> for i64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Decimal> {
        Ok(Decimal::from(*self))
    }
}

impl ConvertTo<Decimal> for u64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Decimal> {
        Ok(Decimal::from(*self))
    }
}

impl ConvertTo<Decimal> for f64 {
    /// Convert a float number to decimal.
    ///
    /// This function will use float's canonical string representation
    /// rather than the accurate value the float represent.
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Decimal> {
        Decimal::from_f64(*self)
    }
}

impl ConvertTo<Decimal> for Real {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        self.into_inner().convert(ctx)
    }
}

impl ConvertTo<Decimal> for &[u8] {
    // FIXME: the err handle is not exactly same as TiDB's,
    //  TiDB's seems has bug, fix this after fix TiDB's
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        let r = Decimal::from_bytes(self).unwrap_or_else(|_| Res::Ok(Decimal::zero()));
        let err = Error::overflow("DECIMAL", "");
        r.into_result_with_overflow_err(ctx, err)
    }
}

impl ConvertTo<Decimal> for std::borrow::Cow<'_, [u8]> {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        self.as_ref().convert(ctx)
    }
}

impl ConvertTo<Decimal> for Bytes {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        self.as_slice().convert(ctx)
    }
}

impl ConvertTo<Decimal> for Json {
    /// Port from TiDB's types.ConvertJSONToDecimal
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        self.as_ref().convert(ctx)
    }
}

impl<'a> ConvertTo<Decimal> for JsonRef<'a> {
    /// Port from TiDB's types.ConvertJSONToDecimal
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        match self.get_type() {
            JsonType::String => {
                Decimal::from_str(self.get_str()?).or_else(|e| {
                    ctx.handle_truncate_err(e)?;
                    // FIXME: if TiDB's MyDecimal::FromString return err,
                    //  it may has res. However, if TiKV's Decimal::from_str
                    //  return err, it has no res, so I return zero here,
                    //  but it may different from TiDB's MyDecimal::FromString
                    Ok(Decimal::zero())
                })
            }
            _ => {
                let r: f64 = self.convert(ctx)?;
                Decimal::from_f64(r)
            }
        }
    }
}

/// Get the first non-digit ascii char in `bs` from `start_idx`.
fn first_non_digit(bs: &[u8], start_idx: usize) -> usize {
    bs.iter()
        .skip(start_idx)
        .position(|c| !(b'0'..=b'9').contains(c))
        .map_or_else(|| bs.len(), |s| s + start_idx)
}

impl FromStr for Decimal {
    type Err = Error;

    fn from_str(s: &str) -> Result<Decimal> {
        match Decimal::from_bytes(s.as_bytes())? {
            Res::Ok(d) => Ok(d),
            Res::Overflow(_) => Err(box_err!("parsing {} will overflow", s)),
            Res::Truncated(_) => Err(box_err!("parsing {} will truncated", s)),
        }
    }
}

impl ToString for Decimal {
    fn to_string(&self) -> String {
        let (mut buf, word_start_idx, int_len, int_cnt, frac_cnt) = self.prepare_buf();
        if self.negative {
            buf.push(b'-');
        }
        let padding = int_len - cmp::max(int_cnt, 1);
        buf.resize(padding as usize + buf.len(), b'0');
        if int_cnt > 0 {
            let base_idx = buf.len();
            let mut idx = base_idx + int_cnt as usize;
            let mut widx = word_start_idx + word_cnt!(int_cnt) as usize;
            buf.resize(idx, 0);
            while idx > base_idx {
                widx -= 1;
                let mut x = self.word_buf[widx];
                for _ in 0..cmp::min((idx - base_idx) as u8, DIGITS_PER_WORD) {
                    idx -= 1;
                    buf[idx] = b'0' + (x % 10) as u8;
                    x /= 10;
                }
            }
        } else {
            buf.push(b'0');
        };
        if frac_cnt > 0 {
            buf.push(b'.');
            let mut widx = word_start_idx + word_cnt!(int_cnt) as usize;
            let exp_idx = buf.len() + frac_cnt as usize;
            while buf.len() < exp_idx {
                let mut x = self.word_buf[widx];
                for _ in 0..cmp::min((exp_idx - buf.len()) as u8, DIGITS_PER_WORD) {
                    buf.push((x / DIG_MASK) as u8 + b'0');
                    x = (x % DIG_MASK) * 10;
                }
                widx += 1;
            }
            while buf.capacity() != buf.len() {
                buf.push(b'0');
            }
        }
        unsafe { String::from_utf8_unchecked(buf) }
    }
}

impl Display for Decimal {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        let mut dec = *self;
        dec = dec
            .round(self.result_frac_cnt as i8, RoundMode::HalfEven)
            .unwrap();
        fmt.write_str(&dec.to_string())
    }
}

impl crate::codec::data_type::AsMySQLBool for Decimal {
    #[inline]
    fn as_mysql_bool(&self, _ctx: &mut EvalContext) -> crate::codec::Result<bool> {
        Ok(!self.is_zero())
    }
}

macro_rules! write_u8 {
    ($writer:ident, $b:expr, $written:ident) => {{
        let mut b = $b;
        if $written == 0 {
            b ^= 0x80;
        }
        $writer.write_bytes(&[b])?;
        $written += 1;
    }};
}

macro_rules! write_word {
    ($writer:expr, $word:expr, $size:expr, $written:ident) => {{
        let word = $word;
        let size = $size;
        let mut data: [u8; 4] = match size {
            1 => [word as u8, 0, 0, 0],
            2 => [(word >> 8) as u8, word as u8, 0, 0],
            3 => [(word >> 16) as u8, (word >> 8) as u8, word as u8, 0],
            4 => [
                (word >> 24) as u8,
                (word >> 16) as u8,
                (word >> 8) as u8,
                word as u8,
            ],
            _ => unreachable!(),
        };
        if $written == 0 {
            data[0] ^= 0x80;
        }
        ($writer).write_bytes(&data[..size as usize])?;
        $written += size;
    }};
}

pub trait DecimalEncoder: NumberEncoder {
    /// Encode decimal to comparable bytes.
    // TODO: resolve following warnings.
    fn write_decimal(&mut self, d: &Decimal, prec: u8, frac: u8) -> Result<Res<()>> {
        self.write_bytes(&[prec, frac])?;
        let mut mask = if d.negative { u32::MAX } else { 0 };
        let mut int_cnt = prec - frac;
        let int_word_cnt = int_cnt / DIGITS_PER_WORD;
        let leading_digits = (int_cnt - int_word_cnt * DIGITS_PER_WORD) as usize;

        let frac_word_cnt = frac / DIGITS_PER_WORD;
        let trailing_digits = (frac - frac_word_cnt * DIGITS_PER_WORD) as usize;
        let mut src_frac_word_cnt = d.frac_cnt / DIGITS_PER_WORD;
        let mut src_trailing_digits = (d.frac_cnt - src_frac_word_cnt * DIGITS_PER_WORD) as usize;

        let int_size = int_word_cnt * WORD_SIZE + DIG_2_BYTES[leading_digits];
        let mut frac_size = frac_word_cnt * WORD_SIZE + DIG_2_BYTES[trailing_digits];
        let src_frac_size = src_frac_word_cnt * WORD_SIZE + DIG_2_BYTES[src_trailing_digits];

        let (mut src_word_start_idx, src_int_cnt) = d.remove_leading_zeroes(d.int_cnt);
        if src_int_cnt + src_frac_size == 0 {
            mask = 0;
            int_cnt = 1;
        }

        let mut src_int_word_cnt = src_int_cnt / DIGITS_PER_WORD;
        let mut src_leading_digits = (src_int_cnt - src_int_word_cnt * DIGITS_PER_WORD) as usize;
        let src_int_size = src_int_word_cnt * WORD_SIZE + DIG_2_BYTES[src_leading_digits];

        let mut written: usize = 0;
        let mut res = Res::Ok(());

        if int_cnt < src_int_cnt {
            src_word_start_idx += (src_int_word_cnt - int_word_cnt) as usize;
            if src_leading_digits > 0 {
                src_word_start_idx += 1;
            }
            if leading_digits > 0 {
                src_word_start_idx -= 1;
            }
            src_int_word_cnt = int_word_cnt;
            src_leading_digits = leading_digits;
            res = Res::Overflow(());
            error!(
                "encode decimal overflow";
                "from" => d.to_string(),
                "prec" => prec,
                "frac" => frac,
            );
        } else if int_size > src_int_size {
            for _ in src_int_size..int_size {
                write_u8!(self, mask as u8, written);
            }
        }

        if frac_size < src_frac_size {
            src_frac_word_cnt = frac_word_cnt;
            src_trailing_digits = trailing_digits;
            res = Res::Truncated(());
            warn!(
                "encode decimal truncated";
                "from" => d.to_string(),
                "prec" => prec,
                "frac" => frac,
            );
        } else if frac_size > src_frac_size && src_trailing_digits > 0 {
            if frac_word_cnt == src_frac_word_cnt {
                src_trailing_digits = trailing_digits;
                frac_size = src_frac_size;
            } else {
                src_frac_word_cnt += 1;
                src_trailing_digits = 0;
            }
        }

        if src_leading_digits > 0 {
            let i = DIG_2_BYTES[src_leading_digits] as usize;
            let x = (d.word_buf[src_word_start_idx] % TEN_POW[src_leading_digits]) ^ mask;
            src_word_start_idx += 1;
            write_word!(self, x, i, written);
        }

        let stop = src_word_start_idx + src_int_word_cnt as usize + src_frac_word_cnt as usize;
        while src_word_start_idx < stop {
            write_word!(self, d.word_buf[src_word_start_idx] ^ mask, 4, written);
            src_word_start_idx += 1;
        }

        if src_trailing_digits > 0 {
            let i = DIG_2_BYTES[src_trailing_digits];
            let lim = if src_frac_word_cnt < frac_word_cnt {
                DIGITS_PER_WORD as usize
            } else {
                trailing_digits
            };
            while src_trailing_digits < lim && DIG_2_BYTES[src_trailing_digits] == i {
                src_trailing_digits += 1;
            }
            let x = (d.word_buf[src_word_start_idx]
                / TEN_POW[DIGITS_PER_WORD as usize - src_trailing_digits])
                ^ mask;
            write_word!(self, x, i as usize, written);
        }

        if frac_size > src_frac_size {
            for _ in (src_frac_size..frac_size).zip(written..(int_size + frac_size) as usize) {
                write_u8!(self, mask as u8, written);
            }
        }
        Ok(res)
    }

    #[inline]
    fn write_decimal_to_chunk(&mut self, v: &Decimal) -> Result<()> {
        let data = unsafe {
            let p = v as *const Decimal as *const u8;
            std::slice::from_raw_parts(p, DECIMAL_STRUCT_SIZE)
        };
        self.write_bytes(data)?;
        Ok(())
    }
}

impl<T: BufferWriter> DecimalEncoder for T {}

pub trait DecimalDatumPayloadChunkEncoder: NumberEncoder + DecimalEncoder {
    #[inline]
    fn write_decimal_to_chunk_by_datum_payload(&mut self, mut src_payload: &[u8]) -> Result<()> {
        let decimal = src_payload.read_decimal()?;
        self.write_decimal_to_chunk(&decimal)
    }
}

impl<T: BufferWriter> DecimalDatumPayloadChunkEncoder for T {}

// Mark as `#[inline]` since in many cases `size` is a constant.
#[inline]
fn read_word<T: BufferReader + ?Sized>(
    data: &mut T,
    size: usize,
    is_first: &mut bool,
) -> Result<u32> {
    // Note: In TiDB's implementation, the first byte to read is flipped:
    // dCopy[0] ^= 0x80
    //
    // In TiKV, we do zero copy so that we need `is_first` flag.
    let buf = data.bytes();
    if buf.len() < size {
        return Err(Error::unexpected_eof());
    }
    let mut first = buf[0];
    if *is_first {
        first ^= 0x80;
        *is_first = false;
    }
    let res = match size {
        1 => i32::from(first as i8) as u32,
        2 => ((i32::from(first as i8) << 8) + i32::from(buf[1])) as u32,
        3 => {
            if first & 128 > 0 {
                (255 << 24)
                    | (u32::from(first) << 16)
                    | (u32::from(buf[1]) << 8)
                    | u32::from(buf[2])
            } else {
                (u32::from(first) << 16) | (u32::from(buf[1]) << 8) | u32::from(buf[2])
            }
        }
        4 => {
            ((i32::from(first as i8) << 24)
                + (i32::from(buf[1]) << 16)
                + (i32::from(buf[2]) << 8)
                + i32::from(buf[3])) as u32
        }
        _ => unreachable!(),
    };
    data.advance(size);
    Ok(res)
}

pub trait DecimalDecoder: NumberDecoder {
    /// `read_decimal` decodes value encoded by `write_decimal`.
    fn read_decimal(&mut self) -> Result<Decimal> {
        if self.bytes().len() < 3 {
            return Err(box_err!("decimal too short: {} < 3", self.bytes().len()));
        }
        let (prec, frac_cnt) = (self.read_u8().unwrap(), self.read_u8().unwrap());

        if prec < frac_cnt {
            return Err(box_err!(
                "invalid decimal, precision {} < frac_cnt {}",
                prec,
                frac_cnt
            ));
        }

        let int_cnt = prec - frac_cnt;
        let int_word_cnt = int_cnt / DIGITS_PER_WORD;
        let leading_digits = (int_cnt - int_word_cnt * DIGITS_PER_WORD) as usize;
        let frac_word_cnt = frac_cnt / DIGITS_PER_WORD;
        let trailing_digits = (frac_cnt - frac_word_cnt * DIGITS_PER_WORD) as usize;
        let mut int_word_to = int_word_cnt;
        if leading_digits > 0 {
            int_word_to += 1;
        }
        let mut frac_word_to = frac_word_cnt;
        if trailing_digits > 0 {
            frac_word_to += 1;
        }
        let mask = if self.bytes()[0] & 0x80 > 0 {
            0
        } else {
            u32::MAX
        };
        let res = fix_word_cnt_err(int_word_to, frac_word_to, WORD_BUF_LEN);
        if !res.is_ok() {
            return Err(box_err!("decoding decimal failed: {:?}", res));
        }
        let mut d = Decimal::new(int_cnt, frac_cnt, mask != 0);
        d.result_frac_cnt = frac_cnt;
        let mut word_idx = 0;
        let mut is_first = true;
        if leading_digits > 0 {
            let i = DIG_2_BYTES[leading_digits];
            d.word_buf[word_idx] = read_word(self, i as usize, &mut is_first)? ^ mask;
            if d.word_buf[word_idx] >= TEN_POW[leading_digits + 1] {
                return Err(box_err!("invalid leading digits for decimal number"));
            }
            if d.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                d.int_cnt -= leading_digits as u8;
            }
        }
        for _ in 0..int_word_cnt {
            d.word_buf[word_idx] = read_word(self, 4, &mut is_first)? ^ mask;
            if d.word_buf[word_idx] > WORD_MAX {
                return Err(box_err!("invalid int part for decimal number"));
            }
            if word_idx > 0 || d.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                d.int_cnt -= DIGITS_PER_WORD;
            }
        }
        for _ in 0..frac_word_cnt {
            d.word_buf[word_idx] = read_word(self, 4, &mut is_first)? ^ mask;
            if d.word_buf[word_idx] > WORD_MAX {
                return Err(box_err!("invalid frac part decimal number"));
            }
            word_idx += 1;
        }
        if trailing_digits > 0 {
            let x = read_word(self, DIG_2_BYTES[trailing_digits] as usize, &mut is_first)? ^ mask;
            d.word_buf[word_idx] =
                match x.checked_mul(TEN_POW[DIGITS_PER_WORD as usize - trailing_digits]) {
                    Some(v) if v <= WORD_MAX => v,
                    _ => {
                        return Err(box_err!("invalid trailing digits for decimal number"));
                    }
                }
        }
        if d.int_cnt == 0 && d.frac_cnt == 0 {
            d = Decimal::zero();
        }
        d.result_frac_cnt = frac_cnt;
        Ok(d)
    }

    /// `read_decimal_from_chunk` decode Decimal encoded by `write_decimal_to_chunk`.
    fn read_decimal_from_chunk(&mut self) -> Result<Decimal> {
        let buf = self.read_bytes(DECIMAL_STRUCT_SIZE)?;
        let d = unsafe {
            let mut d = mem::MaybeUninit::<Decimal>::uninit();
            let p = d.as_mut_ptr() as *mut u8;
            copy_nonoverlapping(buf.as_ptr(), p, DECIMAL_STRUCT_SIZE);
            d.assume_init()
        };
        Ok(d)
    }
}

impl<T: BufferReader> DecimalDecoder for T {}

impl PartialEq for Decimal {
    fn eq(&self, right: &Decimal) -> bool {
        self.cmp(right) == Ordering::Equal
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, right: &Decimal) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl Eq for Decimal {}

impl Ord for Decimal {
    fn cmp(&self, right: &Decimal) -> Ordering {
        if self.negative == right.negative {
            let (carry, ..) = calc_sub_carry(self, right);
            carry.map_or(Ordering::Equal, |carry| {
                if (carry > 0) == self.negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            })
        } else if self.negative {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

impl<'a, 'b> Add<&'a Decimal> for &'b Decimal {
    type Output = Res<Decimal>;

    fn add(self, rhs: &'a Decimal) -> Res<Decimal> {
        let result_frac_cnt = cmp::max(self.result_frac_cnt, rhs.result_frac_cnt);
        let mut res = if self.negative == rhs.negative {
            do_add(self, rhs)
        } else {
            do_sub(self, rhs)
        };
        res.result_frac_cnt = result_frac_cnt;
        res
    }
}

impl<'a, 'b> Sub<&'a Decimal> for &'b Decimal {
    type Output = Res<Decimal>;

    fn sub(self, rhs: &'a Decimal) -> Res<Decimal> {
        let result_frac_cnt = cmp::max(self.result_frac_cnt, rhs.result_frac_cnt);
        let mut res = if self.negative == rhs.negative {
            do_sub(self, rhs)
        } else {
            do_add(self, rhs)
        };
        res.result_frac_cnt = result_frac_cnt;
        res
    }
}

impl<'a, 'b> Mul<&'a Decimal> for &'b Decimal {
    type Output = Res<Decimal>;

    fn mul(self, rhs: &'a Decimal) -> Res<Decimal> {
        do_mul(self, rhs)
    }
}

impl<'a, 'b> Div<&'a Decimal> for &'b Decimal {
    type Output = Option<Res<Decimal>>;

    fn div(self, rhs: &'a Decimal) -> Self::Output {
        self.div(rhs, DEFAULT_DIV_FRAC_INCR)
    }
}

impl Rem for Decimal {
    type Output = Option<Res<Decimal>>;

    #[allow(clippy::op_ref)]
    fn rem(self, rhs: Decimal) -> Option<Res<Decimal>> {
        &self % &rhs
    }
}

impl<'a, 'b> Rem<&'a Decimal> for &'b Decimal {
    type Output = Option<Res<Decimal>>;
    fn rem(self, rhs: &'a Decimal) -> Self::Output {
        let result_frac_cnt = cmp::max(self.result_frac_cnt, rhs.result_frac_cnt);
        let mut res = do_div_mod_impl(self, rhs, 0, true);
        if let Some(ref mut dec) = res {
            dec.result_frac_cnt = result_frac_cnt;
        }
        res
    }
}

impl Neg for Decimal {
    type Output = Decimal;

    fn neg(mut self) -> Decimal {
        if !self.is_zero() {
            self.negative = !self.negative;
        }
        self
    }
}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (int_word_cnt, frac_word_cnt) = (word_cnt!(self.int_cnt), word_cnt!(self.frac_cnt));

        let (stop, mut idx) = (int_word_cnt as usize, 0usize);
        while idx < stop && self.word_buf[idx] == 0 {
            idx += 1;
        }
        let start = idx as usize;
        let int_word_cnt = stop - idx;

        int_word_cnt.hash(state);
        let mut end = (stop + frac_word_cnt as usize - 1) as isize;
        // trims suffix 0(also trims the suffix 0 before the point
        // when there is no digit after point).
        while start as isize <= end && self.word_buf[end as usize] == 0 {
            end -= 1;
        }

        self.word_buf[start..((end + 1) as usize)].hash(state);
        // -0 should be not negative.
        let negative = self.negative && (start as isize <= end);
        negative.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{DEFAULT_DIV_FRAC_INCR, WORD_BUF_LEN};

    use crate::codec::error::ERR_DATA_OUT_OF_RANGE;
    use crate::expr::{EvalConfig, Flag};
    use std::cmp::Ordering;
    use std::collections::hash_map::DefaultHasher;

    use std::sync::Arc;

    #[test]
    fn test_from_i64() {
        let cases = vec![
            (-12345i64, "-12345"),
            (-1, "-1"),
            (1, "1"),
            (-9223372036854775807, "-9223372036854775807"),
            (-9223372036854775808, "-9223372036854775808"),
        ];

        for (num, exp) in cases {
            let dec: Decimal = num.into();
            let dec_str = format!("{}", dec);
            assert_eq!(dec_str, exp);
        }
    }

    #[test]
    fn test_from_u64() {
        let cases = vec![
            (12345u64, "12345"),
            (0, "0"),
            (18446744073709551615, "18446744073709551615"),
        ];

        for (num, exp) in cases {
            let dec: Decimal = num.into();
            let dec_str = format!("{}", dec);
            assert_eq!(dec_str, exp);
        }
    }

    #[test]
    fn test_from_f64() {
        let cs = vec![
            (f64::INFINITY, Err(Error::InvalidDataType(String::new()))),
            (-f64::INFINITY, Err(Error::InvalidDataType(String::new()))),
            (10.123, Ok(Decimal::from_str("10.123").unwrap())),
            (-10.123, Ok(Decimal::from_str("-10.123").unwrap())),
            (10.111, Ok(Decimal::from_str("10.111").unwrap())),
            (-10.111, Ok(Decimal::from_str("-10.111").unwrap())),
            (
                18446744073709552000.0,
                Ok(Decimal::from_str("18446744073709552000").unwrap()),
            ),
            (
                -18446744073709552000.0,
                Ok(Decimal::from_str("-18446744073709552000").unwrap()),
            ),
            // FIXME: because of rust's bug,
            //  (1<<64)(18446744073709551616), (1<<65)(36893488147419103232) can not be represent by f64
            //  so these cases can not pass
            // (18446744073709551616.0, Ok(Decimal::from_str("18446744073709551616").unwrap())),
            // (-18446744073709551616.0, Ok(Decimal::from_str("-18446744073709551616").unwrap())),
            // (36893488147419103000.0, Ok(Decimal::from_str("36893488147419103000.0").unwrap())),
            // (-36893488147419103000.0, Ok(Decimal::from_str("-36893488147419103000.0").unwrap())),
            (
                36893488147419103000.0,
                Ok(Decimal::from_str("36893488147419103000.0").unwrap()),
            ),
            (
                -36893488147419103000.0,
                Ok(Decimal::from_str("-36893488147419103000.0").unwrap()),
            ),
        ];
        for (input, expect) in cs {
            let r = Decimal::from_f64(input);
            let log = format!(
                "input: {}, expect: {:?}, output: {:?}",
                input,
                expect.as_ref().map(|x| x.to_string()),
                r.as_ref().map(|x| x.to_string())
            );
            match expect {
                Err(e) => {
                    assert!(r.is_err(), "{}", log.as_str());
                    match e {
                        Error::InvalidDataType(_) => (),
                        _ => panic!("{}", log.as_str()),
                    }
                }
                Ok(d) => {
                    assert!(r.is_ok(), "{}", log.as_str());
                    assert_eq!(r.unwrap(), d, "{}", log.as_str());
                }
            }
        }
    }

    #[test]
    fn test_to_i64() {
        let cases = vec![
            (
                "18446744073709551615",
                Res::Overflow(9223372036854775807i64),
            ),
            ("-1", Res::Ok(-1)),
            ("1", Res::Ok(1)),
            ("-1.23", Res::Truncated(-1)),
            ("-9223372036854775807", Res::Ok(-9223372036854775807)),
            ("-9223372036854775808", Res::Ok(-9223372036854775808)),
            ("9223372036854775808", Res::Overflow(9223372036854775807)),
            ("-9223372036854775809", Res::Overflow(-9223372036854775808)),
        ];

        for (dec_str, exp) in cases {
            let dec: Decimal = dec_str.parse().unwrap();
            let i = dec.as_i64();
            assert_eq!(i, exp);
        }
    }

    #[test]
    fn test_to_u64() {
        let cases = vec![
            ("12345", Res::Ok(12345u64)),
            ("0", Res::Ok(0)),
            // ULLONG_MAX = 18446744073709551615ULL
            ("18446744073709551615", Res::Ok(18446744073709551615)),
            ("18446744073709551616", Res::Overflow(18446744073709551615)),
            ("-1", Res::Overflow(0)),
            ("1.23", Res::Truncated(1)),
            (
                "9999999999999999999999999.000",
                Res::Overflow(18446744073709551615),
            ),
        ];

        for (dec_str, exp) in cases {
            let dec: Decimal = dec_str.parse().unwrap();
            let i = dec.as_u64();
            assert_eq!(i, exp);
        }
    }

    #[test]
    #[allow(clippy::approx_constant, clippy::excessive_precision)]
    fn test_to_f64() {
        let cases = vec![
            ("12345", 12345f64),
            ("123.45", 123.45),
            ("-123.45", -123.45),
            ("0.00012345000098765", 0.00012345000098765),
            ("1234500009876.5", 1234500009876.5),
            ("3.141592653589793", 3.141592653589793),
            ("3", 3f64),
            ("1234567890123456", 1234567890123456f64),
            ("1234567890123456000", 1234567890123456000f64),
            ("1234.567890123456", 1234.567890123456),
            ("0.1234567890123456", 0.1234567890123456),
            ("0", 0f64),
            ("0.111111111111111", 0.1111111111111110),
            ("0.1111111111111111", 0.1111111111111111),
            ("0.1111111111111119", 0.1111111111111119),
            ("0.000000000000000001", 0.000000000000000001),
            ("0.000000000000000002", 0.000000000000000002),
            ("0.000000000000000003", 0.000000000000000003),
            ("0.000000000000000005", 0.000000000000000005),
            ("0.000000000000000008", 0.000000000000000008),
            ("0.1000000000000001", 0.1000000000000001),
            ("0.1000000000000002", 0.1000000000000002),
            ("0.1000000000000003", 0.1000000000000003),
            ("0.1000000000000005", 0.1000000000000005),
            ("0.1000000000000008", 0.1000000000000008),
        ];

        let mut ctx = EvalContext::default();
        for (dec_str, exp) in cases {
            let dec = dec_str.parse::<Decimal>().unwrap();
            let res = format!("{}", dec);
            assert_eq!(res, dec_str);

            let f: f64 = dec.convert(&mut ctx).unwrap();
            assert!(
                (exp - f).abs() < f64::EPSILON,
                "expect: {}, got: {}",
                exp,
                f
            );
        }
    }

    #[test]
    fn test_shift() {
        let cases = vec![
            (
                WORD_BUF_LEN,
                b"123.123" as &'static [u8],
                1,
                Res::Ok("1231.23"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                1,
                Res::Ok("1234571891.23123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                8,
                Res::Ok("12345718912312345.6789"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                9,
                Res::Ok("123457189123123456.789"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                10,
                Res::Ok("1234571891231234567.89"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                17,
                Res::Ok("12345718912312345678900000"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                18,
                Res::Ok("123457189123123456789000000"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                19,
                Res::Ok("1234571891231234567890000000"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                26,
                Res::Ok("12345718912312345678900000000000000"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                27,
                Res::Ok("123457189123123456789000000000000000"),
            ),
            (
                WORD_BUF_LEN,
                b"123457189.123123456789000",
                28,
                Res::Ok("1234571891231234567890000000000000000"),
            ),
            (
                WORD_BUF_LEN,
                b"000000000000000000000000123457189.123123456789000",
                26,
                Res::Ok("12345718912312345678900000000000000"),
            ),
            (
                WORD_BUF_LEN,
                b"00000000123457189.123123456789000",
                27,
                Res::Ok("123457189123123456789000000000000000"),
            ),
            (
                WORD_BUF_LEN,
                b"00000000000000000123457189.123123456789000",
                28,
                Res::Ok("1234571891231234567890000000000000000"),
            ),
            (WORD_BUF_LEN, b"123", 1, Res::Ok("1230")),
            (WORD_BUF_LEN, b"123", 10, Res::Ok("1230000000000")),
            (WORD_BUF_LEN, b".123", 1, Res::Ok("1.23")),
            (WORD_BUF_LEN, b".123", 10, Res::Ok("1230000000")),
            (WORD_BUF_LEN, b".123", 14, Res::Ok("12300000000000")),
            (WORD_BUF_LEN, b"000.000", 1000, Res::Ok("0")),
            (WORD_BUF_LEN, b"000.", 1000, Res::Ok("0")),
            (WORD_BUF_LEN, b".000", 1000, Res::Ok("0")),
            (WORD_BUF_LEN, b"1", 1000, Res::Overflow("1")),
            (WORD_BUF_LEN, b"123.123", -1, Res::Ok("12.3123")),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -1,
                Res::Ok("12398765432.1123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -2,
                Res::Ok("1239876543.21123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -3,
                Res::Ok("123987654.321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -8,
                Res::Ok("1239.87654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -9,
                Res::Ok("123.987654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -10,
                Res::Ok("12.3987654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -11,
                Res::Ok("1.23987654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -12,
                Res::Ok("0.123987654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -13,
                Res::Ok("0.0123987654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"123987654321.123456789000",
                -14,
                Res::Ok("0.00123987654321123456789"),
            ),
            (
                WORD_BUF_LEN,
                b"00000087654321.123456789000",
                -14,
                Res::Ok("0.00000087654321123456789"),
            ),
            (2, b"123.123", -2, Res::Ok("1.23123")),
            (2, b"123.123", -3, Res::Ok("0.123123")),
            (2, b"123.123", -6, Res::Ok("0.000123123")),
            (2, b"123.123", -7, Res::Ok("0.0000123123")),
            (2, b"123.123", -15, Res::Ok("0.000000000000123123")),
            (2, b"123.123", -16, Res::Truncated("0.000000000000012312")),
            (2, b"123.123", -17, Res::Truncated("0.000000000000001231")),
            (2, b"123.123", -18, Res::Truncated("0.000000000000000123")),
            (2, b"123.123", -19, Res::Truncated("0.000000000000000012")),
            (2, b"123.123", -20, Res::Truncated("0.000000000000000001")),
            (2, b"123.123", -21, Res::Truncated("0")),
            (2, b".000000000123", -1, Res::Ok("0.0000000000123")),
            (2, b".000000000123", -6, Res::Ok("0.000000000000000123")),
            (
                2,
                b".000000000123",
                -7,
                Res::Truncated("0.000000000000000012"),
            ),
            (
                2,
                b".000000000123",
                -8,
                Res::Truncated("0.000000000000000001"),
            ),
            (2, b".000000000123", -9, Res::Truncated("0")),
            (2, b".000000000123", 1, Res::Ok("0.00000000123")),
            (2, b".000000000123", 8, Res::Ok("0.0123")),
            (2, b".000000000123", 9, Res::Ok("0.123")),
            (2, b".000000000123", 10, Res::Ok("1.23")),
            (2, b".000000000123", 17, Res::Ok("12300000")),
            (2, b".000000000123", 18, Res::Ok("123000000")),
            (2, b".000000000123", 19, Res::Ok("1230000000")),
            (2, b".000000000123", 20, Res::Ok("12300000000")),
            (2, b".000000000123", 21, Res::Ok("123000000000")),
            (2, b".000000000123", 22, Res::Ok("1230000000000")),
            (2, b".000000000123", 23, Res::Ok("12300000000000")),
            (2, b".000000000123", 24, Res::Ok("123000000000000")),
            (2, b".000000000123", 25, Res::Ok("1230000000000000")),
            (2, b".000000000123", 26, Res::Ok("12300000000000000")),
            (2, b".000000000123", 27, Res::Ok("123000000000000000")),
            (2, b".000000000123", 28, Res::Overflow("0.000000000123")),
            (
                2,
                b"123456789.987654321",
                -1,
                Res::Truncated("12345678.998765432"),
            ),
            (
                2,
                b"123456789.987654321",
                -2,
                Res::Truncated("1234567.899876543"),
            ),
            (2, b"123456789.987654321", -8, Res::Truncated("1.234567900")),
            (
                2,
                b"123456789.987654321",
                -9,
                Res::Ok("0.123456789987654321"),
            ),
            (
                2,
                b"123456789.987654321",
                -10,
                Res::Truncated("0.012345678998765432"),
            ),
            (
                2,
                b"123456789.987654321",
                -17,
                Res::Truncated("0.000000001234567900"),
            ),
            (
                2,
                b"123456789.987654321",
                -18,
                Res::Truncated("0.000000000123456790"),
            ),
            (
                2,
                b"123456789.987654321",
                -19,
                Res::Truncated("0.000000000012345679"),
            ),
            (
                2,
                b"123456789.987654321",
                -26,
                Res::Truncated("0.000000000000000001"),
            ),
            (2, b"123456789.987654321", -27, Res::Truncated("0")),
            (2, b"123456789.987654321", 1, Res::Truncated("1234567900")),
            (2, b"123456789.987654321", 2, Res::Truncated("12345678999")),
            (
                2,
                b"123456789.987654321",
                4,
                Res::Truncated("1234567899877"),
            ),
            (
                2,
                b"123456789.987654321",
                8,
                Res::Truncated("12345678998765432"),
            ),
            (2, b"123456789.987654321", 9, Res::Ok("123456789987654321")),
            (
                2,
                b"123456789.987654321",
                10,
                Res::Overflow("123456789.987654321"),
            ),
            (2, b"123456789.987654321", 0, Res::Ok("123456789.987654321")),
            (
                WORD_BUF_LEN,
                b"0.0000000070415291131966574",
                -9223372036854775808,
                Res::Truncated("0"),
            ),
            (
                WORD_BUF_LEN,
                b"0.0000000070415291131966574",
                9223372036854775807,
                Res::Overflow("0.0000000070415291131966574"),
            ),
        ];

        for (word_buf_len, dec, shift, exp) in cases {
            let dec = Decimal::from_bytes_with_word_buf(dec, word_buf_len)
                .unwrap()
                .unwrap();
            let shifted = dec.shift_with_word_buf_len(shift, word_buf_len);
            let res = shifted.map(|d| d.to_string());
            assert_eq!(res, exp.map(ToOwned::to_owned));
        }
    }

    #[test]
    fn test_round() {
        let cases = vec![
            (
                "123456789.987654321",
                1,
                Res::Ok("123456790.0"),
                Res::Ok("123456789.9"),
                Res::Ok("123456790.0"),
            ),
            ("15.1", 0, Res::Ok("15"), Res::Ok("15"), Res::Ok("16")),
            ("15.5", 0, Res::Ok("16"), Res::Ok("15"), Res::Ok("16")),
            ("15.9", 0, Res::Ok("16"), Res::Ok("15"), Res::Ok("16")),
            ("-15.1", 0, Res::Ok("-15"), Res::Ok("-15"), Res::Ok("-16")),
            ("-15.5", 0, Res::Ok("-16"), Res::Ok("-15"), Res::Ok("-16")),
            ("-15.9", 0, Res::Ok("-16"), Res::Ok("-15"), Res::Ok("-16")),
            ("15.1", 1, Res::Ok("15.1"), Res::Ok("15.1"), Res::Ok("15.1")),
            (
                "-15.1",
                1,
                Res::Ok("-15.1"),
                Res::Ok("-15.1"),
                Res::Ok("-15.1"),
            ),
            (
                "15.17",
                1,
                Res::Ok("15.2"),
                Res::Ok("15.1"),
                Res::Ok("15.2"),
            ),
            ("15.4", -1, Res::Ok("20"), Res::Ok("10"), Res::Ok("20")),
            ("-15.4", -1, Res::Ok("-20"), Res::Ok("-10"), Res::Ok("-20")),
            ("5.4", -1, Res::Ok("10"), Res::Ok("0"), Res::Ok("10")),
            (".999", 0, Res::Ok("1"), Res::Ok("0"), Res::Ok("1")),
            (
                "999999999",
                -9,
                Res::Ok("1000000000"),
                Res::Ok("0"),
                Res::Ok("1000000000"),
            ),
        ];

        for (dec_str, scale, half_exp, trunc_exp, ceil_exp) in cases {
            let dec = dec_str.parse::<Decimal>().unwrap();
            let res = dec.round(scale, RoundMode::HalfEven).map(|d| d.to_string());
            assert_eq!(res, half_exp.map(|s| s.to_owned()));
            let res = dec.round(scale, RoundMode::Truncate).map(|d| d.to_string());
            assert_eq!(res, trunc_exp.map(|s| s.to_owned()));
            let res = dec.round(scale, RoundMode::Ceiling).map(|d| d.to_string());
            assert_eq!(res, ceil_exp.map(|s| s.to_owned()));
        }
    }

    #[test]
    #[rustfmt::skip]
    fn test_string() {
        let cases = vec![
            (WORD_BUF_LEN, b"12345" as &'static [u8], Res::Ok("12345")),
            (WORD_BUF_LEN, b"12345.", Res::Ok("12345")),
            (WORD_BUF_LEN, b"123.45.", Res::Ok("123.45")),
            (WORD_BUF_LEN, b"-123.45.", Res::Ok("-123.45")),
            (
                WORD_BUF_LEN,
                b".00012345000098765",
                Res::Ok("0.00012345000098765"),
            ),
            (
                WORD_BUF_LEN,
                b".12345000098765",
                Res::Ok("0.12345000098765"),
            ),
            (
                WORD_BUF_LEN,
                b"-.000000012345000098765",
                Res::Ok("-0.000000012345000098765"),
            ),
            (WORD_BUF_LEN, b"1234500009876.5", Res::Ok("1234500009876.5")),
            (WORD_BUF_LEN, b"123E5", Res::Ok("12300000")),
            (WORD_BUF_LEN, b"123E-2", Res::Ok("1.23")),
            (1, b"123450000098765", Res::Overflow("98765")),
            (1, b"123450.000098765", Res::Truncated("123450")),
            (WORD_BUF_LEN, b"123.123", Res::Ok("123.123")),
            (WORD_BUF_LEN, b"123.1230", Res::Ok("123.1230")),
            (WORD_BUF_LEN, b"00123.123", Res::Ok("123.123")),
            (WORD_BUF_LEN, b"1.21", Res::Ok("1.21")),
            (WORD_BUF_LEN, b".21", Res::Ok("0.21")),
            (WORD_BUF_LEN, b"1.00", Res::Ok("1.00")),
            (WORD_BUF_LEN, b"100", Res::Ok("100")),
            (WORD_BUF_LEN, b"-100", Res::Ok("-100")),
            (WORD_BUF_LEN, b"100.00", Res::Ok("100.00")),
            (WORD_BUF_LEN, b"00100.00", Res::Ok("100.00")),
            (WORD_BUF_LEN, b"-100.00", Res::Ok("-100.00")),
            (WORD_BUF_LEN, b"-0.00", Res::Ok("0.00")),
            (WORD_BUF_LEN, b"00.00", Res::Ok("0.00")),
            (WORD_BUF_LEN, b"0.00", Res::Ok("0.00")),
            (WORD_BUF_LEN, b"-2.010", Res::Ok("-2.010")),
            (WORD_BUF_LEN, b"12345", Res::Ok("12345")),
            (WORD_BUF_LEN, b"-12345", Res::Ok("-12345")),
            (WORD_BUF_LEN, b"-3.", Res::Ok("-3")),
            (WORD_BUF_LEN, b"1.456e3", Res::Ok("1456")),
            (WORD_BUF_LEN, b"3.", Res::Ok("3")),
            (WORD_BUF_LEN, b"314e-2", Res::Ok("3.14")),
            (WORD_BUF_LEN, b"1e2", Res::Ok("100")),
            (WORD_BUF_LEN, b"2E-1", Res::Ok("0.2")),
            (WORD_BUF_LEN, b"2E0", Res::Ok("2")),
            (WORD_BUF_LEN, b"2.2E-1", Res::Ok("0.22")),
            (WORD_BUF_LEN, b"2.23E2", Res::Ok("223")),
            (WORD_BUF_LEN, b"2.23E2abc", Res::Ok("223")),
            (WORD_BUF_LEN, b"2.23a2", Res::Ok("2.23")),
            (WORD_BUF_LEN, b"223\xE0\x80\x80", Res::Ok("223")),
            (WORD_BUF_LEN, b"1e -1", Res::Ok("0.1")),
            (WORD_BUF_LEN, b"1e001", Res::Ok("10")),
            (WORD_BUF_LEN, b"1e00", Res::Ok("1")),
            (WORD_BUF_LEN, b"1e1073741823",
             Res::Overflow("999999999999999999999999999999999999999999999999999999999999999999999999999999999")),
            (WORD_BUF_LEN, b"-1e1073741823",
             Res::Overflow("-999999999999999999999999999999999999999999999999999999999999999999999999999999999")),
            (WORD_BUF_LEN, b"135999696916777530000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
             Res::Overflow("0")),
            (WORD_BUF_LEN, b"-0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002932935661422768",
             Res::Truncated("0.000000000000000000000000000000000000000000000000000000000000000000000000")),
            // The following case return truncated in tidb, need to fix it in bytes_to_int_without_context
            (WORD_BUF_LEN, b"1eabc", Res::Ok("1")),
            (WORD_BUF_LEN, b"1e", Res::Ok("1")),
            (WORD_BUF_LEN, b"1e 1ddd", Res::Ok("10")),
            (WORD_BUF_LEN, b"1e - 1", Res::Ok("1")),
            // with word_buf_len 1
            (1, b"123450000098765", Res::Overflow("98765")),
            (1, b"123450.000098765", Res::Truncated("123450")),
        ];

        for (word_buf_len, dec, exp) in cases {
            let d = Decimal::from_bytes_with_word_buf(dec, word_buf_len).unwrap();
            let res = d.map(|d| d.to_string());
            assert_eq!(res, exp.map(|s| s.to_owned()));
        }

        // error cases
        let cases = vec![b"1e18446744073709551620"];
        for case in cases {
            assert!(Decimal::from_bytes(case).is_err());
        }
    }

    #[test]
    fn test_codec() {
        let cases = vec![
            ("-10.55", 4, 2, Res::Ok("-10.55")),
            (
                "0.0123456789012345678912345",
                30,
                25,
                Res::Ok("0.0123456789012345678912345"),
            ),
            ("12345", 5, 0, Res::Ok("12345")),
            ("12345", 10, 3, Res::Ok("12345.000")),
            ("123.45", 10, 3, Res::Ok("123.450")),
            ("-123.45", 20, 10, Res::Ok("-123.4500000000")),
            (
                ".00012345000098765",
                15,
                14,
                Res::Truncated("0.00012345000098"),
            ),
            (
                ".00012345000098765",
                22,
                20,
                Res::Ok("0.00012345000098765000"),
            ),
            (".12345000098765", 30, 20, Res::Ok("0.12345000098765000000")),
            (
                "-.000000012345000098765",
                30,
                20,
                Res::Truncated("-0.00000001234500009876"),
            ),
            ("1234500009876.5", 30, 5, Res::Ok("1234500009876.50000")),
            ("111111111.11", 10, 2, Res::Overflow("11111111.11")),
            ("000000000.01", 7, 3, Res::Ok("0.010")),
            ("123.4", 10, 2, Res::Ok("123.40")),
            ("1000", 3, 0, Res::Overflow("0")),
            (
                "10000000000000000000.23",
                23,
                2,
                Res::Ok("10000000000000000000.23"),
            ),
        ];

        for (dec_str, prec, frac, exp) in cases {
            let dec = dec_str.parse::<Decimal>().unwrap();
            let mut buf = vec![];
            let res = buf.write_decimal(&dec, prec, frac).unwrap();
            let decoded = buf.as_slice().read_decimal().unwrap();
            let res = res.map(|_| decoded.to_string());
            assert_eq!(res, exp.map(|s| s.to_owned()));
        }
    }

    #[test]
    fn test_chunk_codec() {
        let cases = vec![
            "-10.55",
            "0.0123456789012345678912345",
            "12345",
            "12345",
            "123.45",
            ".00012345000098765",
            ".00012345000098765",
            ".12345000098765",
            "1234500009876.5",
            "111111111.11",
            "000000000.01",
            "123.4",
            "1000",
            "10000000000000000000.23",
        ];

        for dec_str in cases {
            let dec = dec_str.parse::<Decimal>().unwrap();
            let mut buf = vec![];
            buf.write_decimal_to_chunk(&dec).unwrap();
            buf.resize(DECIMAL_STRUCT_SIZE, 0);
            let decoded = buf.as_slice().read_decimal_from_chunk().unwrap();
            assert_eq!(decoded, dec);
        }
    }

    #[test]
    fn test_decode_chunk_from_tidb() {
        let src: Vec<u8> = vec![
            3, 3, 3, 0, 123, 0, 0, 0, 0, 2, 46, 27, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let decoded = src.as_slice().read_decimal_from_chunk().unwrap();
        assert_eq!(Decimal::from_f64(123.456).unwrap(), decoded);
    }

    #[test]
    fn test_cmp() {
        let cases = vec![
            ("12", "13", Ordering::Less),
            ("13", "12", Ordering::Greater),
            ("-10", "10", Ordering::Less),
            ("10", "-10", Ordering::Greater),
            ("-12", "-13", Ordering::Greater),
            ("0", "12", Ordering::Less),
            ("-10", "0", Ordering::Less),
            ("4", "4", Ordering::Equal),
            ("4", "4.00", Ordering::Equal),
            ("-1.1", "-1.2", Ordering::Greater),
            ("1.2", "1.1", Ordering::Greater),
            ("1.1", "1.2", Ordering::Less),
        ];

        for (lhs_str, rhs_str, exp) in cases {
            let lhs = lhs_str.parse::<Decimal>().unwrap();
            let rhs = rhs_str.parse::<Decimal>().unwrap();
            assert_eq!(lhs.cmp(&rhs), exp);
        }
    }

    #[test]
    fn test_hash() {
        let cases = vec![
            ("1.00", "1"),
            ("-1.11", "-1.11000000"),
            ("30.20", "30.2"),
            ("0", "-0"),
            ("0.001", "0.001000"),
        ];

        for (lhs_str, rhs_str) in cases {
            let lhs = lhs_str.parse::<Decimal>().unwrap();
            let rhs = rhs_str.parse::<Decimal>().unwrap();
            let mut lhasher = DefaultHasher::new();
            lhs.hash(&mut lhasher);
            let mut rhasher = DefaultHasher::new();
            rhs.hash(&mut rhasher);
            assert_eq!(lhasher.finish(), rhasher.finish());
        }
    }

    #[test]
    fn test_max_decimal() {
        let cases = vec![
            (1, 1, "0.9"),
            (1, 0, "9"),
            (2, 1, "9.9"),
            (4, 2, "99.99"),
            (6, 3, "999.999"),
            (8, 4, "9999.9999"),
            (10, 5, "99999.99999"),
            (12, 6, "999999.999999"),
            (14, 7, "9999999.9999999"),
            (16, 8, "99999999.99999999"),
            (18, 9, "999999999.999999999"),
            (20, 10, "9999999999.9999999999"),
            (20, 20, "0.99999999999999999999"),
            (20, 0, "99999999999999999999"),
            (40, 20, "99999999999999999999.99999999999999999999"),
        ];

        for (prec, frac, exp) in cases {
            let dec = super::max_decimal(prec, frac);
            let res = dec.to_string();
            assert_eq!(&res, exp);
        }
    }

    #[test]
    fn test_add() {
        let a = "2".to_owned() + &"1".repeat(71);
        let b: String = "8".repeat(81);
        let c = "8888888890".to_owned() + &"9".repeat(71);
        let cases = vec![
            (
                ".00012345000098765",
                "123.45",
                Res::Ok("123.45012345000098765"),
            ),
            (".1", ".45", Res::Ok("0.55")),
            (
                "1234500009876.5",
                ".00012345000098765",
                Res::Ok("1234500009876.50012345000098765"),
            ),
            ("9999909999999.5", ".555", Res::Ok("9999910000000.055")),
            ("99999999", "1", Res::Ok("100000000")),
            ("989999999", "1", Res::Ok("990000000")),
            ("999999999", "1", Res::Ok("1000000000")),
            ("12345", "123.45", Res::Ok("12468.45")),
            ("-12345", "-123.45", Res::Ok("-12468.45")),
            ("-12345", "123.45", Res::Ok("-12221.55")),
            ("12345", "-123.45", Res::Ok("12221.55")),
            ("123.45", "-12345", Res::Ok("-12221.55")),
            ("-123.45", "12345", Res::Ok("12221.55")),
            ("5", "-6.0", Res::Ok("-1.0")),
            ("2", "3", Res::Ok("5")),
            ("2454495034", "3451204593", Res::Ok("5905699627")),
            ("24544.95034", ".3451204593", Res::Ok("24545.2954604593")),
            (".1", ".1", Res::Ok("0.2")),
            (".1", "-.1", Res::Ok("0")),
            ("0", "1.001", Res::Ok("1.001")),
            (&a, &b, Res::Ok(&c)),
        ];

        for (lhs_str, rhs_str, exp) in cases {
            let lhs = lhs_str.parse::<Decimal>().unwrap();
            let rhs = rhs_str.parse::<Decimal>().unwrap();

            let res_dec = &lhs + &rhs;
            let res = res_dec.map(|s| s.to_string());
            let exp_str = exp.map(|s| s.to_owned());
            assert_eq!(res, exp_str);

            let res_dec = &rhs + &lhs;
            let res = res_dec.map(|s| s.to_string());
            assert_eq!(res, exp_str);
        }
    }

    #[test]
    fn test_sub() {
        let cases = vec![
            (
                ".00012345000098765",
                "123.45",
                Res::Ok("-123.44987654999901235"),
            ),
            (
                "1234500009876.5",
                ".00012345000098765",
                Res::Ok("1234500009876.49987654999901235"),
            ),
            ("9999900000000.5", ".555", Res::Ok("9999899999999.945")),
            ("1111.5551", "1111.555", Res::Ok("0.0001")),
            (".555", ".555", Res::Ok("0")),
            ("10000000", "1", Res::Ok("9999999")),
            ("1000001000", ".1", Res::Ok("1000000999.9")),
            ("1000000000", ".1", Res::Ok("999999999.9")),
            ("12345", "123.45", Res::Ok("12221.55")),
            ("-12345", "-123.45", Res::Ok("-12221.55")),
            ("123.45", "12345", Res::Ok("-12221.55")),
            ("-123.45", "-12345", Res::Ok("12221.55")),
            ("-12345", "123.45", Res::Ok("-12468.45")),
            ("12345", "-123.45", Res::Ok("12468.45")),
            ("3.10000000000", "2.00", Res::Ok("1.10000000000")),
            ("3.00", "2.0000000000000", Res::Ok("1.0000000000000")),
            (
                "-20048271934704078000000000000000000000000000000000000",
                "-20048271934734512000000000000000000000000000000000000",
                Res::Ok("30434000000000000000000000000000000000000"),
            ),
        ];

        for (lhs_str, rhs_str, exp) in cases {
            let lhs = lhs_str.parse::<Decimal>().unwrap();
            let rhs = rhs_str.parse::<Decimal>().unwrap();
            let res_dec = &lhs - &rhs;
            let res = res_dec.map(|s| s.to_string());
            assert_eq!(res, exp.map(|s| s.to_owned()));
        }
    }

    #[test]
    fn test_mul() {
        let a = "1".to_owned() + &"0".repeat(60);
        let b = "1".to_owned() + &"0".repeat(60);
        let cases = vec![
            ("12", "10", Res::Ok("120")),
            ("0", "-1.1", Res::Ok("0")),
            ("-123.456", "98765.4321", Res::Ok("-12193185.1853376")),
            (
                "-123456000000",
                "98765432100000",
                Res::Ok("-12193185185337600000000000"),
            ),
            ("123456", "987654321", Res::Ok("121931851853376")),
            ("123456", "9876543210", Res::Ok("1219318518533760")),
            ("123", "0.01", Res::Ok("1.23")),
            ("123", "0", Res::Ok("0")),
            (&a, &b, Res::Overflow("0")),
            (
                "0.00000000000000",
                "0.000000000000000000000000000000000000000000000000000000000000000",
                Res::Truncated("0.0000000000000000000000000000000"),
            ),
        ];

        for (lhs_str, rhs_str, exp_str) in cases {
            let lhs: Decimal = lhs_str.parse().unwrap();
            let rhs: Decimal = rhs_str.parse().unwrap();
            let exp = exp_str.map(|s| s.to_owned());
            let res = (&lhs * &rhs).map(|d| d.to_string());
            assert_eq!(res, exp);

            let res = (&rhs * &lhs).map(|d| d.to_string());
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_div_mod() {
        let cases = vec![
            (5, "120", "10", Some("12.000000000"), Some("0")),
            (5, "123", "0.01", Some("12300.000000000"), Some("0.00")),
            (
                5,
                "120",
                "100000000000.00000",
                Some("0.000000001200000000"),
                Some("120.00000"),
            ),
            (5, "123", "0", None, None),
            (5, "123", "0.0", None, None),
            (5, "123", "00.0000000000", None, None),
            (5, "0", "0", None, None),
            (5, "0.0", "0.0", None, None),
            (5, "0.0000000000", "00.0000000000", None, None),
            (
                5,
                "-12193185.1853376",
                "98765.4321",
                Some("-123.456000000000000000"),
                Some("-45037.0370376"),
            ),
            (
                5,
                "121931851853376",
                "987654321",
                Some("123456.000000000"),
                Some("0"),
            ),
            (5, "0", "987", Some("0"), Some("0")),
            (5, "0.0", "987", Some("0"), Some("0")),
            (5, "0.0000000000", "987", Some("0"), Some("0")),
            (5, "1", "3", Some("0.333333333"), Some("1")),
            (
                5,
                "1.000000000000",
                "3",
                Some("0.333333333333333333"),
                Some("1.000000000000"),
            ),
            (5, "1", "1", Some("1.000000000"), Some("0")),
            (
                5,
                "0.0123456789012345678912345",
                "9999999999",
                Some("0.000000000001234567890246913578148141"),
                Some("0.0123456789012345678912345"),
            ),
            (
                5,
                "10.333000000",
                "12.34500",
                Some("0.837019036046982584042122316"),
                Some("10.333000000"),
            ),
            (
                5,
                "10.000000000060",
                "2",
                Some("5.000000000030000000"),
                Some("0.000000000060"),
            ),
            (0, "234", "10", Some("23"), Some("4")),
            (
                0,
                "234.567",
                "10.555",
                Some("22.223306489815253434"),
                Some("2.357"),
            ),
            (
                0,
                "-234.567",
                "10.555",
                Some("-22.223306489815253434"),
                Some("-2.357"),
            ),
            (
                0,
                "234.567",
                "-10.555",
                Some("-22.223306489815253434"),
                Some("2.357"),
            ),
            (
                0,
                "99999999999999999999999999999999999999",
                "3",
                Some("33333333333333333333333333333333333333"),
                Some("0"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "1",
                "1",
                Some("1.000000000"),
                Some("0"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "1.00",
                "1",
                Some("1.000000000"),
                Some("0.00"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "1",
                "1.000",
                Some("1.000000000"),
                Some("0.000"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "0.0000000001",
                "1.0",
                Some("0.000000000100000000000000000"),
                Some("0.0000000001"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "2",
                "3",
                Some("0.666666666"),
                Some("2"),
            ),
            (0, "1", "2.0", Some("0.500000000"), Some("1.0")),
            (0, "1.0", "2", Some("0.500000000"), Some("1.0")),
            (0, "2.23", "3", Some("0.743333333"), Some("2.23")),
            (
                DEFAULT_DIV_FRAC_INCR,
                "51",
                "0.003430",
                Some("14868.804664723032069970"),
                Some("0.002760"),
            ),
            (
                5,
                "51",
                "0.003430",
                Some("14868.804664723032069970"),
                Some("0.002760"),
            ),
            (
                0,
                "51",
                "0.003430",
                Some("14868.804664723"),
                Some("0.002760"),
            ),
            (
                5,
                "3428138243708624600000000000000000000000000000000000",
                "0.000000000000000000000000000000000000000000010962196522059515",
                Some(
                    "312723662343590746587750435944686855597018456899102054479447138416084646758822",
                ),
                Some("0.000000000000000000000000000000000003564345362392880000000000"),
            ),
            (
                0,
                "-0.000000000000000000000000000000000000000000004078816115216077",
                "770994069125765500000000000000000000000000000",
                Some("0.000000000000000000000000000000000000000000000000000000000000000"),
                Some("-0.000000000000000000000000000000000000000000004078816115216077"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "-125",
                "489466941506",
                Some("0.000000000"),
                Some("-125"),
            ),
            (
                DEFAULT_DIV_FRAC_INCR,
                "-56",
                "489466941506",
                Some("0.000000000"),
                Some("-56"),
            ),
        ];

        for (frac_incr, lhs_str, rhs_str, div_exp, rem_exp) in cases {
            let lhs: Decimal = lhs_str.parse().unwrap();
            let rhs: Decimal = rhs_str.parse().unwrap();
            let res =
                super::do_div_mod(&lhs, &rhs, frac_incr, false).map(|d| d.unwrap().to_string());
            assert_eq!(res, div_exp.map(|s| s.to_owned()));

            let res =
                super::do_div_mod(&lhs, &rhs, frac_incr, true).map(|d| d.unwrap().to_string());
            assert_eq!(res, rem_exp.map(|s| s.to_owned()));
        }

        let div_cases = vec![(
            "-43791957044243810000000000000000000000000000000000000000000000000000000000000",
            "-0.0000000000000000000000000000000000000000000000000012867433602814482",
            Res::Overflow(
                "34033171179267041433424155279291553259014210153022524070386565694757521640",
            ),
        )];
        for (lhs_str, rhs_str, rem_exp) in div_cases {
            let lhs: Decimal = lhs_str.parse().unwrap();
            let rhs: Decimal = rhs_str.parse().unwrap();
            let res = (&lhs / &rhs).unwrap().map(|d| d.to_string());
            assert_eq!(res, rem_exp.map(|s| s.to_owned()))
        }
    }

    #[test]
    fn test_neg() {
        let cases = vec![
            ("123.45", "-123.45"),
            ("1", "-1"),
            ("1234500009876.5", "-1234500009876.5"),
            ("1111.5551", "-1111.5551"),
            ("0.555", "-0.555"),
            ("0", "0"),
            ("0.0", "0.0"),
            ("0.00", "0.00"),
        ];

        for (pos, neg) in cases {
            let pos_dec: Decimal = pos.parse().unwrap();
            let res = -pos_dec;
            assert_eq!(format!("{}", res), neg);
            assert!((&pos_dec + &res).is_zero());

            let neg_dec: Decimal = neg.parse().unwrap();
            let res = -neg_dec;
            assert_eq!(format!("{}", res), pos);
            assert!((&neg_dec + &res).is_zero());
        }

        let max_dec = super::max_or_min_dec(false, 40, 20);
        let min_dec = super::max_or_min_dec(true, 40, 20);
        assert_eq!(min_dec, -max_dec);
        assert_eq!(max_dec, -min_dec);
    }

    #[test]
    fn test_max_or_min_decimal() {
        let cases = vec![
            (1, 1, "0.9"),
            (1, 0, "9"),
            (2, 1, "9.9"),
            (4, 2, "99.99"),
            (6, 3, "999.999"),
            (8, 4, "9999.9999"),
            (10, 5, "99999.99999"),
            (12, 6, "999999.999999"),
            (14, 7, "9999999.9999999"),
            (16, 8, "99999999.99999999"),
            (18, 9, "999999999.999999999"),
            (20, 10, "9999999999.9999999999"),
            (20, 20, "0.99999999999999999999"),
            (20, 0, "99999999999999999999"),
            (40, 20, "99999999999999999999.99999999999999999999"),
        ];

        for (prec, frac, exp) in cases {
            let positive = super::max_or_min_dec(false, prec, frac);
            let res = positive.to_string();
            assert_eq!(&res, exp);
            let negative = super::max_or_min_dec(true, prec, frac);
            let mut negative_exp = String::from("-");
            negative_exp.push_str(exp);
            let res = negative.to_string();
            assert_eq!(res, negative_exp);
        }
    }

    #[test]
    fn test_ceil() {
        let cases = vec![
            ("12345", "12345"),
            ("0.99999", "1"),
            ("-0.99999", "0"),
            ("18446744073709551615", "18446744073709551615"),
            ("18446744073709551616", "18446744073709551616"),
            ("-18446744073709551615", "-18446744073709551615"),
            ("-18446744073709551616", "-18446744073709551616"),
            ("-1", "-1"),
            ("1.23", "2"),
            ("-1.23", "-1"),
            ("1.00000", "1"),
            ("-1.00000", "-1"),
            (
                "9999999999999999999999999.001",
                "10000000000000000000000000",
            ),
        ];
        for (input, exp) in cases {
            let dec: Decimal = input.parse().unwrap();
            let exp: Decimal = exp.parse().unwrap();
            let got = dec.ceil().unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_floor() {
        let cases = vec![
            ("12345", "12345"),
            ("0.99999", "0"),
            ("-0.99999", "-1"),
            ("18446744073709551615", "18446744073709551615"),
            ("18446744073709551616", "18446744073709551616"),
            ("-18446744073709551615", "-18446744073709551615"),
            ("-18446744073709551616", "-18446744073709551616"),
            ("-1", "-1"),
            ("1.23", "1"),
            ("-1.23", "-2"),
            ("00001.00000", "1"),
            ("-00001.00000", "-1"),
            ("9999999999999999999999999.001", "9999999999999999999999999"),
        ];
        for (input, exp) in cases {
            let dec: Decimal = input.parse().unwrap();
            let exp: Decimal = exp.parse().unwrap();
            let got = dec.floor().unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_bytes_to_decimal() {
        let cases: Vec<(&[u8], Decimal)> = vec![
            (
                b"123456.1",
                ConvertTo::<Decimal>::convert(&123456.1, &mut EvalContext::default()).unwrap(),
            ),
            (
                b"-123456.1",
                ConvertTo::<Decimal>::convert(&-123456.1, &mut EvalContext::default()).unwrap(),
            ),
            (b"123456", Decimal::from(123456)),
            (b"-123456", Decimal::from(-123456)),
        ];
        let mut ctx = EvalContext::default();
        for (s, expect) in cases {
            let got: Decimal = s.convert(&mut ctx).unwrap();
            assert_eq!(got, expect, "from {:?}, expect: {} got: {}", s, expect, got);
        }

        // OVERFLOWING
        let big = (0..85).map(|_| '9').collect::<String>();
        let val: Result<Decimal> = big.as_bytes().convert(&mut ctx);
        assert!(val.is_err(), "expected error, but got {:?}", val);
        assert_eq!(val.unwrap_err().code(), ERR_DATA_OUT_OF_RANGE);

        // OVERFLOW_AS_WARNING
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
        let val: Decimal = big.as_bytes().convert(&mut ctx).unwrap();
        let max = max_decimal(WORD_BUF_LEN * DIGITS_PER_WORD, 0);
        assert_eq!(val, max, "expect: {}, got: {}", val, max);
        assert_eq!(ctx.warnings.warning_cnt, 1);
        assert_eq!(ctx.warnings.warnings[0].get_code(), ERR_DATA_OUT_OF_RANGE);
    }

    #[test]
    fn test_into_result_impl() {
        // Truncated cases
        let mut ctx = EvalContext::default();
        let truncated_res = Res::Truncated(2333);
        let truncated_err_cases = vec![Error::truncated(), Error::truncated_wrong_val("", "")];

        for error in truncated_err_cases {
            assert_eq!(
                error.code(),
                truncated_res
                    .into_result_impl(&mut ctx, Some(error), None)
                    .unwrap_err()
                    .code()
            );
        }

        // TRUNCATE_AS_WARNING
        let mut ctx = EvalContext::new(std::sync::Arc::new(EvalConfig::from_flag(
            Flag::TRUNCATE_AS_WARNING,
        )));
        let truncated_res = Res::Truncated(2333);

        assert!(
            truncated_res
                .into_result_impl(&mut ctx, Some(Error::truncated()), None)
                .is_ok()
        );

        // Overflow cases
        let mut ctx = EvalContext::default();
        let overflow_res = Res::Overflow(666);
        let error = Error::overflow("", "");
        assert_eq!(
            error.code(),
            overflow_res
                .into_result_impl(&mut ctx, None, Some(error))
                .unwrap_err()
                .code(),
        );

        // OVERFLOW_AS_WARNING
        let mut ctx = EvalContext::new(std::sync::Arc::new(EvalConfig::from_flag(
            Flag::OVERFLOW_AS_WARNING,
        )));
        let error = Error::overflow("", "");
        assert!(
            overflow_res
                .into_result_impl(&mut ctx, None, Some(error))
                .is_ok()
        );
    }
}
