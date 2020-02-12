// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Implements SQL `LIKE`.
//!
//! This implementation needs refactor.
//!
//! 1. It is not effective. Consider target = 'aaaaaaaaaaaaaaa' and pattern = 'a%a%a%a%a%a%b'.
//!    See https://research.swtch.com/glob
//!
//! 2. It should support non-binary mode (and binary mode) and do case insensitive comparing
//!    in non-binary mode.

use crate::expr::Result;
use crate::expr_util::collation::{Charset, Collator};

pub fn like<C: Collator>(target: &[u8], pattern: &[u8], escape: u8) -> Result<bool> {
    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < pattern.len() || tx < target.len() {
        if px < pattern.len() {
            let c = pattern[px];
            match c {
                b'_' => {
                    if let Some(off) = C::Charset::advance_one(&target[tx..]) {
                        px += 1;
                        tx += off;
                        continue;
                    }
                }
                b'%' => {
                    // update the backtrace point.
                    next_px = px;
                    px += 1;
                    next_tx = tx + C::Charset::advance_one(&target[tx..]).unwrap_or(1);
                    continue;
                }
                pc => {
                    if pc == escape && px + 1 < pattern.len() {
                        px += 1;
                    }
                    if let (Some(poff), Some(toff)) = (
                        C::Charset::advance_one(&pattern[px..]),
                        C::Charset::advance_one(&target[tx..]),
                    ) {
                        match C::sort_compare(&target[tx..tx + toff], &pattern[px..px + poff]) {
                            Ok(std::cmp::Ordering::Equal) => {
                                tx += toff;
                                px += poff;
                                continue;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        // mismatch and backtrace to last %.
        if 0 < next_tx && next_tx <= target.len() {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return Ok(false);
    }

    Ok(true)
}
