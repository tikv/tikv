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

pub fn like(target: &[u8], pattern: &[u8], escape: u32) -> Result<bool> {
    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < pattern.len() || tx < target.len() {
        if px < pattern.len() {
            let c = pattern[px];
            match c {
                b'_' => {
                    if tx < target.len() {
                        px += 1;
                        tx += 1;
                        continue;
                    }
                }
                b'%' => {
                    // update the backtrace point.
                    next_px = px;
                    next_tx = tx + 1;
                    px += 1;
                    continue;
                }
                mut pc => {
                    if u32::from(pc) == escape && px + 1 < pattern.len() {
                        px += 1;
                        pc = pattern[px];
                    }
                    if tx < target.len() && target[tx] == pc {
                        tx += 1;
                        px += 1;
                        continue;
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
