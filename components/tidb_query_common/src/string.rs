// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

pub enum TrimDirection {
    Both = 1,
    Leading,
    Trailing,
}

impl TrimDirection {
    pub fn from_i64(i: i64) -> Option<Self> {
        match i {
            1 => Some(TrimDirection::Both),
            2 => Some(TrimDirection::Leading),
            3 => Some(TrimDirection::Trailing),
            _ => None,
        }
    }
}

#[inline]
pub fn trim(s: &str, pat: &str, direction: TrimDirection) -> Vec<u8> {
    let r = match direction {
        TrimDirection::Leading => s.trim_start_matches(pat),
        TrimDirection::Trailing => s.trim_end_matches(pat),
        _ => s.trim_start_matches(pat).trim_end_matches(pat),
    };
    r.to_string().into_bytes()
}
